use crate::{
    models::ModelSelection,
    protocol::{
        AgentNode, CoordinatorBootstrap, CoordinatorSettings, RuntimeCommand, RuntimeEventKind,
        WorkerLaunch,
    },
    runtime::{FrameReader, Runtime, ShutdownSignal},
    service::{SOCKET_NAME, ShutdownReason, connect, serve_with_transport, write_frame},
};
use std::{fs, path::PathBuf, time::Duration};
use tokio::{
    io::{AsyncReadExt, BufReader},
    net::UnixStream,
    sync::oneshot,
    task::JoinHandle,
    time::{sleep, timeout},
};

const INSTRUCTIONS: &str = "Startup configuration reached real Pi";

struct Starting {
    directory: tempfile::TempDir,
    script: PathBuf,
    stop: Option<oneshot::Sender<()>>,
    task: JoinHandle<anyhow::Result<()>>,
    pid: libc::pid_t,
}

impl Starting {
    async fn held(instructions: String) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let script = directory.path().join("held-startup.mjs");
        let bridge = std::env::var_os("AGENT_RUNTIME_PATH")
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("runtime/dist/src/bridge.js")
            });
        // Hold the actual child at an OS signal boundary before importing Pi.
        // It never substitutes protocol replies or coordinator behavior.
        fs::write(
            &script,
            format!(
                r#"
import {{writeFileSync}} from 'node:fs';
import {{join}} from 'node:path';
import {{pathToFileURL}} from 'node:url';
const keepAlive = setInterval(() => {{}}, 1000);
await new Promise(resolve => {{
  process.once('SIGUSR2', resolve);
  writeFileSync(join(process.argv[2], 'held.pid'), String(process.pid));
}});
clearInterval(keepAlive);
await import(pathToFileURL({}).href);
"#,
                serde_json::to_string(&bridge.canonicalize().unwrap()).unwrap()
            ),
        )
        .unwrap();
        let state = directory.path().to_path_buf();
        let runtime_script = script.clone();
        let (stop, stopped) = oneshot::channel();
        let task = tokio::spawn(async move {
            serve_with_transport(
                &runtime_script,
                &state,
                None,
                crate::tailnet::PeerPolicy::new(&[], &[]).unwrap(),
                crate::tailnet::Daemon::default(),
                Some(CoordinatorBootstrap {
                    settings: CoordinatorSettings {
                        model: ModelSelection::default(),
                        instructions,
                        context_repositories: vec![],
                        sources: vec![],
                    },
                    node: None,
                    background: None,
                }),
                async {
                    let _ = stopped.await;
                    Ok(ShutdownReason::Signal(ShutdownSignal::Sigterm))
                },
            )
            .await
        });
        let pid = timeout(Duration::from_secs(5), async {
            loop {
                if let Ok(pid) = fs::read_to_string(directory.path().join("held.pid")) {
                    break pid.parse().unwrap();
                }
                sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        Self {
            directory,
            script,
            stop: Some(stop),
            task,
            pid,
        }
    }

    fn signal(&self, signal: libc::c_int) {
        assert_eq!(unsafe { libc::kill(self.pid, signal) }, 0);
    }

    async fn client(&mut self) -> UnixStream {
        timeout(Duration::from_secs(10), async {
            loop {
                if let Ok(stream) = connect(self.directory.path()).await {
                    return stream;
                }
                if self.task.is_finished() {
                    panic!(
                        "Coordinator stopped before readiness: {:?}",
                        (&mut self.task).await
                    );
                }
                sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap()
    }

    async fn stop(mut self) {
        self.stop.take().unwrap().send(()).unwrap();
        timeout(Duration::from_secs(8), self.task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_reaped_and_unlocked(&self.directory, self.pid);
    }
}

fn assert_reaped_and_unlocked(directory: &tempfile::TempDir, pid: libc::pid_t) {
    assert!(!directory.path().join(SOCKET_NAME).exists());
    assert_eq!(unsafe { libc::kill(pid, 0) }, -1, "Pi was not reaped");
    assert_eq!(
        std::io::Error::last_os_error().raw_os_error(),
        Some(libc::ESRCH)
    );
    // The child inherits this lock; completion must release both owners.
    let lock = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(directory.path().join("writer.lock"))
        .unwrap();
    fs2::FileExt::try_lock_exclusive(&lock).unwrap();
}

async fn configured(stream: UnixStream) -> Vec<AgentNode> {
    let (reader, mut writer) = stream.into_split();
    write_frame(
        &mut writer,
        &RuntimeCommand::Environment { telemetry: false },
    )
    .await
    .unwrap();
    let mut reader = FrameReader::new(BufReader::new(reader));
    let event: RuntimeEventKind = serde_json::from_slice(
        &timeout(Duration::from_secs(5), reader.next_frame())
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    let RuntimeEventKind::Environment {
        settings, nodes, ..
    } = event
    else {
        panic!("Expected the real coordinator environment, got {event:?}");
    };
    assert_eq!(settings.instructions, INSTRUCTIONS);
    let event: RuntimeEventKind =
        serde_json::from_slice(&reader.next_frame().await.unwrap().unwrap()).unwrap();
    assert!(matches!(event, RuntimeEventKind::Done));
    nodes
}

#[tokio::test]
async fn initialization_waits_for_real_pi_beyond_the_metadata_deadline() {
    let mut service = Starting::held(INSTRUCTIONS.into()).await;
    assert!(!service.directory.path().join(SOCKET_NAME).exists());
    assert!(Runtime::start(&service.script, service.directory.path()).is_err());
    // Ordinary metadata times out while this real child still holds startup.
    sleep(Duration::from_secs(17)).await;
    service.signal(libc::SIGUSR2);
    configured(service.client().await).await;
    service.stop().await;
}

#[tokio::test]
async fn shutdown_during_initialization_reaps_pi_without_binding_a_listener() {
    // This cannot fit in the child's unread pipe, so shutdown also interrupts
    // request admission rather than only waiting for a coordinator reply.
    let mut service = Starting::held("p".repeat(2 * 1024 * 1024)).await;
    assert!(!service.directory.path().join(SOCKET_NAME).exists());
    assert!(Runtime::start(&service.script, service.directory.path()).is_err());
    service.stop.take().unwrap().send(()).unwrap();
    let stopped = timeout(Duration::from_secs(8), &mut service.task).await;
    if stopped.is_err() {
        // Let the baseline finish cleanly before reporting its missed shutdown.
        service.signal(libc::SIGUSR2);
        let _ = timeout(Duration::from_secs(10), &mut service.task).await;
    }
    stopped
        .expect("Shutdown waited for Pi initialization")
        .unwrap()
        .unwrap();
    assert_reaped_and_unlocked(&service.directory, service.pid);
}

#[tokio::test]
async fn ordinary_worker_metadata_still_times_out_when_real_pi_stalls() {
    let mut service = Starting::held(INSTRUCTIONS.into()).await;
    service.signal(libc::SIGUSR2);
    configured(service.client().await).await;
    service.signal(libc::SIGSTOP);
    let mut worker = service.client().await;
    write_frame(
        &mut worker,
        &RuntimeCommand::ConnectWorker {
            node: AgentNode {
                host_id: "11111111-1111-4111-8111-111111111111".into(),
                name: "Startup fixture worker".into(),
                rex_server: None,
                agent_program: "/example/agent".into(),
                home_directory: "/example".into(),
                coordinator: Some("tailscale://100.64.0.10".into()),
                launch: WorkerLaunch {
                    state_dir: None,
                    runtime: None,
                    config: None,
                    instructions: None,
                    model: None,
                },
                capabilities: None,
                machine: None,
            },
        },
    )
    .await
    .unwrap();
    let mut response = Vec::new();
    let closed = timeout(Duration::from_secs(20), worker.read_to_end(&mut response)).await;
    service.signal(libc::SIGCONT);
    let registered = timeout(Duration::from_secs(5), async {
        loop {
            let nodes = configured(service.client().await).await;
            if nodes
                .iter()
                .any(|node| node.host_id == "11111111-1111-4111-8111-111111111111")
            {
                break;
            }
            // Registration and environment reads execute concurrently in Pi.
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    service.stop().await;
    closed
        .expect("Worker metadata lost its ordinary deadline")
        .unwrap();
    assert!(response.is_empty(), "A stalled worker was declared ready");
    registered.expect("The worker registration did not reach real Pi");
}
