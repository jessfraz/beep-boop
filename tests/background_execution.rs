//! Scheduled actors use the real Pi runtime, Rust broker and process executor.
//! Only the external model responses are synthetic.
use std::{collections::BTreeMap, fs, path::Path, sync::Arc, time::Duration};

use agent_cli::{
    background::{self, BackgroundSettings},
    broker::Broker,
    jobs::{JobSchedule, ScheduledJob},
    models::ModelSelection,
    protocol::{
        AgentNode, CoordinatorBootstrap, CoordinatorSettings, InputMode, RuntimeCommand,
        RuntimeEventKind, TaskId, TaskRecord, WorkerLaunch,
    },
    runtime::{Runtime, RuntimeCall},
};
use serde::Serialize;
use tokio::time::{sleep, timeout};

#[derive(Serialize)]
struct CommandArguments {
    program: String,
    args: Vec<String>,
    timeout_seconds: u16,
}
#[derive(Serialize)]
#[serde(untagged)]
enum ModelStep {
    Tool {
        tool: &'static str,
        args: CommandArguments,
        commentary: &'static str,
    },
    Text {
        text: &'static str,
    },
}

struct BackgroundOwner(tokio::task::JoinHandle<anyhow::Result<()>>);
impl Drop for BackgroundOwner {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn collect(mut call: RuntimeCall) -> Vec<RuntimeEventKind> {
    let mut events = Vec::new();
    loop {
        let event = timeout(Duration::from_secs(8), call.events.recv())
            .await
            .expect("The runtime request did not finish")
            .expect("The runtime request closed without its terminal event");
        if let RuntimeEventKind::Failure { message, .. } = &event {
            panic!("Runtime request failed: {message}");
        }
        if matches!(event, RuntimeEventKind::Done) {
            return events;
        }
        assert!(
            !matches!(event, RuntimeEventKind::HostCall { .. }),
            "Background effects must retain their background transport owner"
        );
        events.push(event);
    }
}

async fn task(runtime: &Runtime, id: &TaskId) -> (TaskRecord, String) {
    collect(
        runtime
            .request(RuntimeCommand::ReadTask {
                task_id: id.clone(),
            })
            .await
            .unwrap(),
    )
    .await
    .into_iter()
    .find_map(|event| match event {
        RuntimeEventKind::Task { task, history } => Some((*task, history)),
        _ => None,
    })
    .expect("The task must retain its native conversation")
}

fn job(id: &str, prompt: &str, directory: &Path) -> ScheduledJob {
    ScheduledJob {
        id: id.to_owned().try_into().unwrap(),
        title: format!("Background {id}"),
        prompt: prompt.into(),
        model: ModelSelection::default(),
        timezone: "Etc/UTC".to_owned().try_into().unwrap(),
        cwd: directory.to_string_lossy().into_owned(),
        revision: "fixture".into(),
        schedules: vec![JobSchedule {
            id: "daily".to_owned().try_into().unwrap(),
            recurrence: "DTSTART:20990101T090000Z\nRRULE:FREQ=DAILY"
                .to_owned()
                .try_into()
                .unwrap(),
            focus: None,
            prompt: None,
        }],
    }
}

async fn started(path: &Path) -> i32 {
    timeout(Duration::from_secs(8), async {
        loop {
            if let Ok(value) = fs::read_to_string(path)
                && value.ends_with('\n')
            {
                return value.trim().parse().unwrap();
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("The job's real host command did not start")
}

#[tokio::test]
async fn stopping_one_background_actor_drains_its_real_process_and_preserves_the_other_actor() {
    let directory = tempfile::tempdir().unwrap();
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let fixture = root.join("runtime/dist/test/fixtures/coordinator-runtime.js");
    assert!(fixture.is_file(), "Build the runtime before this test");
    let queue_path = directory.path().join("queue.json");
    let alpha_pid = directory.path().join("alpha.pid");
    let beta_pid = directory.path().join("beta.pid");
    let release_beta = directory.path().join("release-beta");
    let completed_beta = directory.path().join("completed-beta");
    let process = |pid: &Path, completion: Option<(&Path, &Path)>| {
        let held = match completion {
            None => "setInterval(() => {}, 1000);".to_owned(),
            Some((release, completed)) => format!(
                "const timer=setInterval(() => {{ if (fs.existsSync({})) {{ fs.writeFileSync({}, 'completed once'); clearInterval(timer); }} }}, 10);",
                serde_json::to_string(release).unwrap(),
                serde_json::to_string(completed).unwrap()
            ),
        };
        ModelStep::Tool {
            tool: "exec",
            args: CommandArguments {
                program: "node".into(),
                args: vec![
                    "-e".into(),
                    format!(
                        "const fs=require('node:fs'); fs.appendFileSync({}, String(process.pid)+'\\n'); {held}",
                        serde_json::to_string(pid).unwrap()
                    ),
                ],
                timeout_seconds: 30,
            },
            commentary: "Checking the owned background command.",
        }
    };
    let queue = BTreeMap::from([
        (
            "JOB_ALPHA",
            vec![
                process(&alpha_pid, None),
                ModelStep::Text {
                    text: "Alpha ended",
                },
            ],
        ),
        (
            "JOB_BETA",
            vec![
                process(&beta_pid, Some((&release_beta, &completed_beta))),
                ModelStep::Text {
                    text: "Beta completed with its real host receipt.",
                },
            ],
        ),
        (
            "ALPHA_RECOVERY",
            vec![ModelStep::Text {
                text: "The same saved alpha conversation continued.",
            }],
        ),
    ]);
    fs::write(&queue_path, serde_json::to_vec(&queue).unwrap()).unwrap();
    let script = directory.path().join("runtime.mjs");
    fs::write(
        &script,
        format!(
            "process.env.OPENAI_API_KEY='sk-synthetic-fixture';\nprocess.env.AGENT_TEST_MODEL_QUEUE={};\nawait import({});\n",
            serde_json::to_string(&queue_path).unwrap(),
            serde_json::to_string(&fixture).unwrap()
        ),
    )
    .unwrap();
    let instructions = directory.path().join("AGENTS.md");
    fs::write(&instructions, "Use only invented fixture data.\n").unwrap();
    let host_id = "11111111-1111-4111-8111-111111111111";
    let bootstrap = CoordinatorBootstrap {
        node: Some(AgentNode {
            host_id: host_id.into(),
            name: "fixture".into(),
            home_directory: directory.path().to_string_lossy().into_owned(),
            agent_program: "/fixture/agent".into(),
            rex_server: None,
            coordinator: None,
            capabilities: None,
            machine: None,
            launch: WorkerLaunch {
                instructions: Some(instructions.to_string_lossy().into_owned()),
                state_dir: None,
                runtime: None,
                config: None,
                model: None,
            },
        }),
        settings: CoordinatorSettings {
            model: ModelSelection::default(),
            instructions: String::new(),
            context_repositories: vec![],
            sources: vec![],
        },
        background: None,
    };
    let runtime = Arc::new(Runtime::start(&script, directory.path()).unwrap());
    collect(
        runtime
            .request(RuntimeCommand::ConfigureCoordinator {
                bootstrap: bootstrap.clone(),
            })
            .await
            .unwrap(),
    )
    .await;
    collect(
        runtime
            .request(RuntimeCommand::SetConnectedNodes {
                host_ids: vec![host_id.into()],
            })
            .await
            .unwrap(),
    )
    .await;
    let alpha = job("alpha", "JOB_ALPHA", directory.path());
    let beta = job("beta", "JOB_BETA", directory.path());
    let broker = Arc::new(Broker::new(Some(&bootstrap), directory.path()).unwrap());
    let running = runtime.clone();
    let background = BackgroundOwner(tokio::spawn(async move {
        background::run(
            running,
            broker,
            BackgroundSettings {
                poll_interval_seconds: 60,
                sources: false,
                jobs: Some(vec![alpha, beta]),
            },
        )
        .await
    }));
    timeout(Duration::from_secs(8), async {
        loop {
            let events = collect(runtime.request(RuntimeCommand::ListJobs).await.unwrap()).await;
            if events.iter().any(|event| {
                matches!(event, RuntimeEventKind::ScheduledJobs { jobs } if jobs.len() == 2)
            }) {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let mut ids = Vec::new();
    for name in ["alpha", "beta"] {
        ids.push(
            collect(
                runtime
                    .request(RuntimeCommand::RunJob {
                        job_id: name.to_owned().try_into().unwrap(),
                        schedule_id: None,
                        request_id: format!("manual-{name}"),
                    })
                    .await
                    .unwrap(),
            )
            .await
            .into_iter()
            .find_map(|event| match event {
                RuntimeEventKind::JobRun { run } => Some(run.task_id),
                _ => None,
            })
            .unwrap(),
        );
    }
    let (alpha_process, beta_process) = tokio::join!(started(&alpha_pid), started(&beta_pid));
    let (alpha_task, _) = task(&runtime, &ids[0]).await;
    let (beta_task, _) = task(&runtime, &ids[1]).await;
    assert!(matches!(
        alpha_task.worker,
        agent_cli::protocol::WorkerState::Unassigned
    ));
    assert!(matches!(
        beta_task.worker,
        agent_cli::protocol::WorkerState::Unassigned
    ));
    let mut view = runtime
        .request(RuntimeCommand::AttachConversation {
            conversation_id: alpha_task.conversation_id.clone(),
            input_ids: vec![],
        })
        .await
        .unwrap();
    timeout(Duration::from_secs(8), async {
        loop {
            if matches!(
                view.events.recv().await,
                Some(RuntimeEventKind::ConversationReady { working: true, .. })
            ) {
                break;
            }
        }
    })
    .await
    .expect("A background job must expose its native live progress");
    collect(
        runtime
            .request(RuntimeCommand::ConversationInput {
                origin: None,
                conversation_id: alpha_task.conversation_id.clone(),
                input_id: "unplaced-alpha".into(),
                input: "ALPHA_UNPLACED".into(),
                mode: InputMode::FollowUp,
                context: None,
                attachments: None,
            })
            .await
            .unwrap(),
    )
    .await;
    collect(
        runtime
            .request(RuntimeCommand::ConversationInterrupt {
                conversation_id: alpha_task.conversation_id.clone(),
            })
            .await
            .unwrap(),
    )
    .await;
    // A zero signal inspects process existence without changing either process.
    assert_eq!(unsafe { libc::kill(alpha_process, 0) }, -1);
    assert_eq!(unsafe { libc::kill(beta_process, 0) }, 0);
    assert!(!completed_beta.exists());
    fs::write(&release_beta, "release").unwrap();
    timeout(Duration::from_secs(8), async {
        loop {
            if task(&runtime, &ids[1]).await.0.summary
                == "Beta completed with its real host receipt."
            {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        fs::read_to_string(&completed_beta).unwrap(),
        "completed once"
    );
    collect(
        runtime
            .request(RuntimeCommand::ConversationInput {
                origin: None,
                conversation_id: alpha_task.conversation_id.clone(),
                input_id: "explicit-alpha-continuation".into(),
                input: "ALPHA_RECOVERY".into(),
                mode: InputMode::FollowUp,
                context: None,
                attachments: None,
            })
            .await
            .unwrap(),
    )
    .await;
    timeout(Duration::from_secs(8), async {
        loop {
            if task(&runtime, &ids[0]).await.0.summary
                == "The same saved alpha conversation continued."
            {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    runtime.detach(view.request_id).await;
    background.0.abort();
    timeout(Duration::from_secs(8), async {
        while Arc::strong_count(&runtime) > 1 {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("The stopped background owner must release its runtime");
    Arc::try_unwrap(runtime)
        .ok()
        .unwrap()
        .close()
        .await
        .unwrap();
    let runtime = Runtime::start(&script, directory.path()).unwrap();
    let (reopened, history) = task(&runtime, &ids[0]).await;
    assert_eq!(reopened.conversation_id, alpha_task.conversation_id);
    assert!(history.contains("JOB_ALPHA") && history.contains("ALPHA_RECOVERY"));
    assert!(!history.contains("ALPHA_UNPLACED"));
    assert_eq!(fs::read_to_string(&alpha_pid).unwrap().lines().count(), 1);
    assert_eq!(fs::read_to_string(&beta_pid).unwrap().lines().count(), 1);
    runtime.close().await.unwrap();
}
