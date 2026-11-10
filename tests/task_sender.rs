//! Real host execution and CLI serialization retain the calling conversation.
use agent_cli::{
    host::{ACTOR_CONVERSATION_ENV, Executor},
    protocol::{HostCommand, HostResult, InputMode, InputOrigin, RuntimeCommand, RuntimeEventKind},
    runtime::FrameReader,
};
use std::{fs, os::unix::fs::PermissionsExt, process::Stdio, time::Duration};
use tokio::{
    io::{AsyncWriteExt, BufReader},
    net::UnixListener,
    process::Command,
    sync::watch,
    time::timeout,
};

#[tokio::test]
async fn host_exec_and_real_tasks_send_preserve_explicit_actor_and_clear_inherited_actor() {
    const CHILD: &str = "BEEP_BOOP_TEST_TASK_SENDER_CHILD";
    const TEST: &str =
        "host_exec_and_real_tasks_send_preserve_explicit_actor_and_clear_inherited_actor";
    if std::env::var_os(CHILD).is_none() {
        // Isolate the inherited environment without mutating this test process.
        let output = timeout(
            Duration::from_secs(30),
            Command::new(std::env::current_exe().unwrap())
                .args(["--exact", TEST, "--nocapture"])
                .env(CHILD, "true")
                .env(ACTOR_CONVERSATION_ENV, "999")
                .output(),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }
    for cancellable in [false, true] {
        for sender in [None, Some("42")] {
            let directory = tempfile::tempdir().unwrap();
            let state = directory.path().join("state");
            fs::create_dir(&state).unwrap();
            fs::set_permissions(&state, fs::Permissions::from_mode(0o700)).unwrap();
            let config = directory.path().join("config.toml");
            fs::write(&config, "").unwrap();
            let listener = UnixListener::bind(state.join("agent.sock")).unwrap();
            let task_id = "11111111-1111-4111-8111-111111111111";
            let input_id = "22222222-2222-4222-8222-222222222222";
            let server = tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                let (reader, mut writer) = stream.into_split();
                let bytes = FrameReader::new(BufReader::new(reader))
                    .next_frame()
                    .await
                    .unwrap()
                    .unwrap();
                let command: RuntimeCommand = serde_json::from_slice(&bytes).unwrap();
                for event in [
                    RuntimeEventKind::InputAccepted {
                        input_id: input_id.into(),
                        work: None,
                    },
                    RuntimeEventKind::Done,
                ] {
                    writer
                        .write_all(&serde_json::to_vec(&event).unwrap())
                        .await
                        .unwrap();
                    writer.write_all(b"\n").await.unwrap();
                }
                command
            });
            let command = HostCommand::Exec {
                program: env!("CARGO_BIN_EXE_beep-boop").into(),
                args: vec![
                    "--json".into(),
                    "--config".into(),
                    config.to_str().unwrap().into(),
                    "--state-dir".into(),
                    state.to_str().unwrap().into(),
                    "tasks".into(),
                    "send".into(),
                    task_id.into(),
                    "Recovered source evidence".into(),
                    "--input-id".into(),
                    input_id.into(),
                ],
                cwd: directory.path().to_str().unwrap().into(),
                timeout_seconds: 5,
                sender_conversation_id: sender.map(str::to_owned),
            };
            let executor = Executor::new(directory.path());
            let (_cancel, cancelled) = watch::channel(false);
            let result = if cancellable {
                executor.execute_cancelled(command, cancelled).await
            } else {
                executor.execute(command).await
            };
            assert!(
                matches!(&result, HostResult::Process { exit_code: 0, .. }),
                "{result:?}"
            );
            let RuntimeCommand::TaskInput {
                origin,
                sender_conversation_id,
                task_id: received_task,
                input_id: received_input,
                input,
                mode,
            } = server.await.unwrap()
            else {
                panic!("Expected the actual tasks-send command");
            };
            assert_eq!(sender_conversation_id.as_deref(), sender);
            assert_eq!(origin, sender.is_none().then_some(InputOrigin::Cli));
            assert_eq!(received_task.to_string(), task_id);
            assert_eq!(received_input, input_id);
            assert_eq!(input, "Recovered source evidence");
            assert!(matches!(mode, InputMode::Steer));
            assert_eq!(std::env::var(ACTOR_CONVERSATION_ENV).unwrap(), "999");
        }
    }
}

#[tokio::test]
async fn tasks_send_rejects_noncanonical_actor_identity_before_delivery() {
    let directory = tempfile::tempdir().unwrap();
    fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let config = directory.path().join("config.toml");
    fs::write(&config, "").unwrap();
    for invalid in ["", "0", "01", "+1", "-1", "1.0", " 1", "9007199254740992"] {
        let output = timeout(
            Duration::from_secs(5),
            Command::new(env!("CARGO_BIN_EXE_beep-boop"))
                .arg("--config")
                .arg(&config)
                .arg("--state-dir")
                .arg(directory.path())
                .args([
                    "tasks",
                    "send",
                    "11111111-1111-4111-8111-111111111111",
                    "Context",
                ])
                .env(ACTOR_CONVERSATION_ENV, invalid)
                .stdin(Stdio::null())
                .output(),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(
            !output.status.success(),
            "Accepted invalid identity {invalid:?}"
        );
        assert!(
            String::from_utf8_lossy(&output.stderr)
                .contains("Actor conversation identity must be a canonical positive native ID"),
            "{invalid:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(!directory.path().join("agent.sock").exists());
    }
}
