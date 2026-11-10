use crate::{
    host::Executor,
    protocol::{HostCommand, HostResult},
};
use serde::Deserialize;

#[derive(Deserialize)]
struct FailureWire {
    delivery: Option<String>,
    message: String,
}

#[tokio::test]
async fn process_start_failure_reports_not_started_and_keeps_its_actual_cause() {
    let directory = tempfile::tempdir().unwrap();
    let program = directory
        .path()
        .join("missing-program")
        .to_string_lossy()
        .into_owned();
    let cwd = directory.path().to_string_lossy().into_owned();
    let result = Executor::new(directory.path())
        .execute(HostCommand::Exec {
            sender_conversation_id: None,
            program: program.clone(),
            args: vec!["invented-sensitive-argument".into()],
            cwd: cwd.clone(),
            timeout_seconds: 1,
        })
        .await;
    assert!(matches!(result, HostResult::Failed { .. }));
    let wire: FailureWire = serde_json::from_slice(&serde_json::to_vec(&result).unwrap()).unwrap();
    assert_eq!(wire.delivery.as_deref(), Some("not_started"));
    assert!(wire.message.contains(&program));
    assert!(wire.message.contains(&cwd));
    assert!(
        wire.message
            .contains(&std::io::Error::from_raw_os_error(libc::ENOENT).to_string())
    );
    assert!(!wire.message.contains("invented-sensitive-argument"));
}
