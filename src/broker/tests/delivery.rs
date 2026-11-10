use super::{Worker, close, effect, event, runtime};
use crate::{
    broker::Broker,
    host::Executor,
    protocol::{HostCommand, HostResult, RuntimeEventKind},
};
use serde::Deserialize;
use std::sync::Arc;

#[derive(Deserialize)]
struct FailureWire {
    delivery: Option<String>,
}

fn delivery(result: &HostResult) -> Option<String> {
    serde_json::from_slice::<FailureWire>(&serde_json::to_vec(result).unwrap())
        .unwrap()
        .delivery
}

#[tokio::test]
async fn a_node_missing_before_admission_reports_not_started() {
    let directory = tempfile::tempdir().unwrap();
    let broker = Broker::new(None, directory.path()).unwrap();
    let marker = directory.path().join("must-not-run");
    let result = broker
        .execute(effect(
            "22222222-2222-4222-8222-222222222222",
            HostCommand::WriteFile {
                path: marker.to_string_lossy().into_owned(),
                content: "must not execute".into(),
            },
        ))
        .await;
    assert!(matches!(result, HostResult::Failed { .. }));
    assert_eq!(delivery(&result).as_deref(), Some("not_started"));
    assert!(!marker.exists());
}

#[tokio::test]
async fn losing_a_real_write_receipt_reports_uncertain_without_replaying_the_write() {
    let (directory, runtime) = runtime().await;
    let broker = Arc::new(Broker::new(None, directory.path()).unwrap());
    let host = "22222222-2222-4222-8222-222222222222";
    let mut worker = Worker::connect(broker.clone(), runtime.clone(), host).await;
    let marker = directory.path().join("written-once");
    let pending = {
        let broker = broker.clone();
        let path = marker.to_string_lossy().into_owned();
        tokio::spawn(async move {
            broker
                .execute(effect(
                    host,
                    HostCommand::WriteFile {
                        path,
                        content: "actual write".into(),
                    },
                ))
                .await
        })
    };
    let RuntimeEventKind::HostCall { command, .. } = event(&mut worker.reader).await else {
        panic!("Expected an owned write")
    };
    let result = Executor::new(directory.path()).execute(*command).await;
    assert!(matches!(result, HostResult::Written));
    worker.close().await;
    let result = pending.await.unwrap();
    assert_eq!(delivery(&result).as_deref(), Some("outcome_uncertain"));
    assert_eq!(std::fs::read_to_string(marker).unwrap(), "actual write");
    close(runtime).await;
}

#[test]
fn legacy_host_failures_make_no_delivery_claim() {
    let result: HostResult =
        serde_json::from_str(r#"{"type":"failed","message":"Legacy failure"}"#).unwrap();
    assert!(delivery(&result).is_none());
}
