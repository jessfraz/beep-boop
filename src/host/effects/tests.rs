use crate::{
    host::{
        Executor,
        effects::{Admission, Capacity, HostEffects},
    },
    protocol::{HostCommand, HostResult},
};
use std::{path::Path, sync::Arc};
use tokio::{
    sync::{oneshot, watch},
    time::{Duration, sleep, timeout},
};

async fn wait_for(path: &Path) {
    timeout(Duration::from_secs(5), async {
        while !path.exists() {
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn registered_worker_capacity_refuses_before_starting_an_effect() {
    let directory = tempfile::tempdir().unwrap();
    let executor = Arc::new(Executor::new(directory.path()));
    let (release, released) = watch::channel(false);
    let mut effects = HostEffects::new(Capacity::RegisteredWorker);
    for index in 0..32 {
        let executor = executor.clone();
        let mut released = released.clone();
        let path = directory.path().join(format!("write-{index}"));
        let admission = effects
            .spawn(format!("call-{index}"), |cancelled| async move {
                while !*released.borrow_and_update() {
                    released.changed().await.unwrap();
                }
                executor
                    .execute_cancelled(
                        HostCommand::WriteFile {
                            path: path.to_string_lossy().into_owned(),
                            content: "written".into(),
                        },
                        cancelled,
                    )
                    .await
            })
            .unwrap();
        assert!(matches!(admission, Admission::Started));
    }
    let unexpected = directory.path().join("unexpected");
    let rejected = effects
        .spawn("extra".into(), |_| {
            std::fs::write(&unexpected, "must not execute").unwrap();
            async { HostResult::Written }
        })
        .unwrap();
    assert!(
        matches!(rejected, Admission::Rejected(reply) if reply.call_id=="extra" && matches!(reply.result,HostResult::Failed{..}))
    );
    assert!(!unexpected.exists());
    release.send(true).unwrap();
    let mut draining = effects.shutdown();
    let mut completed = 0;
    while let Some(reply) = draining.next().await {
        assert!(matches!(reply.unwrap().result, HostResult::Written));
        completed += 1;
    }
    assert_eq!(completed, 32);
    for index in 0..32 {
        assert_eq!(
            std::fs::read_to_string(directory.path().join(format!("write-{index}"))).unwrap(),
            "written"
        );
    }
}

#[tokio::test]
async fn dropping_an_owner_retains_an_admitted_write_until_its_actual_result() {
    let directory = tempfile::tempdir().unwrap();
    let executor = Arc::new(Executor::new(directory.path()));
    let output = directory.path().join("output");
    let path = output.to_string_lossy().into_owned();
    let (started, running) = oneshot::channel();
    let (release, released) = oneshot::channel();
    let (receipt, recorded) = oneshot::channel();
    let mut effects = HostEffects::new(Capacity::Unbounded);
    effects
        .spawn("write".into(), |cancelled| async move {
            started.send(()).unwrap();
            released.await.unwrap();
            let result = executor
                .execute_cancelled(
                    HostCommand::WriteFile {
                        path,
                        content: "complete".into(),
                    },
                    cancelled,
                )
                .await;
            receipt.send(result.clone()).unwrap();
            result
        })
        .unwrap();
    running.await.unwrap();
    drop(effects);
    release.send(()).unwrap();
    assert!(matches!(
        timeout(Duration::from_secs(5), recorded)
            .await
            .unwrap()
            .unwrap(),
        HostResult::Written
    ));
    assert_eq!(std::fs::read_to_string(output).unwrap(), "complete");
}

#[tokio::test]
async fn cancel_reaps_a_running_process_and_releases_its_call_identity() {
    let directory = tempfile::tempdir().unwrap();
    let started = directory.path().join("started");
    let executor = Arc::new(Executor::new(directory.path()));
    let program = format!(
        "require('node:fs').writeFileSync({},'started');setInterval(()=>{{}},1000)",
        serde_json::to_string(&started).unwrap()
    );
    let cwd = directory.path().to_string_lossy().into_owned();
    let mut effects = HostEffects::new(Capacity::Unbounded);
    effects
        .spawn("process".into(), |cancelled| async move {
            executor
                .execute_cancelled(
                    HostCommand::Exec {
                        sender_conversation_id: None,
                        program: "node".into(),
                        args: vec!["--eval".into(), program],
                        cwd,
                        timeout_seconds: 30,
                    },
                    cancelled,
                )
                .await
        })
        .unwrap();
    wait_for(&started).await;
    assert!(!effects.cancel("missing"));
    assert!(effects.cancel("process"));
    let reply = timeout(Duration::from_secs(5), effects.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(reply.call_id, "process");
    assert!(matches!(
        reply.result,
        HostResult::Process { exit_code: 143, .. }
    ));
    assert!(!effects.cancel("process"));
    assert!(matches!(
        effects
            .spawn("process".into(), |_| async { HostResult::Written })
            .unwrap(),
        Admission::Started
    ));
    let mut draining = effects.shutdown();
    assert!(matches!(
        draining.next().await.unwrap().unwrap().result,
        HostResult::Written
    ));
    assert!(draining.next().await.is_none());
}

#[tokio::test]
async fn duplicate_ids_reject_before_replacing_cancellation_or_starting_another_write() {
    let directory = tempfile::tempdir().unwrap();
    let executor = Arc::new(Executor::new(directory.path()));
    let (release, released) = oneshot::channel();
    let output = directory
        .path()
        .join("original")
        .to_string_lossy()
        .into_owned();
    let mut effects = HostEffects::new(Capacity::Unbounded);
    effects
        .spawn("same".into(), |cancelled| async move {
            released.await.unwrap();
            executor
                .execute_cancelled(
                    HostCommand::WriteFile {
                        path: output,
                        content: "original".into(),
                    },
                    cancelled,
                )
                .await
        })
        .unwrap();
    let unexpected = directory.path().join("duplicate");
    assert!(
        effects
            .spawn("same".into(), |_| {
                std::fs::write(&unexpected, "must not start").unwrap();
                async { HostResult::Written }
            })
            .is_err()
    );
    assert!(!unexpected.exists());
    assert!(effects.cancel("same"));
    release.send(()).unwrap();
    let mut draining = effects.shutdown();
    let reply = draining.next().await.unwrap().unwrap();
    assert_eq!(reply.call_id, "same");
    assert!(matches!(reply.result, HostResult::Written));
    assert!(draining.next().await.is_none());
    assert_eq!(
        std::fs::read_to_string(directory.path().join("original")).unwrap(),
        "original"
    );
}
