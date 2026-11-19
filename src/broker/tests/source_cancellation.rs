use std::time::Duration;

use serde::Deserialize;
use tokio::{net::TcpStream, sync::watch, task::JoinSet, time::timeout};

use crate::{
    background::{BackgroundRequest, BackgroundResponse},
    broker::Broker,
    protocol::{HostCommand, HostResult},
    sources::SourceConfig,
};

fn source(command: Vec<String>) -> HostCommand {
    HostCommand::Background {
        request: BackgroundRequest::PollSource {
            source: SourceConfig::Slack {
                enabled: true,
                command,
                account: "fixture-workspace".into(),
            },
            state: None,
        },
    }
}

#[tokio::test]
async fn cancelling_background_source_read_stops_its_helper_and_descendant() {
    #[derive(Deserialize)]
    struct Fixture {
        pid: i32,
        descendant: i32,
        port: u16,
    }
    let directory = tempfile::tempdir().unwrap();
    let marker = directory.path().join("source-child.json");
    let script = r#"
const {spawn} = require('node:child_process');
const {writeFileSync} = require('node:fs');
const child = spawn(process.execPath, ['--eval', `
  const server = require('node:net').createServer();
  server.listen(0, '127.0.0.1', () => process.stdout.write(JSON.stringify({pid:process.pid,port:server.address().port})));
`], {stdio:['ignore','pipe','ignore']});
child.stdout.once('data', data => {
  const descendant = JSON.parse(data.toString());
  writeFileSync(process.argv[1], JSON.stringify({pid:process.pid,descendant:descendant.pid,port:descendant.port}));
});
setInterval(() => {}, 1000);
"#;
    let broker = Broker::new(None, directory.path()).unwrap();
    let command = source(vec![
        "node".into(),
        "--eval".into(),
        script.into(),
        marker.to_string_lossy().into_owned(),
    ]);
    let (cancel, cancelled) = watch::channel(false);
    // JoinSet owns cleanup even if a fixture assertion fails before cancel.
    let mut calls = JoinSet::new();
    calls.spawn(async move { broker.execute_cancelled(command, cancelled).await });
    let fixture: Fixture = timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(contents) = tokio::fs::read(&marker).await
                && let Ok(fixture) = serde_json::from_slice(&contents)
            {
                break fixture;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("The actual Slack helper and its descendant must start");
    assert_ne!(fixture.pid, fixture.descendant);
    assert!(
        TcpStream::connect(("127.0.0.1", fixture.port))
            .await
            .is_ok()
    );
    cancel.send(true).unwrap();
    let receipt = timeout(Duration::from_secs(2), calls.join_next()).await;
    if receipt.is_err() {
        // The fail-first run must clean its fixture rather than leave a
        // fifteen-minute helper behind after demonstrating the regression.
        calls.abort_all();
        while calls.join_next().await.is_some() {}
    }
    timeout(Duration::from_secs(5), async {
        loop {
            let reaped = unsafe { libc::kill(fixture.pid, 0) } == -1
                && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH);
            if reaped
                && TcpStream::connect(("127.0.0.1", fixture.port))
                    .await
                    .is_err()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("Cancellation must reap the helper and release its descendant's socket");
    let result = receipt
        .expect("The broker must honor cancellation rather than await the source helper timeout")
        .unwrap()
        .unwrap();
    assert!(
        matches!(result, HostResult::Failed { delivery: None, .. }),
        "A started source read cannot report not_started: {result:?}"
    );
}

#[tokio::test]
async fn pre_cancelled_background_source_read_does_not_start_a_helper() {
    let directory = tempfile::tempdir().unwrap();
    let marker = directory.path().join("must-not-run");
    let broker = Broker::new(None, directory.path()).unwrap();
    let (cancel, cancelled) = watch::channel(true);
    let result = broker
        .execute_cancelled(
            source(vec![
                "node".into(),
                "--eval".into(),
                "require('node:fs').writeFileSync(process.argv[1], 'started'); process.stdout.write(JSON.stringify({ok:true,messages:{matches:[],paging:{next_cursor:null,total:0}}}));".into(),
                marker.to_string_lossy().into_owned(),
            ]),
            cancelled,
        )
        .await;
    drop(cancel);
    assert!(matches!(result, HostResult::Failed { .. }));
    assert!(
        !marker.exists(),
        "A cancelled owner cannot start a new source helper"
    );
}

#[tokio::test]
async fn a_closed_uncancelled_owner_keeps_the_real_background_source_result() {
    let directory = tempfile::tempdir().unwrap();
    let broker = Broker::new(None, directory.path()).unwrap();
    let (owner, cancelled) = watch::channel(false);
    drop(owner);
    let result = broker
        .execute_cancelled(
            source(vec![
                "node".into(),
                "--eval".into(),
                "process.stdout.write(JSON.stringify({ok:true,messages:{matches:[],paging:{next_cursor:null,total:0}}}));".into(),
            ]),
            cancelled,
        )
        .await;
    assert!(matches!(
        result,
        HostResult::Background {
            response: BackgroundResponse::SourcePage { .. }
        }
    ));
}
