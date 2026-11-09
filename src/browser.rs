//! Browser handles belong to one conversation, host and live connection.

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncWriteExt, BufReader},
    process::{ChildStdin, Command},
    sync::{Mutex, mpsc, watch},
    task::JoinHandle,
    time::timeout,
};
use ts_rs::TS;
use uuid::Uuid;

use crate::runtime::FrameReader;

/// Selects an existing Chrome connection. Pairing credentials arrive through the
/// helper environment and never enter these serialized helper arguments.
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema, TS)]
#[serde(deny_unknown_fields)]
pub struct BrowserConfig {
    pub user_data_dir: Option<PathBuf>,
    pub profile_dir_name: Option<String>,
    // Native CDP connects to an existing browser on an explicitly configured
    // loopback port; it never creates a browser profile.
    pub debugging_port: Option<u16>,
    // Optional credential launcher prefix, e.g. ["with-credentials", "browser", "--"].
    // It wraps only the browser helper, so unavailable browser credentials do
    // not prevent the rest of Agent from starting.
    pub credential_command: Option<Vec<String>>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, TS)]
#[serde(deny_unknown_fields)]
pub struct BrowserRequest {
    pub owner_id: String,
    pub session_id: Uuid,
    pub operation_id: String,
    pub action: BrowserAction,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, TS)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum BrowserAction {
    Status,
    ListUserTabs,
    ClaimUserTab {
        tab: BrowserUserTab,
    },
    Open {
        url: String,
    },
    NewTab {
        url: String,
    },
    Snapshot {
        tab_id: u32,
    },
    Navigate {
        tab_id: u32,
        url: String,
    },
    Click {
        tab_id: u32,
        element_ref: String,
    },
    Fill {
        tab_id: u32,
        element_ref: String,
        value: String,
    },
    PressKey {
        tab_id: u32,
        key: String,
    },
    RunCode {
        tab_id: u32,
        code: String,
    },
    WaitFor {
        tab_id: u32,
        text: String,
    },
    HandleDialog {
        tab_id: u32,
        accept: bool,
        prompt_text: Option<String>,
    },
    CloseTab {
        tab_id: u32,
    },
    RetainTab {
        tab_id: u32,
    },
    Release,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema, TS)]
#[serde(deny_unknown_fields)]
pub struct BrowserTab {
    pub id: u32,
    pub url: String,
    pub title: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema, TS)]
#[serde(deny_unknown_fields)]
pub struct BrowserUserTab {
    pub target_id: String,
    pub url: String,
    pub title: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, TS)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum BrowserResponse {
    UserTabs {
        session_id: Uuid,
        tabs: Vec<BrowserUserTab>,
    },
    Ready {
        session_id: Uuid,
        tabs: Vec<BrowserTab>,
        output: String,
        is_error: bool,
    },
    Closed {
        session_id: Uuid,
        tabs: Vec<BrowserTab>,
        output: String,
    },
    Disconnected {
        session_id: Uuid,
        message: String,
    },
    Failed {
        session_id: Uuid,
        message: String,
    },
    Uncertain {
        session_id: Uuid,
        message: String,
    },
}

impl BrowserResponse {
    pub fn session_id(&self) -> Uuid {
        match self {
            Self::UserTabs { session_id, .. }
            | Self::Ready { session_id, .. }
            | Self::Closed { session_id, .. }
            | Self::Disconnected { session_id, .. }
            | Self::Failed { session_id, .. }
            | Self::Uncertain { session_id, .. } => *session_id,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, TS)]
#[serde(deny_unknown_fields)]
pub struct BrowserReply {
    pub operation_id: String,
    pub response: BrowserResponse,
}

/// Private helper protocol. Native control must be acknowledged before a page
/// enters the helper's usable tab inventory, including asynchronous popups.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, TS)]
#[serde(rename_all = "snake_case")]
pub enum BrowserNativeControlAction {
    Acquire,
    Release,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, TS)]
#[serde(deny_unknown_fields)]
pub struct BrowserNativeControlRequest {
    pub native_control_id: Uuid,
    pub target_id: String,
    pub action: BrowserNativeControlAction,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, TS)]
#[serde(deny_unknown_fields)]
pub struct BrowserNativeControlReply {
    pub native_control_id: Uuid,
    pub granted: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, TS)]
#[serde(untagged)]
pub enum BrowserHelperInput {
    Operation(BrowserRequest),
    NativeControl(BrowserNativeControlReply),
}

#[derive(Deserialize)]
#[serde(untagged)]
enum BrowserHelperOutput {
    Operation(BrowserReply),
    NativeControl(BrowserNativeControlRequest),
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, TS)]
#[serde(rename_all = "snake_case")]
pub enum BrowserConnectionState {
    Opening,
    Connected,
    Uncertain,
    Disconnected,
    Released,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, TS)]
pub struct BrowserBinding {
    pub owner_id: String,
    pub host_id: String,
    pub session_id: Uuid,
    pub state: BrowserConnectionState,
    pub pending_operation: Option<String>,
    pub response: Option<BrowserResponse>,
}

enum HelperShutdown {
    Terminate,
    Kill,
}

struct Process {
    session_id: Uuid,
    input: Arc<Mutex<Option<ChildStdin>>>,
    replies: mpsc::UnboundedReceiver<Result<BrowserReply>>,
    reader: Option<JoinHandle<()>>,
    shutdown: mpsc::UnboundedSender<HelperShutdown>,
    exited: watch::Receiver<bool>,
    monitor: Option<JoinHandle<()>>,
}

impl Process {
    fn start(
        runtime: &Path,
        config: &BrowserConfig,
        session_id: Uuid,
        claims: Arc<Mutex<HashMap<String, Uuid>>>,
    ) -> Result<Self> {
        let helper = runtime.with_file_name("browser-host.js");
        ensure!(
            helper.is_file(),
            "Browser runtime is missing; update Beep Boop on this host"
        );
        let mut command = if let Some((program, arguments)) = config
            .credential_command
            .as_deref()
            .and_then(|command| command.split_first())
        {
            let mut command = Command::new(program);
            command.args(arguments).arg("node");
            command
        } else {
            Command::new("node")
        };
        let mut child = command
            .arg(helper)
            .arg(serde_json::to_string(config)?)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            // Closing stdin asks the helper to disconnect its MCP child. Chrome
            // is externally owned and must outlive this process.
            .kill_on_drop(false)
            .spawn()
            .context("Could not start the browser runtime")?;
        let input = Arc::new(Mutex::new(Some(
            child.stdin.take().context("Browser runtime has no input")?,
        )));
        let output = child
            .stdout
            .take()
            .context("Browser runtime has no output")?;
        let (sender, replies) = mpsc::unbounded_channel();
        let (shutdown, mut signals) = mpsc::unbounded_channel();
        let (exit, exited) = watch::channel(false);
        let reader_exited = exited.clone();
        let monitor_claims = claims.clone();
        // This task owns Child through wait(), even when the connection is
        // idle or its Process is dropped. EOF alone cannot prove control ended.
        let monitor = tokio::spawn(async move {
            let status = loop {
                tokio::select! {
                    status = child.wait() => break status,
                    Some(signal) = signals.recv() => match signal {
                        HelperShutdown::Terminate => {
                            if let Some(pid) = child.id().and_then(|id| i32::try_from(id).ok()) {
                                unsafe { libc::kill(pid, libc::SIGTERM); }
                            }
                        }
                        HelperShutdown::Kill => { let _ = child.start_kill(); }
                    }
                }
            };
            if status.is_ok() {
                let mut claims = monitor_claims.lock().await;
                // Publish under the ledger lock: a queued pipe frame cannot
                // register another claim after this dead session is cleared.
                let _ = exit.send(true);
                claims.retain(|_, owner| *owner != session_id);
            }
        });
        let writer = input.clone();
        // Read while the helper is idle too. A popup must acquire the same
        // ledger entry as an explicit claim before its first helper effect.
        let reader = tokio::spawn(async move {
            let mut output = FrameReader::new(BufReader::new(output));
            let result: Result<()> = async {
                while let Some(frame) = output.next_frame().await? {
                    match serde_json::from_slice::<BrowserHelperOutput>(&frame)
                        .context("Invalid browser helper frame")?
                    {
                        BrowserHelperOutput::Operation(reply) => {
                            ensure!(
                                reply.response.session_id() == session_id,
                                "Browser helper session changed"
                            );
                            if sender.send(Ok(reply)).is_err() {
                                break;
                            }
                        }
                        BrowserHelperOutput::NativeControl(request) => {
                            let granted = {
                                let mut claims = claims.lock().await;
                                if *reader_exited.borrow() {
                                    false
                                } else {
                                    match request.action {
                                        BrowserNativeControlAction::Acquire => {
                                            if claims
                                                .get(&request.target_id)
                                                .is_none_or(|owner| *owner == session_id)
                                            {
                                                claims.insert(request.target_id, session_id);
                                                true
                                            } else {
                                                false
                                            }
                                        }
                                        BrowserNativeControlAction::Release => {
                                            if claims
                                                .get(&request.target_id)
                                                .is_some_and(|owner| *owner != session_id)
                                            {
                                                false
                                            } else {
                                                claims.remove(&request.target_id);
                                                true
                                            }
                                        }
                                    }
                                }
                            };
                            let mut line = serde_json::to_vec(&BrowserNativeControlReply {
                                native_control_id: request.native_control_id,
                                granted,
                            })?;
                            line.push(b'\n');
                            let mut input = writer.lock().await;
                            let input = input.as_mut().context("Browser connection is closed")?;
                            input.write_all(&line).await?;
                            input.flush().await?;
                        }
                    }
                }
                Ok(())
            }
            .await;
            if let Err(error) = result {
                let _ = sender.send(Err(error));
            }
        });
        Ok(Self {
            session_id,
            input,
            replies,
            reader: Some(reader),
            shutdown,
            exited,
            monitor: Some(monitor),
        })
    }

    async fn exchange(&mut self, request: &BrowserRequest) -> Result<BrowserReply> {
        {
            let mut input = self.input.lock().await;
            let input = input.as_mut().context("Browser connection is closed")?;
            let mut line = serde_json::to_vec(request)?;
            line.push(b'\n');
            input.write_all(&line).await?;
            input.flush().await?;
        }
        let reply = self
            .replies
            .recv()
            .await
            .context("Browser runtime disconnected")??;
        ensure!(
            reply.operation_id == request.operation_id
                && reply.response.session_id() == request.session_id,
            "Browser reply belongs to another operation or connection"
        );
        Ok(reply)
    }

    async fn wait_for_exit(&mut self) -> bool {
        loop {
            if *self.exited.borrow() {
                return true;
            }
            if self.exited.changed().await.is_err() {
                return false;
            }
        }
    }

    async fn close(&mut self) {
        self.input.lock().await.take();
        if !matches!(
            timeout(Duration::from_secs(5), self.wait_for_exit()).await,
            Ok(true)
        ) {
            let _ = self.shutdown.send(HelperShutdown::Terminate);
            if !matches!(
                timeout(Duration::from_secs(5), self.wait_for_exit()).await,
                Ok(true)
            ) {
                let _ = self.shutdown.send(HelperShutdown::Kill);
                let _ = self.wait_for_exit().await;
            }
        }
        if let Some(reader) = self.reader.take() {
            reader.abort();
            let _ = reader.await;
        }
        if let Some(monitor) = self.monitor.take() {
            let _ = monitor.await;
        }
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        // The reader owns a writer clone for claim acknowledgements. Abort it
        // so dropping the host still closes stdin and gives the helper EOF.
        if let Some(reader) = &self.reader {
            reader.abort();
        }
        // Keep the monitor alive to reap Child and release its session's claims.
    }
}

/// The host service owns connections across chat turns and client detachment.
pub struct BrowserHost {
    runtime: Option<PathBuf>,
    config: Option<BrowserConfig>,
    sessions: Mutex<HashMap<String, Arc<Mutex<Process>>>>,
    native_input: Mutex<()>,
    tab_claims: Arc<Mutex<HashMap<String, Uuid>>>,
}

impl Default for BrowserHost {
    fn default() -> Self {
        Self {
            runtime: None,
            config: None,
            sessions: Mutex::new(HashMap::new()),
            native_input: Mutex::new(()),
            tab_claims: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl BrowserHost {
    pub fn new(runtime: &Path, config: Option<BrowserConfig>) -> Self {
        Self {
            runtime: Some(runtime.to_owned()),
            config,
            sessions: Mutex::new(HashMap::new()),
            native_input: Mutex::new(()),
            tab_claims: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn execute(
        &self,
        request: BrowserRequest,
        cancelled: tokio::sync::watch::Receiver<bool>,
    ) -> BrowserResponse {
        match self.request(&request, &cancelled).await {
            Ok(response) => response,
            Err(_) => BrowserResponse::Uncertain { session_id: request.session_id,
                message: "The browser did not confirm this operation. Inspect its outcome before repeating it; browser effects are never replayed automatically.".into() },
        }
    }

    async fn request(
        &self,
        request: &BrowserRequest,
        cancelled: &tokio::sync::watch::Receiver<bool>,
    ) -> Result<BrowserResponse> {
        let mut sessions = self.sessions.lock().await;
        if *cancelled.borrow() {
            return Ok(BrowserResponse::Failed {
                session_id: request.session_id,
                message: "Browser operation was interrupted before it started.".into(),
            });
        }
        let opens = matches!(request.action, BrowserAction::Open { .. });
        let discovers = matches!(request.action, BrowserAction::ListUserTabs);
        let starts = opens || discovers && !sessions.contains_key(&request.owner_id);
        if starts {
            if sessions.contains_key(&request.owner_id) {
                return Ok(BrowserResponse::Failed { session_id: request.session_id,
                    message: "This conversation already owns a browser connection; inspect or release it before opening another.".into() });
            }
            let Some(config) = &self.config else {
                return Ok(BrowserResponse::Failed { session_id: request.session_id,
                    message: "Chrome is not configured on this host. Add a [browser] section to its private Beep Boop configuration and configure extension pairing or a native debugging port for its existing Chrome.".into() });
            };
            let runtime = self
                .runtime
                .as_deref()
                .context("Browser runtime is unavailable")?;
            let process = match Process::start(runtime, config, request.session_id, self.tab_claims.clone()) {
                Ok(process) => process,
                Err(_) => return Ok(BrowserResponse::Failed {
                    session_id: request.session_id,
                    message: "The browser runtime could not start. Build or update Beep Boop on this host before connecting Chrome.".into(),
                }),
            };
            sessions.insert(request.owner_id.clone(), Arc::new(Mutex::new(process)));
        }
        let Some(process) = sessions.get(&request.owner_id).cloned() else {
            return Ok(BrowserResponse::Disconnected { session_id: request.session_id,
                message: "This host has no live browser connection for this conversation. Previous tab handles are stale; open a new connection to continue.".into() });
        };
        drop(sessions);
        let mut live = process.lock().await;
        if live.session_id != request.session_id {
            return Ok(BrowserResponse::Disconnected { session_id: request.session_id,
                message: "This browser handle belongs to an older connection; inspect the current conversation's browser state.".into() });
        }
        // Helpers share Chrome's native focus. Keep activation and its effect
        // together, including tab creation/navigation and active-tab cleanup.
        // Reads and dialog answers must still unblock another owner's renderer.
        let _native_input = if matches!(
            request.action,
            BrowserAction::Status
                | BrowserAction::ListUserTabs
                | BrowserAction::Snapshot { .. }
                | BrowserAction::WaitFor { .. }
                | BrowserAction::RetainTab { .. }
                | BrowserAction::HandleDialog { .. }
        ) {
            None
        } else {
            let mut interrupted = cancelled.clone();
            tokio::select! {
                guard = self.native_input.lock() => Some(guard),
                () = async {
                    loop {
                        if *interrupted.borrow() { break; }
                        if interrupted.changed().await.is_err() {
                            std::future::pending::<()>().await;
                        }
                    }
                } => None,
            }
        };
        if *cancelled.borrow() {
            if starts {
                // Allocation can finish while an interrupt arrives. No browser
                // action has started, so release the newly allocated helper.
                live.close().await;
                let mut sessions = self.sessions.lock().await;
                if sessions
                    .get(&request.owner_id)
                    .is_some_and(|saved| Arc::ptr_eq(saved, &process))
                {
                    sessions.remove(&request.owner_id);
                }
            }
            return Ok(BrowserResponse::Failed {
                session_id: request.session_id,
                message: "Browser operation was interrupted before it started.".into(),
            });
        }
        if let BrowserAction::ClaimUserTab { tab } = &request.action {
            let claims = self.tab_claims.lock().await;
            if claims
                .get(&tab.target_id)
                .is_some_and(|session| *session != request.session_id)
            {
                return Ok(BrowserResponse::Failed {
                    session_id: request.session_id,
                    message: "This tab is controlled by another conversation.".into(),
                });
            }
        }
        let result = timeout(Duration::from_secs(150), live.exchange(request)).await;
        let result = match result {
            Ok(Ok(mut reply)) => {
                if let BrowserResponse::UserTabs { tabs, .. } = &mut reply.response {
                    let claims = self.tab_claims.lock().await;
                    tabs.retain(|tab| !claims.contains_key(&tab.target_id));
                }
                Ok(reply.response)
            }
            Ok(Err(error)) => Err(error),
            Err(error) => Err(error.into()),
        };
        let release = match &result {
            Ok(BrowserResponse::Closed { .. } | BrowserResponse::Disconnected { .. }) | Err(_) => {
                true
            }
            Ok(BrowserResponse::Failed { .. }) => starts,
            _ => false,
        };
        if release {
            live.close().await;
            let mut sessions = self.sessions.lock().await;
            if sessions
                .get(&request.owner_id)
                .is_some_and(|saved| Arc::ptr_eq(saved, &process))
            {
                sessions.remove(&request.owner_id);
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::time::Duration;

    use tokio::{
        fs,
        sync::watch,
        time::{sleep, timeout},
    };
    use uuid::Uuid;

    use crate::browser::{
        BrowserAction, BrowserConfig, BrowserHost, BrowserRequest, BrowserResponse,
    };

    const FIXTURE: &str = r#"
import { appendFileSync, existsSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { createInterface } from 'node:readline';
const {user_data_dir: directory} = JSON.parse(process.argv[2]);
writeFileSync(join(directory, 'pid'), String(process.pid));
writeFileSync(join(directory, 'wrapper'), process.env.AGENT_BROWSER_FIXTURE_WRAPPER ?? '');
const input = createInterface({input:process.stdin});
input.on('line', async line => {
  const request = JSON.parse(line);
  appendFileSync(join(directory, 'effects'), request.operation_id + '\n');
  if (request.operation_id === 'leased') {
    writeFileSync(join(directory, 'leased'), 'leased');
    while (!existsSync(join(directory, 'continue'))) await new Promise(resolve => setTimeout(resolve, 10));
  }
  if (request.operation_id === 'held') await new Promise(resolve => setTimeout(resolve, 200));
  if (request.operation_id === 'wrong-receipt') {
    process.stdout.write(JSON.stringify({operation_id:'other-operation',response:{type:'ready',session_id:request.session_id,tabs:[],output:'PRIVATE_FIXTURE_SENTINEL',is_error:false}})+'\n');
    return;
  }
  process.stdout.write(JSON.stringify({operation_id:request.operation_id,response:request.action.type==='release'
    ? {type:'closed',session_id:request.session_id,tabs:[],output:'released'}
    : {type:'ready',session_id:request.session_id,tabs:[],output:request.operation_id,is_error:false}})+'\n');
});
input.on('close', () => { writeFileSync(join(directory, 'closed'), 'closed'); });
"#;

    async fn host(directory: &Path) -> BrowserHost {
        fs::write(directory.join("browser-host.js"), FIXTURE)
            .await
            .unwrap();
        BrowserHost::new(
            &directory.join("bridge.js"),
            Some(BrowserConfig {
                user_data_dir: Some(directory.to_owned()),
                profile_dir_name: None,
                debugging_port: None,
                credential_command: None,
            }),
        )
    }

    fn request(
        owner: impl ToString,
        session: Uuid,
        operation: &str,
        action: BrowserAction,
    ) -> BrowserRequest {
        BrowserRequest {
            owner_id: owner.to_string(),
            session_id: session,
            operation_id: operation.into(),
            action,
        }
    }

    async fn wait_for(path: &Path) {
        timeout(Duration::from_secs(3), async {
            while !path.exists() {
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn queued_input_cancels_while_other_owners_read_and_handle_dialogs() {
        let directory = tempfile::tempdir().unwrap();
        let host = std::sync::Arc::new(host(directory.path()).await);
        let reader = "1";
        let reader_session = Uuid::new_v4();
        let (_stop, cancelled) = watch::channel(false);
        assert!(matches!(
            host.execute(
                request(
                    reader,
                    reader_session,
                    "reader-open",
                    BrowserAction::Open {
                        url: "about:blank".into()
                    }
                ),
                cancelled.clone()
            )
            .await,
            BrowserResponse::Ready { .. }
        ));
        let active_host = host.clone();
        let active_cancelled = cancelled.clone();
        let active = tokio::spawn(async move {
            active_host
                .execute(
                    request(
                        Uuid::new_v4(),
                        Uuid::new_v4(),
                        "leased",
                        BrowserAction::Open {
                            url: "about:blank".into(),
                        },
                    ),
                    active_cancelled,
                )
                .await
        });
        wait_for(&directory.path().join("leased")).await;
        let queued_owner = Uuid::new_v4();
        let queued_host = host.clone();
        let (stop_queued, queued_cancelled) = watch::channel(false);
        let mut queued = tokio::spawn(async move {
            queued_host
                .execute(
                    request(
                        queued_owner,
                        Uuid::new_v4(),
                        "queued",
                        BrowserAction::Open {
                            url: "about:blank".into(),
                        },
                    ),
                    queued_cancelled,
                )
                .await
        });
        timeout(Duration::from_secs(3), async {
            while !host
                .sessions
                .lock()
                .await
                .contains_key(&queued_owner.to_string())
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let reads = timeout(Duration::from_secs(3), async {
            let status = host
                .execute(
                    request(
                        reader,
                        reader_session,
                        "reader-status",
                        BrowserAction::Status,
                    ),
                    cancelled.clone(),
                )
                .await;
            let dialog = host
                .execute(
                    request(
                        reader,
                        reader_session,
                        "reader-dialog",
                        BrowserAction::HandleDialog {
                            tab_id: 1,
                            accept: true,
                            prompt_text: None,
                        },
                    ),
                    cancelled,
                )
                .await;
            (status, dialog)
        })
        .await;
        stop_queued.send(true).unwrap();
        let cancelled_reply = timeout(Duration::from_secs(3), &mut queued).await;
        fs::write(directory.path().join("continue"), "continue")
            .await
            .unwrap();
        let active_reply = active.await.unwrap();
        // Drain held input before assertions, including a failed queue test.
        if cancelled_reply.is_err() {
            let _ = queued.await;
        }
        assert!(matches!(active_reply, BrowserResponse::Ready { .. }));
        let (status, dialog) = reads.unwrap();
        assert!(matches!(status, BrowserResponse::Ready { .. }));
        assert!(matches!(dialog, BrowserResponse::Ready { .. }));
        assert!(matches!(
            cancelled_reply.unwrap().unwrap(),
            BrowserResponse::Failed { .. }
        ));
        assert!(
            !host
                .sessions
                .lock()
                .await
                .contains_key(&queued_owner.to_string())
        );
        let effects = fs::read_to_string(directory.path().join("effects"))
            .await
            .unwrap();
        assert!(effects.contains("reader-status\n") && effects.contains("reader-dialog\n"));
        assert!(!effects.contains("queued\n"));
    }

    #[tokio::test]
    async fn browser_credentials_wrap_only_the_owned_helper() {
        let directory = tempfile::tempdir().unwrap();
        let mut host = host(directory.path()).await;
        host.config.as_mut().unwrap().credential_command = Some(vec![
            "env".into(),
            "AGENT_BROWSER_FIXTURE_WRAPPER=wrapped".into(),
        ]);
        let owner = Uuid::new_v4();
        let session = Uuid::new_v4();
        let (_stop, cancelled) = watch::channel(false);
        assert!(matches!(
            host.execute(
                request(
                    owner,
                    session,
                    "open",
                    BrowserAction::Open {
                        url: "about:blank".into()
                    }
                ),
                cancelled.clone()
            )
            .await,
            BrowserResponse::Ready { .. }
        ));
        assert_eq!(
            fs::read_to_string(directory.path().join("wrapper"))
                .await
                .unwrap(),
            "wrapped"
        );
        assert!(matches!(
            host.execute(
                request(owner, session, "release", BrowserAction::Release),
                cancelled
            )
            .await,
            BrowserResponse::Closed { .. }
        ));
        wait_for(&directory.path().join("closed")).await;
    }

    #[tokio::test]
    async fn host_retains_connection_and_rejects_stale_handles_without_retargeting() {
        let directory = tempfile::tempdir().unwrap();
        let host = host(directory.path()).await;
        let owner = Uuid::new_v4();
        let session = Uuid::new_v4();
        let (_stop, cancelled) = watch::channel(false);
        assert!(matches!(
            host.execute(
                request(
                    owner,
                    session,
                    "open",
                    BrowserAction::Open {
                        url: "https://example.com".into()
                    }
                ),
                cancelled.clone()
            )
            .await,
            BrowserResponse::Ready { .. }
        ));
        let stale = Uuid::new_v4();
        assert!(
            matches!(host.execute(request(owner, stale, "stale", BrowserAction::Status), cancelled.clone()).await, BrowserResponse::Disconnected { session_id, .. } if session_id == stale)
        );
        assert!(matches!(
            host.execute(
                request(owner, session, "status", BrowserAction::Status),
                cancelled.clone()
            )
            .await,
            BrowserResponse::Ready { .. }
        ));
        assert!(matches!(
            host.execute(
                request(owner, session, "release", BrowserAction::Release),
                cancelled.clone()
            )
            .await,
            BrowserResponse::Closed { .. }
        ));
        assert_eq!(
            fs::read_to_string(directory.path().join("effects"))
                .await
                .unwrap(),
            "open\nstatus\nrelease\n"
        );
        wait_for(&directory.path().join("closed")).await;
        assert!(matches!(
            host.execute(
                request(owner, session, "after-close", BrowserAction::Status),
                cancelled
            )
            .await,
            BrowserResponse::Disconnected { .. }
        ));
    }

    #[tokio::test]
    async fn cancellation_keeps_issued_effect_but_fences_a_call_waiting_for_its_connection() {
        let directory = tempfile::tempdir().unwrap();
        let host = host(directory.path()).await;
        let owner = Uuid::new_v4();
        let session = Uuid::new_v4();
        let (_stop, cancelled) = watch::channel(false);
        let (stop_queued, queued_cancelled) = watch::channel(false);
        let opened = host.execute(
            request(
                owner,
                session,
                "held",
                BrowserAction::Open {
                    url: "https://example.com".into(),
                },
            ),
            cancelled.clone(),
        );
        let queued = async {
            wait_for(&directory.path().join("effects")).await;
            let operation = host.execute(
                request(
                    owner,
                    session,
                    "must-not-start",
                    BrowserAction::NewTab {
                        url: "https://example.com/another".into(),
                    },
                ),
                queued_cancelled,
            );
            let stop = async {
                sleep(Duration::from_millis(20)).await;
                stop_queued.send(true).unwrap();
            };
            let (result, ()) = tokio::join!(operation, stop);
            result
        };
        let (first, second) = tokio::join!(opened, queued);
        assert!(matches!(first, BrowserResponse::Ready { .. }));
        assert!(matches!(second, BrowserResponse::Failed { .. }));
        assert_eq!(
            fs::read_to_string(directory.path().join("effects"))
                .await
                .unwrap(),
            "held\n"
        );
        host.execute(
            request(owner, session, "release", BrowserAction::Release),
            cancelled,
        )
        .await;
    }

    #[tokio::test]
    async fn a_mismatched_receipt_is_uncertain_and_closes_only_its_owned_helper() {
        let directory = tempfile::tempdir().unwrap();
        let host = host(directory.path()).await;
        let owner = Uuid::new_v4();
        let session = Uuid::new_v4();
        let (_stop, cancelled) = watch::channel(false);
        let result = host
            .execute(
                request(
                    owner,
                    session,
                    "wrong-receipt",
                    BrowserAction::Open {
                        url: "https://example.com".into(),
                    },
                ),
                cancelled.clone(),
            )
            .await;
        let BrowserResponse::Uncertain {
            session_id,
            message,
        } = result
        else {
            panic!("An unrelated receipt must not confirm an effect");
        };
        assert_eq!(session_id, session);
        assert!(!message.contains("PRIVATE_FIXTURE_SENTINEL"));
        wait_for(&directory.path().join("closed")).await;
        assert!(matches!(
            host.execute(
                request(owner, session, "inspect", BrowserAction::Status),
                cancelled
            )
            .await,
            BrowserResponse::Disconnected { .. }
        ));
        assert_eq!(
            fs::read_to_string(directory.path().join("effects"))
                .await
                .unwrap(),
            "wrong-receipt\n"
        );
    }

    #[tokio::test]
    async fn dropping_the_host_disconnects_its_helper_through_eof() {
        let directory = tempfile::tempdir().unwrap();
        let host = host(directory.path()).await;
        let (_stop, cancelled) = watch::channel(false);
        let result = host
            .execute(
                request(
                    Uuid::new_v4(),
                    Uuid::new_v4(),
                    "open",
                    BrowserAction::Open {
                        url: "https://example.com".into(),
                    },
                ),
                cancelled,
            )
            .await;
        assert!(matches!(result, BrowserResponse::Ready { .. }));
        let pid: i32 = fs::read_to_string(directory.path().join("pid"))
            .await
            .unwrap()
            .parse()
            .unwrap();
        drop(host);
        wait_for(&directory.path().join("closed")).await;
        timeout(Duration::from_secs(3), async {
            while unsafe { libc::kill(pid, 0) } == 0 {
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }
}
