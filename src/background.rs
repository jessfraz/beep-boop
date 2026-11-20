//! Typed background work; Pi owns every timer and durable execution checkpoint.
use anyhow::{Context, Result, ensure};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use ts_rs::TS;

use crate::{
    host::effects::{Admission, Capacity, HostEffects},
    jobs::{self, JobSchedule, OccurrencePlan, ScheduledJob, TimezoneId, UtcMillis},
    protocol::SourceState,
    sources::{SourceBatch, SourceCheckpoint, SourceConfig},
};

/// The service owns this transport until shutdown. Timer callbacks must never
/// retain a chat connection or execute on a fallback host after disconnect.
pub async fn run(
    runtime: std::sync::Arc<crate::runtime::Runtime>,
    broker: std::sync::Arc<crate::broker::Broker>,
    settings: BackgroundSettings,
) -> Result<()> {
    use crate::protocol::{RuntimeCommand, RuntimeEventKind};
    let mut call = runtime
        .request(RuntimeCommand::RunBackground { settings })
        .await?;
    let mut effects = HostEffects::new(Capacity::Unbounded);
    let result = async { loop {
        tokio::select! {
            event = call.events.recv() => match event.context("Background transport closed")? {
                RuntimeEventKind::HostCall { call_id, command } => {
                    let broker = broker.clone();
                    if let Admission::Rejected(reply) = effects.spawn(call_id, |cancelled| async move {
                        broker.execute_cancelled(*command, cancelled).await
                    })? {
                        runtime.reply(call.request_id, reply.call_id, reply.result).await?;
                    }
                }
                RuntimeEventKind::HostCancel { call_id } => {
                    effects.cancel(&call_id);
                }
                RuntimeEventKind::Done => break Ok(()),
                RuntimeEventKind::Failure { message, .. } => break Err(anyhow::anyhow!(message)),
                _ => {}
            },
            effect = effects.next(), if !effects.is_empty() => {
                let reply = effect?.context("Background effect queue closed")?;
                runtime.reply(call.request_id, reply.call_id, reply.result).await?;
            }
            _ = runtime.stopped() => break Ok(()),
            _ = runtime.stopping() => break Ok(()),
        }
    } }.await;
    let mut draining = effects.shutdown();
    let mut drained = Ok(());
    while let Some(reply) = draining.next().await {
        let delivery = match reply {
            Ok(reply) => {
                runtime
                    .reply(call.request_id, reply.call_id, reply.result)
                    .await
            }
            Err(error) => Err(error),
        };
        drained = drained.and(delivery);
    }
    runtime.detach(call.request_id).await;
    result.and(drained)
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, TS)]
pub struct BackgroundSettings {
    pub poll_interval_seconds: u32,
    pub sources: bool,
    pub jobs: Option<Vec<ScheduledJob>>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, TS)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BackgroundCadence {
    Interval {
        seconds: u32,
    },
    Calendar {
        schedule: JobSchedule,
        timezone: TimezoneId,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, TS)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BackgroundRequest {
    Plan {
        cadence: BackgroundCadence,
        after: Option<UtcMillis>,
        now: UtcMillis,
    },
    PollSources {
        sources: Vec<SourceConfig>,
        states: Vec<SourceState>,
        cursor: Option<SourcePollCursor>,
    },
    DiscoverSources {
        sources: Vec<SourceConfig>,
        states: Vec<SourceState>,
    },
    PollSource {
        source: SourceConfig,
        state: Option<SourceState>,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, TS)]
pub struct SourceRead {
    pub batch: SourceBatch,
    pub expected_checkpoint: Option<SourceCheckpoint>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, TS)]
pub struct SourcePollCursor {
    pub source: crate::sources::SourceKey,
    pub page: u32,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, TS)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BackgroundResponse {
    Plan {
        plan: OccurrencePlan,
    },
    Sources {
        reads: Vec<SourceRead>,
        cursor: Option<SourcePollCursor>,
    },
    SourceCatalog {
        sources: Vec<SourceConfig>,
        reads: Vec<SourceRead>,
    },
    SourcePage {
        read: Box<SourceRead>,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, TS)]
pub struct ScheduledOccurrence {
    pub id: String,
    pub job: ScheduledJob,
    pub schedule_id: jobs::ScheduleId,
    pub scheduled_at: UtcMillis,
    /// New admissions capture a fresh task; older retained work keeps its
    /// original actor and receipts when this field is absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub task_id: Option<crate::protocol::TaskId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub manual: Option<JobManualRequest>,
}

/// Presence records a directly requested run and its obligation to publish the
/// actual result or blocker, even when no additional input was supplied.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct JobManualRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub input: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, TS)]
pub struct JobSyncReport {
    pub added: u32,
    pub updated: u32,
    pub removed: u32,
    pub unchanged: u32,
}

/// Durable admission of one requested run, independent of its execution result.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, TS, PartialEq, Eq)]
pub struct JobRunReceipt {
    pub request_id: String,
    pub task_id: crate::protocol::TaskId,
    pub job_id: jobs::JobId,
    pub schedule_id: jobs::ScheduleId,
    pub revision: String,
    pub admitted_at: UtcMillis,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub manual: Option<JobManualRequest>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, TS)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BackgroundTriggerKey {
    Builtin {
        name: BuiltinTrigger,
    },
    Job {
        job_id: jobs::JobId,
        schedule_id: jobs::ScheduleId,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, TS)]
#[serde(rename_all = "snake_case")]
pub enum BuiltinTrigger {
    Sources,
    SourceTriage,
    Resources,
    History,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, TS)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BackgroundWork {
    Sources,
    SourceTriage,
    Resources,
    History,
    Job {
        occurrence: Box<ScheduledOccurrence>,
    },
    ManualJob {
        occurrence: Box<ScheduledOccurrence>,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, TS)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BackgroundTaskInput {
    Trigger {
        key: BackgroundTriggerKey,
        generation: u32,
        cadence: BackgroundCadence,
        job: Option<Box<ScheduledJob>>,
    },
    Work {
        key: BackgroundTriggerKey,
        work: BackgroundWork,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, TS)]
#[serde(tag = "phase", rename_all = "snake_case")]
pub enum BackgroundCheckpoint {
    Tick { cursor: Option<UtcMillis> },
    Execute { attempt: u32 },
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, TS)]
pub struct BackgroundError {
    pub key: BackgroundTriggerKey,
    pub message: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, TS)]
pub struct BackgroundStatus {
    pub jobs: u32,
    pub triggers: u32,
    pub pending: u32,
    pub errors: Vec<BackgroundError>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, TS)]
pub struct JobScheduleStatus {
    pub schedule_id: jobs::ScheduleId,
    pub next_at: Option<UtcMillis>,
    pub last_at: Option<UtcMillis>,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, TS)]
pub struct ScheduledJobStatus {
    pub job: ScheduledJob,
    pub schedules: Vec<JobScheduleStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub task_id: Option<crate::protocol::TaskId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub last_run: Option<JobExecution>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub progress: Option<JobProgress>,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, JsonSchema, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum JobProgressStatus {
    Normal,
    Overdue,
    Unavailable,
}

/// Observation of existing work, not another execution state machine.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, TS)]
pub struct JobProgress {
    pub status: JobProgressStatus,
    pub checked_at: UtcMillis,
    pub last_progress_at: Option<UtcMillis>,
    pub pending_work: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub blocker: Option<String>,
}

/// A view of the existing Pi execution, never a second execution state machine.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, TS)]
pub struct JobExecution {
    pub schedule_id: jobs::ScheduleId,
    pub scheduled_at: UtcMillis,
    pub started_at: Option<UtcMillis>,
    pub finished_at: Option<UtcMillis>,
    pub revision: String,
    pub state: JobExecutionState,
    pub task_id: Option<crate::protocol::TaskId>,
    pub output: Option<String>,
    pub output_truncated: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, TS)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum JobExecutionState {
    Queued,
    Running,
    Waiting,
    Retrying {
        at: UtcMillis,
        message: Option<String>,
    },
    Blocked {
        message: String,
    },
    Completed,
    Failed {
        message: String,
    },
    Stopped,
}

pub async fn execute(request: BackgroundRequest) -> Result<BackgroundResponse> {
    match request {
        BackgroundRequest::Plan {
            cadence,
            after,
            now,
        } => {
            let plan = match cadence {
                BackgroundCadence::Interval { seconds } => interval(seconds, after, now)?,
                BackgroundCadence::Calendar { schedule, timezone } => {
                    jobs::plan(&schedule, &timezone, after, now)?
                }
            };
            Ok(BackgroundResponse::Plan { plan })
        }
        BackgroundRequest::PollSources {
            sources,
            states,
            cursor,
        } => {
            let (reads, cursor) = crate::intake::poll(&sources, &states, cursor).await?;
            Ok(BackgroundResponse::Sources { reads, cursor })
        }
        BackgroundRequest::DiscoverSources { sources, states } => {
            let (sources, reads) = crate::intake::discover(&sources, &states).await?;
            Ok(BackgroundResponse::SourceCatalog { sources, reads })
        }
        BackgroundRequest::PollSource { source, state } => {
            let read = crate::intake::poll_source(&source, state.as_ref()).await?;
            Ok(BackgroundResponse::SourcePage {
                read: Box::new(read),
            })
        }
    }
}

fn interval(seconds: u32, after: Option<UtcMillis>, now: UtcMillis) -> Result<OccurrencePlan> {
    ensure!(seconds > 0, "Background intervals must be positive");
    let every = i64::from(seconds) * 1000;
    let next = after
        .map_or(now.get(), |previous| previous.get().max(now.get()))
        .checked_add(every)
        .context("Background deadline overflow")?;
    // Intervals coalesce overdue work and retain fixed-delay source polling.
    let due = after
        .is_none_or(|previous| previous.get() <= now.get())
        .then_some(now);
    Ok(OccurrencePlan {
        due,
        next: Some(UtcMillis::new(next)?),
    })
}

#[cfg(test)]
mod tests {
    use crate::background::{JobManualRequest, JobProgress, JobProgressStatus, JobRunReceipt};

    #[test]
    fn manual_job_receipts_distinguish_legacy_admission_from_direct_result_obligations() {
        let mut receipt: JobRunReceipt = serde_json::from_str(
            r#"{
                "request_id":"retained-request",
                "task_id":"11111111-1111-4111-8111-111111111111",
                "job_id":"example-review",
                "schedule_id":"daily",
                "revision":"one",
                "admitted_at":1
            }"#,
        )
        .unwrap();
        assert!(receipt.manual.is_none());
        receipt.manual = Some(JobManualRequest { input: None });
        let direct: JobRunReceipt =
            serde_json::from_slice(&serde_json::to_vec(&receipt).unwrap()).unwrap();
        assert_eq!(direct, receipt);
        assert_eq!(direct.manual, Some(JobManualRequest { input: None }));

        receipt.manual = Some(JobManualRequest {
            input: Some("Review newly arrived mail now.".into()),
        });
        let requested: JobRunReceipt =
            serde_json::from_slice(&serde_json::to_vec(&receipt).unwrap()).unwrap();
        assert_ne!(requested, direct);
        assert_eq!(
            requested.manual.unwrap().input.as_deref(),
            Some("Review newly arrived mail now.")
        );
    }

    #[test]
    fn workflow_progress_reports_unavailable_coverage_explicitly() {
        let progress: JobProgress = serde_json::from_str(
            r#"{"status":"unavailable","checked_at":1,"last_progress_at":null,
                "pending_work":0,"blocker":"Native progress read is unavailable"}"#,
        )
        .unwrap();
        assert_eq!(progress.status, JobProgressStatus::Unavailable);
        assert!(progress.last_progress_at.is_none());
        let restored: JobProgress =
            serde_json::from_slice(&serde_json::to_vec(&progress).unwrap()).unwrap();
        assert_eq!(restored.status, progress.status);
        assert_eq!(restored.blocker, progress.blocker);
        assert!(
            serde_json::from_str::<JobProgress>(
                r#"{"status":"unknown","checked_at":1,"last_progress_at":null,"pending_work":0}"#
            )
            .is_err()
        );
    }
}
