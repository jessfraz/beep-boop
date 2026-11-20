import { createHash } from "node:crypto";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { defineDoc, defineEntry, defineTask, LiveDoc, ROOT_CONVERSATION_ID, type Harness, type Registry,
  type HarnessInspection, type NextTaskState, type Task, type TaskId as NativeTaskId, type TaskInspection, type Tx } from "@earendil-works/pi-durable";
import type { BackgroundCadence } from "./generated/BackgroundCadence.js";
import type { BackgroundCheckpoint } from "./generated/BackgroundCheckpoint.js";
import type { BackgroundSettings } from "./generated/BackgroundSettings.js";
import type { BackgroundStatus } from "./generated/BackgroundStatus.js";
import type { BackgroundTaskInput } from "./generated/BackgroundTaskInput.js";
import type { BackgroundTriggerKey } from "./generated/BackgroundTriggerKey.js";
import type { BackgroundWork } from "./generated/BackgroundWork.js";
import type { OccurrencePlan } from "./generated/OccurrencePlan.js";
import type { ScheduledJob } from "./generated/ScheduledJob.js";
import type { ScheduledOccurrence } from "./generated/ScheduledOccurrence.js";
import type { ScheduledJobStatus } from "./generated/ScheduledJobStatus.js";
import type { JobSchedule } from "./generated/JobSchedule.js";
import type { JobSyncReport } from "./generated/JobSyncReport.js";
import type { JobRunReceipt } from "./generated/JobRunReceipt.js";
import type { JobExecution } from "./generated/JobExecution.js";
import type { JobExecutionState } from "./generated/JobExecutionState.js";
import type { RuntimeCommand } from "./generated/RuntimeCommand.js";
import { TaskError, TaskStore } from "./tasks.js";
import { occurrencePrompt, scheduledTaskId } from "./scheduled.js";
import { validateInput } from "./attachments.js";
import { validRequest } from "./protocol.js";
import type { JsonValue } from "@earendil-works/pi-ai";

type Trigger = { key: BackgroundTriggerKey; generation: number; revision: string;
  task: NativeTaskId<null> | null; cursor: number | null; last: number | null; next: number | null; enabled: boolean;
  error: string | null; error_scope: "plan" | "work" | null; recovery_of?: NativeTaskId };
type QueuedWork = { key: BackgroundTriggerKey; work: BackgroundWork };
type JobPreparation = { type: "replay"; receipt: JobRunReceipt } | { type: "admit" };
type RunReference = { task: NativeTaskId<null>; request_id: string; schedule_id: string; scheduled_at: number; revision: string; actor_task_id?: string; recovery_of?: NativeTaskId };
type Lane = { id: string; active: NativeTaskId<null> | null; pending: QueuedWork[]; actor_task_id: string | null;
  active_runs?: NativeTaskId<null>[]; last_run?: RunReference };
type State = { triggers: Trigger[]; lanes: Lane[]; jobs: ScheduledJob[] };
const State = defineDoc<State>({ kind: "agent.background", scope: "session", version: 1,
  initial: () => ({ triggers: [], lanes: [], jobs: [] }), checkpointWhen: () => true });
type NativeTask = Task<BackgroundTaskInput, BackgroundCheckpoint, null, object>;
/** Native indexed admission evidence; omitted from model context. */
export const JobRunReceiptEntry = defineEntry<JobRunReceipt>("agent.job_run");

export type BackgroundCallbacks = {
  ready?(signal: AbortSignal): Promise<void>;
  plan(cadence: BackgroundCadence, after: number | null, now: number, signal: AbortSignal): Promise<OccurrencePlan>;
  refreshSources(signal: AbortSignal): Promise<void>;
  triageSources(signal: AbortSignal): Promise<void>;
  collectResources(signal: AbortSignal): Promise<void>;
  indexHistory(signal: AbortSignal): Promise<void>;
  jobActor?(job: ScheduledJob): Promise<string | undefined>;
  runJob(occurrence: ScheduledOccurrence, signal: AbortSignal, actorId: string): Promise<void>;
};

function key(value: BackgroundTriggerKey): string {
  return value.type === "builtin" ? JSON.stringify([value.type, value.name])
    : JSON.stringify([value.type, value.job_id, value.schedule_id]);
}
function laneId(value: BackgroundTriggerKey): string {
  return value.type === "builtin" ? key(value) : JSON.stringify([value.type, value.job_id]);
}
function same(left: BackgroundTriggerKey, right: BackgroundTriggerKey): boolean { return key(left) === key(right); }
function terminal(): NextTaskState<BackgroundCheckpoint, null> {
  return { status: "terminal", outcome: { status: "completed", result: null } };
}
function failure(error: unknown): string { return error instanceof Error ? error.message : "Background execution failed"; }
function terminalFailure(task: TaskInspection["record"] | undefined): string | undefined {
  if (task?.state.status !== "terminal") return;
  const outcome = task.state.outcome;
  return outcome.status === "failed" || outcome.status === "faulted" ? outcome.error.message
    : outcome.status === "orphaned" ? outcome.reason : undefined;
}
function object(value: JsonValue | undefined): value is { [key: string]: JsonValue } {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}
function retainedJobWork(value: JsonValue): QueuedWork | undefined {
  if (!object(value) || value.type !== "work" || !object(value.key) || value.key.type !== "job" ||
      typeof value.key.job_id !== "string" || typeof value.key.schedule_id !== "string" || !object(value.work) ||
      value.work.type !== "job" && value.work.type !== "manual_job" || !object(value.work.occurrence)) return;
  const occurrence = value.work.occurrence;
  if (typeof occurrence.id !== "string" || typeof occurrence.schedule_id !== "string" ||
      typeof occurrence.scheduled_at !== "number" || !Number.isSafeInteger(occurrence.scheduled_at)) return;
  const candidate: unknown = { version: 1, id: 0, command: { type: "sync_jobs", jobs: [occurrence.job] } };
  if (!validRequest(candidate) || candidate.command.type !== "sync_jobs") return;
  const job = candidate.command.jobs[0];
  if (job === undefined || job.id !== value.key.job_id || occurrence.schedule_id !== value.key.schedule_id ||
      !job.schedules.some(schedule => schedule.id === occurrence.schedule_id)) return;
  if (occurrence.task_id !== undefined && typeof occurrence.task_id !== "string") return;
  if (occurrence.manual !== undefined && (!object(occurrence.manual) ||
      occurrence.manual.input !== undefined && typeof occurrence.manual.input !== "string")) return;
  return { key: { type: "job", job_id: job.id, schedule_id: occurrence.schedule_id },
    work: { type: value.work.type, occurrence: { id: occurrence.id, job, schedule_id: occurrence.schedule_id, scheduled_at: occurrence.scheduled_at,
      ...(typeof occurrence.task_id === "string" ? { task_id: occurrence.task_id } : {}),
      ...(object(occurrence.manual) ? { manual: typeof occurrence.manual.input === "string" ? { input: occurrence.manual.input } : {} } : {}) } } };
}
function occurrenceId(value: BackgroundTriggerKey, scheduledAt: number): string {
  // A configuration revision diagnoses a run, but never creates a second occurrence.
  return createHash("sha256").update(JSON.stringify([key(value), scheduledAt])).digest("hex");
}
function revision(job: ScheduledJob, schedule: JobSchedule): string {
  return createHash("sha256").update(JSON.stringify([job.id, job.title, job.prompt,
    job.model.provider, job.model.model_id, job.model.reasoning, job.model.fast, job.timezone, job.cwd,
    schedule.id, schedule.recurrence, schedule.focus, schedule.prompt,
    ...(job.priority === "critical" ? [["priority", job.priority]] : [])])).digest("hex");
}
function selectedSchedule(job: ScheduledJob, request: Extract<RuntimeCommand, { type: "run_job" }>): JobSchedule {
  const neutral = job.schedules.filter(schedule => schedule.focus === null && schedule.prompt === null);
  const schedule = request.schedule_id === null
    ? job.schedules.length === 1 ? job.schedules[0] : neutral.find(schedule => schedule.id === "main") ?? neutral[0]
    : job.schedules.find((schedule) => schedule.id === request.schedule_id);
  if (schedule === undefined) throw new TaskError(request.schedule_id === null
    ? `Choose a schedule for ${job.id}: ${job.schedules.map((schedule) => schedule.id).join(", ")}`
    : `Unknown schedule for ${job.id}; choose ${job.schedules.map((schedule) => schedule.id).join(", ")}`);
  return schedule;
}

/** Sources and jobs share this single native Pi state machine and its durable timer. */
export class BackgroundScheduler {
  private readonly task: NativeTask;
  constructor(private readonly session: Harness, registry: Registry, private readonly tasks: TaskStore, private readonly callbacks: BackgroundCallbacks) {
    this.task = defineTask<BackgroundTaskInput, BackgroundCheckpoint, null>({
      name: "agent.background", version: 1,
      initial: (input) => input.type === "trigger" ? { phase: "tick", cursor: null } : { phase: "execute", attempt: 0 },
      phases: {
        tick: async (task, runtime, context) => {
          if (task.input.type !== "trigger") throw new Error("Background timer has execution input");
          await callbacks.ready?.(runtime.signal);
          runtime.signal.throwIfAborted();
          const input = task.input;
          const savedCursor = (await session.snapshot(State, context))?.triggers.find(item => same(item.key, input.key))?.cursor;
          const cursor = task.state.checkpoint.cursor ?? savedCursor ?? null, observedAt = runtime.now();
          let plan: OccurrencePlan;
          try { plan = await callbacks.plan(input.cadence, cursor, observedAt, runtime.signal); }
          catch (error) {
            await runtime.commit(async (tx) => {
              const state = await tx.doc(State);
              const saved = state.triggers.find((trigger) => same(trigger.key, input.key));
              if (saved === undefined || !saved.enabled || saved.generation !== input.generation) return terminal();
              saved.error = failure(error); saved.error_scope = "plan"; saved.next = runtime.now() + 60_000;
              return { status: "sleeping", until: saved.next, checkpoint: { phase: "tick", cursor } };
            }, context);
            return;
          }
          await runtime.commit(async (tx) => {
            const state = await tx.doc(State);
            const saved = state.triggers.find((trigger) => same(trigger.key, input.key));
            if (saved === undefined || !saved.enabled || saved.generation !== input.generation || saved.cursor !== cursor) return terminal();
            if (saved.error_scope === "plan") { saved.error = null; saved.error_scope = null; }
            const due = plan.due;
            if (due !== null && (saved.cursor === null || due > saved.cursor)) {
              let work: BackgroundWork;
              if (input.key.type === "builtin") work = { type: input.key.name };
              else {
                if (input.job === null) throw new Error("A job timer has no reviewed definition");
                const id = occurrenceId(input.key, due);
                work = { type: "job", occurrence: { id, job: input.job,
                  task_id: scheduledTaskId(id), schedule_id: input.key.schedule_id, scheduled_at: due } };
              }
              await this.enqueueIn(tx, state, { key: input.key, work });
              saved.last = due;
            }
            const nextCursor = due ?? cursor ?? observedAt;
            saved.cursor = nextCursor; saved.next = plan.next;
            if (plan.next === null) return terminal();
            if (plan.next <= observedAt) throw new Error("Background planner returned a deadline that is already due");
            return { status: "sleeping", until: plan.next, checkpoint: { phase: "tick", cursor: nextCursor } };
          }, context);
        },
        execute: async (task, runtime, context) => {
          if (task.input.type !== "work") throw new Error("Background execution has timer input");
          await callbacks.ready?.(runtime.signal);
          runtime.signal.throwIfAborted();
          const input = task.input;
          try {
            switch (input.work.type) {
              case "sources": await callbacks.refreshSources(runtime.signal); break;
              case "source_triage": await callbacks.triageSources(runtime.signal); break;
              case "resources": await callbacks.collectResources(runtime.signal); break;
              case "history": await callbacks.indexHistory(runtime.signal); break;
              case "job":
              case "manual_job": {
                const occurrence = input.work.occurrence;
                let actorId = await runtime.memo<string>("job_actor_task_id", context);
                if (actorId === undefined) {
                  // Work admitted before actor reuse still owns its original task.
                  const legacy = await this.session.commit(async (tx) =>
                    (await this.tasks.findIn(tx, scheduledTaskId(occurrence.id)))?.id, context);
                  const candidate = occurrence.task_id ?? legacy ?? await this.actorCandidate(occurrence.job);
                  const created = await this.session.commit(async (tx) => {
                    const state = await tx.doc(State);
                    const created = occurrence.task_id === undefined
                      ? await this.actorIn(tx, state, occurrence.job, candidate, legacy !== undefined)
                      : await this.freshActorIn(tx, state, occurrence);
                    const reference = state.lanes.find((lane) => lane.id === laneId(input.key))?.last_run;
                    if (reference?.task === runtime.taskId) reference.actor_task_id = created.id;
                    return created;
                  }, context);
                  if (created.native !== undefined) this.tasks.rememberConversation(created.native);
                  actorId = await runtime.memo("job_actor_task_id", created.id, context);
                }
                await callbacks.runJob(occurrence, runtime.signal, actorId);
                break;
              }
            }
          } catch (error) {
            if (runtime.signal.aborted) throw error;
            await runtime.commit(async (tx) => {
              const state = await tx.doc(State), saved = state.triggers.find((trigger) => same(trigger.key, input.key));
              if (saved !== undefined) { saved.error = failure(error); saved.error_scope = "work"; }
              // Retain the same admitted work. Its consumer reconciles its stable request identity.
              return { status: "sleeping", until: runtime.now() + Math.min(3_600_000, 60_000 * 2 ** Math.min(task.state.checkpoint.attempt, 6)),
                checkpoint: { phase: "execute", attempt: task.state.checkpoint.attempt + 1 } };
            }, context);
            return;
          }
          await runtime.commit(async (tx) => {
            const state = await tx.doc(State), lane = state.lanes.find((item) => item.id === laneId(input.key));
            // Polling and model judgment have separate durable lanes. Admit
            // judgment after a poll without holding the next source read open.
            if (input.work.type === "sources") await this.enqueueIn(tx, state, {
              key: { type: "builtin", name: "source_triage" }, work: { type: "source_triage" },
            });
            if (lane?.active === runtime.taskId) {
              lane.active = null;
              const pending = lane.pending.shift();
              if (pending !== undefined) lane.active = await this.createWorkIn(tx, pending);
            }
            if (lane?.active_runs !== undefined) lane.active_runs = lane.active_runs.filter(id => id !== runtime.taskId);
            const saved = state.triggers.find((trigger) => same(trigger.key, input.key));
            if (saved?.error_scope === "work") { saved.error = null; saved.error_scope = null; }
            return terminal();
          }, context);
        },
      },
      abort: async (_task, runtime, context) => { await runtime.commit(() => ({ status: "terminal", outcome: { status: "aborted" } }), context); },
    });
    registry.install({ name: "background", tasks: [this.task] });
  }
  private async createWorkIn(tx: Tx, work: QueuedWork): Promise<NativeTaskId<null>> {
    // Work is conversation-owned, so replacing its timer cannot abort an admitted run.
    const task = await tx.createTask(this.task, { type: "work", ...work }, {
      ownership: { kind: "conversation" }, conversationId: ROOT_CONVERSATION_ID, background: true,
    });
    if (work.work.type === "job" || work.work.type === "manual_job") {
      const occurrence = work.work.occurrence;
      // Retain only a lookup reference. Pi remains authoritative for timing,
      // lifecycle and the exact submission's answer, including after reopen.
      this.laneIn(await tx.doc(State), laneId(work.key)).last_run = {
        task, request_id: occurrence.id, schedule_id: occurrence.schedule_id,
        scheduled_at: occurrence.scheduled_at, revision: occurrence.job.revision,
        ...(occurrence.task_id === undefined ? {} : { actor_task_id: occurrence.task_id }),
      };
    }
    return task;
  }
  private async enqueueIn(tx: Tx, state: State, work: QueuedWork): Promise<void> {
    const lane = this.laneIn(state, laneId(work.key));
    if ((work.work.type === "job" || work.work.type === "manual_job") && work.work.occurrence.task_id !== undefined) {
      await this.freshActorIn(tx, state, work.work.occurrence);
      const native = await this.createWorkIn(tx, work);
      const current = this.laneIn(state, laneId(work.key));
      current.active_runs ??= [];
      current.active_runs.push(native);
      return;
    }
    if (lane.active === null) { lane.active = await this.createWorkIn(tx, work); return; }
    if (work.work.type === "manual_job") { lane.pending.push(work); return; }
    const previous = lane.pending.findIndex((item) => item.work.type !== "manual_job" && same(item.key, work.key));
    if (previous < 0) lane.pending.push(work); else lane.pending[previous] = work;
  }
  private laneIn(state: State, identity: string): Lane {
    if (!state.lanes.some((item) => item.id === identity)) state.lanes.push({ id: identity, active: null, pending: [], actor_task_id: null });
    // Reacquire the committed document's draft after insertion. Mutating the
    // plain object passed to push would bypass Chord's tracked update.
    const lane = state.lanes.find((item) => item.id === identity);
    if (lane === undefined) throw new Error("Background admission lost its serial lane");
    lane.actor_task_id ??= null;
    return lane;
  }
  private async freshActorIn(tx: Tx, state: State, occurrence: ScheduledOccurrence) {
    if (occurrence.task_id === undefined || occurrence.task_id !== scheduledTaskId(occurrence.id)) {
      throw new TaskError("A new occurrence must retain its exact fresh task identity");
    }
    const job = occurrence.job, lane = this.laneIn(state, JSON.stringify(["job", job.id]));
    const refs = [{ provider: "scheduled_jobs", account: "local", thread_id: job.id }];
    const owners = lane.actor_task_id === null ? [...new Set((await this.tasks.sourceLinksIn(tx, refs, 2)).map(link => link.task_id))] : [];
    if (owners.length > 1) throw new TaskError("The canonical workflow owner is ambiguous");
    const owner = await this.actorIn(tx, state, job, lane.actor_task_id ?? owners[0] ?? scheduledTaskId("job:" + job.id));
    return this.tasks.createIn(tx, { id: occurrence.task_id, title: job.title, outcome: job.prompt, model: job.model,
      execution: "coordinator", workflow: { job, owner_task_id: owner.id, work_key: "occurrence:" + occurrence.id } });
  }
  private async actorCandidate(job: ScheduledJob): Promise<string> {
    const lane = (await this.session.snapshot(State, BACKGROUND_CONTEXT))?.lanes.find((item) => item.id === JSON.stringify(["job", job.id]));
    return lane?.actor_task_id ?? await this.callbacks.jobActor?.(job) ?? scheduledTaskId("job:" + job.id);
  }
  private async actorIn(tx: Tx, state: State, job: ScheduledJob, candidate: string, legacy = false) {
    const lane = this.laneIn(state, JSON.stringify(["job", job.id]));
    const id = legacy ? candidate : lane.actor_task_id ?? candidate;
    const previous = await this.tasks.findIn(tx, id);
    if (previous !== undefined && !previous.sources.some((source) => source.provider === "scheduled_jobs" &&
        source.account === "local" && source.thread_id === job.id)) throw new TaskError("This task is not owned by the scheduled job");
    if (previous === undefined && id !== scheduledTaskId("job:" + job.id)) throw new TaskError("The scheduled job's owned actor is unavailable; reconcile its existing task");
    const created = await this.tasks.createIn(tx, { id, title: job.title, outcome: job.prompt, model: job.model,
      sources: [{ provider: "scheduled_jobs", account: "local", thread_id: job.id }] });
    lane.actor_task_id ??= created.id;
    return created;
  }
  private async receiptIn(tx: Tx, request: Extract<RuntimeCommand, { type: "run_job" }>): Promise<JobRunReceipt | undefined> {
    const saved = await tx.submissionByRequest(ROOT_CONVERSATION_ID, "job-run:" + request.request_id);
    let receipt: JobRunReceipt | undefined;
    if (saved !== undefined) {
      if (saved.type !== "write" || saved.status !== "done") throw new TaskError("The original job run admission needs reconciliation");
      receipt = (await tx.entry(JobRunReceiptEntry, saved.entry))?.data;
      if (receipt === undefined) throw new TaskError("The original job run receipt is unavailable");
    } else {
      const previous = await this.tasks.findIn(tx, scheduledTaskId("manual:" + request.request_id));
      if (previous === undefined) return undefined;
      receipt = previous.scheduled_run;
    }
    if (receipt === undefined || receipt.request_id !== request.request_id || receipt.job_id !== request.job_id ||
        (request.schedule_id !== null && receipt.schedule_id !== request.schedule_id) || receipt.manual?.input !== request.input) {
      throw new TaskError("This job run request identity already belongs to another selection");
    }
    return { ...receipt };
  }
  async start(settings: BackgroundSettings): Promise<BackgroundStatus> {
    await this.session.root(BACKGROUND_CONTEXT);
    await this.builtins(settings);
    if (settings.jobs !== null) await this.syncJobs(settings.jobs);
    this.session.resume();
    return this.status();
  }
  async runJob(request: Extract<RuntimeCommand, { type: "run_job" }>): Promise<JobRunReceipt> {
    if (request.request_id.trim().length === 0 || request.request_id.length > 128) throw new TaskError("A job run needs a stable request identity");
    if (request.input !== undefined && (request.input.length > 16000 || request.input.includes("\0"))) throw new TaskError("A requested job input must be bounded text");
    const occurrenceId = "manual:" + request.request_id;
    await this.session.root(BACKGROUND_CONTEXT);
    const prepared = await this.session.commit(async (tx): Promise<JobPreparation> => {
      const receipt = await this.receiptIn(tx, request);
      if (receipt !== undefined) return { type: "replay", receipt };
      const job = (await tx.doc(State)).jobs.find((job) => job.id === request.job_id);
      if (job === undefined) throw new TaskError("Unknown scheduled job; list jobs to choose its readable ID");
      selectedSchedule(job, request);
      return { type: "admit" };
    }, BACKGROUND_CONTEXT);
    if (prepared.type === "replay") { this.session.resume(); return prepared.receipt; }
    const result = await this.session.commit(async (tx) => {
      // Replay must work after the source file was edited or removed.
      const previous = await this.receiptIn(tx, request);
      if (previous !== undefined) return { receipt: previous, native: undefined };
      const state = await tx.doc(State), job = state.jobs.find((job) => job.id === request.job_id);
      if (job === undefined) throw new TaskError("Unknown scheduled job; list jobs to choose its readable ID");
      const schedule = selectedSchedule(job, request);
      const admitted_at = Date.now(), manual = request.input === undefined ? {} : { input: request.input };
      const occurrence: ScheduledOccurrence = { id: occurrenceId, job, task_id: scheduledTaskId(occurrenceId),
        schedule_id: schedule.id, scheduled_at: admitted_at, manual };
      validateInput(occurrencePrompt(occurrence));
      const created = await this.freshActorIn(tx, state, occurrence);
      const receipt: JobRunReceipt = { request_id: request.request_id, task_id: created.id, job_id: job.id,
        schedule_id: schedule.id, revision: job.revision, admitted_at, manual };
      await this.tasks.updateIn(tx, created.id, task => { task.scheduled_run = receipt; });
      // This passive metadata write and queue admission share the native commit.
      const entry = await tx.appendEntry(JobRunReceiptEntry, ROOT_CONVERSATION_ID, { data: receipt });
      await tx.createSubmission({ type: "write", status: "done", conversationId: ROOT_CONVERSATION_ID,
        requestId: "job-run:" + request.request_id, entry: entry.id });
      await this.enqueueIn(tx, state, { key: { type: "job", job_id: job.id, schedule_id: schedule.id },
        work: { type: "manual_job", occurrence } });
      return { receipt, native: created.native };
    }, BACKGROUND_CONTEXT);
    if (result.native !== undefined) this.tasks.rememberConversation(result.native);
    this.session.resume();
    return result.receipt;
  }
  private async replaceIn(tx: Tx, state: State, key: BackgroundTriggerKey, revision: string,
    cadence: BackgroundCadence, job: ScheduledJob | null): Promise<NativeTaskId<null> | null> {
    const previous = state.triggers.find((item) => same(item.key, key));
    if (previous?.enabled && previous.revision === revision) return null;
    const old = previous?.task ?? null;
    const saved: Trigger = { key, generation: (previous?.generation ?? 0) + 1, revision,
      task: null, cursor: null, last: previous?.last ?? null, next: null, enabled: true, error: null, error_scope: null };
    saved.task = await tx.createTask(this.task, { type: "trigger", key, generation: saved.generation, cadence, job }, {
      ownership: { kind: "conversation" }, conversationId: ROOT_CONVERSATION_ID, background: true,
    });
    if (previous === undefined) state.triggers.push(saved); else Object.assign(previous, saved);
    return old;
  }
  private async builtins(settings: BackgroundSettings): Promise<void> {
    const old = await this.session.commit(async (tx) => {
      const state = await tx.doc(State), retired: NativeTaskId<null>[] = [];
      const names: Extract<BackgroundTriggerKey, { type: "builtin" }>["name"][] = ["sources", "source_triage", "resources", "history"];
      for (const name of names) {
        const seconds = name === "resources" ? 300 : settings.poll_interval_seconds;
        // Both source callbacks own --no-sources semantics. The judgment timer
        // also resumes admitted work and due checks when polling cannot finish.
        const replaced = await this.replaceIn(tx, state, { type: "builtin", name }, JSON.stringify([seconds, settings.sources]),
          { type: "interval", seconds }, null);
        if (replaced !== null) retired.push(replaced);
      }
      return retired;
    }, BACKGROUND_CONTEXT);
    for (const id of old) await this.session.abortTask(id, BACKGROUND_CONTEXT);
  }
  async syncJobs(jobs: ScheduledJob[]): Promise<JobSyncReport> {
    const desired = new Set<string>(), ids = new Set<string>();
    for (const job of jobs) {
      if (ids.has(job.id)) throw new Error("Duplicate scheduled job identity");
      ids.add(job.id);
      for (const schedule of job.schedules) {
        const identity = key({ type: "job", job_id: job.id, schedule_id: schedule.id });
        if (desired.has(identity)) throw new Error("Duplicate scheduled job identity");
        desired.add(identity);
      }
    }
    await this.session.root(BACKGROUND_CONTEXT);
    const result = await this.session.commit(async (tx) => {
      const state = await tx.doc(State), old: NativeTaskId<null>[] = [];
      const before = state.jobs ?? [];
      const report: JobSyncReport = { added: 0, updated: 0, removed: before.filter((job) => !ids.has(job.id)).length, unchanged: 0 };
      for (const job of jobs) {
        const previous = before.find((saved) => saved.id === job.id);
        if (previous === undefined) report.added += 1;
        else if (previous.revision === job.revision) report.unchanged += 1; else report.updated += 1;
      }
      for (const trigger of state.triggers) {
        if (trigger.key.type !== "job" || !trigger.enabled || desired.has(key(trigger.key))) continue;
        trigger.enabled = false; trigger.generation += 1; trigger.next = null;
        if (trigger.task !== null) old.push(trigger.task);
        trigger.task = null;
      }
      for (const job of jobs) for (const schedule of job.schedules) {
        const identity: BackgroundTriggerKey = { type: "job", job_id: job.id, schedule_id: schedule.id };
        const replaced = await this.replaceIn(tx, state, identity, revision(job, schedule),
          { type: "calendar", schedule, timezone: job.timezone }, job);
        if (replaced !== null) old.push(replaced);
      }
      state.jobs = jobs;
      return { old, report };
    }, BACKGROUND_CONTEXT);
    // The generation commit fences old invocations before their asynchronous abort handlers run.
    for (const id of result.old) await this.session.abortTask(id, BACKGROUND_CONTEXT);
    await this.tasks.monitorJobs(jobs);
    return result.report;
  }
  async status(): Promise<BackgroundStatus> {
    const state = await this.session.snapshot(State, BACKGROUND_CONTEXT);
    const triggers = state?.triggers.filter((trigger) => trigger.enabled) ?? [];
    const errors = await this.errors(state);
    return { jobs: new Set(triggers.flatMap((trigger) => trigger.key.type === "job" ? [trigger.key.job_id] : [])).size,
      triggers: triggers.length, pending: state?.lanes.reduce((count, lane) => count + lane.pending.length +
        Number(lane.active !== null) + (lane.active_runs?.length ?? 0), 0) ?? 0,
      errors: (state?.triggers ?? []).flatMap((trigger) => {
        const message = errors.get(key(trigger.key));
        return message === undefined ? [] : [{ key: trigger.key, message }];
      }) };
  }
  private async errors(state: State | undefined, inspected?: HarnessInspection): Promise<Map<string, string>> {
    const inspection = inspected ?? await this.session.inspect(BACKGROUND_CONTEXT);
    const running = new Set(inspection.tasks.filter(task => task.record.kind === "agent.background" && task.state.kind === "running")
      .map(task => task.record.id));
    const blocked = new Map<NativeTaskId, Extract<TaskInspection["state"], { kind: "blocked" }>>();
    for (const task of inspection.tasks) if (task.record.kind === "agent.background" && task.state.kind === "blocked") {
      blocked.set(task.record.id, task.state);
    }
    const candidates = new Set<NativeTaskId>([
      ...(state?.triggers.flatMap(trigger => trigger.task === null ? [] : [trigger.task]) ?? []),
      ...(state?.lanes.flatMap(lane => [...(lane.active === null ? [] : [lane.active]), ...(lane.active_runs ?? [])]) ?? []),
    ]);
    const failures = await this.session.commit(async tx => {
      const entries = await Promise.all([...candidates].map(async id => ({ id, error: terminalFailure(await tx.task(id)) })));
      const errors = new Map<NativeTaskId, string>();
      for (const entry of entries) if (entry.error !== undefined) errors.set(entry.id, entry.error);
      return errors;
    }, BACKGROUND_CONTEXT);
    const errors = new Map<string, string>();
    for (const trigger of state?.triggers ?? []) {
      const lane = state?.lanes.find((item) => item.id === laneId(trigger.key));
      const error = trigger.task === null ? undefined : blocked.get(trigger.task);
      const workError = lane?.active == null ? (lane?.active_runs ?? []).map(id => blocked.get(id)).find(item => item !== undefined) : blocked.get(lane.active);
      const paused = workError ?? error;
      // Keep the saved attempt's diagnostic, but do not publish it as current
      // while that exact work is executing. Job lanes are shared by schedules.
      const reference = lane?.last_run;
      const executing = trigger.key.type === "builtin" ? lane?.active != null && running.has(lane.active)
        : reference !== undefined && running.has(reference.task) && reference.schedule_id === trigger.key.schedule_id;
      let message = trigger.error_scope === "work" && executing ? null : trigger.error;
      const failedWork = [lane?.active, ...(lane?.active_runs ?? [])].find(id => id != null && failures.has(id));
      const terminal = failedWork == null ? trigger.task === null ? undefined : failures.get(trigger.task) : failures.get(failedWork);
      if (terminal !== undefined) message = terminal;
      if (paused !== undefined) {
        message = `Background work is blocked by its native task definition (${paused.reason})`;
      }
      if (message !== null && message !== undefined) errors.set(key(trigger.key), message);
    }
    return errors;
  }
  async listJobs(inspected?: HarnessInspection): Promise<ScheduledJobStatus[]> {
    const state = await this.session.snapshot(State, BACKGROUND_CONTEXT);
    const errors = await this.errors(state, inspected);
    return Promise.all((state?.jobs ?? []).map(async (job) => {
      const task_id = state?.lanes.find((lane) => lane.id === JSON.stringify(["job", job.id]))?.actor_task_id;
      return { job: structuredClone(job), ...(task_id === null || task_id === undefined ? {} : { task_id }),
      progress: await this.tasks.workflowProgress(job.id),
      schedules: job.schedules.map((schedule) => {
      const trigger = state?.triggers.find((item) => item.key.type === "job" && item.key.job_id === job.id && item.key.schedule_id === schedule.id);
      return { schedule_id: schedule.id, next_at: trigger?.next ?? null, last_at: trigger?.last ?? null,
        error: trigger === undefined ? null : errors.get(key(trigger.key)) ?? null };
    }) };
    }));
  }
  /** The current root generation must own, or block, this lane's admitted
   * input. A saved actor alone does not authorize retrying other work. */
  private async actorGeneration(id: string, lane: Lane | undefined, inspection: HarnessInspection): Promise<{ generation: TaskInspection; schedule_id: string; actor_id: string } | undefined> {
    const active = lane?.last_run !== undefined && lane.active_runs?.includes(lane.last_run.task) ? lane.last_run.task : lane?.active;
    if (active == null) return;
    const input = inspection.tasks.find(task => task.record.id === active && task.record.kind === "agent.background")?.record.input;
    if (input === null || typeof input !== "object" || Array.isArray(input) || input.type !== "work") return;
    const key = input.key, work = input.work;
    if (key === null || typeof key !== "object" || Array.isArray(key) || key.type !== "job" || key.job_id !== id || typeof key.schedule_id !== "string" ||
      work === null || typeof work !== "object" || Array.isArray(work) || (work.type !== "job" && work.type !== "manual_job")) return;
    const occurrence = work.occurrence;
    if (occurrence === null || typeof occurrence !== "object" || Array.isArray(occurrence) || typeof occurrence.id !== "string") return;
    const requestId = occurrence.id;
    const actorId = (lane?.last_run?.task === active ? lane.last_run.actor_task_id : undefined) ?? lane?.actor_task_id;
    if (actorId === null || actorId === undefined) return;
    const conversationId = await this.session.commit(async tx => {
      const actor = await this.tasks.findIn(tx, actorId);
      return actor !== undefined && (actor.workflow?.job.id === id || actor.sources.some(source => source.provider === "scheduled_jobs" && source.account === "local" && source.thread_id === id))
        ? actor.conversation_id : undefined;
    }, BACKGROUND_CONTEXT);
    if (conversationId === undefined) return;
    const conversation = await this.tasks.conversation(conversationId);
    const run = (await this.session.snapshot(LiveDoc, conversation.id, BACKGROUND_CONTEXT))?.run;
    if (run === undefined) return;
    const current = await this.session.commit(async tx => {
      const submission = await tx.submissionByRequest(conversation.id, requestId);
      return submission?.type === "input" && (submission.status === "queued" ||
        submission.status === "placed" && run.inputs.includes(submission.id)) ? run.taskId : undefined;
    }, BACKGROUND_CONTEXT);
    const generation = inspection.tasks.find(task => task.record.id === current && task.record.conversationId === conversation.id &&
      task.record.kind === "pi.generation" && task.record.owner === undefined && !task.record.background);
    return generation === undefined ? undefined : { generation, schedule_id: key.schedule_id, actor_id: actorId };
  }
  async getJob(id: string): Promise<ScheduledJobStatus> {
    const inspection = await this.session.inspect(BACKGROUND_CONTEXT);
    let job = (await this.listJobs(inspection)).find((item) => item.job.id === id);
    if (job === undefined) throw new Error("Unknown scheduled job");
    const state = await this.session.snapshot(State, BACKGROUND_CONTEXT);
    const lane = state?.lanes.find((item) => item.id === JSON.stringify(["job", id]));
    const reference = lane?.last_run;
    const actorWork = await this.actorGeneration(id, lane, inspection);
    const blocked = actorWork?.generation.state.kind === "blocked" ? actorWork.generation.state.reason : undefined;
    if (blocked !== undefined) {
      const message = `Job model generation is blocked by its native task definition (${blocked})`;
      job = { ...job, schedules: job.schedules.map(schedule => schedule.schedule_id === actorWork?.schedule_id ? { ...schedule, error: message } : schedule) };
    }
    // Older state has no reference; never invent execution times from an
    // admission timestamp or present another occurrence's summary as its output.
    if (reference === undefined) return job;
    const live = inspection.tasks.find((item) => item.record.id === reference.task);
    const record = live?.record ?? await this.session.getTask(reference.task, BACKGROUND_CONTEXT);
    if (record === undefined) return job;
    const trigger = state?.triggers.find((item) => item.key.type === "job" && item.key.job_id === id && item.key.schedule_id === reference.schedule_id);
    let execution: JobExecutionState;
    if (blocked !== undefined && (lane?.active === reference.task || lane?.active_runs?.includes(reference.task))) execution = { status: "blocked", message: blocked };
    else if (live?.state.kind === "blocked") execution = { status: "blocked", message: live.state.reason };
    else if (live?.state.kind === "pending" || record.state.status === "pending") execution = { status: "queued" };
    else switch (record.state.status) {
      case "running": execution = { status: "running" }; break;
      case "waiting":
      case "completing": execution = { status: "waiting" }; break;
      case "sleeping": execution = record.state.until === undefined ? { status: "waiting" } : { status: "retrying", at: record.state.until,
        message: (lane?.active === reference.task || lane?.active_runs?.includes(reference.task)) && trigger?.error_scope === "work" ? trigger.error : null }; break;
      case "terminal": {
        const outcome = record.state.outcome;
        execution = outcome.status === "completed" ? { status: "completed" }
          : outcome.status === "failed" || outcome.status === "faulted" ? { status: "failed", message: outcome.error.message }
          : outcome.status === "orphaned" ? { status: "blocked", message: outcome.reason ?? "Task definition unavailable" }
          : { status: "stopped" };
        break;
      }
    }
    const actor = reference.actor_task_id === undefined ? undefined : await this.tasks.get(reference.actor_task_id);
    let output: string | null = null;
    if (actor !== undefined) {
      const conversation = await this.tasks.conversation(actor.conversation_id);
      const answer = await this.session.commit(async (tx) => {
        const submission = await tx.submissionByRequest(conversation.id, reference.request_id);
        return submission?.type === "input" && submission.status === "done" ? tx.entry(submission.answer) : undefined;
      }, BACKGROUND_CONTEXT);
      if (answer !== undefined) {
        output = answer.model?.flatMap((message) => message.role !== "assistant" ? []
          : typeof message.content === "string" ? [message.content]
            : message.content.flatMap((part) => part.type === "text" ? [part.text] : [])).join("\n") ?? null;
      }
    }
    const characters = output === null ? [] : Array.from(output);
    const last_run: JobExecution = { schedule_id: reference.schedule_id, scheduled_at: reference.scheduled_at,
      started_at: record.startedAt ?? null, finished_at: record.endedAt ?? null, revision: reference.revision,
      state: execution, task_id: actor?.id ?? null, output: output === null ? null : characters.slice(0, 16000).join(""),
      output_truncated: characters.length > 16000 };
    return { ...job, last_run };
  }
  /** Native terminal receipts are immutable. Re-admit only the selected
   * domain timer, preserving its committed cursor and accepted work. */
  private async retryTimers(select: (trigger: Trigger) => boolean): Promise<boolean> {
    return this.session.commit(async tx => {
      const state = await tx.doc(State), selected = state.triggers.filter(item => item.enabled && select(item));
      const records = new Map<NativeTaskId, Awaited<ReturnType<Tx["task"]>>>();
      await Promise.all(selected.map(async trigger => {
        if (trigger.task !== null) records.set(trigger.task, await tx.task(trigger.task));
      }));
      let recovered = selected.some(trigger => trigger.recovery_of !== undefined);
      for (const trigger of selected) {
        const previous = trigger.task === null ? undefined : records.get(trigger.task);
        if (previous === undefined || previous.kind !== "agent.background" || previous.state.status !== "terminal" ||
            previous.state.outcome.status === "completed" || previous.state.outcome.status === "aborted") continue;
        let cadence: BackgroundCadence, job: ScheduledJob | null = null;
        if (trigger.key.type === "job") {
          const jobId = trigger.key.job_id, scheduleId = trigger.key.schedule_id;
          job = state.jobs.find(item => item.id === jobId) ?? null;
          const schedule = job?.schedules.find(item => item.id === scheduleId);
          if (job === null || schedule === undefined) throw new TaskError("The interrupted timer's applied job is unavailable");
          cadence = { type: "calendar", schedule, timezone: job.timezone };
        } else {
          const input = previous.input;
          if (!object(input) || input.type !== "trigger" || !object(input.cadence) || input.cadence.type !== "interval" ||
              typeof input.cadence.seconds !== "number" || !Number.isSafeInteger(input.cadence.seconds) || input.cadence.seconds < 1) {
            throw new TaskError("The interrupted timer's interval is unavailable");
          }
          cadence = { type: "interval", seconds: input.cadence.seconds };
        }
        trigger.generation++; trigger.recovery_of ??= previous.id;
        trigger.task = await tx.createTask(this.task, { type: "trigger", key: trigger.key, generation: trigger.generation, cadence, job },
          { ownership: { kind: "conversation" }, conversationId: ROOT_CONVERSATION_ID, background: true });
        recovered = true;
      }
      return recovered;
    }, BACKGROUND_CONTEXT);
  }
  /** Continue one exact occurrence through a new, idempotent input. Native
   * tool outcomes stay settled and the owning conversation retains its effects. */
  async retryJob(id: string): Promise<ScheduledJobStatus> {
    const state = await this.session.snapshot(State, BACKGROUND_CONTEXT);
    if (!state?.jobs.some(job => job.id === id)) throw new TaskError("Unknown scheduled job");
    let recovered = await this.retryTimers(trigger => trigger.key.type === "job" && trigger.key.job_id === id);
    const lane = state.lanes.find(item => item.id === JSON.stringify(["job", id]));
    const legacy = lane?.last_run === undefined && lane?.active != null ? await this.session.getTask(lane.active, BACKGROUND_CONTEXT) : undefined;
    const legacyWork = legacy?.kind === "agent.background" ? retainedJobWork(legacy.input) : undefined;
    const legacyOccurrence = legacyWork?.work.type === "job" || legacyWork?.work.type === "manual_job" ? legacyWork.work.occurrence : undefined;
    const reference: RunReference | undefined = lane?.last_run ?? (lane?.active != null && legacyOccurrence !== undefined ? {
      task: lane.active, request_id: legacyOccurrence.id, schedule_id: legacyOccurrence.schedule_id, scheduled_at: legacyOccurrence.scheduled_at,
      revision: legacyOccurrence.job.revision, ...(lane.actor_task_id === null ? {} : { actor_task_id: lane.actor_task_id }),
    } : undefined);
    if (reference !== undefined) {
      const previous = await this.session.getTask(reference.task, BACKGROUND_CONTEXT);
      const work = previous?.kind === "agent.background" ? retainedJobWork(previous.input) : undefined;
      const actorId = reference.actor_task_id ?? lane?.actor_task_id;
      const actor = actorId === undefined || actorId === null ? undefined : await this.tasks.get(actorId);
      const conversation = actor === undefined ? undefined : await this.tasks.conversation(actor.conversation_id);
      const original = conversation === undefined ? undefined : await this.session.commit(tx =>
        tx.submissionByRequest(conversation.id, reference.request_id), BACKGROUND_CONTEXT);
      const interrupted = original?.type === "input" && original.status === "unanswered";
      const terminalWork = previous?.state.status === "terminal" && previous.state.outcome.status !== "completed" && previous.state.outcome.status !== "aborted";
      if (work !== undefined && (interrupted || terminalWork)) {
        if ((work.work.type !== "job" && work.work.type !== "manual_job") || work.work.occurrence.id !== reference.request_id ||
            work.key.type !== "job" || work.key.job_id !== id) throw new TaskError("The interrupted job's ownership changed");
        if (interrupted && actor !== undefined && conversation !== undefined) {
          if (actor.workflow?.job.id !== id && !actor.sources.some(source => source.provider === "scheduled_jobs" && source.account === "local" && source.thread_id === id)) {
            throw new TaskError("The interrupted input is not owned by this job");
          }
          const recoveryId = "scheduled-recovery:" + original.id;
          const recovery = await this.session.commit(tx => tx.submissionByRequest(conversation.id, recoveryId), BACKGROUND_CONTEXT);
          if (recovery === undefined && !actor.pending.some(input => input.id === recoveryId)) await this.tasks.enqueue(actor.id,
            `Continue the unanswered scheduled input ${JSON.stringify(reference.request_id)} after an explicit retry. ` +
            "Its original input and completed tool results remain in this conversation. Inspect prior outcomes and effects before continuing unfinished work; never repeat an uncertain effect. " +
            "Before finishing, use record_task with " + `reconciles_input=${JSON.stringify(reference.request_id)}, a verified summary, and the actual task state.`,
            recoveryId, undefined, "follow_up");
        }
        const retired = await this.session.commit(async tx => {
          const current = await tx.doc(State), target = current.lanes.find(item => item.id === JSON.stringify(["job", id]));
          if (target?.last_run?.task !== reference.task && !(target?.last_run === undefined && target?.active === reference.task)) {
            if (target?.last_run?.recovery_of === (reference.recovery_of ?? reference.task)) return undefined;
            throw new TaskError("The interrupted job changed before recovery admission");
          }
          if (target === undefined) throw new TaskError("The interrupted job lane is unavailable");
          if (target.last_run?.recovery_of !== undefined && previous?.state.status !== "terminal") return undefined;
          const independent = target.active_runs?.includes(reference.task) === true;
          if (!independent && target.active !== null && target.active !== reference.task) throw new TaskError("Another occurrence now owns the job lane");
          const replacement = await this.createWorkIn(tx, work);
          if (independent) target.active_runs = (target.active_runs ?? []).map(native => native === reference.task ? replacement : native);
          else target.active = replacement;
          target.last_run = { ...reference, task: replacement, recovery_of: reference.recovery_of ?? reference.task };
          return previous?.state.status === "terminal" ? undefined : reference.task;
        }, BACKGROUND_CONTEXT);
        if (retired !== undefined) await this.session.abortTask(retired, BACKGROUND_CONTEXT);
        recovered = true;
      } else if (reference.recovery_of !== undefined) recovered = true;
    }
    if (!recovered) throw new TaskError("This job has no interrupted run or timer to retry");
    this.session.resume();
    return this.getJob(id);
  }
  /** Explicit recovery preserves the existing history lane and its queued
   * passes. Startup must keep Pi's crash-loop protection intact. */
  async retryHistoryIndex(): Promise<BackgroundStatus> {
    const state = await this.session.snapshot(State, BACKGROUND_CONTEXT);
    const trigger = state?.triggers.find(trigger => trigger.enabled && trigger.key.type === "builtin" && trigger.key.name === "history");
    if (state === undefined || trigger === undefined) throw new TaskError("No enabled history indexing timer exists");
    let recovered = await this.retryTimers(item => item.key.type === "builtin" && item.key.name === "history");
    recovered = await this.session.commit(async tx => {
      const current = await tx.doc(State), lane = current.lanes.find(item => item.id === laneId(trigger.key));
      const previous = lane?.active == null ? undefined : await tx.task(lane.active);
      if (lane === undefined || previous?.kind !== "agent.background" || previous.state.status !== "terminal" ||
          previous.state.outcome.status === "completed" || previous.state.outcome.status === "aborted") return recovered;
      lane.active = await this.createWorkIn(tx, { key: trigger.key, work: { type: "history" } });
      return true;
    }, BACKGROUND_CONTEXT);
    if (!recovered) throw new TaskError("History indexing has no interrupted work or timer to retry");
    this.session.resume();
    return this.status();
  }
}
