import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { defineDoc, defineDocFamily, defineEntry, defineTask, InboxDoc, LiveDoc, ROOT_CONVERSATION_ID,
  type ConversationId, type EntryId, type Harness, type Registry, type TaskId, type Tx } from "@earendil-works/pi-durable";
import type { JobProgress } from "./generated/JobProgress.js";
import type { TaskRecord } from "./generated/TaskRecord.js";
import type { ScheduledOccurrence } from "./generated/ScheduledOccurrence.js";
import type { ScheduledJob } from "./generated/ScheduledJob.js";
import type { TaskStore } from "./tasks.js";
import { OrderedIndex } from "./ordered-index.js";
import { UserNoticeEntry } from "./transcript.js";

const CHECK_INTERVAL = 30_000;
const CRITICAL_TIMEOUT = 120_000;
const NORMAL_TIMEOUT = 600_000;
const BATCH = 32;
const MONITOR_NOTICE = "Workflow status updates are unavailable right now.";
type Recovery = { kind: "resume" | "wake"; identity: string } | { kind: "continue"; identity: string; input_id: string; content: string; fingerprint: string };
type Alert = { request_id: string; conversation_id: string; evidence_id: string; text: string };
type Run = {
  task_id: string; job_id: string; title: string; conversation_id: string; critical: boolean;
  admitted_at: number; last_progress_at: number; deadline_at: number; index_key: string | null;
  active: boolean; status: JobProgress["status"]; blocker: string | null; fingerprint: string; state_fingerprint: string;
  latest_entry: EntryId | null; input_id: string | null; episode: number; alert: Alert | null;
  recovery: { identity: string; task: TaskId<null> } | null;
};
type NoticeProgress = Pick<Run, "status" | "admitted_at" | "last_progress_at">;
type Aggregate = { pending: number; overdue: number; unavailable: number; checked_at: number; last_progress_at: number | null; blocker: string | null;
  incident?: { episode: number; active: boolean; notice: TaskId<null> | null } };
const Runs = defineDocFamily<{ run: Run | null }, null>({ kind: "agent.workflow.progress.run", scope: "session", family: true, version: 1,
  initial: () => ({ run: null }), checkpointWhen: () => true });
const Jobs = defineDocFamily<Aggregate, null>({ kind: "agent.workflow.progress.job", scope: "session", family: true, version: 1,
  initial: () => ({ pending: 0, overdue: 0, unavailable: 0, checked_at: 0, last_progress_at: null, blocker: null }), checkpointWhen: () => true });
const Control = defineDoc<{ task: TaskId<null> | null; recovery_of: TaskId<null> | null; checked_at: number; unavailable: Alert | null }>({
  kind: "agent.workflow.progress", scope: "session", version: 1,
  initial: () => ({ task: null, recovery_of: null, checked_at: 0, unavailable: null }), checkpointWhen: () => true });
const Evidence = defineEntry<{ task_id: string | null; job_id: string | null; episode: number; checked_at: number; blocker: string }>("agent.workflow.progress.alert");

function timeKey(time: number, id: string): string { return String(Math.max(0, Math.floor(time))).padStart(16, "0") + ":" + id; }
function timeout(run: Pick<Run, "critical">): number { return run.critical ? CRITICAL_TIMEOUT : NORMAL_TIMEOUT; }
function fingerprint(task: TaskRecord): string { return JSON.stringify([task.summary, task.state, task.agents.map(agent => [agent.id, agent.state])]); }
function stateFingerprint(task: TaskRecord): string { return JSON.stringify([task.state, task.agents.map(agent => [agent.id, agent.state])]); }
function recoveryInput(id: string): boolean { return /^(?:background|scheduled|quota|workflow)-(?:recovery|reconcile|wait):/.test(id); }
function untimedWait(task: TaskRecord): boolean { return task.state.state === "waiting" && task.state.next_check === null && task.pending.length === 0; }
function clearIncident(aggregate: Aggregate): void {
  if (aggregate.overdue === 0 && aggregate.unavailable === 0 && aggregate.incident !== undefined) aggregate.incident.active = false;
}
function conversationIdentity(value: unknown): value is ConversationId { return typeof value === "number" && Number.isSafeInteger(value) && value > 0; }
function nativeConversation(value: string): ConversationId {
  const id: unknown = Number(value);
  if (!conversationIdentity(id) || String(id) !== value) throw new Error("Workflow conversation identity is unavailable");
  return id;
}

function progressNotice(run: NoticeProgress & { title: string }, observedAt: number): string {
  const title = run.title.trim().replace(/\s+/gu, " ");
  const name = title.length === 0 ? "This workflow" : title.charAt(0).toLocaleUpperCase() + title.slice(1);
  if (run.status === "unavailable") return `${name} status is unavailable right now.`;
  const delayed = `${name} is delayed.`;
  if (!Number.isFinite(run.last_progress_at) || run.last_progress_at <= 0 || run.last_progress_at > observedAt) {
    return `${delayed} Its last progress time is unavailable.`;
  }
  if (run.last_progress_at <= run.admitted_at) return `${delayed} No progress has been reported yet.`;
  const minutes = Math.floor((observedAt - run.last_progress_at) / 60_000), hours = Math.floor(minutes / 60);
  const elapsed = minutes < 1 ? "less than a minute" : hours === 0
    ? `${minutes} minute${minutes === 1 ? "" : "s"}` : `${hours} hour${hours === 1 ? "" : "s"}`;
  return `${delayed} Last progress was ${elapsed} ago.`;
}

/** Tracks owned work, rather than the newest run of a job. The ordered index
 * contains only active deadlines; original inputs and tool receipts stay in Pi. */
export class WorkflowProgress {
  private readonly deadlines = new OrderedIndex<{ id: string }>("agent.workflow.progress.deadline");
  private readonly timer;
  private readonly recovery;
  private readonly notice;
  private reconciles: ((tx: Tx, taskId: string, conversation: ConversationId, requestId: string) => Promise<boolean>) | undefined;
  constructor(private readonly session: Harness, registry: Registry, private readonly tasks: TaskStore,
    private readonly now: () => number = Date.now) {
    this.notice = defineTask<{ job_id: string; episode: number; task_id: string; title: string; alert: Alert; progress?: NoticeProgress }, { phase: "publish"; attempt: number }, null>({
      name: "agent.workflow_progress_notice", version: 1, initial: () => ({ phase: "publish", attempt: 0 }),
      phases: { publish: async (task, runtime, context) => {
        try {
          const root = await session.root(context);
          await runtime.commit(async tx => {
            const job = await tx.doc(Jobs, task.input.job_id, null), input = task.input;
            const prior = await tx.submissionByRequest(root.id, input.alert.request_id);
            if (prior !== undefined && prior.type !== "write") throw new Error("Workflow notice identity belongs to another submission type");
            // Eligibility and native admission share a commit. Clearing the
            // incident cannot race a stale alert into Main; published history stays.
            if (prior === undefined && job.incident?.episode === input.episode && job.incident.active && job.overdue + job.unavailable > 0) {
              await root.submit(tx, { type: "write", requestId: input.alert.request_id, entry: { kind: UserNoticeEntry.kind, data: {
                task_id: input.task_id, source_conversation_id: input.alert.conversation_id, source_entry_id: input.alert.evidence_id,
                text: input.progress === undefined ? `${input.title} needs attention.` : progressNotice({ title: input.title, ...input.progress }, runtime.now()),
                notice_origin: { job_id: input.job_id, title: input.title },
              } } });
            }
            return { status: "terminal", outcome: { status: "completed", result: null } };
          }, context);
        } catch (error) {
          if (runtime.signal.aborted) throw error;
          const attempt = Math.min(6, task.state.checkpoint.attempt + 1);
          await runtime.commit(() => ({ status: "sleeping", until: runtime.now() + Math.min(60_000, 1000 * 2 ** attempt),
            checkpoint: { phase: "publish", attempt } }), context);
        }
      } },
      abort: async (_task, runtime, context) => { await runtime.commit(() => ({ status: "terminal", outcome: { status: "aborted" } }), context); },
    });
    this.timer = defineTask<Record<string, never>, { phase: "check"; turn: boolean }, null>({
      name: "agent.workflow_progress", version: 1, initial: () => ({ phase: "check", turn: false }),
      phases: { check: async (task, runtime, context) => {
        const observedAt = runtime.now();
        const due = await this.deadlines.range(session, "due", { before: timeKey(observedAt + 1, ""), limit: BATCH });
        for (const item of due) {
          runtime.signal.throwIfAborted();
          try { await this.check(item.value.id, observedAt); }
          catch (error) { await this.unavailable(item.value.id, observedAt, error); }
        }
        const next = await this.deadlines.range(session, "due", { limit: 1 });
        await runtime.commit(async tx => {
          const control = await tx.doc(Control); control.checked_at = observedAt; control.recovery_of = null;
          const deadline = next[0] === undefined ? observedAt + CHECK_INTERVAL : Number(next[0].key.slice(0, 16));
          // Another native recovery can reinsert due work at the same clock
          // time. Each bounded pass still advances its durable checkpoint.
          const checkpoint: { phase: "check"; turn: boolean } = { phase: "check", turn: !task.state.checkpoint.turn };
          return deadline <= observedAt ? { status: "running", checkpoint }
            : { status: "sleeping", until: Math.min(deadline, observedAt + CHECK_INTERVAL), checkpoint };
        }, context);
      } },
      abort: async (_task, runtime, context) => { await runtime.commit(() => ({ status: "terminal", outcome: { status: "aborted" } }), context); },
    });
    this.recovery = defineTask<{ id: string; recovery: Recovery }, { phase: "recover" }, null>({
      name: "agent.workflow_progress_recovery", version: 1, initial: () => ({ phase: "recover" }),
      phases: { recover: async (task, runtime, context) => {
        const { id, recovery } = task.input;
        try {
          if (recovery.kind === "resume") await tasks.resumeWorkflow(id);
          else {
            let dispatch = true;
            if (recovery.kind === "continue") {
              const current = await tasks.enqueueWorkflowRecovery(id, recovery.content, recovery.input_id, async (tx, task) => {
                const run = (await tx.doc(Runs, id, null)).run;
                const conversation = nativeConversation(task.conversation_id);
                const live = await tx.doc(LiveDoc, conversation), inbox = await tx.doc(InboxDoc, conversation);
                const native = run?.input_id == null ? undefined : await tx.submissionByRequest(conversation, run.input_id);
                const futureWait = task.state.state === "waiting" && task.state.next_check !== null && Date.parse(task.state.next_check) > runtime.now();
                const externalWait = untimedWait(task) && live.runFailure === undefined && !(native?.type === "input" && native.status === "unanswered");
                return run !== null && run.active && run.recovery?.identity === recovery.identity && run.fingerprint === recovery.fingerprint &&
                  task.state.state !== "resolved" && !futureWait && !externalWait &&
                  live.run === undefined && inbox.items.every(item => item.mode === "write");
              });
              dispatch = current.state.state !== "resolved" && current.pending.some(input => input.id === recovery.input_id);
            }
            if (dispatch) await tasks.wakeBackground(id, runtime.signal);
          }
        } catch (error) {
          await runtime.commit(async tx => {
            const run = (await tx.doc(Runs, id, null)).run;
            if (run?.active && run.recovery?.identity === recovery.identity) {
              run.blocker = error instanceof Error ? error.message : "The workflow's retained recovery could not start";
              await this.scheduleIn(tx, run, runtime.now());
            }
          }, context);
        }
        await runtime.commit(() => ({ status: "terminal", outcome: { status: "completed", result: null } }), context);
      } },
      abort: async (_task, runtime, context) => { await runtime.commit(() => ({ status: "terminal", outcome: { status: "aborted" } }), context); },
    });
    registry.install({ name: "workflow-progress", tasks: [this.timer, this.recovery, this.notice] });
    tasks.useMonitor(this);
  }
  useReceipts(reconciles: (tx: Tx, taskId: string, conversation: ConversationId, requestId: string) => Promise<boolean>): void {
    this.reconciles = reconciles;
  }
  async ownsLegacy(task: TaskRecord): Promise<boolean> {
    const run = (await this.session.snapshot(Runs, task.id, BACKGROUND_CONTEXT))?.run;
    return run != null && run.active && run.conversation_id === task.conversation_id &&
      task.sources.some(source => source.provider === "scheduled_jobs" && source.account === "local" && source.thread_id === run.job_id);
  }
  private async scheduleIn(tx: Tx, run: Run, at: number): Promise<void> {
    if (run.index_key !== null) await this.deadlines.deleteIn(tx, "due", run.index_key);
    run.index_key = run.active ? timeKey(at, run.task_id) : null;
    if (run.index_key !== null) await this.deadlines.setIn(tx, "due", run.index_key, { id: run.task_id });
  }
  /** This hook shares the task's admission/update commit, including its exact
   * original request ID before the native outbox is consumed. */
  async trackIn(tx: Tx, task: TaskRecord, verified = false): Promise<void> {
    const work = task.workflow;
    if (work === undefined && !task.sources.some(source => source.provider === "scheduled_jobs" && source.account === "local")) return;
    const state = await tx.doc(Runs, task.id, null), observedAt = this.now(), signature = fingerprint(task), stateSignature = stateFingerprint(task);
    if (work === undefined && state.run === null) return;
    if (state.run === null && work !== undefined) state.run = { task_id: task.id, job_id: work.job.id, title: work.job.title, conversation_id: task.conversation_id,
      critical: work.job.priority === "critical", admitted_at: observedAt, last_progress_at: observedAt,
      deadline_at: observedAt + (work.job.priority === "critical" ? CRITICAL_TIMEOUT : NORMAL_TIMEOUT), index_key: null,
      active: true, status: "normal", blocker: null, fingerprint: signature, state_fingerprint: stateSignature, latest_entry: null,
      input_id: null, episode: 0, alert: null, recovery: null };
    const run = state.run;
    if (run === null) return;
    if (work !== undefined && run.job_id !== work.job.id || run.conversation_id !== task.conversation_id) throw new Error("Workflow progress ownership changed");
    const aggregate = await tx.doc(Jobs, run.job_id, null);
    if (run.index_key === null && run.active) aggregate.pending++;
    run.input_id ??= task.pending.find(input => !recoveryInput(input.id))?.id ?? null;
    const progress = verified || run.state_fingerprint !== stateSignature;
    run.fingerprint = signature; run.state_fingerprint = stateSignature;
    if (progress) {
      if (run.active && run.status === "overdue") aggregate.overdue--;
      if (run.active && run.status === "unavailable") aggregate.unavailable--;
      run.status = "normal"; run.blocker = null;
      run.last_progress_at = observedAt; run.deadline_at = observedAt + timeout(run);
      if (run.alert !== null) { run.episode++; run.alert = null; }
      aggregate.last_progress_at = Math.max(aggregate.last_progress_at ?? 0, observedAt);
    }
    if (!run.active && (work !== undefined && task.state.state !== "resolved" || task.pending.length > 0)) {
      run.active = true; aggregate.pending++; run.status = "normal"; run.blocker = null;
      run.admitted_at = observedAt; run.last_progress_at = observedAt; run.deadline_at = observedAt + timeout(run);
      run.input_id = task.pending.find(input => !recoveryInput(input.id))?.id ?? null;
      run.recovery = null; run.alert = null; run.episode++;
    }
    if (run.active) {
      const wait = task.state.state === "waiting" && task.pending.length === 0 && task.state.next_check !== null ? Date.parse(task.state.next_check) : NaN;
      await this.scheduleIn(tx, run, task.state.state === "resolved" ? observedAt + CHECK_INTERVAL
        : Number.isFinite(wait) && wait > observedAt ? wait : run.deadline_at);
    }
    clearIncident(aggregate);
  }
  async trackScheduledIn(tx: Tx, task: TaskRecord, occurrence: ScheduledOccurrence): Promise<void> {
    if (task.workflow === undefined && !task.sources.some(source => source.provider === "scheduled_jobs" && source.account === "local" && source.thread_id === occurrence.job.id)) {
      throw new Error("The retained scheduled actor has no canonical workflow ownership");
    }
    await this.trackIn(tx, task.workflow === undefined ? { ...task, workflow: {
      job: occurrence.job, owner_task_id: task.id, work_key: "legacy-occurrence:" + occurrence.id,
    } } : task);
    const run = (await tx.doc(Runs, task.id, null)).run;
    if (run !== null) run.input_id ??= occurrence.id;
  }
  async adoptJobs(jobs: readonly ScheduledJob[]): Promise<void> {
    // Older versions kept scheduled/source inputs on the canonical actor.
    // Adopt only current owned work by indexed job links and native receipts.
    for (const job of jobs) for (const owner of await this.tasks.findBySources([
      { provider: "scheduled_jobs", account: "local", thread_id: job.id },
    ], 2)) {
      if (owner.workflow !== undefined) continue;
      await this.session.commit(async tx => {
        const task = await this.tasks.findIn(tx, owner.id);
        if (task === undefined || task.state.state === "resolved") return;
        const conversation = nativeConversation(task.conversation_id), live = await tx.doc(LiveDoc, conversation), inbox = await tx.doc(InboxDoc, conversation);
        if (task.pending.length === 0 && live.run === undefined && live.runFailure === undefined && inbox.items.every(item => item.mode === "write")) return;
        const input = live.run?.inputs[0] ?? live.runFailure?.inputs[0];
        const original = input === undefined ? undefined : await tx.submission(input);
        const captured = task.pending.find(input => input.scheduled !== undefined)?.scheduled;
        const requestId = captured?.id ?? task.pending[0]?.id ?? original?.requestId;
        await this.trackIn(tx, { ...task, workflow: { job: captured?.job ?? job, owner_task_id: task.id, work_key: "retained-actor:" + task.id } });
        const run = (await tx.doc(Runs, task.id, null)).run;
        if (run !== null && requestId !== undefined) run.input_id ??= requestId;
      }, BACKGROUND_CONTEXT);
    }
  }
  async start(jobs: readonly ScheduledJob[] = []): Promise<void> {
    await this.session.root(BACKGROUND_CONTEXT);
    await this.adoptJobs(jobs);
    const alert = await this.session.commit(async tx => {
      const control = await tx.doc(Control), previous = control.task === null ? undefined : await tx.task(control.task);
      if (previous?.state.status !== "terminal" && control.task !== null) return undefined;
      if (previous !== undefined && control.recovery_of !== null) {
        if (control.unavailable === null) {
          const blocker = "Workflow progress monitoring stopped again during recovery; its durable deadlines remain retained";
          const entry = await tx.appendEntry(Evidence, ROOT_CONVERSATION_ID,
            { data: { task_id: null, job_id: null, episode: 0, checked_at: this.now(), blocker } });
          control.unavailable = { request_id: "workflow-monitor-unavailable:" + control.recovery_of,
            conversation_id: ROOT_CONVERSATION_ID.toString(), evidence_id: entry.id.toString(), text: MONITOR_NOTICE };
        }
        return { ...control.unavailable, text: MONITOR_NOTICE };
      }
      control.recovery_of = control.task;
      control.task = await tx.createTask(this.timer, {}, { ownership: { kind: "conversation" }, conversationId: ROOT_CONVERSATION_ID, background: true });
      control.unavailable = null; return undefined;
    }, BACKGROUND_CONTEXT);
    if (alert !== undefined) await this.publish(alert);
    this.session.resume();
  }
  private async stageIncidentIn(tx: Tx, aggregate: Aggregate, run: Run, observedAt: number, legacy: boolean, retained: Alert | null): Promise<void> {
    aggregate.incident ??= { episode: legacy ? 0 : 1, active: true, notice: null };
    const incident = aggregate.incident;
    if (!incident.active) { incident.episode++; incident.active = true; incident.notice = null; }
    // A legacy incident may already have a native publication. Adopt its first
    // retained request rather than minting another notice for the same outage.
    if (incident.notice !== null || incident.episode === 0 && retained === null) return;
    const saved = incident.episode === 0 ? retained : run.alert;
    if (saved === null) throw new Error("Workflow incident has no retained alert evidence");
    const alert = { ...saved, text: progressNotice(run, observedAt), request_id: incident.episode === 0 ? saved.request_id
      : "workflow-job-progress:" + JSON.stringify([run.job_id, incident.episode]) };
    incident.notice = await tx.createTask(this.notice, { job_id: run.job_id, episode: incident.episode, task_id: run.task_id, title: run.title, alert,
      progress: { status: run.status, admitted_at: run.admitted_at, last_progress_at: run.last_progress_at } },
      { ownership: { kind: "conversation" }, conversationId: ROOT_CONVERSATION_ID, background: true });
  }
  private async check(id: string, observedAt: number): Promise<void> {
    return this.session.commit(async tx => {
      const state = await tx.doc(Runs, id, null), run = state.run;
      if (run === null || !run.active) return;
      const task = await this.tasks.findIn(tx, id), conversationId = nativeConversation(run.conversation_id);
      const live = await tx.doc(LiveDoc, conversationId), inbox = await tx.doc(InboxDoc, conversationId);
      const native = run.input_id === null ? undefined : await tx.submissionByRequest(conversationId, run.input_id);
      const failureInputs = live.runFailure === undefined ? [] : await Promise.all(live.runFailure.inputs.map(input => tx.submission(input)));
      const entries = await tx.scanEntries({ conversationId, order: "descending", ...(run.latest_entry === null ? {} : { minEntryId: run.latest_entry }) }, 16);
      const aggregate = await tx.doc(Jobs, run.job_id, null); aggregate.checked_at = observedAt;
      const busy = live.run !== undefined || inbox.items.some(item => item.mode !== "write");
      if (task !== undefined && task.state.state === "resolved" && !busy && await this.tasks.canReleaseIn(tx, id) &&
          (native?.type === "input" && native.status === "done" || run.input_id !== null && native?.status === "unanswered" &&
            await this.reconciles?.(tx, id, conversationId, run.input_id) === true)) {
        aggregate.pending--; if (run.status === "overdue") aggregate.overdue--; if (run.status === "unavailable") aggregate.unavailable--;
        run.active = false; clearIncident(aggregate); await this.scheduleIn(tx, run, observedAt); return;
      }
      const latest = run.latest_entry;
      const newEntries = entries.items.filter(entry => latest === null || entry.id > latest);
      const progressAt = newEntries.reduce((latest, entry) => Math.max(latest, ...entry.model?.flatMap(message =>
        message.role === "toolResult" && !message.isError && Number.isFinite(message.timestamp) && message.timestamp <= observedAt
          ? [message.timestamp] : []) ?? []), run.last_progress_at);
      if (entries.items[0] !== undefined) run.latest_entry = entries.items[0].id;
      if (progressAt > run.last_progress_at) {
        if (run.status === "overdue") aggregate.overdue--; if (run.status === "unavailable") aggregate.unavailable--;
        run.status = "normal"; run.blocker = null; run.last_progress_at = progressAt; run.deadline_at = progressAt + timeout(run);
        if (run.alert !== null) { run.episode++; run.alert = null; }
        aggregate.last_progress_at = Math.max(aggregate.last_progress_at ?? 0, progressAt);
        clearIncident(aggregate);
      }
      const wait = task?.state.state === "waiting" && task.state.next_check !== null ? Date.parse(task.state.next_check) : NaN;
      if (task !== undefined && !busy && untimedWait(task) && live.runFailure === undefined && !(native?.type === "input" && native.status === "unanswered")) {
        if (run.status === "overdue") aggregate.overdue--; if (run.status === "unavailable") aggregate.unavailable--;
        run.status = "normal"; run.blocker = null; run.recovery = null;
        if (run.alert !== null) { run.episode++; run.alert = null; }
        clearIncident(aggregate);
        await this.scheduleIn(tx, run, observedAt + timeout(run)); return;
      }
      if (task !== undefined && !busy && task.pending.length === 0 && Number.isFinite(wait) && wait > observedAt) {
        await this.scheduleIn(tx, run, wait); return;
      }
      if (observedAt < run.deadline_at) { await this.scheduleIn(tx, run, run.deadline_at); return; }
      const legacy = aggregate.overdue + aggregate.unavailable > 0, retained = run.alert;
      const unavailable = task === undefined;
      if (run.status === "overdue") aggregate.overdue--; if (run.status === "unavailable") aggregate.unavailable--;
      run.status = unavailable ? "unavailable" : "overdue";
      if (unavailable) aggregate.unavailable++; else aggregate.overdue++;
      const originalFailure = failureInputs.find(input => input?.type === "input" && input.status === "unanswered" && input.requestId !== undefined);
      const failedRecovery = failureInputs.some(input => input?.requestId !== undefined && recoveryInput(input.requestId)) ||
        !busy && run.recovery !== null && task?.pending.some(input => recoveryInput(input.id)) === true;
      run.blocker = unavailable ? "The owned task is unavailable; coverage is incomplete"
        : failedRecovery ? "The retained recovery stopped without verified resolution; another recovery was not cloned"
        : live.runFailure !== undefined ? `Its native input is unanswered (${live.runFailure.reason})` +
          (live.runFailure.detail === undefined ? "" : ": " + live.runFailure.detail.slice(0, 320))
        : busy ? "Its native work has made no verified progress before its deadline"
        : task.pending.length > 0 || native === undefined ? "Its admitted input has not completed dispatch"
        : task.state.state === "waiting" ? "Its waiting state has no future recovery check"
        : "Its native turn ended without verified task resolution";
      aggregate.blocker = run.blocker;
      if (run.alert === null) {
        const evidence = await tx.appendEntry(Evidence, conversationId,
          { data: { task_id: id, job_id: run.job_id, episode: run.episode, checked_at: observedAt, blocker: run.blocker } });
        run.alert = { request_id: `workflow-progress:${id}:${run.episode}`, conversation_id: run.conversation_id, evidence_id: evidence.id.toString(),
          text: progressNotice(run, observedAt) };
      }
      await this.stageIncidentIn(tx, aggregate, run, observedAt, legacy, retained);
      let recovery: Recovery | undefined;
      if (!unavailable && !busy && !failedRecovery) {
        if (originalFailure?.requestId !== undefined && live.runFailure !== undefined &&
            ["interrupted", "restart_limit", "faulted", "orphaned"].includes(live.runFailure.reason)) {
          recovery = { kind: "resume", identity: "native:" + originalFailure.id };
        } else if (task.pending[0] !== undefined) recovery = { kind: "wake", identity: "pending:" + task.pending[0].id };
        else if (native?.type === "input" && native.status === "done") {
          const identity = Number.isFinite(wait) ? "wait:" + wait : "completed:" + native.id;
          recovery = { kind: "continue", identity, fingerprint: run.fingerprint, input_id: "workflow-reconcile:" + id + ":" + identity,
            content: "This owned workflow reached its recorded progress deadline without verified resolution. " +
              "Inspect its exact retained input, completed tool receipts and current provider evidence. Continue only unfinished authorized work; never repeat uncertain effects. " +
              "Preserve user decisions and future waiting checks. Use record_task with verified evidence and the actual resolved or waiting state before finishing." };
        }
      }
      if (recovery !== undefined && run.recovery?.identity !== recovery.identity) {
        const nativeTask = await tx.createTask(this.recovery, { id, recovery },
          { ownership: { kind: "conversation" }, conversationId, background: true });
        run.recovery = { identity: recovery.identity, task: nativeTask };
      }
      await this.scheduleIn(tx, run, observedAt + timeout(run));
    }, BACKGROUND_CONTEXT);
  }
  private async unavailable(id: string, observedAt: number, error: unknown): Promise<void> {
    return this.session.commit(async tx => {
      const run = (await tx.doc(Runs, id, null)).run;
      if (run === null || !run.active) return;
      const aggregate = await tx.doc(Jobs, run.job_id, null);
      const legacy = aggregate.overdue + aggregate.unavailable > 0, retained = run.alert;
      if (run.status === "overdue") aggregate.overdue--;
      if (run.status !== "unavailable") aggregate.unavailable++;
      run.status = "unavailable";
      run.blocker = "Progress coverage for this owned task is unavailable: " + (error instanceof Error ? error.message.slice(0, 500) : "its current native evidence could not be read");
      aggregate.blocker = run.blocker; aggregate.checked_at = observedAt;
      if (run.alert === null) {
        const evidence = await tx.appendEntry(Evidence, ROOT_CONVERSATION_ID,
          { data: { task_id: id, job_id: run.job_id, episode: run.episode, checked_at: observedAt, blocker: run.blocker } });
        run.alert = { request_id: `workflow-progress:${id}:${run.episode}`, conversation_id: ROOT_CONVERSATION_ID.toString(),
          evidence_id: evidence.id.toString(), text: progressNotice(run, observedAt) };
      }
      await this.stageIncidentIn(tx, aggregate, run, observedAt, legacy, retained);
      await this.scheduleIn(tx, run, observedAt + timeout(run));
    }, BACKGROUND_CONTEXT);
  }
  private async publish(alert: Alert): Promise<void> {
    const root = await this.session.root(BACKGROUND_CONTEXT);
    await root.submit({ type: "write", requestId: alert.request_id, entry: { kind: UserNoticeEntry.kind, data: {
      task_id: null, source_conversation_id: alert.conversation_id, source_entry_id: alert.evidence_id, text: alert.text,
    } } }, BACKGROUND_CONTEXT);
  }
  async status(jobId: string): Promise<JobProgress> {
    const observedAt = this.now(), control = await this.session.snapshot(Control, BACKGROUND_CONTEXT), aggregate = await this.session.snapshot(Jobs, jobId, BACKGROUND_CONTEXT);
    const timer = control?.task == null ? undefined : await this.session.getTask(control.task, BACKGROUND_CONTEXT);
    const unavailable = timer === undefined || timer.state.status === "terminal" || control?.checked_at === 0 ||
      observedAt - (control?.checked_at ?? 0) > 2 * CHECK_INTERVAL || control?.unavailable !== null && control?.unavailable !== undefined;
    const failure = timer?.state.status === "terminal" && (timer.state.outcome.status === "failed" || timer.state.outcome.status === "faulted")
      ? timer.state.outcome.error.message : undefined;
    const status = unavailable || (aggregate?.unavailable ?? 0) > 0 ? "unavailable" : (aggregate?.overdue ?? 0) > 0 ? "overdue" : "normal";
    return { status, checked_at: control?.checked_at ?? 0, last_progress_at: aggregate?.last_progress_at ?? null,
      pending_work: aggregate?.pending ?? 0, ...(status === "normal" ? {} : { blocker: unavailable
        ? "Workflow progress monitoring is unavailable; retained work has not been declared healthy" + (failure === undefined ? "" : ": " + failure)
        : aggregate?.blocker ?? "Workflow progress coverage is incomplete" }) };
  }
}
