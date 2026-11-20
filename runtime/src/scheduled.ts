import { createHash } from "node:crypto";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import type { Conversation, ConversationId, Cursor, EntryRecord, Harness } from "@earendil-works/pi-durable";
import type { ScheduledOccurrence } from "./generated/ScheduledOccurrence.js";
import type { ScheduledJob } from "./generated/ScheduledJob.js";
import { SourceJournal } from "./sources.js";
import { publishMainNotice, TaskError, TaskStore } from "./tasks.js";
import { isQuotaExhaustion } from "./quota.js";

/** A stable UUID keeps the ordinary task API authoritative across replay. */
export function scheduledTaskId(occurrence: string): string {
  const bytes = createHash("sha256").update(occurrence).digest();
  bytes[6] = (bytes[6]! & 0x0f) | 0x40; bytes[8] = (bytes[8]! & 0x3f) | 0x80;
  const hex = bytes.toString("hex", 0, 16);
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

export function occurrencePrompt(occurrence: ScheduledOccurrence): string {
  const schedule = occurrence.job.schedules.find((schedule) => schedule.id === occurrence.schedule_id);
  if (schedule === undefined) throw new TaskError("The admitted job schedule is unavailable");
  return [occurrence.job.prompt,
    ...(schedule.prompt === null ? [] : [`This occurrence's focus${schedule.focus === null ? "" : ` (${schedule.focus})`}:\n${schedule.prompt}`]),
    `Scheduled occurrence: ${occurrence.id}. Start from current evidence in this fresh conversation. ` +
      "Read only relevant durable checkpoints, user decisions and uncertain-effect receipts explicitly; do not load previous-run transcripts.",
    ...(occurrence.manual?.input === undefined ? [] : [`Current user request:\n${occurrence.manual.input}`]),
    ...(occurrence.manual === undefined ? [] : ["This is a directly requested run. Return a concise actual result or concrete blocker even when there is nothing new; do not finish silently."]),
  ].join("\n\n");
}

/** Adopt only a job-owned chat; a mutable, nonunique title is not ownership. */
export async function openJobActor(job: ScheduledJob, tasks: TaskStore): Promise<string | undefined> {
  const owners = await tasks.findBySources([{ provider: "scheduled_jobs", account: "local", thread_id: job.id }], 2);
  if (owners.length > 1) throw new TaskError("The canonical workflow owner is ambiguous");
  return owners[0]?.id;
}

async function requestedResult(occurrence: ScheduledOccurrence, conversation: Conversation, tasks: TaskStore, id: string): Promise<void> {
  if (occurrence.manual === undefined) return;
  let cursor: Cursor | undefined, answer: EntryRecord | undefined;
  const publishedCalls = new Set<string>(), notifications = new Map<string, string>();
  for (;;) {
    const page = await conversation.entries({ order: "descending" }, 64, cursor, BACKGROUND_CONTEXT);
    for (const entry of page.items) for (const message of entry.model ?? []) {
      if (message.role === "toolResult" && message.toolName === "notify_user" && !message.isError &&
          message.content.some(part => part.type === "text" && part.text === "Published to the main chat.")) publishedCalls.add(message.toolCallId);
      if (message.role === "assistant") for (const part of message.content) {
        if (part.type === "toolCall" && part.name === "notify_user" && typeof part.arguments.text === "string") {
          notifications.set(part.id, part.arguments.text.trim());
        }
      }
      if (answer === undefined && message.role === "assistant" && message.stopReason === "stop") answer = entry;
    }
    if (page.next === undefined) break;
    cursor = page.next;
  }
  if (answer === undefined) throw new TaskError("The requested job has no completed result; its reply obligation remains open");
  const text = answer.model?.flatMap(message => message.role === "assistant"
    ? message.content.flatMap(part => part.type === "text" ? [part.text] : []) : []).join("\n").trim() ?? "";
  if (text.length === 0) throw new TaskError("The requested job returned an empty result; its reply obligation remains open");
  // A progress update does not discharge the result obligation. Suppress only
  // the exact final result that this occurrence has already delivered.
  if ([...publishedCalls].some(call => notifications.get(call) === text)) return;
  const task = await tasks.get(id);
  const published = await publishMainNotice(tasks.session, conversation, answer, id, task.sources,
    { key: "manual-job-result:" + occurrence.id, text, events: [], origin: { job_id: occurrence.job.id, title: occurrence.job.title } });
  if (published !== "published") throw new TaskError("The requested result is queued for the main chat but is not delivered yet");
}

async function reconciledTask(session: Harness, tasks: TaskStore, sources: SourceJournal, id: string,
  conversation: ConversationId, requestId: string): Promise<boolean> {
  // A successful exact recovery settles that input while the actor can continue
  // other work. Resolving the whole task still requires quiescence.
  return session.commit(async (tx) => await sources.reconcilesIn(tx, id, conversation, requestId) ||
    (await tasks.canSettleScheduledIn(tx, id, conversation) && await tasks.canReleaseIn(tx, id)), BACKGROUND_CONTEXT);
}

async function requireReconciledTask(session: Harness, tasks: TaskStore, sources: SourceJournal, id: string, requestId: string): Promise<void> {
  const conversation = await tasks.conversation((await tasks.get(id)).conversation_id);
  if (!await reconciledTask(session, tasks, sources, id, conversation.id, requestId)) {
    throw new TaskError("The scheduled turn stopped without an answer; resolve its task or explicitly reconcile its exact input in a successful recovery turn before this occurrence can settle");
  }
}

/** Quota recovery and an explicitly admitted retry retain the original receipt.
 * Completed or uncertain tools are evidence for the new input, never replayed tasks. */
async function recoverScheduledTask(session: Harness, tasks: TaskStore, sources: SourceJournal, id: string,
  requestId: string, signal: AbortSignal): Promise<void> {
  const task = await tasks.get(id), conversation = await tasks.conversation(task.conversation_id);
  if (await reconciledTask(session, tasks, sources, id, conversation.id, requestId)) return;
  const original = await session.commit(tx => tx.submissionByRequest(conversation.id, requestId), BACKGROUND_CONTEXT);
  if (original?.type === "input" && original.status === "unanswered") {
    const explicitId = "scheduled-recovery:" + original.id;
    const explicit = await session.commit(tx => tx.submissionByRequest(conversation.id, explicitId), BACKGROUND_CONTEXT);
    const explicitPending = task.pending.some(input => input.id === explicitId);
    const quota = original.reason === "model_error" && typeof original.detail === "string" && isQuotaExhaustion(original.detail);
    if (!quota && explicit === undefined && !explicitPending) { await requireReconciledTask(session, tasks, sources, id, requestId); return; }
    if (signal.aborted) throw new TaskError("Scheduled work stopped with the coordinator; its admission is retained");
    const recoveryId = explicit !== undefined || explicitPending ? explicitId : "quota-recovery:" + original.id;
    const recovery = recoveryId === explicitId ? explicit : await session.commit(tx => tx.submissionByRequest(conversation.id, recoveryId), BACKGROUND_CONTEXT);
    if (recovery === undefined && recoveryId !== explicitId) {
      await tasks.enqueue(id, `Continue the unanswered scheduled input ${JSON.stringify(requestId)} after quota exhaustion. ` +
        "Its original input and completed tool results remain in this conversation. Verify prior effects and continue only unfinished work; do not repeat completed actions. " +
        "Preserve existing authorization and waiting decisions. Before finishing, use record_task with " +
        `reconciles_input=${JSON.stringify(requestId)}, a verified summary, and the actual current task state.`, recoveryId, undefined, "follow_up");
    }
    // A terminal recovery is evidence to inspect, never permission for another
    // automatic attempt after an abort, unrelated error or missing reconciliation.
    if (recovery === undefined || recovery.status === "queued" || recovery.status === "placed") await tasks.runBackground(id, signal);
  }
  await requireReconciledTask(session, tasks, sources, id, requestId);
}

/** Every new occurrence owns a fresh actor; replay and recovery retain only
 * that occurrence's conversation and exact native receipts. */
export async function runScheduled(occurrence: ScheduledOccurrence, signal: AbortSignal,
  services: { session: Harness; tasks: TaskStore; sources: SourceJournal },
  id = scheduledTaskId(occurrence.id)): Promise<void> {
  const { session, tasks, sources } = services;
  if (occurrence.task_id !== undefined && occurrence.task_id !== id) throw new TaskError("The admitted occurrence's task identity changed");
  const fresh = id === scheduledTaskId(occurrence.id);
  let owner: string | undefined;
  if (fresh) {
    owner = await openJobActor(occurrence.job, tasks);
    if (owner === undefined) owner = (await tasks.create({ id: scheduledTaskId("job:" + occurrence.job.id),
      title: occurrence.job.title, outcome: occurrence.job.prompt, model: occurrence.job.model,
      sources: [{ provider: "scheduled_jobs", account: "local", thread_id: occurrence.job.id }] })).id;
  }
  const task = await tasks.create({ id, title: occurrence.job.title,
    outcome: occurrence.job.prompt, model: occurrence.job.model,
    ...(owner === undefined ? { sources: [{ provider: "scheduled_jobs", account: "local", thread_id: occurrence.job.id }] }
      : { execution: "coordinator", workflow: { job: occurrence.job, owner_task_id: owner, work_key: "occurrence:" + occurrence.id } }) });
  const conversation = await tasks.conversation(task.conversation_id);
  await tasks.trackScheduledProgress(task.id, occurrence);
  await session.commit((tx) => tasks.inference.assignClassIn(tx, conversation,
    occurrence.job.priority === "critical" ? "critical" : "cron", occurrence.job.priority === "critical" ? occurrence.job.id : undefined), BACKGROUND_CONTEXT);
  let submission = await session.commit((tx) => tx.submissionByRequest(conversation.id, occurrence.id), BACKGROUND_CONTEXT);
  if (submission?.status === "done") { await requestedResult(occurrence, conversation, tasks, id); return; }
  if (submission?.status === "unanswered") {
    await recoverScheduledTask(session, tasks, sources, id, occurrence.id, signal);
    await requestedResult(occurrence, conversation, tasks, id); return;
  }
  if (submission === undefined && !task.pending.some((input) => input.id === occurrence.id)) {
    await tasks.enqueueScheduled(id, occurrencePrompt(occurrence), occurrence);
  }
  let failure: unknown, revision = 0, wake: (() => void) | undefined;
  const observation = session.observeCommits({ advance: () => { revision += 1; wake?.(); }, end: () => {} });
  const stop = () => observation.detach();
  const aborted = () => wake?.(); signal.addEventListener("abort", aborted);
  try {
    const previousOutcome = (await sources.events({ provider: "task_outcomes", thread_id: id, limit: 1 }))[0]?.key;
    void tasks.runBackground(id).catch((error: unknown) => { failure = error; }).finally(() => { revision += 1; wake?.(); });
    for (;;) {
      if (signal.aborted) throw new TaskError("Scheduled work stopped with the coordinator; its admission is retained");
      const observed = revision;
      submission = await session.commit((tx) => tx.submissionByRequest(conversation.id, occurrence.id), BACKGROUND_CONTEXT);
      if (submission?.status === "unanswered") {
        await requireReconciledTask(session, tasks, sources, id, occurrence.id);
        await requestedResult(occurrence, conversation, tasks, id); return;
      }
      if (submission?.status === "done") { await requestedResult(occurrence, conversation, tasks, id); return; }
      if (failure !== undefined) throw failure;
      const latest = (await sources.events({ provider: "task_outcomes", thread_id: id, limit: 1 }))[0];
      if (latest?.key !== previousOutcome && latest?.outcome?.state.state === "failed") throw new TaskError(latest.outcome.state.reason);
      await new Promise<void>((resolve) => { wake = resolve; if (revision !== observed || signal.aborted) resolve(); });
      wake = undefined;
    }
  } finally { stop(); signal.removeEventListener("abort", aborted); }
}
