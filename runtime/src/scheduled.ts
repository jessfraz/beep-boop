import { createHash } from "node:crypto";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import type { ConversationId, Harness } from "@earendil-works/pi-durable";
import type { ScheduledOccurrence } from "./generated/ScheduledOccurrence.js";
import type { ScheduledJob } from "./generated/ScheduledJob.js";
import { SourceJournal } from "./sources.js";
import { TaskError, TaskStore } from "./tasks.js";
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
    `Scheduled occurrence: ${occurrence.id}. Continue related unfinished tasks and retain their decisions.`,
  ].join("\n\n");
}

/** Adopt only a job-owned chat; a mutable, nonunique title is not ownership. */
export async function openJobActor(job: ScheduledJob, tasks: TaskStore): Promise<string | undefined> {
  return (await tasks.list()).find((task) => task.sources.some((source) =>
    source.provider === "scheduled_jobs" && source.account === "local" && source.thread_id === job.id))?.id;
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

/** Older versions settled quota failures permanently. Admit one continuation in
 * the same actor, retaining its original input and completed effects as evidence. */
async function recoverQuotaTask(session: Harness, tasks: TaskStore, sources: SourceJournal, id: string,
  requestId: string, signal: AbortSignal): Promise<void> {
  const task = await tasks.get(id), conversation = await tasks.conversation(task.conversation_id);
  if (await reconciledTask(session, tasks, sources, id, conversation.id, requestId)) return;
  const original = await session.commit(tx => tx.submissionByRequest(conversation.id, requestId), BACKGROUND_CONTEXT);
  if (original?.type === "input" && original.status === "unanswered" && original.reason === "model_error" &&
      typeof original.detail === "string" && isQuotaExhaustion(original.detail)) {
    if (signal.aborted) throw new TaskError("Scheduled work stopped with the coordinator; its admission is retained");
    const recoveryId = "quota-recovery:" + original.id;
    const recovery = await session.commit(tx => tx.submissionByRequest(conversation.id, recoveryId), BACKGROUND_CONTEXT);
    if (recovery === undefined) {
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

/** Run the existing task actor in the coordinator, then settle its exact Pi input. */
export async function runScheduled(occurrence: ScheduledOccurrence, signal: AbortSignal,
  services: { session: Harness; tasks: TaskStore; sources: SourceJournal },
  id = scheduledTaskId(occurrence.id)): Promise<void> {
  const { session, tasks, sources } = services;
  const task = await tasks.create({ id, title: occurrence.job.title,
    outcome: occurrence.job.prompt, model: occurrence.job.model,
    sources: [{ provider: "scheduled_jobs", account: "local", thread_id: occurrence.job.id }] });
  const conversation = await tasks.conversation(task.conversation_id);
  await session.commit((tx) => tasks.inference.assignClassIn(tx, conversation, "cron"), BACKGROUND_CONTEXT);
  let submission = await session.commit((tx) => tx.submissionByRequest(conversation.id, occurrence.id), BACKGROUND_CONTEXT);
  if (submission?.status === "done") return;
  if (submission?.status === "unanswered") { await recoverQuotaTask(session, tasks, sources, id, occurrence.id, signal); return; }
  if (submission === undefined && !task.pending.some((input) => input.id === occurrence.id)) {
    await tasks.enqueueScheduled(id, occurrencePrompt(occurrence), occurrence);
  }
  let failure: unknown, revision = 0, wake: (() => void) | undefined;
  const stop = session.subscribeCommits(() => { revision += 1; wake?.(); });
  const aborted = () => wake?.(); signal.addEventListener("abort", aborted);
  try {
    const previousOutcome = (await sources.events({ provider: "task_outcomes", thread_id: id, limit: 1 }))[0]?.key;
    void tasks.runBackground(id).catch((error: unknown) => { failure = error; }).finally(() => { revision += 1; wake?.(); });
    for (;;) {
      if (signal.aborted) throw new TaskError("Scheduled work stopped with the coordinator; its admission is retained");
      const observed = revision;
      submission = await session.commit((tx) => tx.submissionByRequest(conversation.id, occurrence.id), BACKGROUND_CONTEXT);
      if (submission?.status === "unanswered") { await requireReconciledTask(session, tasks, sources, id, occurrence.id); return; }
      if (submission?.status === "done") return;
      if (failure !== undefined) throw failure;
      const latest = (await sources.events({ provider: "task_outcomes", thread_id: id, limit: 1 }))[0];
      if (latest?.key !== previousOutcome && latest?.outcome?.state.state === "failed") throw new TaskError(latest.outcome.state.reason);
      await new Promise<void>((resolve) => { wake = resolve; if (revision !== observed || signal.aborted) resolve(); });
      wake = undefined;
    }
  } finally { stop(); signal.removeEventListener("abort", aborted); }
}
