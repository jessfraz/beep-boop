import { createHash } from "node:crypto";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import type { Harness } from "@earendil-works/pi-durable";
import type { ScheduledOccurrence } from "./generated/ScheduledOccurrence.js";
import type { ScheduledJob } from "./generated/ScheduledJob.js";
import { SourceJournal } from "./sources.js";
import { TaskError, TaskStore } from "./tasks.js";

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

async function requireReconciledTask(session: Harness, tasks: TaskStore, sources: SourceJournal, id: string, requestId: string): Promise<void> {
  const conversation = await tasks.conversation((await tasks.get(id)).conversation_id);
  if (!await session.commit(async (tx) => await tasks.canSettleScheduledIn(tx, id, conversation.id) &&
      (await tasks.canReleaseIn(tx, id) || await sources.reconcilesIn(tx, id, conversation.id, requestId)), BACKGROUND_CONTEXT)) {
    throw new TaskError("The scheduled turn stopped without an answer; resolve its task or explicitly reconcile its exact input in a successful recovery turn before this occurrence can settle");
  }
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
  let submission = await session.commit((tx) => tx.submissionByRequest(conversation.id, occurrence.id), BACKGROUND_CONTEXT);
  if (submission?.status === "done") return;
  if (submission?.status === "unanswered") { await requireReconciledTask(session, tasks, sources, id, occurrence.id); return; }
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
