import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import type { JsonValue } from "@earendil-works/chord";
import { BACKGROUND_CONTEXT, withAbortSignal } from "@earendil-works/chord/context";
import { openNodeSqliteDatabase } from "@earendil-works/chord/sqlite/node";
import { builtinModels } from "@earendil-works/pi-ai/providers/all";
import { AssistantEntry, createRegistry, defineDoc, GenerationTask, Harness, LiveDoc, ToolTask, UserEntry,
  type GenerationCheckpoint, type TaskId } from "@earendil-works/pi-durable";
import { SqliteStorage } from "@earendil-works/pi-durable/storage/sqlite";
import { BackgroundScheduler } from "../src/background.js";
import type { ScheduledJob } from "../src/generated/ScheduledJob.js";
import { DEFAULT_MODEL } from "../src/models.js";
import { scheduledTaskId } from "../src/scheduled.js";
import { TaskError, TaskStore } from "../src/tasks.js";
import { Executions } from "../src/tools.js";
import { pump } from "./fixtures/background-runtime.js";

const LegacyState = defineDoc<{ lanes: { id: string; last_run?: JsonValue }[] }>({
  kind: "agent.background", scope: "session", version: 1, initial: () => ({ lanes: [] }), checkpointWhen: () => true,
});
const job: ScheduledJob = { id: "generation-review", title: "Retained review", prompt: "Read existing evidence.",
  model: DEFAULT_MODEL, timezone: "Etc/UTC", revision: "original", cwd: "/",
  schedules: [{ id: "daily", recurrence: "DTSTART:20270101T090000Z\nRRULE:FREQ=DAILY", focus: null, prompt: null }] };

async function open(directory: string) {
  const storage = await SqliteStorage.open(await openNodeSqliteDatabase(join(directory, "session.sqlite")));
  const registry = createRegistry();
  const session = await Harness.open(storage, { models: builtinModels(), registry, identity: { attribution: null } }, BACKGROUND_CONTEXT);
  const tasks = new TaskStore(session);
  const scheduler = new BackgroundScheduler(session, registry, tasks, {
    // Calendar calculation is outside this regression. Pi owns the actual
    // execution, parked generation, durable input handle and retry timer.
    plan: async (_cadence, _after, now) => ({ due: null, next: now + 3_600_000 }),
    refreshSources: async () => {}, triageSources: async () => {}, collectResources: async () => {}, indexHistory: async () => {},
    runJob: async (occurrence, signal, actorId) => {
      const actor = await tasks.get(actorId), conversation = await tasks.conversation(actor.conversation_id);
      const record = await session.commit(tx => tx.submissionByRequest(conversation.id, occurrence.id), BACKGROUND_CONTEXT);
      assert.ok(record, "Existing admitted work must retain its native input");
      const submission = await session.submission(record.id, BACKGROUND_CONTEXT); assert.ok(submission);
      await submission.wait(withAbortSignal(signal, BACKGROUND_CONTEXT));
    },
  });
  return { session, storage, tasks, scheduler };
}

test("job inspection leaves an actor without a current native run untouched", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-job-no-generation-")), app = await open(directory);
  try {
    await app.scheduler.syncJobs([job]);
    const receipt = await app.scheduler.runJob({ type: "run_job", job_id: job.id, schedule_id: null, request_id: "not-admitted" });
    const actor = await app.tasks.get(receipt.task_id), conversation = await app.tasks.conversation(actor.conversation_id);
    const before = await app.session.snapshot(LiveDoc, conversation.id, BACKGROUND_CONTEXT);
    assert.equal(before?.run, undefined);
    await app.scheduler.getJob(job.id);
    assert.deepEqual(await app.session.snapshot(LiveDoc, conversation.id, BACKGROUND_CONTEXT), before);
    assert.deepEqual(await app.tasks.get(receipt.task_id), actor);
    await assert.rejects(app.scheduler.retryJob(job.id), /parked/);
  } finally { await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

for (const mode of ["queued", "legacy", "stopped"]) test(`job inspection and retry preserve the ${mode} actor generation and its other work`, { timeout: 15_000 }, async () => {
  const legacy = mode === "legacy";
  const directory = await mkdtemp(join(tmpdir(), "agent-job-generation-"));
  let app = await open(directory);
  const controller = new AbortController(), executions = new Executions();
  let worker: Promise<unknown> | undefined, actorConversation: string | undefined;
  const otherJob = { ...job, id: "unrelated-review" }, until = Date.now() + 3_600_000;
  const checkpoint: GenerationCheckpoint = { phase: "retry", attempt: 1, until };
  const protectedIDs: TaskId[] = [];
  try {
    await app.scheduler.syncJobs([job, otherJob]);
    const actor = await app.tasks.create({ id: scheduledTaskId("job:" + job.id), title: job.title, outcome: job.prompt,
      model: job.model, sources: [{ provider: "scheduled_jobs", account: "local", thread_id: job.id }] });
    const otherActor = await app.tasks.create({ id: scheduledTaskId("job:" + otherJob.id), title: otherJob.title, outcome: otherJob.prompt,
      model: otherJob.model, sources: [{ provider: "scheduled_jobs", account: "local", thread_id: otherJob.id }] });
    const conversation = await app.tasks.conversation(actor.conversation_id), other = await app.tasks.conversation(otherActor.conversation_id);
    actorConversation = actor.conversation_id;
    const binding = { host_id: "11111111-1111-4111-8111-111111111111", session_id: "session:22222222-2222-4222-8222-222222222222",
      window_id: "window:33333333-3333-4333-8333-333333333333", block_id: "block:44444444-4444-4444-8444-444444444444" };
    const host = { host_id: binding.host_id, name: "Fixture worker", rex_server: null, coordinator: null, agent_program: process.execPath, cwd: "/" };
    await app.tasks.update(actor.id, task => { task.worker = { state: "bound", allocation_id: "retained-allocation", host, binding }; });
    const current = await conversation.commit(async tx => {
      const entry = await tx.appendEntry(UserEntry, conversation.id,
        { model: [{ role: "user", content: "Retained job request", timestamp: 1 }] });
      const original = await tx.createSubmission({ type: "input", conversationId: conversation.id,
        requestId: legacy ? "manual:retained" : "previous-job", status: "placed", entry: entry.id });
      const generation = await tx.createTask(GenerationTask, {}, { ownership: { kind: "conversation" } });
      const live = await tx.doc(LiveDoc, conversation.id); live.run = { taskId: generation, inputs: [original.id] };
      live.generation = { attempt: 1 };
      const assistant = await tx.appendEntry(AssistantEntry, conversation.id, {});
      const tool = await tx.createTask(ToolTask, { kind: "model", assistant: assistant.id, callId: "never-replay" },
        { ownership: { kind: "conversation" } });
      const owned = await tx.createTask(GenerationTask, {}, { ownership: { kind: "task", taskId: tool } });
      const background = await tx.createTask(GenerationTask, {}, { ownership: { kind: "conversation" }, background: true });
      const stale = await tx.createTask(GenerationTask, {}, { ownership: { kind: "conversation" } });
      protectedIDs.push(tool, owned, background, stale);
      return { generation, submission: original.id };
    }, BACKGROUND_CONTEXT);
    const otherGeneration = await other.commit(async tx => {
      const entry = await tx.appendEntry(UserEntry, other.id, { model: [{ role: "user", content: "Independent job", timestamp: 1 }] });
      const submission = await tx.createSubmission({ type: "input", conversationId: other.id, requestId: "manual:other", status: "placed", entry: entry.id });
      const generation = await tx.createTask(GenerationTask, {}, { ownership: { kind: "conversation" } });
      const live = await tx.doc(LiveDoc, other.id); live.run = { taskId: generation, inputs: [submission.id] }; live.generation = { attempt: 1 };
      return generation;
    }, BACKGROUND_CONTEXT);
    // Persist genuine native crash records while the Harness is paused. The
    // future retry checkpoint prevents any provider request or model double.
    for (const id of [current.generation, ...protectedIDs, otherGeneration]) {
      const record = await app.session.commit(tx => tx.task(id), BACKGROUND_CONTEXT); assert.ok(record);
      assert.ok(record.state.status === "pending");
      await app.storage.commit([{ type: "task", value: id === otherGeneration
        ? { ...record, state: { status: "sleeping", until, checkpoint } }
        : { ...record, restarts: 5, state: { status: "pending", checkpoint: id === protectedIDs[0] ? record.state.checkpoint : checkpoint } } }], BACKGROUND_CONTEXT);
    }
    await app.session.close(BACKGROUND_CONTEXT); app = await open(directory);
    if (!legacy) await (await app.tasks.conversation(actor.conversation_id)).submit({ type: "input", requestId: "manual:retained",
      content: "Retained queued occurrence", whenBusy: "followUp" }, BACKGROUND_CONTEXT);
    const receipt = await app.scheduler.runJob({ type: "run_job", job_id: job.id, schedule_id: null, request_id: "retained" });
    const queued = await app.scheduler.runJob({ type: "run_job", job_id: job.id, schedule_id: null, request_id: "queued" });
    const unrelated = await app.scheduler.runJob({ type: "run_job", job_id: otherJob.id, schedule_id: null, request_id: "other" });
    assert.equal(receipt.task_id, actor.id); assert.equal(queued.task_id, actor.id); assert.equal(unrelated.task_id, otherActor.id);
    await pump(async () => typeof (await app.scheduler.getJob(job.id)).last_run?.started_at === "number");
    if (legacy) await app.session.commit(async tx => {
      const lane = (await tx.doc(LegacyState)).lanes.find(item => item.id === JSON.stringify(["job", job.id]));
      assert.ok(lane); delete lane.last_run;
    }, BACKGROUND_CONTEXT);
    const inputBefore = await app.session.commit(tx => tx.submissionByRequest(conversation.id, "manual:retained"), BACKGROUND_CONTEXT);
    const originalBefore = await app.session.getTask(current.generation, BACKGROUND_CONTEXT); assert.ok(originalBefore);
    const protectedBefore = await Promise.all([...protectedIDs, otherGeneration].map(id => app.session.getTask(id, BACKGROUND_CONTEXT)));
    const statusBefore = await app.scheduler.status(), before = await app.scheduler.getJob(job.id);
    if (!legacy) assert.match(before.schedules[0]?.error ?? "", /parked.*restart/iu,
      "Inspection must show the actor's parked generation instead of a running background waiter");
    if (legacy) assert.equal(before.last_run, undefined, "Legacy timing must remain unknown");
    else assert.deepEqual(before.last_run?.state, { status: "blocked", message: "restart_limit" });
    assert.deepEqual(await app.session.getTask(current.generation, BACKGROUND_CONTEXT), originalBefore, "Inspection must not retry work");
    await assert.rejects(app.scheduler.retryJob(otherJob.id), /parked/);
    await assert.rejects(app.scheduler.retryJob(job.id), error => error instanceof TaskError,
      "A saved allocation must not resume inference before its real execution owner binds");
    assert.deepEqual(await app.session.getTask(current.generation, BACKGROUND_CONTEXT), originalBefore);
    const noHost = async () => assert.fail("Native retry must not dispatch host effects");
    worker = app.tasks.run({ type: "run_task", task_id: actor.id, allocation_id: "retained-allocation", binding,
      instructions: "Retain the original admitted configuration.", model: job.model, cwd: "/", interactive: true },
    (native, executionHost, model) => executions.bind(native.id.toString(),
      { cwd: executionHost.cwd, hosts: [executionHost], model, role: "worker", call: noHost }), [], noHost, undefined,
    { signal: controller.signal, async ready() {}, answer() { assert.fail("The retained retry deadline has not elapsed"); }, idle() {} });
    await pump(() => app.tasks.isRunning(actor.id));
    if (mode === "stopped") {
      controller.abort();
      await assert.rejects(app.scheduler.retryJob(job.id), /existing worker must attach/);
      assert.deepEqual(await app.session.getTask(current.generation, BACKGROUND_CONTEXT), originalBefore,
        "A stopped execution owner must not grant another model attempt");
      assert.deepEqual(await Promise.all([...protectedIDs, otherGeneration].map(id => app.session.getTask(id, BACKGROUND_CONTEXT))), protectedBefore);
      assert.equal((await app.scheduler.status()).pending, statusBefore.pending);
      return;
    }
    await app.scheduler.retryJob(job.id);
    if (legacy) assert.match(before.schedules[0]?.error ?? "", /parked.*restart/iu,
      "Legacy job inspection must diagnose the actor without inventing run timing");
    await pump(async () => (await app.session.getTask(current.generation, BACKGROUND_CONTEXT))?.state.status === "sleeping");
    const resumed = await app.session.getTask(current.generation, BACKGROUND_CONTEXT); assert.ok(resumed);
    assert.equal(resumed.id, current.generation); assert.equal(resumed.restarts ?? 0, 0);
    assert.deepEqual(resumed.state.checkpoint, checkpoint);
    assert.deepEqual(await app.session.commit(tx => tx.submissionByRequest(conversation.id, "manual:retained"), BACKGROUND_CONTEXT), inputBefore);
    assert.deepEqual(await Promise.all([...protectedIDs, otherGeneration].map(id => app.session.getTask(id, BACKGROUND_CONTEXT))), protectedBefore);
    assert.equal((await app.scheduler.status()).pending, statusBefore.pending, "Queued occurrences must stay in their original lanes");
    assert.deepEqual(await app.scheduler.runJob({ type: "run_job", job_id: job.id, schedule_id: null, request_id: "queued" }), queued);
    assert.equal((await app.tasks.list()).length, 2);
    const after = await app.scheduler.getJob(job.id); assert.equal(after.task_id, actor.id);
    assert.equal(after.last_run === undefined, legacy); assert.equal(after.schedules[0]?.error, null);
    await assert.rejects(app.scheduler.retryJob(job.id), /parked/, "A live retry must not grant another generation attempt");
  } finally {
    controller.abort();
    if (worker !== undefined && actorConversation !== undefined) await (await app.tasks.conversation(actorConversation)).abort(BACKGROUND_CONTEXT);
    await worker?.catch(() => {});
    await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true });
  }
});
