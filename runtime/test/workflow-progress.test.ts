import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { fauxAssistantMessage, fauxToolCall } from "@earendil-works/pi-ai";
import type { ScheduledJob } from "../src/generated/ScheduledJob.js";
import { WorkflowProgress } from "../src/workflow-progress.js";
import { runScheduled } from "../src/scheduled.js";
import { BackgroundScheduler } from "../src/background.js";
import { UserNoticeEntry } from "../src/transcript.js";
import { exampleJob, FakeClock, planned, pump } from "./fixtures/background-runtime.js";
import { openScheduled, scheduledOccurrence } from "./fixtures/scheduled-runtime.js";

async function open(directory: string, clock: FakeClock) {
  const app = await openScheduled(directory, "original", clock);
  const progress = new WorkflowProgress(app.session, app.registry, app.tasks, clock.now);
  progress.useReceipts((tx, id, conversation, request) => app.sources.reconcilesIn(tx, id, conversation, request));
  return { ...app, progress };
}
async function work(app: Awaited<ReturnType<typeof open>>, job: ScheduledJob, key: string) {
  const owner = await app.tasks.create({ title: job.title, outcome: job.prompt });
  const task = await app.tasks.create({ title: job.title, outcome: job.prompt, model: job.model, execution: "coordinator",
    workflow: { job, owner_task_id: owner.id, work_key: key } });
  await app.tasks.enqueue(task.id, "Review this owned input under the captured workflow authority.", "original:" + key);
  return task;
}
async function notices(app: Awaited<ReturnType<typeof open>>) {
  const root = await app.session.root(BACKGROUND_CONTEXT);
  return (await root.entries({ order: "ascending" }, 100, undefined, BACKGROUND_CONTEXT)).items.filter(UserNoticeEntry.is);
}
async function fail(app: Awaited<ReturnType<typeof open>>, id: string) {
  app.faux.setResponses([() => { throw new Error("The fixture provider is temporarily unavailable"); }]);
  await app.tasks.runBackground(id).catch(() => {});
}

test("an older overdue workflow survives restart and a newer success without duplicate Main alerts", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-workflow-progress-")), clock = new FakeClock();
  let app = await open(directory, clock);
  try {
    const job = { ...exampleJob(), priority: "critical" } satisfies ScheduledJob;
    const older = await work(app, job, "older"); await fail(app, older.id);
    await app.progress.start();
    clock.advance(120_001);
    await pump(async () => (await app.progress.status(job.id)).status === "overdue" && (await notices(app)).length === 1);
    const before = (await notices(app))[0]; assert.ok(before);
    assert.equal(before.model, undefined, "Monitoring must not fabricate assistant or user context");
    const olderConversation = await app.tasks.conversation(older.conversation_id);
    const original = await app.session.commit(tx => tx.submissionByRequest(olderConversation.id, "original:older"), BACKGROUND_CONTEXT);
    assert.equal(original?.status, "unanswered");
    const newer = await work(app, job, "newer");
    app.faux.setResponses([
      fauxAssistantMessage(fauxToolCall("exec", { program: "status-cli", args: ["current"] }), { stopReason: "toolUse" }),
      fauxAssistantMessage(fauxToolCall("record_task", { summary: "Current readback verified.", state: { state: "resolved", evidence: "Verified current host readback." } }), { stopReason: "toolUse" }),
      fauxAssistantMessage("The current review is verified."),
    ]);
    await app.tasks.runBackground(newer.id);
    clock.advance(30_001);
    await pump(async () => (await app.progress.status(job.id)).pending_work === 1);
    assert.equal((await app.progress.status(job.id)).status, "overdue", "The newest successful run cannot hide older stalled work");
    const timer = (await app.session.inspect(BACKGROUND_CONTEXT)).tasks.find(item => item.record.kind === "agent.workflow_progress");
    assert.ok(timer); await app.session.abortTask(timer.record.id, BACKGROUND_CONTEXT);
    await pump(async () => (await app.session.getTask(timer.record.id, BACKGROUND_CONTEXT))?.state.status === "terminal");
    await app.close(); app = await open(directory, clock); await app.progress.start();
    assert.equal((await app.session.getTask(timer.record.id, BACKGROUND_CONTEXT))?.state.status, "terminal", "Timer recovery must preserve the original terminal receipt");
    clock.advance(120_001);
    await pump(async () => (await app.progress.status(job.id)).checked_at === clock.value);
    assert.deepEqual((await notices(app)).map(entry => entry.id), [before.id]);
    assert.equal(app.faux.state.callCount, 0, "Monitoring and publication recovery require no model loop");
  } finally { await app.close(); await rm(directory, { recursive: true, force: true }); }
});

test("future expected waits stay quiet while independent critical workflows meet stricter deadlines", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-workflow-waits-")), clock = new FakeClock(), app = await open(directory, clock);
  try {
    const waitingJob = { ...exampleJob(), id: "expected-wait", priority: "critical" } satisfies ScheduledJob;
    const waiting = await work(app, waitingJob, "expected-wait");
    app.faux.setResponses([
      fauxAssistantMessage(fauxToolCall("record_task", { summary: "The provider's next status check is scheduled.",
        state: { state: "waiting", actor: "provider", next_check: new Date(clock.value + 3_600_000).toISOString() } }), { stopReason: "toolUse" }),
      fauxAssistantMessage("Waiting until the recorded check."),
    ]);
    await app.tasks.runBackground(waiting.id);
    const firstJob = { ...exampleJob(), id: "first-critical", priority: "critical" } satisfies ScheduledJob;
    const secondJob = { ...exampleJob(), id: "second-critical", priority: "critical" } satisfies ScheduledJob;
    const ordinaryJob = { ...exampleJob(), id: "ordinary" };
    const first = await work(app, firstJob, "first"), second = await work(app, secondJob, "second"), ordinary = await work(app, ordinaryJob, "ordinary");
    await fail(app, first.id); await fail(app, second.id); await fail(app, ordinary.id);
    await app.progress.start(); clock.advance(120_001);
    await pump(async () => (await notices(app)).length === 2);
    assert.equal((await app.progress.status(waitingJob.id)).status, "normal");
    assert.equal((await app.progress.status(firstJob.id)).status, "overdue");
    assert.equal((await app.progress.status(secondJob.id)).status, "overdue");
    assert.equal((await app.progress.status(ordinaryJob.id)).status, "normal");
    assert.deepEqual(new Set((await notices(app)).map(entry => entry.data.notice_origin?.job_id)), new Set([firstJob.id, secondJob.id]));
  } finally { await app.close(); await rm(directory, { recursive: true, force: true }); }
});

test("completed native input without task resolution remains monitored and visible coverage is explicit", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-workflow-unresolved-")), clock = new FakeClock(), app = await open(directory, clock);
  try {
    const job = { ...exampleJob(), priority: "critical" } satisfies ScheduledJob;
    assert.equal((await app.progress.status(job.id)).status, "unavailable");
    const task = await work(app, job, "unresolved");
    app.faux.setResponses([fauxAssistantMessage("The model turn ended without a verified task outcome.")]);
    await app.tasks.runBackground(task.id);
    app.tasks.useBackgroundExecution({ available: () => false, extensions: () => [], bind: () => () => {},
      call: async () => ({ type: "failed", message: "The fixture execution transport is offline" }) });
    await app.progress.start(); clock.advance(120_001);
    await pump(async () => (await notices(app)).length === 1);
    assert.equal((await app.progress.status(job.id)).pending_work, 1);
    const progress = await app.progress.status(job.id);
    assert.equal(progress.status, "overdue", progress.blocker ?? "Expected overdue progress");
    clock.advance(120_001);
    await pump(async () => (await app.progress.status(job.id)).checked_at === clock.value);
    assert.equal((await notices(app)).length, 1);
    assert.equal(app.faux.state.callCount, 1);
  } finally { await app.close(); await rm(directory, { recursive: true, force: true }); }
});

test("one retained continuation verifies unfinished work without repeating its completed host effect", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-workflow-reconcile-")), clock = new FakeClock(), app = await open(directory, clock);
  try {
    const job = { ...exampleJob(), priority: "critical" } satisfies ScheduledJob;
    const task = await work(app, job, "needs-reconciliation");
    app.faux.setResponses([
      fauxAssistantMessage(fauxToolCall("exec", { program: "status-cli", args: ["current"] }), { stopReason: "toolUse" }),
      fauxAssistantMessage("The read finished, but the task outcome has not been recorded."),
      context => {
        assert.match(JSON.stringify(context.messages), /never repeat uncertain effects/);
        return fauxAssistantMessage(fauxToolCall("record_task", { summary: "The retained current readback was verified.",
          state: { state: "resolved", evidence: "Verified the already completed readback without another host action." } }), { stopReason: "toolUse" });
      },
      fauxAssistantMessage("Verified and resolved."),
    ]);
    await app.tasks.runBackground(task.id);
    await app.progress.start(); clock.advance(120_001);
    await pump(async () => (await app.tasks.get(task.id)).state.state === "resolved" && !app.tasks.isRunning(task.id));
    clock.advance(30_001);
    await pump(async () => (await app.progress.status(job.id)).pending_work === 0);
    assert.equal(app.commands.filter(command => command.type === "exec").length, 1);
    assert.equal((await notices(app)).length, 1);
    assert.equal((await app.progress.status(job.id)).status, "normal");
    clock.advance(600_001);
    await pump(async () => (await app.progress.status(job.id)).checked_at === clock.value);
    assert.equal(app.faux.state.callCount, 4);
    assert.equal((await notices(app)).length, 1);
  } finally { await app.close(); await rm(directory, { recursive: true, force: true }); }
});

for (const boundary of ["scheduled consumer", "startup adoption", "job sync"]) test(`a retained legacy job actor remains covered through ${boundary} without changing its ownership or conversation`, async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-workflow-legacy-")), clock = new FakeClock(), app = await open(directory, clock);
  try {
    const job = { ...exampleJob(), priority: "critical" } satisfies ScheduledJob;
    const legacy = await app.tasks.create({ title: job.title, outcome: job.prompt, model: job.model,
      sources: [{ provider: "scheduled_jobs", account: "local", thread_id: job.id }] });
    if (boundary === "scheduled consumer") {
      app.faux.setResponses([() => { throw new Error("The retained scheduled input failed"); }]);
      await runScheduled({ ...scheduledOccurrence, job }, new AbortController().signal, app, legacy.id).catch(() => {});
      assert.equal((await app.progress.status(job.id)).pending_work, 1);
    } else {
      await app.tasks.enqueueScheduled(legacy.id, "Continue the retained owned input.", { ...scheduledOccurrence, job });
      await fail(app, legacy.id);
    }
    if (boundary === "job sync") {
      const scheduler = new BackgroundScheduler(app.session, app.registry, app.tasks, {
        plan: async (cadence, after, now) => planned(cadence, after, now), refreshSources: async () => {},
        triageSources: async () => {}, collectResources: async () => {}, indexHistory: async () => {},
        runJob: (occurrence, signal, actor) => runScheduled(occurrence, signal, app, actor),
      });
      await scheduler.syncJobs([job]);
      assert.equal((await app.progress.status(job.id)).pending_work, 1);
    }
    await app.progress.start(boundary === "job sync" ? [] : [job]); clock.advance(120_001);
    await pump(async () => (await app.progress.status(job.id)).checked_at === clock.value);
    const retained = await app.tasks.get(legacy.id), status = await app.progress.status(job.id);
    assert.equal(retained.workflow, undefined); assert.equal(retained.conversation_id, legacy.conversation_id);
    assert.deepEqual(retained.sources, legacy.sources);
    assert.equal(status.status, "overdue"); assert.equal(status.pending_work, 1);
    assert.equal((await notices(app)).length, 1);
  } finally { await app.close(); await rm(directory, { recursive: true, force: true }); }
});

test("resolution while recovery attaches resources fences obsolete continuation admission", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-workflow-recovery-fence-")), clock = new FakeClock(), app = await open(directory, clock);
  let entered = () => {}, release = () => {};
  const attaching = new Promise<void>(resolve => { entered = resolve; }), held = new Promise<void>(resolve => { release = resolve; });
  try {
    const job = { ...exampleJob(), priority: "critical" } satisfies ScheduledJob;
    const task = await work(app, job, "admission-fence");
    app.faux.setResponses([fauxAssistantMessage("The original turn ended without a recorded outcome.")]);
    await app.tasks.runBackground(task.id);
    app.tasks.guardResources({ attach: async id => { if (id === task.id) { entered(); await held; } } });
    await app.progress.start(); clock.advance(120_001); await attaching;
    const recovery = (await app.session.inspect(BACKGROUND_CONTEXT)).tasks.find(item => item.record.kind === "agent.workflow_progress_recovery");
    assert.ok(recovery);
    await app.tasks.update(task.id, value => { value.state = { state: "resolved", evidence: "Verified current outcome while recovery admission waited." }; });
    clock.advance(30_001);
    await pump(async () => (await app.progress.status(job.id)).pending_work === 0);
    release();
    await pump(async () => (await app.session.getTask(recovery.record.id, BACKGROUND_CONTEXT))?.state.status === "terminal");
    const resolved = await app.tasks.get(task.id);
    assert.equal(resolved.state.state, "resolved"); assert.deepEqual(resolved.pending, []);
    assert.equal((await app.progress.status(job.id)).pending_work, 0);
    assert.equal(app.faux.state.callCount, 1);
  } finally { release(); await app.close(); await rm(directory, { recursive: true, force: true }); }
});

test("a delayed failure summary cannot postpone the original verified-progress deadline", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-workflow-failure-progress-")), clock = new FakeClock(), app = await open(directory, clock);
  try {
    const job = { ...exampleJob(), priority: "critical" } satisfies ScheduledJob;
    const task = await work(app, job, "failure-diagnostic"); await fail(app, task.id);
    await app.progress.start(); clock.advance(60_000);
    await app.tasks.update(task.id, current => { current.summary = "A later failure diagnostic, with no verified result or state transition."; });
    clock.advance(60_001);
    await pump(async () => (await app.progress.status(job.id)).checked_at === clock.value);
    assert.equal((await app.progress.status(job.id)).status, "overdue");
    assert.equal((await notices(app)).length, 1);
  } finally { await app.close(); await rm(directory, { recursive: true, force: true }); }
});
