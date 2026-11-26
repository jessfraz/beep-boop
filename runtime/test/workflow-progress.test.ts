import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { fauxAssistantMessage, fauxToolCall } from "@earendil-works/pi-ai";
import { defineDocFamily, type TaskId } from "@earendil-works/pi-durable";
import type { ScheduledJob } from "../src/generated/ScheduledJob.js";
import type { TaskRecord } from "../src/generated/TaskRecord.js";
import type { JobProgress } from "../src/generated/JobProgress.js";
import { WorkflowProgress } from "../src/workflow-progress.js";
import { runScheduled } from "../src/scheduled.js";
import { BackgroundScheduler } from "../src/background.js";
import { UserNoticeEntry } from "../src/transcript.js";
import { exampleJob, FakeClock, planned, pump } from "./fixtures/background-runtime.js";
import { openScheduled, scheduledOccurrence } from "./fixtures/scheduled-runtime.js";

function object(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}
async function open(directory: string, clock: FakeClock) {
  const app = await openScheduled(directory, "original", clock);
  const progress = new WorkflowProgress(app.session, app.registry, app.tasks, clock.now);
  progress.useReceipts((tx, id, conversation, request) => app.sources.reconcilesIn(tx, id, conversation, request),
    (id, conversation, request, signal) => app.sources.prepareReconciliations(id, conversation, request, signal));
  return { ...app, progress };
}
async function work(app: Awaited<ReturnType<typeof open>>, job: ScheduledJob, key: string, ownerId?: string) {
  const owner = ownerId ?? (await app.tasks.create({ title: job.title, outcome: job.prompt })).id;
  const task = await app.tasks.create({ title: job.title, outcome: job.prompt, model: job.model, execution: "coordinator",
    workflow: { job, owner_task_id: owner, work_key: key } });
  await app.tasks.enqueue(task.id, "Review this owned input under the captured workflow authority.", "original:" + key);
  return task;
}
async function notices(app: Awaited<ReturnType<typeof open>>) {
  const root = await app.session.root(BACKGROUND_CONTEXT);
  return (await root.entries({ order: "ascending" }, 100, undefined, BACKGROUND_CONTEXT)).items.filter(UserNoticeEntry.is);
}
async function noticeEvidence(app: Awaited<ReturnType<typeof open>>, notice: Awaited<ReturnType<typeof notices>>[number]) {
  const conversation = await app.tasks.conversation(notice.data.source_conversation_id ?? "1");
  const entry = (await conversation.entries({ order: "ascending" }, 100, undefined, BACKGROUND_CONTEXT)).items
    .find(entry => entry.id.toString() === notice.data.source_entry_id);
  assert.ok(entry); assert.equal(entry.kind, "agent.workflow.progress.alert"); return entry;
}
const RetainedRun = defineDocFamily<{ run: { title: string; last_progress_at: number; blocker: string | null; active: boolean; status: JobProgress["status"];
  recovery: { identity: string; task: TaskId<null> } | null;
  alert: { request_id: string; conversation_id: string; evidence_id: string; text: string } | null } | null }, null>({
  kind: "agent.workflow.progress.run", scope: "session", family: true, version: 1, initial: () => ({ run: null }), checkpointWhen: () => true,
});
const JobState = defineDocFamily<{ overdue: number; unavailable: number; incident?: { episode: number; active: boolean; notice: TaskId<null> | null } }, null>({
  kind: "agent.workflow.progress.job", scope: "session", family: true, version: 1,
  initial: () => ({ overdue: 0, unavailable: 0 }), checkpointWhen: () => true,
});
async function fail(app: Awaited<ReturnType<typeof open>>, id: string) {
  app.faux.setResponses([() => { throw new Error("The fixture provider is temporarily unavailable"); }]);
  await app.tasks.runBackground(id).catch(() => {});
}

test("an admitted workflow without a native input reports a dispatch blocker", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-workflow-dispatch-")), clock = new FakeClock(), app = await open(directory, clock);
  try {
    const job = { ...exampleJob(), priority: "critical" } satisfies ScheduledJob;
    const owner = await app.tasks.create({ title: job.title, outcome: job.prompt });
    const task = await app.tasks.create({ title: job.title, outcome: job.prompt, model: job.model, execution: "coordinator",
      workflow: { job, owner_task_id: owner.id, work_key: "admitted-before-dispatch" } });
    await app.progress.start(); clock.advance(120_001);
    await pump(async () => (await app.progress.status(job.id)).status === "overdue" && (await notices(app)).length === 1);
    const notice = (await notices(app))[0]; assert.ok(notice);
    assert.equal(notice.data.text, "Example review is delayed. No progress has been reported yet.");
    const status = await app.progress.status(job.id);
    assert.match(status.blocker ?? "", /admitted input has not completed dispatch/);
    assert.deepEqual((await noticeEvidence(app, notice)).data,
      { task_id: task.id, job_id: job.id, episode: 0, checked_at: clock.value, blocker: status.blocker });
    assert.deepEqual((await app.tasks.get(task.id)).pending, []);
    assert.equal(app.faux.state.callCount, 0);
  } finally { await app.close(); await rm(directory, { recursive: true, force: true }); }
});

for (const [minutes, elapsed] of [[12, "12 minutes"], [60, "1 hour"], [125, "2 hours"]] satisfies [number, string][]) {
  test(`workflow delay notices describe ${elapsed} in plain language while retaining exact diagnostic evidence`, async () => {
    const directory = await mkdtemp(join(tmpdir(), "agent-workflow-human-progress-")), clock = new FakeClock(), app = await open(directory, clock);
    let release = () => {};
    const recovery = new Promise<void>(resolve => { release = resolve; });
    try {
      const job = { ...exampleJob(), title: "package tracking", priority: "critical" } satisfies ScheduledJob;
      const task = await work(app, job, "relative-age"); clock.advance(1000);
      app.faux.setResponses([
        fauxAssistantMessage(fauxToolCall("record_task", { summary: "The provider's current result has been checked.",
          state: { state: "waiting", actor: "provider", next_check: new Date(clock.value + 60_000).toISOString() } }), { stopReason: "toolUse" }),
        fauxAssistantMessage("The current result is recorded."),
      ]);
      await app.tasks.runBackground(task.id);
      app.tasks.guardResources({ attach: async () => recovery });
      app.tasks.useBackgroundExecution({ available: () => false, extensions: () => [], bind: () => () => {},
        call: async () => ({ type: "failed", message: "The fixture execution transport is offline" }) });
      const progressedAt = clock.value;
      await app.progress.start(); await pump(async () => (await app.progress.status(job.id)).last_progress_at === progressedAt);
      clock.advance(minutes * 60_000 + 1); await pump(async () => (await notices(app)).length === 1);
      const notice = (await notices(app))[0]; assert.ok(notice);
      assert.equal(notice.data.text, `Package tracking is delayed. Last progress was ${elapsed} ago.`);
      assert.deepEqual(notice.data.notice_origin, { job_id: job.id, title: job.title });
      assert.deepEqual((await noticeEvidence(app, notice)).data,
        { task_id: task.id, job_id: job.id, episode: 0, checked_at: clock.value, blocker: "Its waiting state has no future recovery check" });
      assert.equal(app.commands.filter(command => command.type === "exec").length, 0);
    } finally { release(); await app.close(); await rm(directory, { recursive: true, force: true }); }
  });
}

test("unknown workflow progress times do not become invented elapsed times", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-workflow-unknown-progress-")), clock = new FakeClock(), app = await open(directory, clock);
  try {
    const job = { ...exampleJob(), priority: "critical" } satisfies ScheduledJob, task = await work(app, job, "unknown-time");
    await fail(app, task.id);
    await app.session.commit(async tx => { const saved = await tx.doc(RetainedRun, task.id, null); assert.ok(saved.run); saved.run.last_progress_at = 0; }, BACKGROUND_CONTEXT);
    await app.progress.start(); clock.advance(120_001); await pump(async () => (await notices(app)).length === 1);
    assert.equal((await notices(app))[0]?.data.text, "Example review is delayed. Its last progress time is unavailable.");
  } finally { await app.close(); await rm(directory, { recursive: true, force: true }); }
});

test("unavailable workflow notices keep raw errors and machine identities in diagnostic evidence", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-workflow-human-unavailable-")), clock = new FakeClock(), app = await open(directory, clock);
  const originalFind = app.tasks.findIn.bind(app.tasks);
  try {
    const job = { ...exampleJob(), title: "package tracking", priority: "critical" } satisfies ScheduledJob, task = await work(app, job, "unavailable");
    const detail = `native fixture failure for ${task.id} at ${new Date(clock.value).toISOString()}`;
    app.tasks.findIn = async (tx, id) => { if (id === task.id) throw new Error(detail); return originalFind(tx, id); };
    await app.progress.start(); clock.advance(120_001); await pump(async () => (await notices(app)).length === 1);
    const notice = (await notices(app))[0]; assert.ok(notice);
    assert.equal(notice.data.text, "Package tracking status is unavailable right now.");
    const status = await app.progress.status(job.id); assert.equal(status.status, "unavailable"); assert.ok(status.blocker?.includes(detail));
    assert.deepEqual((await noticeEvidence(app, notice)).data,
      { task_id: task.id, job_id: job.id, episode: 0, checked_at: clock.value, blocker: status.blocker });
    assert.equal(app.faux.state.callCount, 0);
  } finally { app.tasks.findIn = originalFind; await app.close(); await rm(directory, { recursive: true, force: true }); }
});

for (const published of [false, true]) test(`retained ${published ? "published" : "unsent"} legacy workflow alerts preserve native publication identity on upgrade`, async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-workflow-retained-copy-")), clock = new FakeClock();
  let app = await open(directory, clock);
  try {
    const job = { ...exampleJob(), priority: "critical" } satisfies ScheduledJob, task = await work(app, job, "retained-copy");
    await fail(app, task.id);
    await app.progress.start();
    const timer = (await app.session.inspect(BACKGROUND_CONTEXT)).tasks.find(item => item.record.kind === "agent.workflow_progress"); assert.ok(timer);
    const originalRoot = app.session.root.bind(app.session);
    let interrupted = false;
    app.session.root = async () => { interrupted = true; throw new Error("Fixture interruption before native notice admission"); };
    clock.advance(120_001);
    await pump(async () => interrupted);
    app.session.root = originalRoot;
    if ((await app.session.getTask(timer.record.id, BACKGROUND_CONTEXT))?.state.status !== "terminal") await app.session.abortTask(timer.record.id, BACKGROUND_CONTEXT);
    await pump(async () => (await app.session.getTask(timer.record.id, BACKGROUND_CONTEXT))?.state.status === "terminal");
    // Model a pre-incident store: only its per-task alert and native receipt
    // existed. Retire this producer's held publication before removing its projection.
    const staged = (await app.session.snapshot(JobState, job.id, BACKGROUND_CONTEXT))?.incident?.notice;
    if (staged !== null && staged !== undefined) {
      await app.session.abortTask(staged, BACKGROUND_CONTEXT);
      await pump(async () => (await app.session.getTask(staged, BACKGROUND_CONTEXT))?.state.status === "terminal");
      await app.session.commit(async tx => { delete (await tx.doc(JobState, job.id, null)).incident; }, BACKGROUND_CONTEXT);
    }
    const retained = (await app.session.snapshot(RetainedRun, task.id, BACKGROUND_CONTEXT))?.run;
    assert.ok(retained?.alert); const alert = retained.alert;
    const legacy = `${retained.title} needs attention: ${retained.blocker}. Last verified progress: ${new Date(retained.last_progress_at).toISOString()}. Task: ${task.id}.`;
    await app.session.commit(async tx => { const saved = (await tx.doc(RetainedRun, task.id, null)).run; assert.ok(saved?.alert); saved.alert.text = legacy; }, BACKGROUND_CONTEXT);
    if (published) {
      const root = await app.session.root(BACKGROUND_CONTEXT);
      await root.submit({ type: "write", requestId: alert.request_id, entry: { kind: UserNoticeEntry.kind, data: {
        task_id: task.id, source_conversation_id: alert.conversation_id, source_entry_id: alert.evidence_id,
        text: legacy, notice_origin: { job_id: job.id, title: job.title },
      } } }, BACKGROUND_CONTEXT);
    }
    const prior = (await notices(app))[0];
    await app.close(); app = await open(directory, clock); await app.progress.start(); clock.advance(120_001);
    await pump(async () => (await notices(app)).length === 1 && (await app.progress.status(job.id)).checked_at === clock.value);
    const notice = (await notices(app))[0]; assert.ok(notice);
    assert.equal(notice.data.text, published ? legacy : "Example review is delayed. No progress has been reported yet.");
    assert.equal(notice.data.source_entry_id, alert.evidence_id); assert.equal(notice.data.source_conversation_id, alert.conversation_id);
    if (prior !== undefined) assert.equal(notice.id, prior.id);
    const root = await app.session.root(BACKGROUND_CONTEXT);
    const receipt = await app.session.commit(tx => tx.submissionByRequest(root.id, alert.request_id), BACKGROUND_CONTEXT);
    assert.ok(receipt?.type === "write" && receipt.status === "done"); assert.equal(receipt.entry, notice.id);
    assert.equal(app.faux.state.callCount, 0);
  } finally { await app.close(); await rm(directory, { recursive: true, force: true }); }
});

test("five stalled tasks in one job produce one incident notice until every stalled task recovers", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-job-incident-")), clock = new FakeClock(), app = await open(directory, clock);
  try {
    const job = { ...exampleJob(), priority: "critical" } satisfies ScheduledJob;
    const owner = await app.tasks.create({ title: job.title, outcome: job.prompt });
    const stalled: TaskRecord[] = [];
    for (let index = 0; index < 5; index++) { const task = await work(app, job, "stalled-" + index, owner.id); stalled.push(task); await fail(app, task.id); }
    await app.progress.start(); clock.advance(120_001);
    await pump(async () => (await app.session.snapshot(JobState, job.id, BACKGROUND_CONTEXT))?.overdue === 5 && (await app.progress.status(job.id)).checked_at === clock.value);
    await pump(async () => (await notices(app)).length > 0);
    assert.equal((await notices(app)).length, 1, "A stalled job must not emit one Main notice per task");
    assert.equal((await app.progress.status(job.id)).pending_work, 5);
    for (const task of stalled) {
      const conversation = await app.tasks.conversation(task.conversation_id), entries = await conversation.entries({ order: "ascending" }, 100, undefined, BACKGROUND_CONTEXT);
      assert.equal(entries.items.filter(entry => entry.kind === "agent.workflow.progress.alert").length, 1);
    }
    const healthy = await work(app, job, "newer-healthy", owner.id);
    app.faux.setResponses([fauxAssistantMessage(fauxToolCall("record_task", { summary: "Current provider check completed.",
      state: { state: "waiting", actor: "provider", next_check: new Date(clock.value + 86_400_000).toISOString() } }), { stopReason: "toolUse" }), fauxAssistantMessage("")]);
    await app.tasks.runBackground(healthy.id);
    assert.equal((await app.progress.status(job.id)).status, "overdue", "A newer healthy task cannot hide older stalled work");
    const first = stalled[0]; assert.ok(first);
    await app.tasks.update(first.id, task => { task.state = { state: "waiting", actor: "provider", next_check: new Date(clock.value + 86_400_000).toISOString() }; });
    assert.equal((await app.session.snapshot(JobState, job.id, BACKGROUND_CONTEXT))?.overdue, 4);
    clock.advance(120_001); await pump(async () => (await app.progress.status(job.id)).checked_at === clock.value);
    assert.equal((await notices(app)).length, 1);
    for (const task of stalled.slice(1)) await app.tasks.update(task.id, current => { current.state = {
      state: "waiting", actor: "provider", next_check: new Date(clock.value + 86_400_000).toISOString(),
    }; });
    const cleared = await app.session.snapshot(JobState, job.id, BACKGROUND_CONTEXT);
    assert.equal(cleared?.overdue, 0); assert.equal(cleared?.incident?.active, false);
    const next = await work(app, job, "next-incident", owner.id); await fail(app, next.id);
    clock.advance(120_001); await pump(async () => (await notices(app)).length >= 2);
    assert.equal((await notices(app)).length, 2); assert.equal((await app.session.snapshot(JobState, job.id, BACKGROUND_CONTEXT))?.incident?.episode, 2);
  } finally { await app.close(); await rm(directory, { recursive: true, force: true }); }
});

for (const all of [false, true]) test(`pending job incident publication ${all ? "stays quiet after all recovery" : "survives its first task recovering"}`, async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-job-incident-fence-")), clock = new FakeClock(), app = await open(directory, clock);
  let release = () => {};
  const held = new Promise<void>(resolve => { release = resolve; });
  const originalRoot = app.session.root.bind(app.session);
  const originalFind = app.tasks.findIn.bind(app.tasks);
  try {
    const job = { ...exampleJob(), priority: "critical" } satisfies ScheduledJob, tasks: TaskRecord[] = [];
    for (let index = 0; index < 2; index++) { const task = await work(app, job, "fenced-" + index); tasks.push(task); await fail(app, task.id); }
    const unavailable = tasks[1]; assert.ok(unavailable);
    app.tasks.findIn = async (tx, id) => { if (id === unavailable.id) throw new Error("Fixture task evidence is unavailable"); return originalFind(tx, id); };
    await app.progress.start();
    let entered = false;
    app.session.root = async (context, options) => { if (!entered) { entered = true; await held; } return originalRoot(context, options); };
    clock.advance(120_001); await pump(async () => { const state = await app.session.snapshot(JobState, job.id, BACKGROUND_CONTEXT);
      return entered && state !== undefined && state.overdue + state.unavailable === 2; });
    const publication = (await app.session.snapshot(JobState, job.id, BACKGROUND_CONTEXT))?.incident?.notice; assert.ok(publication);
    const input = (await app.session.getTask(publication, BACKGROUND_CONTEXT))?.input; assert.ok(object(input) && typeof input.task_id === "string");
    const first = tasks.find(task => task.id === input.task_id); assert.ok(first);
    app.tasks.findIn = originalFind;
    for (const task of all ? tasks : [first]) await app.tasks.update(task.id, current => { current.state = {
      state: "waiting", actor: "provider", next_check: new Date(clock.value + 86_400_000).toISOString(),
    }; });
    release(); app.session.root = originalRoot;
    await pump(async () => (await app.session.getTask(publication, BACKGROUND_CONTEXT))?.state.status === "terminal");
    assert.equal((await notices(app)).length, all ? 0 : 1);
    const remaining = await app.session.snapshot(JobState, job.id, BACKGROUND_CONTEXT); assert.ok(remaining);
    assert.equal(remaining.overdue + remaining.unavailable, all ? 0 : 1);
  } finally { release(); app.session.root = originalRoot; app.tasks.findIn = originalFind; await app.close(); await rm(directory, { recursive: true, force: true }); }
});

test("a staged job incident survives reopening before and after its atomic native notice publication", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-job-incident-reopen-")), clock = new FakeClock();
  let app = await open(directory, clock);
  try {
    const job = { ...exampleJob(), priority: "critical" } satisfies ScheduledJob, task = await work(app, job, "durable-incident"); await fail(app, task.id);
    await app.progress.start();
    const originalRoot = app.session.root.bind(app.session); let interrupted = false;
    app.session.root = async () => { interrupted = true; throw new Error("Fixture publication transport is unavailable"); };
    clock.advance(120_001); await pump(async () => interrupted);
    app.session.root = originalRoot;
    const incident = (await app.session.snapshot(JobState, job.id, BACKGROUND_CONTEXT))?.incident; assert.ok(incident);
    const publication = incident.notice; assert.ok(publication);
    await pump(async () => (await app.session.getTask(publication, BACKGROUND_CONTEXT))?.state.status === "sleeping");
    assert.deepEqual(await notices(app), []);
    await app.close(); app = await open(directory, clock); await app.progress.start(); clock.advance(5000);
    await pump(async () => (await notices(app)).length === 1);
    const notice = (await notices(app))[0]; assert.ok(notice); assert.equal(notice.data.task_id, task.id);
    const root = await app.session.root(BACKGROUND_CONTEXT), request = "workflow-job-progress:" + JSON.stringify([job.id, incident.episode]);
    const receipt = await app.session.commit(tx => tx.submissionByRequest(root.id, request), BACKGROUND_CONTEXT);
    assert.ok(receipt?.type === "write" && receipt.status === "done"); assert.equal(receipt.entry, notice.id);
    assert.equal((await app.session.getTask(publication, BACKGROUND_CONTEXT))?.state.status, "terminal");
    await app.close(); app = await open(directory, clock); await app.progress.start(); clock.advance(120_001);
    await pump(async () => (await app.progress.status(job.id)).checked_at === clock.value);
    assert.deepEqual((await notices(app)).map(entry => entry.id), [notice.id]);
    assert.equal(app.faux.state.callCount, 0);
  } finally { await app.close(); await rm(directory, { recursive: true, force: true }); }
});

test("a delayed and reopened job notice computes relative age at its first native admission", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-job-notice-age-")), clock = new FakeClock();
  let app = await open(directory, clock);
  try {
    const job = { ...exampleJob(), priority: "critical" } satisfies ScheduledJob, task = await work(app, job, "delayed-publication");
    clock.advance(1000);
    app.faux.setResponses([fauxAssistantMessage(fauxToolCall("record_task", { summary: "A current provider result was recorded.",
      state: { state: "waiting", actor: "provider", next_check: new Date(clock.value + 60_000).toISOString() } }), { stopReason: "toolUse" }),
    () => { throw new Error("The fixture provider stopped after recording progress"); }]);
    await app.tasks.runBackground(task.id).catch(() => {});
    const progressedAt = clock.value;
    await app.progress.start();
    const originalRoot = app.session.root.bind(app.session); let interrupted = false;
    app.session.root = async () => { interrupted = true; throw new Error("Fixture notice publication is unavailable"); };
    clock.advance(120_001); await pump(async () => interrupted); app.session.root = originalRoot;
    const stagedAt = clock.value, publication = (await app.session.snapshot(JobState, job.id, BACKGROUND_CONTEXT))?.incident?.notice; assert.ok(publication);
    await pump(async () => (await app.session.getTask(publication, BACKGROUND_CONTEXT))?.state.status === "sleeping");
    assert.deepEqual(await notices(app), []);
    await app.close(); app = await open(directory, clock); clock.advance(58 * 60_000); await app.progress.start();
    await pump(async () => (await notices(app)).length === 1);
    const notice = (await notices(app))[0]; assert.ok(notice);
    assert.equal(notice.data.text, "Example review is delayed. Last progress was 1 hour ago.");
    const evidence = (await noticeEvidence(app, notice)).data; assert.ok(object(evidence)); assert.equal(evidence.checked_at, stagedAt);
    assert.equal((await app.progress.status(job.id)).last_progress_at, progressedAt);
    await app.close(); app = await open(directory, clock); clock.advance(60 * 60_000); await app.progress.start();
    await pump(async () => (await app.progress.status(job.id)).checked_at === clock.value);
    assert.deepEqual((await notices(app)).map(entry => ({ id: entry.id, text: entry.data.text })), [{ id: notice.id, text: notice.data.text }]);
  } finally { await app.close(); await rm(directory, { recursive: true, force: true }); }
});

test("waiting for a user without a timer stays quiet across reopen and wakes for the user's reply", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-workflow-user-wait-")), clock = new FakeClock();
  let app = await open(directory, clock), release = () => {};
  const recovery = new Promise<void>(resolve => { release = resolve; });
  try {
    const job = { ...exampleJob(), priority: "critical" } satisfies ScheduledJob, task = await work(app, job, "user-wait");
    app.faux.setResponses([
      fauxAssistantMessage(fauxToolCall("notify_user", { text: "Which of these two options should be used?" }), { stopReason: "toolUse" }),
      fauxAssistantMessage(fauxToolCall("record_task", { summary: "The question is delivered; waiting for the user's choice.",
        state: { state: "waiting", actor: "user", next_check: null } }), { stopReason: "toolUse" }), fauxAssistantMessage(""),
    ]);
    await app.tasks.runBackground(task.id); app.tasks.guardResources({ attach: async () => recovery });
    const delivered = (await notices(app)).map(entry => entry.id); assert.equal(delivered.length, 1);
    await app.progress.start(); clock.advance(120_001);
    await pump(async () => (await app.progress.status(job.id)).checked_at === clock.value);
    assert.deepEqual((await notices(app)).map(entry => entry.id), delivered, "An already delivered question must not cause a missing-timer warning");
    assert.equal((await app.progress.status(job.id)).status, "normal"); assert.equal((await app.progress.status(job.id)).pending_work, 1);
    const retained = (await app.session.snapshot(RetainedRun, task.id, BACKGROUND_CONTEXT))?.run; assert.ok(retained?.active); assert.equal(retained.recovery, null);
    assert.deepEqual(await app.tasks.dueChecks(clock.value), []); assert.equal(app.faux.state.callCount, 3);
    release(); await app.close(); app = await open(directory, clock); await app.progress.start(); clock.advance(600_001);
    await pump(async () => (await app.progress.status(job.id)).checked_at === clock.value);
    assert.deepEqual((await notices(app)).map(entry => entry.id), delivered); assert.equal(app.faux.state.callCount, 0);
    assert.equal((await app.progress.status(job.id)).pending_work, 1);
    await app.tasks.enqueue(task.id, "Use the first option.", "user-choice");
    app.faux.setResponses([fauxAssistantMessage(fauxToolCall("record_task", { summary: "The user's choice is processed.",
      state: { state: "resolved", evidence: "The exact new reply was handled under the existing workflow." } }), { stopReason: "toolUse" }), fauxAssistantMessage("Done.")]);
    await app.tasks.runBackground(task.id); clock.advance(30_001);
    await pump(async () => (await app.progress.status(job.id)).pending_work === 0);
    assert.equal((await app.tasks.get(task.id)).state.state, "resolved"); assert.deepEqual((await notices(app)).map(entry => entry.id), delivered);
  } finally { release(); await app.close(); await rm(directory, { recursive: true, force: true }); }
});

for (const boundary of ["past-due timer", "new pending input", "native failure"]) test(`an untimed waiting guard does not hide ${boundary}`, async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-workflow-wait-boundary-")), clock = new FakeClock(), app = await open(directory, clock);
  let release = () => {};
  const held = new Promise<void>(resolve => { release = resolve; });
  try {
    const job = { ...exampleJob(), priority: "critical" } satisfies ScheduledJob, task = await work(app, job, boundary);
    app.faux.setResponses([fauxAssistantMessage(fauxToolCall("record_task", { summary: "The current waiting party is recorded.",
      state: { state: "waiting", actor: "provider", next_check: boundary === "past-due timer" ? new Date(clock.value + 60_000).toISOString() : null } }), { stopReason: "toolUse" }),
    ...(boundary === "native failure" ? [() => { throw new Error("The fixture native input is unanswered"); }] : [fauxAssistantMessage("")])]);
    await app.tasks.runBackground(task.id).catch(() => {});
    if (boundary === "new pending input") {
      await app.tasks.enqueue(task.id, "New evidence needs processing.", "new-evidence");
      await app.tasks.update(task.id, current => { current.state = { state: "waiting", actor: "provider", next_check: null }; });
    }
    app.tasks.guardResources({ attach: async () => held });
    app.tasks.useBackgroundExecution({ available: () => false, extensions: () => [], bind: () => () => {},
      call: async () => ({ type: "failed", message: "The fixture execution transport is offline" }) });
    await app.progress.start(); clock.advance(120_001);
    await pump(async () => (await notices(app)).length === 1);
    assert.equal((await app.progress.status(job.id)).status, "overdue");
    assert.equal((await app.progress.status(job.id)).pending_work, 1);
  } finally { release(); await app.close(); await rm(directory, { recursive: true, force: true }); }
});

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
