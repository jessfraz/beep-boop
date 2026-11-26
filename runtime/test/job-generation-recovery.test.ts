import { commitWrites } from "@earendil-works/pi-durable/testing";
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { copyJson } from "@earendil-works/chord/json";
import { openNodeSqliteDatabase } from "@earendil-works/chord/sqlite/node";
import { fauxAssistantMessage, fauxToolCall } from "@earendil-works/pi-ai";
import { raceWithAbortSignal } from "@earendil-works/pi-ai/utils/abort";
import { defineDoc, GenerationTask, LiveDoc, UserEntry, type TaskId } from "@earendil-works/pi-durable";
import { SqliteStorage } from "@earendil-works/pi-durable/storage/sqlite";
import { BackgroundScheduler, type BackgroundCallbacks } from "../src/background.js";
import type { ScheduledJob } from "../src/generated/ScheduledJob.js";
import type { BackgroundTriggerKey } from "../src/generated/BackgroundTriggerKey.js";
import type { BackgroundWork } from "../src/generated/BackgroundWork.js";
import { openJobActor, runScheduled, scheduledTaskId } from "../src/scheduled.js";
import { openScheduled } from "./fixtures/scheduled-runtime.js";
import { FakeClock, pump } from "./fixtures/background-runtime.js";
import { fixtureModel } from "./model.js";

const State = defineDoc<{ lanes: { id: string; active: TaskId | null; actor_task_id: string | null;
  active_runs?: TaskId[]; last_run?: { task: TaskId }; recoveries?: { task: TaskId; recovery_of?: TaskId; actor_task_id?: string }[];
  pending: { key: BackgroundTriggerKey; work: BackgroundWork }[] }[] }>({
  kind: "agent.background", scope: "session", version: 1, initial: () => ({ lanes: [] }), checkpointWhen: () => true,
});
const job: ScheduledJob = { id: "generation-review", title: "Retained review", prompt: "Read existing evidence.",
  model: fixtureModel, timezone: "Etc/UTC", revision: "original", cwd: "/workspace/example",
  schedules: [{ id: "daily", recurrence: "DTSTART:20270101T090000Z\nRRULE:FREQ=DAILY", focus: null, prompt: null }] };

function background(app: Awaited<ReturnType<typeof openScheduled>>, runJob?: BackgroundCallbacks["runJob"]) {
  return new BackgroundScheduler(app.session, app.registry, app.tasks, {
    plan: async (_cadence, _after, now) => ({ due: null, next: now + 3_600_000 }),
    refreshSources: async () => {}, triageSources: async () => {}, collectResources: async () => {}, indexHistory: async () => {},
    jobActor: applied => openJobActor(applied, app.tasks),
    runJob: runJob ?? ((occurrence, signal, actor) => runScheduled(occurrence, signal, app, actor)),
  });
}
function gate() {
  let release = () => {};
  const promise = new Promise<void>(resolve => { release = resolve; });
  return { promise, release };
}

/** Seed a real native invocation interrupted at the crash limit, before any provider request. */
async function interrupted(app: Awaited<ReturnType<typeof openScheduled>>, directory: string, requestId: string, legacy = false) {
  const scheduler = background(app, async () => { throw new Error("The fixture host stopped before native input placement"); });
  await scheduler.syncJobs([job]);
  await scheduler.runJob({ type: "run_job", job_id: job.id, schedule_id: null, request_id: "queued" });
  const receipt = await scheduler.runJob({ type: "run_job", job_id: job.id, schedule_id: null, request_id: "retained" });
  await pump(async () => (await app.session.inspect(BACKGROUND_CONTEXT)).tasks.filter(item => {
    const input = item.record.input;
    return typeof input === "object" && input !== null && "type" in input && input.type === "work";
  }).every(item => item.state.kind === "sleeping"));
  await app.close(); app = await openScheduled(directory);
  const work = (await app.session.inspect(BACKGROUND_CONTEXT)).tasks.filter(item => {
    const input = item.record.input;
    return typeof input === "object" && input !== null && "type" in input && input.type === "work";
  });
  await commitWrites(app.storage, work.map(item => {
    assert.ok(item.record.state.status === "sleeping");
    return { type: "task", value: { ...item.record, state: { status: "pending", checkpoint: item.record.state.checkpoint } } };
  }), BACKGROUND_CONTEXT);
  const occurrenceActor = await app.tasks.get(receipt.task_id);
  const owner = occurrenceActor.workflow?.owner_task_id; assert.ok(owner);
  const actor = legacy ? await app.tasks.get(owner) : occurrenceActor;
  if (legacy) {
    const retained = work.find(task => task.record.memos?.job_actor_task_id === occurrenceActor.id); assert.ok(retained);
    const record = await app.session.getTask(retained.record.id, BACKGROUND_CONTEXT); assert.ok(record?.state.status === "pending");
    const input = copyJson(record.input);
    assert.ok(typeof input === "object" && input !== null && !Array.isArray(input) && typeof input.work === "object" && input.work !== null &&
      !Array.isArray(input.work) && typeof input.work.occurrence === "object" && input.work.occurrence !== null && !Array.isArray(input.work.occurrence));
    delete input.work.occurrence.task_id;
    input.work.occurrence.id = requestId;
    await commitWrites(app.storage, [{ type: "task", value: { ...record, state: record.state, input,
      memos: { ...record.memos, job_actor_task_id: actor.id } } }], BACKGROUND_CONTEXT);
  }
  const conversation = await app.tasks.conversation(actor.conversation_id);
  await app.tasks.inference.configure(conversation, job.model, { instructions: job.prompt, cwd: job.cwd });
  const retained = await conversation.commit(async tx => {
    const entry = await tx.appendEntry(UserEntry, conversation.id, { model: [{ role: "user", content: "Retained job request", timestamp: 1 }] });
    const original = await tx.createSubmission({ type: "input", conversationId: conversation.id, requestId, status: "placed", entry: entry.id });
    const generation = await tx.createTask(GenerationTask, {}, { ownership: { kind: "conversation" } });
    const live = await tx.doc(LiveDoc, conversation.id); live.run = { taskId: generation, inputs: [original.id], turns: 0 };
    live.generation = { attempt: 1 };
    return { generation, submission: original.id, entry: entry.id };
  }, BACKGROUND_CONTEXT);
  const generation = await app.session.getTask(retained.generation, BACKGROUND_CONTEXT); assert.ok(generation);
  await commitWrites(app.storage, [{ type: "task", value: { ...generation, restarts: 5, state: { status: "pending",
    checkpoint: { phase: "request", attempt: 1, model: { provider: "missing", modelId: "unavailable" },
      thinkingLevel: "off", streamOptions: {}, cutoff: retained.entry } } } }], BACKGROUND_CONTEXT);
  return { ...retained, actor, app };
}

test("job inspection leaves an actor without a native run untouched", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-job-no-generation-")), app = await openScheduled(directory);
  try {
    const scheduler = background(app); await scheduler.syncJobs([job]);
    const actor = await app.tasks.create({ id: scheduledTaskId("job:" + job.id), title: job.title, outcome: job.prompt,
      model: job.model, execution: "coordinator", sources: [{ provider: "scheduled_jobs", account: "local", thread_id: job.id }] });
    const conversation = await app.tasks.conversation(actor.conversation_id), before = await app.session.snapshot(LiveDoc, conversation.id, BACKGROUND_CONTEXT);
    const result = await scheduler.getJob(job.id);
    assert.equal(result.last_run, undefined); assert.equal(result.task_id, undefined);
    assert.deepEqual(await app.session.snapshot(LiveDoc, conversation.id, BACKGROUND_CONTEXT), before);
    assert.deepEqual(await app.tasks.get(actor.id), actor); assert.equal(app.faux.state.callCount, 0);
    await assert.rejects(scheduler.retryJob(job.id), /interrupted|parked/);
  } finally { await app.close(); await rm(directory, { recursive: true, force: true }); }
});

test("explicit retry wakes one retained sleeping occurrence without cloning its replacement or replaying effects", { timeout: 15_000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-job-preplacement-retry-")), clock = new FakeClock();
  let app = await openScheduled(directory, "original", clock), available = false;
  const held = gate(), entered = gate();
  const callback: BackgroundCallbacks["runJob"] = async (occurrence, signal, actor) => {
    if (!available) throw new Error("The fixture stopped before native input placement");
    await runScheduled(occurrence, signal, app, actor);
  };
  try {
    let scheduler = background(app, callback); await scheduler.syncJobs([job]);
    const request = { type: "run_job", request_id: "retained-sleeping", job_id: job.id, schedule_id: null } satisfies Parameters<BackgroundScheduler["runJob"]>[0];
    const receipt = await scheduler.runJob(request);
    await pump(async () => (await scheduler.getJob(job.id)).last_run?.state.status === "retrying");
    const initial = (await app.session.snapshot(State, BACKGROUND_CONTEXT))?.lanes.find(lane => lane.id === JSON.stringify(["job", job.id]))?.last_run;
    assert.ok(initial);
    const original = await app.session.getTask(initial.task, BACKGROUND_CONTEXT); assert.ok(original);
    const task = await app.tasks.get(receipt.task_id), conversationId = task.conversation_id;
    const conversation = await app.tasks.conversation(conversationId);
    assert.equal(await app.session.commit(tx => tx.submissionByRequest(conversation.id, "manual:retained-sleeping"), BACKGROUND_CONTEXT), undefined);
    assert.equal(app.faux.state.callCount, 0);
    await app.close(); app = await openScheduled(directory, "restored", clock);
    scheduler = background(app, callback); await scheduler.syncJobs([job]);
    await scheduler.retryJob(job.id);
    await pump(async () => (await scheduler.getJob(job.id)).last_run?.state.status === "retrying");
    const replacement = (await app.session.snapshot(State, BACKGROUND_CONTEXT))?.lanes.find(lane => lane.id === JSON.stringify(["job", job.id]))?.last_run;
    assert.ok(replacement); assert.notEqual(replacement.task, initial.task);
    const replacementTask = await app.session.getTask(replacement.task, BACKGROUND_CONTEXT); assert.ok(replacementTask);
    assert.deepEqual(replacementTask.input, original.input);
    await pump(async () => (await app.session.getTask(initial.task, BACKGROUND_CONTEXT))?.state.status === "terminal");
    await Promise.all([scheduler.retryJob(job.id), scheduler.retryJob(job.id)]);
    assert.equal((await app.session.snapshot(State, BACKGROUND_CONTEXT))?.lanes.find(lane => lane.id === JSON.stringify(["job", job.id]))?.last_run?.task, replacement.task);
    assert.deepEqual(await scheduler.runJob(request), receipt);
    assert.equal((await app.tasks.get(receipt.task_id)).conversation_id, conversationId);
    available = true;
    app.faux.setResponses([async () => {
      entered.release(); await held.promise;
      return fauxAssistantMessage(fauxToolCall("exec", { program: "example", args: ["verify"] }), { stopReason: "toolUse" });
    }, fauxAssistantMessage("The retained occurrence verified its original work once.")]);
    clock.advance(60_000); await entered.promise;
    await scheduler.retryJob(job.id);
    assert.equal((await app.session.snapshot(State, BACKGROUND_CONTEXT))?.lanes.find(lane => lane.id === JSON.stringify(["job", job.id]))?.last_run?.task, replacement.task);
    held.release(); await pump(async () => (await scheduler.status()).pending === 0);
    assert.equal((await scheduler.getJob(job.id)).last_run?.state.status, "completed");
    await scheduler.retryJob(job.id);
    assert.equal((await app.session.snapshot(State, BACKGROUND_CONTEXT))?.lanes.find(lane => lane.id === JSON.stringify(["job", job.id]))?.last_run?.task, replacement.task);
    assert.equal(app.commands.filter(command => command.type === "exec").length, 1);
    assert.equal(app.faux.state.callCount, 2);
    assert.deepEqual(await scheduler.runJob(request), receipt);
  } finally { held.release(); await app.close(); await rm(directory, { recursive: true, force: true }); }
});

for (const variant of ["current", "legacy", "legacy with newer healthy run", "independent with newer healthy run", "canonical with newer healthy run"]) test(`explicit job recovery uses one new input in the ${variant} retained actor`, { timeout: 20_000 }, async () => {
  const canonical = variant.startsWith("canonical"), legacy = variant.startsWith("legacy") || canonical, newer = variant.includes("newer");
  const originalRequest = canonical ? "legacy:retained" : "manual:retained";
  const directory = await mkdtemp(join(tmpdir(), "agent-job-generation-")); let app = await openScheduled(directory);
  const held = gate(), entered = gate(), independent = gate(); let recovery: Promise<unknown> | undefined, duplicate: Promise<unknown> | undefined;
  const callback: BackgroundCallbacks["runJob"] = async (occurrence, signal, actor) => {
    if (occurrence.id === "manual:independent") await raceWithAbortSignal(independent.promise, signal);
    else await runScheduled(occurrence, signal, app, actor);
  };
  try {
    const retained = await interrupted(app, directory, originalRequest, canonical || legacy && !newer); app = retained.app;
    await app.close(); app = await openScheduled(directory);
    const ended = await app.session.waitForTask(retained.generation, BACKGROUND_CONTEXT); assert.equal(ended.state.outcome.status, "failed");
    assert.equal(app.faux.state.callCount, 0);
    const conversation = await app.tasks.conversation(retained.actor.conversation_id);
    const original = await app.session.commit(tx => tx.submissionByRequest(conversation.id, originalRequest), BACKGROUND_CONTEXT);
    assert.ok(original?.status === "unanswered"); assert.equal(original.reason, "restart_limit");
    app.configureBackground(async command => {
      let selected = command;
      while (selected.type === "on_node") selected = selected.command;
      if (selected.type === "exec" && selected.args[0] === "uncertain") {
        app.commands.push(selected);
        return { type: "failed", delivery: "outcome_uncertain", message: "The fixture lost confirmation after dispatch" };
      }
      return app.call(command);
    });
    app.faux.setResponses([
      fauxAssistantMessage(fauxToolCall("exec", { program: "example", args: ["settled"] }), { stopReason: "toolUse" }),
      fauxAssistantMessage(fauxToolCall("exec", { program: "example", args: ["uncertain"] }), { stopReason: "toolUse" }),
      fauxAssistantMessage("The independent occurrence completed without retrying its uncertain effect."),
    ]);
    let scheduler = background(app, callback); await scheduler.syncJobs([job]);
    const queued = await scheduler.runJob({ type: "run_job", job_id: job.id, schedule_id: null, request_id: "queued" });
    const queuedActor = await app.tasks.get(queued.task_id), queuedConversation = await app.tasks.conversation(queuedActor.conversation_id);
    await pump(async () => (await app.session.commit(tx => tx.submissionByRequest(queuedConversation.id, "manual:queued"), BACKGROUND_CONTEXT))?.status === "done");
    const otherReceipt = await app.session.commit(tx => tx.submissionByRequest(queuedConversation.id, "manual:queued"), BACKGROUND_CONTEXT);
    const otherEffects = await app.session.commit(tx => tx.scanTasks({ conversationId: queuedConversation.id, kind: "pi.tool" }, 100), BACKGROUND_CONTEXT);
    assert.equal(otherEffects.items.length, 2);
    const first = await scheduler.runJob({ type: "run_job", job_id: job.id, schedule_id: null, request_id: "retained" });
    await pump(async () => {
      const run = (await app.session.snapshot(State, BACKGROUND_CONTEXT))?.lanes.find(lane => lane.id === JSON.stringify(["job", job.id]))?.last_run;
      return run !== undefined && (await app.session.getTask(run.task, BACKGROUND_CONTEXT))?.state.status === "sleeping";
    });
    const before = await app.session.snapshot(State, BACKGROUND_CONTEXT); assert.ok(before);
    const lane = before.lanes.find(lane => lane.id === JSON.stringify(["job", job.id])); assert.ok(lane?.last_run);
    const retainedWork = await app.session.getTask(lane.last_run.task, BACKGROUND_CONTEXT); assert.ok(retainedWork);
    if (legacy) {
      if (!newer) await app.tasks.update(retained.actor.id, task => { delete task.workflow;
        task.sources = [{ provider: "scheduled_jobs", account: "local", thread_id: job.id }]; });
      await app.session.commit(async tx => {
        const lane = (await tx.doc(State)).lanes.find(item => item.id === JSON.stringify(["job", job.id])); assert.ok(lane?.last_run);
        lane.active = lane.last_run.task; lane.active_runs = []; lane.actor_task_id = retained.actor.id; delete lane.last_run;
      }, BACKGROUND_CONTEXT);
      await app.close();
      app = await openScheduled(directory); scheduler = background(app, callback); await scheduler.syncJobs([job]);
    }
    let newerReceipt: Awaited<ReturnType<typeof scheduler.runJob>> | undefined;
    if (newer) {
      await app.close();
      const storage = await SqliteStorage.open(await openNodeSqliteDatabase(join(directory, "session.sqlite")));
      try {
        const record = await storage.task(retainedWork.id, BACKGROUND_CONTEXT); assert.ok(record?.state.status === "sleeping");
        const input = copyJson(record.input);
        assert.ok(typeof input === "object" && input !== null && !Array.isArray(input) && typeof input.work === "object" && input.work !== null &&
          !Array.isArray(input.work) && typeof input.work.occurrence === "object" && input.work.occurrence !== null && !Array.isArray(input.work.occurrence));
        if (legacy) delete input.work.occurrence.task_id;
        await commitWrites(storage, [{ type: "task", value: { ...record, input, restarts: 5,
          state: { status: "pending", checkpoint: record.state.checkpoint } } }], BACKGROUND_CONTEXT);
      } finally { await storage.close(BACKGROUND_CONTEXT); }
      app = await openScheduled(directory); scheduler = background(app, callback); await scheduler.syncJobs([job]);
      const ended = await app.session.waitForTask(retainedWork.id, BACKGROUND_CONTEXT);
      assert.equal(ended.state.outcome.status, "faulted");
      assert.equal((await app.session.getTask(retainedWork.id, BACKGROUND_CONTEXT))?.memos, undefined);
      if (!canonical) {
        const owner = await app.tasks.create({ id: scheduledTaskId("replacement-owner"), title: "Current workflow owner", outcome: job.prompt,
          model: job.model, sources: [{ provider: "scheduled_jobs", account: "local", thread_id: job.id }] });
        await app.session.commit(async tx => {
          const lane = (await tx.doc(State)).lanes.find(item => item.id === JSON.stringify(["job", job.id])); assert.ok(lane);
          lane.actor_task_id = owner.id;
        }, BACKGROUND_CONTEXT);
      }
      await scheduler.runJob({ type: "run_job", job_id: job.id, schedule_id: null, request_id: "independent" });
      app.faux.setResponses([fauxAssistantMessage("The newer independent occurrence completed.")]);
      newerReceipt = await scheduler.runJob({ type: "run_job", job_id: job.id, schedule_id: null, request_id: "newer" });
      await pump(async () => (await scheduler.getJob(job.id)).last_run?.state.status === "completed");
    }
    const diagnostics = await scheduler.getJob(job.id);
    if (canonical) {
      assert.equal(await app.session.commit(async tx => (await app.tasks.findIn(tx, scheduledTaskId(originalRequest)))?.id, BACKGROUND_CONTEXT), undefined);
      const before = await app.session.commit(tx => tx.scanTasks({ kind: "agent.background" }, 100), BACKGROUND_CONTEXT);
      await app.session.commit(async tx => {
        const lane = (await tx.doc(State)).lanes.find(item => item.id === JSON.stringify(["job", job.id])); assert.ok(lane);
        lane.actor_task_id = queuedActor.id;
      }, BACKGROUND_CONTEXT);
      await assert.rejects(scheduler.retryJob(job.id), /original actor is unavailable/);
      assert.deepEqual((await app.session.commit(tx => tx.scanTasks({ kind: "agent.background" }, 100), BACKGROUND_CONTEXT)).items.map(task => task.id), before.items.map(task => task.id));
      assert.deepEqual((await app.tasks.get(queuedActor.id)).pending, queuedActor.pending);
      await app.session.commit(async tx => {
        const lane = (await tx.doc(State)).lanes.find(item => item.id === JSON.stringify(["job", job.id])); assert.ok(lane);
        lane.actor_task_id = retained.actor.id;
      }, BACKGROUND_CONTEXT);
    }
    if (legacy && !newer) assert.equal(diagnostics.last_run, undefined, "Missing historical timing stays unknown before a new recovery runs");
    const laneBefore = (await app.session.snapshot(State, BACKGROUND_CONTEXT))?.lanes.find(item => item.id === JSON.stringify(["job", job.id]));
    const independentBefore = laneBefore?.active_runs?.filter(id => id !== retainedWork.id);
    const pending = (await app.session.snapshot(State, BACKGROUND_CONTEXT))?.lanes.find(item => item.id === JSON.stringify(["job", job.id]))?.pending;
    app.faux.setResponses([
      async context => {
        entered.release(); await held.promise;
        assert.ok(context.messages.some(message => message.role === "user" && typeof message.content === "string" && message.content.includes("Retained job request")));
        return fauxAssistantMessage(fauxToolCall("record_task", { summary: "Verified the retained work without replaying any prior effects.",
          reconciles_input: originalRequest, state: { state: "waiting", actor: "Upstream check", next_check: null } }), { stopReason: "toolUse" });
      },
      fauxAssistantMessage("The interrupted input is reconciled."),
    ]);
    const callsBefore = app.faux.state.callCount;
    const effectsBefore = app.commands.filter(command => command.type === "exec");
    const nativeBeforeRecovery = await app.session.getTask(retainedWork.id, BACKGROUND_CONTEXT);
    let recoveryFailure: unknown;
    recovery = scheduler.retryJob(job.id); void recovery.catch(error => { recoveryFailure = error; });
    await pump(() => recoveryFailure !== undefined || app.faux.state.callCount === callsBefore + 1);
    if (recoveryFailure !== undefined) throw recoveryFailure;
    await entered.promise;
    duplicate = scheduler.retryJob(job.id); void duplicate.catch(() => {});
    const recoveryId = "scheduled-recovery:" + original.id;
    const readRecovery = () => app.session.commit(tx => tx.submissionByRequest(conversation.id, recoveryId), BACKGROUND_CONTEXT);
    await pump(async () => await readRecovery() !== undefined);
    const admitted = await readRecovery(); assert.ok(admitted);
    assert.equal(app.faux.state.callCount, callsBefore + 1, "Repeated retry may not create another in-flight inference");
    assert.deepEqual(await app.session.getTask(retained.generation, BACKGROUND_CONTEXT), ended, "Terminal generation history is immutable");
    assert.deepEqual((await app.session.snapshot(State, BACKGROUND_CONTEXT))?.lanes.find(item => item.id === JSON.stringify(["job", job.id]))?.pending, pending);
    if (newer) {
      assert.deepEqual(await app.session.getTask(retainedWork.id, BACKGROUND_CONTEXT), nativeBeforeRecovery,
        "The faulted native occurrence remains immutable");
      const lane = (await app.session.snapshot(State, BACKGROUND_CONTEXT))?.lanes.find(item => item.id === JSON.stringify(["job", job.id]));
      assert.deepEqual(lane?.last_run, laneBefore?.last_run, "Recovering older work must preserve the latest occurrence reference");
      assert.equal(lane?.recoveries?.find(reference => reference.recovery_of === retainedWork.id)?.actor_task_id, retained.actor.id);
      assert.equal(lane?.actor_task_id, laneBefore?.actor_task_id, "Recovery must not replace the current canonical owner");
      assert.ok(independentBefore?.every(id => lane?.active_runs?.includes(id)), "Independent work retains its native owner");
      const current = await scheduler.getJob(job.id);
      assert.deepEqual(current.last_run, diagnostics.last_run);
      assert.deepEqual(current.schedules.map(({ schedule_id, next_at, last_at }) => ({ schedule_id, next_at, last_at })),
        diagnostics.schedules.map(({ schedule_id, next_at, last_at }) => ({ schedule_id, next_at, last_at })));
    }
    held.release(); await recovery; await duplicate.catch(error => { assert.match(error.message, /interrupted|running|recovery|parked/); });
    await pump(async () => (await readRecovery())?.status === "done");
    assert.equal((await readRecovery())?.id, admitted.id);
    assert.equal(await app.session.commit(tx => app.sources.reconcilesIn(tx, retained.actor.id, conversation.id, originalRequest), BACKGROUND_CONTEXT), true);
    assert.deepEqual(await app.session.commit(tx => tx.submissionByRequest(conversation.id, originalRequest), BACKGROUND_CONTEXT), original);
    assert.equal((await app.tasks.list()).length, newer ? canonical ? 5 : 6 : 3);
    if (!legacy) assert.equal(first.task_id, retained.actor.id);
    assert.notEqual(queued.task_id, retained.actor.id);
    assert.deepEqual(await app.session.commit(tx => tx.submissionByRequest(queuedConversation.id, "manual:queued"), BACKGROUND_CONTEXT), otherReceipt,
      "Recovery must not replace another occurrence's native receipt");
    assert.deepEqual(app.commands.filter(command => command.type === "exec"), effectsBefore, "Recovery must never replay a prior host effect");
    assert.deepEqual(await app.session.commit(tx => tx.scanTasks({ conversationId: queuedConversation.id, kind: "pi.tool" }, 100), BACKGROUND_CONTEXT), otherEffects,
      "Settled and uncertain native tool receipts remain immutable");
    assert.deepEqual(await scheduler.runJob({ type: "run_job", job_id: job.id, schedule_id: null, request_id: "queued" }), queued);
    const receipts = await app.storage.scanSubmissions({ conversationId: conversation.id }, 100, undefined, BACKGROUND_CONTEXT);
    assert.equal(receipts.items.filter(input => input.requestId === recoveryId).length, 1);
    if (newerReceipt !== undefined) {
      const request = { type: "run_job", job_id: job.id, schedule_id: null, request_id: "newer" } satisfies Parameters<BackgroundScheduler["runJob"]>[0];
      assert.deepEqual(await scheduler.runJob(request), newerReceipt);
      const beforeReopen = await scheduler.getJob(job.id);
      await app.close(); app = await openScheduled(directory); scheduler = background(app, callback); await scheduler.syncJobs([job]);
      const beforeRepeat = await app.session.commit(tx => tx.scanTasks({ kind: "agent.background" }, 100), BACKGROUND_CONTEXT);
      await scheduler.retryJob(job.id);
      assert.deepEqual((await app.session.commit(tx => tx.scanTasks({ kind: "agent.background" }, 100), BACKGROUND_CONTEXT)).items.map(task => task.id), beforeRepeat.items.map(task => task.id),
        "A repeated retry after reopen must not create another native recovery");
      assert.deepEqual((await scheduler.getJob(job.id)).last_run, beforeReopen.last_run);
      const lane = (await app.session.snapshot(State, BACKGROUND_CONTEXT))?.lanes.find(item => item.id === JSON.stringify(["job", job.id]));
      assert.equal(lane?.recoveries?.find(reference => reference.recovery_of === retainedWork.id)?.actor_task_id, retained.actor.id);
      assert.deepEqual(await scheduler.runJob(request), newerReceipt);
      assert.equal(app.faux.state.callCount, 0, "Settled recovery and newer occurrences must not run again after reopen");
    }
  } finally {
    held.release(); independent.release(); await recovery?.catch(() => {}); await duplicate?.catch(() => {}); await app.close(); await rm(directory, { recursive: true, force: true });
  }
});
