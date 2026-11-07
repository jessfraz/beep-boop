import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { openNodeSqliteDatabase } from "@earendil-works/chord/sqlite/node";
import { defineDoc, type TaskId } from "@earendil-works/pi-durable";
import { SqliteStorage } from "@earendil-works/pi-durable/storage/sqlite";
import type { BackgroundStatus } from "../src/generated/BackgroundStatus.js";
import type { BackgroundTriggerKey } from "../src/generated/BackgroundTriggerKey.js";
import type { BackgroundWork } from "../src/generated/BackgroundWork.js";
import { FakeClock, exampleJob, openBackground, pump } from "./fixtures/background-runtime.js";

const State = defineDoc<{
  triggers: { key: BackgroundTriggerKey; task: TaskId<null> | null; enabled: boolean; next: number | null }[];
  lanes: { id: string; active: TaskId<null> | null; pending: { key: BackgroundTriggerKey; work: BackgroundWork }[] }[];
}>({ kind: "agent.background", scope: "session", version: 1,
  initial: () => ({ triggers: [], lanes: [] }), checkpointWhen: () => true });
const historyLane = JSON.stringify(["builtin", "history"]);
const settings = { poll_interval_seconds: 60, sources: false, jobs: [exampleJob()] };
const retryable = (value: object): value is { retryHistoryIndex(): Promise<BackgroundStatus> } =>
  "retryHistoryIndex" in value && typeof value.retryHistoryIndex === "function";

async function park(directory: string, ids: TaskId<null>[]): Promise<void> {
  const storage = await SqliteStorage.open(await openNodeSqliteDatabase(join(directory, "session.sqlite")));
  try {
    const records = await Promise.all(ids.map(id => storage.task(id, BACKGROUND_CONTEXT)));
    await storage.commit(records.map(record => {
      assert.ok(record?.state.status === "running" || record?.state.status === "pending" || record?.state.status === "sleeping");
      return { type: "task", value: { ...record, restarts: 5, state: { status: "pending", checkpoint: record.state.checkpoint } } };
    }), BACKGROUND_CONTEXT);
  } finally { await storage.close(BACKGROUND_CONTEXT); }
}

function gate() {
  let release = () => {};
  const promise = new Promise<void>(resolve => { release = resolve; });
  return { release, wait: (signal: AbortSignal) => new Promise<void>((resolve, reject) => {
    const abort = () => reject(new Error("Owned index invocation stopped"));
    if (signal.aborted) { abort(); return; }
    signal.addEventListener("abort", abort, { once: true });
    promise.then(() => { signal.removeEventListener("abort", abort); resolve(); });
  }) };
}

test("explicit history recovery resumes the same parked native work and retains its queued pass and healthy timer", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-history-index-retry-")), clock = new FakeClock();
  const held = gate(), otherHeld = gate(), resumed = gate();
  let initialCalls = 0, otherCalls = 0, resumedCalls = 0;
  let app = await openBackground(directory, clock, {
    indexHistory: async signal => { initialCalls += 1; await held.wait(signal); },
    runJob: async (_occurrence, signal) => { otherCalls += 1; await otherHeld.wait(signal); },
  });
  try {
    await app.scheduler.start(settings);
    await pump(() => initialCalls === 1);
    await app.scheduler.runJob({ type: "run_job", job_id: exampleJob().id, schedule_id: null, request_id: "unrelated-owned-run" });
    await pump(() => otherCalls === 1);
    clock.advance(60_000);
    await pump(async () => (await app.session.snapshot(State, BACKGROUND_CONTEXT))?.lanes
      .find(lane => lane.id === historyLane)?.pending.length === 1);
    const seeded = await app.session.snapshot(State, BACKGROUND_CONTEXT); assert.ok(seeded);
    const lane = seeded.lanes.find(lane => lane.id === historyLane); assert.ok(lane?.active !== null && lane?.active !== undefined);
    const history = lane.active, queued = structuredClone(lane.pending);
    const timer = seeded.triggers.find(trigger => trigger.key.type === "builtin" && trigger.key.name === "history")?.task;
    const other = seeded.lanes.find(lane => lane.id === JSON.stringify(["job", exampleJob().id]))?.active;
    assert.ok(timer !== null && timer !== undefined); assert.ok(other !== null && other !== undefined);
    await app.session.close(BACKGROUND_CONTEXT);
    // Restore actual Pi crash records while the owning Harness is closed.
    // The lane, checkpoint and queued pass remain the production records.
    await park(directory, [history, other]);
    app = await openBackground(directory, clock, {
      indexHistory: async signal => { resumedCalls += 1; await resumed.wait(signal); },
      runJob: async () => { assert.fail("History recovery must leave another parked job untouched"); },
    });
    await app.scheduler.start(settings);
    const before = await app.session.getTask(history, BACKGROUND_CONTEXT); assert.ok(before);
    const timerBefore = await app.session.getTask(timer, BACKGROUND_CONTEXT), otherBefore = await app.session.getTask(other, BACKGROUND_CONTEXT);
    assert.equal((await app.session.inspect(BACKGROUND_CONTEXT)).tasks.find(task => task.record.id === history)?.state.kind, "blocked");
    assert.equal(resumedCalls, 0, "Startup must not reset the crash-loop budget");
    assert.ok(retryable(app.scheduler), "Parked native history work needs an explicit retry API");
    const result = await app.scheduler.retryHistoryIndex();
    await pump(() => resumedCalls === 1);
    const running = await app.session.getTask(history, BACKGROUND_CONTEXT); assert.ok(running?.state.status === "running");
    assert.equal(running.id, before.id); assert.equal(running.restarts ?? 0, 0);
    assert.deepEqual(running.input, before.input); assert.deepEqual(running.state.checkpoint, before.state.checkpoint);
    const active = (await app.session.snapshot(State, BACKGROUND_CONTEXT))?.lanes.find(lane => lane.id === historyLane);
    assert.equal(active?.active, history); assert.deepEqual(active?.pending, queued);
    assert.deepEqual(await app.session.getTask(timer, BACKGROUND_CONTEXT), timerBefore);
    assert.deepEqual(await app.session.getTask(other, BACKGROUND_CONTEXT), otherBefore);
    assert.equal(result.errors.some(error => error.key.type === "builtin" && error.key.name === "history"), false);
    await assert.rejects(app.scheduler.retryHistoryIndex(), /parked/);
    resumed.release();
    await pump(async () => resumedCalls === 2 && (await app.session.snapshot(State, BACKGROUND_CONTEXT))?.lanes
      .find(lane => lane.id === historyLane)?.active === null);
    const settled = await app.session.getTask(history, BACKGROUND_CONTEXT); assert.ok(settled?.state.status === "terminal");
    assert.equal(settled.state.outcome.status, "completed");
    assert.deepEqual(await app.session.getTask(other, BACKGROUND_CONTEXT), otherBefore);
    assert.deepEqual(await app.session.getTask(timer, BACKGROUND_CONTEXT), timerBefore);
    assert.equal((await app.session.snapshot(State, BACKGROUND_CONTEXT))?.lanes.find(lane => lane.id === historyLane)?.pending.length, 0);
    await assert.rejects(app.scheduler.retryHistoryIndex(), /parked/);
  } finally {
    held.release(); otherHeld.release(); resumed.release();
    await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true });
  }
});

test("explicit history recovery resumes an enabled parked timer without admitting another index pass", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-history-timer-retry-")), clock = new FakeClock();
  let app = await openBackground(directory, clock), calls = 0;
  try {
    await app.scheduler.start(settings);
    await pump(async () => (await app.scheduler.status()).pending === 0 &&
      (await app.session.snapshot(State, BACKGROUND_CONTEXT))?.triggers.every(trigger => trigger.next !== null) === true);
    const state = await app.session.snapshot(State, BACKGROUND_CONTEXT); assert.ok(state);
    const timer = state.triggers.find(trigger => trigger.key.type === "builtin" && trigger.key.name === "history")?.task;
    assert.ok(timer !== null && timer !== undefined);
    await app.session.close(BACKGROUND_CONTEXT); await park(directory, [timer]);
    app = await openBackground(directory, clock, { indexHistory: async () => { calls += 1; } });
    await app.scheduler.start(settings);
    assert.equal((await app.session.inspect(BACKGROUND_CONTEXT)).tasks.find(task => task.record.id === timer)?.state.kind, "blocked");
    const before = await app.session.getTask(timer, BACKGROUND_CONTEXT); assert.ok(before);
    assert.ok(retryable(app.scheduler)); await app.scheduler.retryHistoryIndex();
    await pump(async () => (await app.session.getTask(timer, BACKGROUND_CONTEXT))?.state.status === "sleeping");
    const after = await app.session.getTask(timer, BACKGROUND_CONTEXT); assert.ok(after?.state.status === "sleeping");
    assert.equal(after.id, before.id); assert.equal(after.restarts ?? 0, 0);
    assert.deepEqual(after.input, before.input); assert.deepEqual(after.state.checkpoint, before.state.checkpoint);
    assert.deepEqual(await app.session.snapshot(State, BACKGROUND_CONTEXT), state); assert.equal(calls, 0);
  } finally { await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("history recovery refuses absent, healthy and disabled native timers without creating work", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-history-index-no-retry-")), clock = new FakeClock();
  let app = await openBackground(directory, clock);
  try {
    assert.ok(retryable(app.scheduler));
    await assert.rejects(app.scheduler.retryHistoryIndex(), /history|History/);
    assert.equal(await app.session.snapshot(State, BACKGROUND_CONTEXT), undefined);
    await app.scheduler.start(settings);
    await pump(async () => (await app.scheduler.status()).pending === 0 &&
      (await app.session.snapshot(State, BACKGROUND_CONTEXT))?.triggers.every(trigger => trigger.next !== null) === true);
    const before = await app.session.snapshot(State, BACKGROUND_CONTEXT); assert.ok(before);
    await assert.rejects(app.scheduler.retryHistoryIndex(), /parked/);
    assert.deepEqual(await app.session.snapshot(State, BACKGROUND_CONTEXT), before);
    const timer = before.triggers.find(trigger => trigger.key.type === "builtin" && trigger.key.name === "history")?.task;
    assert.ok(timer !== null && timer !== undefined);
    await app.session.commit(async tx => {
      const state = await tx.doc(State), trigger = state.triggers.find(trigger => trigger.task === timer); assert.ok(trigger);
      trigger.enabled = false;
    }, BACKGROUND_CONTEXT);
    await app.session.close(BACKGROUND_CONTEXT); await park(directory, [timer]);
    app = await openBackground(directory, clock); assert.ok(retryable(app.scheduler));
    const disabled = await app.session.getTask(timer, BACKGROUND_CONTEXT);
    await assert.rejects(app.scheduler.retryHistoryIndex(), /history|History/);
    assert.deepEqual(await app.session.getTask(timer, BACKGROUND_CONTEXT), disabled);
  } finally { await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});
