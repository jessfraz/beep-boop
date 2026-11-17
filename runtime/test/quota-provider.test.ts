import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { createModels, fauxAssistantMessage, fauxProvider, fauxToolCall, type Model, type Api, type ProviderRequestOptions } from "@earendil-works/pi-ai";
import { createRegistry, Harness } from "@earendil-works/pi-durable";
import { openNodeSqliteDatabase } from "@earendil-works/chord/sqlite/node";
import { SqliteStorage } from "@earendil-works/pi-durable/storage/sqlite";
import { QuotaRecovery } from "../src/quota.js";
import { FakeClock, pump, START } from "./fixtures/background-runtime.js";

const exhausted = () => fauxAssistantMessage("", {
  stopReason: "error", errorMessage: "subscription_sharing_usage_limit_exceeded",
});

async function openQuota(directory: string, clock: FakeClock,
  account: (model: Model<Api>, options: ProviderRequestOptions) => Promise<string> = async () => "synthetic-account") {
  const models = createModels(), native = fauxProvider({ provider: "openai", models: [{ id: "fixture" }], tokensPerSecond: 1_000_000,
    deferred: { pendingFetches: 1, pollAfterMs: 10 } });
  let credential = "synthetic-original-token";
  let waits = 0;
  models.setProvider({ ...native.provider, auth: { apiKey: { name: "Test account", resolve: async () => ({ auth: { apiKey: credential } }) } } });
  const provider = models.getProvider("openai"); assert.ok(provider);
  const session = await Harness.open(await SqliteStorage.open(await openNodeSqliteDatabase(join(directory, "session.sqlite"))),
    { models, registry: createRegistry(), now: clock.now, setTimer: clock.setTimer, identity: { attribution: null } }, BACKGROUND_CONTEXT);
  await session.root(BACKGROUND_CONTEXT);
  const quota = new QuotaRecovery(models, Promise.resolve(session), account, { now: clock.now,
    setTimer: (callback, delay, options) => { waits += 1; return clock.setTimer(callback, delay, options); }, random: () => 0 });
  models.setProvider(quota.wrapProvider(provider)); quota.wrapComplete();
  const model = models.getModel("openai", "fixture"); assert.ok(model);
  return { models, native, session, model, waits: () => waits, rotate: (value: string) => { credential = value; } };
}

test("complete calls wait through quota and refresh credentials before continuing", { timeout: 10_000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-quota-complete-")), clock = new FakeClock();
  const app = await openQuota(directory, clock);
  try {
    app.native.setResponses([exhausted(), (_context, options) => {
      assert.equal(options?.apiKey, "synthetic-refreshed-token");
      return fauxAssistantMessage("Summary complete.");
    }]);
    let finished = false;
    const result = app.models.completeSimple(app.model, { messages: [{ role: "user", content: "Summarize existing work.", timestamp: START }] });
    void result.then(() => { finished = true; });
    await pump(() => app.waits() > 0);
    assert.equal(finished, false, "Compaction-style callers must never receive an unsupported deferred result");
    app.rotate("synthetic-refreshed-token"); clock.advance(300_000);
    await pump(() => finished);
    assert.equal((await result).stopReason, "stop");
    assert.equal(app.native.state.callCount, 2);
  } finally { await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("server reset deadlines suppress requests and cancellation releases only its own wait", { timeout: 10_000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-quota-deadline-")), clock = new FakeClock();
  const app = await openQuota(directory, clock), controller = new AbortController();
  const context = { messages: [{ role: "user", content: "Continue the retained response.", timestamp: START }] } satisfies Parameters<typeof app.models.streamSimple>[1];
  try {
    app.native.setResponses([async (_context, options, _state, model) => {
      await options?.onResponse?.({ status: 429, headers: { "retry-after": "7200" } }, model);
      return exhausted();
    }, fauxAssistantMessage("Capacity restored.")]);
    const deferred = await app.models.streamSimple(app.model, context).result(); assert.ok(deferred.deferred);
    const waiting = app.models.completeSimple(app.model, context, { signal: controller.signal });
    void waiting.catch(() => {});
    await pump(() => app.waits() > 0);
    controller.abort(); await assert.rejects(waiting, /abort/iu);
    clock.advance(3_600_000);
    const stillWaiting = await app.models.fetchDeferred(app.model, deferred.deferred);
    assert.equal(stillWaiting.stopReason, "deferred");
    assert.equal(app.native.state.callCount, 1);
    clock.advance(3_600_000);
    assert.equal((await app.models.fetchDeferred(app.model, deferred.deferred)).stopReason, "stop");
    assert.equal(app.native.state.callCount, 2);
  } finally { controller.abort(); await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("separate accounts and native deferred requests retain their upstream behavior", { timeout: 10_000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-quota-accounts-")), clock = new FakeClock();
  const app = await openQuota(directory, clock, async (_model, options) => options.apiKey ?? "ambient");
  const context = { messages: [{ role: "user", content: "Check this account.", timestamp: START }] } satisfies Parameters<typeof app.models.streamSimple>[1];
  try {
    app.native.setResponses([exhausted(), fauxAssistantMessage("Other account works."), fauxAssistantMessage("Native deferred reply.")]);
    assert.equal((await app.models.streamSimple(app.model, context, { apiKey: "synthetic-first-account" }).result()).stopReason, "deferred");
    assert.equal((await app.models.completeSimple(app.model, context, { apiKey: "synthetic-second-account" })).stopReason, "stop");
    const native = await app.models.streamSimple(app.model, context, { apiKey: "synthetic-second-account", deferred: true }).result();
    assert.equal(native.stopReason, "deferred"); assert.ok(native.deferred);
    const pending = await app.models.fetchDeferred(app.model, native.deferred, { apiKey: "synthetic-second-account" });
    assert.equal(pending.stopReason, "deferred");
    assert.equal((await app.models.fetchDeferred(app.model, native.deferred, { apiKey: "synthetic-second-account" })).stopReason, "stop");
    assert.equal(app.native.state.deferredFetchCount, 2);
  } finally { await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("a successful quota response can replay after reopen before its consumer commits", { timeout: 10_000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-quota-result-")), clock = new FakeClock();
  let app = await openQuota(directory, clock);
  const context = { messages: [{ role: "user", content: "Continue the original request.", timestamp: START }] } satisfies Parameters<typeof app.models.streamSimple>[1];
  try {
    app.native.setResponses([exhausted(), fauxAssistantMessage("The retained response completed.")]);
    const pending = await app.models.streamSimple(app.model, context).result(); assert.ok(pending.deferred);
    clock.advance(300_000);
    const completed = await app.models.fetchDeferred(app.model, pending.deferred);
    assert.equal(completed.stopReason, "stop");
    await app.session.close(BACKGROUND_CONTEXT);
    app = await openQuota(directory, clock);
    app.native.setResponses([() => { assert.fail("A completed retained response must not run inference again"); }]);
    const replay = await app.models.fetchDeferred(app.model, pending.deferred);
    assert.equal(replay.stopReason, "stop");
    assert.deepEqual(replay.content, completed.content);
    assert.deepEqual(replay.usage, completed.usage);
    assert.equal(app.native.state.callCount, 0);
  } finally { await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("a slow quota check retains exclusive admission past its crash-recovery lease", { timeout: 10_000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-quota-slow-")), clock = new FakeClock();
  const app = await openQuota(directory, clock), context = { messages: [{ role: "user", content: "Finish reasoning.", timestamp: START }] } satisfies Parameters<typeof app.models.streamSimple>[1];
  let release = () => {}, started = () => {};
  const held = new Promise<void>(resolve => { release = resolve; });
  const checking = new Promise<void>(resolve => { started = resolve; });
  try {
    app.native.setResponses([exhausted(), async () => { started(); await held; return fauxAssistantMessage("Reasoning finished."); }, fauxAssistantMessage("Another request finished.")]);
    const pending = await app.models.streamSimple(app.model, context).result(); assert.ok(pending.deferred);
    clock.advance(300_000);
    const result = app.models.fetchDeferred(app.model, pending.deferred); void result.catch(() => {});
    await checking;
    clock.advance(120_000);
    const another = await app.models.streamSimple(app.model, context).result();
    assert.equal(another.stopReason, "deferred", "A live long-running request must keep its exclusive capacity check");
    assert.equal(app.native.state.callCount, 2);
    release(); assert.equal((await result).stopReason, "stop");
  } finally { release(); await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

