import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { openNodeSqliteDatabase } from "@earendil-works/chord/sqlite/node";
import { createModels, fauxAssistantMessage, fauxProvider, type AssistantMessage } from "@earendil-works/pi-ai";
import { normalizeContext } from "@earendil-works/pi-ai/utils/transcript";
import { builtinModels } from "@earendil-works/pi-ai/providers/all";
import { configure, createRegistry, defineDoc, Harness, ProviderDoc } from "@earendil-works/pi-durable";
import { SqliteStorage } from "@earendil-works/pi-durable/storage/sqlite";
import { InferenceCapacity, estimatedInferenceTokens } from "../src/inference-capacity.js";
import type { InferenceClass } from "../src/generated/InferenceClass.js";
import type { ModelSelection } from "../src/generated/ModelSelection.js";
import { ConversationModels, DEFAULT_MODEL } from "../src/models.js";
import { QuotaRecovery } from "../src/quota.js";
import { FakeClock, pump, START } from "./fixtures/background-runtime.js";
import { fixtureModel } from "./model.js";

const zeroReservations = {
  foreground: { requests: 0, tokens: 0 }, cron: { requests: 0, tokens: 0 },
  fresh: { requests: 0, tokens: 0 }, backfill: { requests: 0, tokens: 0 },
};
const context = (text = "Review the new evidence.") => ({ messages: [{ role: "user", content: text, timestamp: START }] }) satisfies Parameters<ReturnType<typeof createModels>["streamSimple"]>[1];

async function openControlled(capacity: InferenceCapacity) {
  const directory = await mkdtemp(join(tmpdir(), "agent-inference-capacity-"));
  const clock = new FakeClock(), models = createModels();
  const native = fauxProvider({ provider: "openai", models: [{ id: "fixture" }], tokensPerSecond: 1_000_000 });
  const workClasses = new Map<string, InferenceClass>(), controllers = new Map<string, AbortController>();
  const active = new Map<string, { release(): void; fail(): void }>(), starts: string[] = [], pending: Promise<AssistantMessage>[] = [];
  const failures = new Set<string>(), throws = new Set<string>(); let maxActive = 0;
  native.setResponses(Array.from({ length: 128 }, () => async (_context, options) => {
    const id = options?.sessionId;
    assert.ok(id);
    starts.push(id);
    let release = () => {}, fail = () => {};
    const wait = new Promise<void>((resolve, reject) => {
      release = resolve; fail = () => reject(new Error("Synthetic provider failure"));
    });
    const abort = () => fail();
    active.set(id, { release, fail }); maxActive = Math.max(maxActive, active.size);
    options?.signal?.addEventListener("abort", abort, { once: true });
    try {
      await wait;
      return failures.has(id) ? fauxAssistantMessage("", { stopReason: "error", errorMessage: "subscription_sharing_usage_limit_exceeded" })
        : fauxAssistantMessage("Reviewed.");
    } finally { options?.signal?.removeEventListener("abort", abort); active.delete(id); }
  }));
  models.setProvider({ ...native.provider, streamSimple: (model, messages, options) => {
    if (throws.has(options?.sessionId ?? "")) throw new Error("Synthetic setup failure");
    return native.provider.streamSimple(model, messages, options);
  } });
  const provider = models.getProvider("openai"); assert.ok(provider);
  const session = await Harness.open(await SqliteStorage.open(await openNodeSqliteDatabase(join(directory, "session.sqlite"))),
    { models, registry: createRegistry(), now: clock.now, setTimer: clock.setTimer, identity: { attribution: null } }, BACKGROUND_CONTEXT);
  const quota = new QuotaRecovery(models, Promise.resolve(session), async (_model, options) => options.apiKey ?? "synthetic-account",
    { now: clock.now, random: () => 0 }, { capacity, workClass: async (_model, options) => workClasses.get(options.sessionId ?? "") ?? "foreground" });
  models.setProvider(quota.wrapProvider(provider));
  const model = models.getModel("openai", "fixture"); assert.ok(model);
  const request = (id: string, workClass: InferenceClass, options: { text?: string; account?: string; output?: number } = {}) => {
    workClasses.set(id, workClass); const controller = new AbortController(); controllers.set(id, controller);
    const result = models.streamSimple(model, context(options.text), { sessionId: id, signal: controller.signal,
      apiKey: options.account ?? "synthetic-account", maxTokens: options.output ?? 16 }).result();
    pending.push(result); return result;
  };
  return { models, model, session, clock, starts, active, request, controllers, failures, throws, maxActive: () => maxActive,
    release(id: string) { const held = active.get(id); assert.ok(held, `${id} must be running`); held.release(); },
    async close() {
      for (const controller of controllers.values()) controller.abort();
      for (const held of active.values()) held.release();
      await Promise.allSettled(pending); await session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true });
    } };
}

test("backfill leaves real wrapped-provider capacity for foreground, fresh work and cron", async () => {
  const app = await openControlled(new InferenceCapacity());
  try {
    const bulk = Array.from({ length: 6 }, (_, index) => app.request(`bulk-${index}`, "backfill"));
    await pump(() => app.starts.length >= 4);
    await new Promise<void>(resolve => setImmediate(resolve));
    assert.equal(app.starts.length, 4, "Backfill must leave reserved provider capacity for other work");
    const foreground = app.request("interactive", "foreground"), cron = app.request("scheduled", "cron"), fresh = app.request("new-signal", "fresh");
    await pump(() => app.active.has("interactive") && app.active.has("scheduled") && app.active.has("new-signal"));
    assert.equal(app.starts.filter(id => id.startsWith("bulk-")).length, 4);
    app.release("interactive"); app.release("scheduled"); app.release("new-signal");
    assert.equal((await foreground).stopReason, "stop"); assert.equal((await cron).stopReason, "stop"); assert.equal((await fresh).stopReason, "stop");
    for (let completed = 0; completed < 6; completed += 1) {
      const id = `bulk-${completed}`; await pump(() => app.active.has(id)); app.release(id);
    }
    assert.ok((await Promise.all(bulk)).every(result => result.stopReason === "stop"));
    assert.ok(app.maxActive() <= 8);
  } finally { await app.close(); }
});

test("estimated tokens bound overlap even when request slots remain free", async () => {
  const models = createModels(); models.setProvider(fauxProvider({ provider: "openai", models: [{ id: "fixture" }] }).provider);
  const example = models.getModel("openai", "fixture"); assert.ok(example);
  const cost = estimatedInferenceTokens(example, normalizeContext(context()), { maxTokens: 16 });
  const app = await openControlled(new InferenceCapacity({ maxConcurrent: 4, maxEstimatedTokens: cost * 2,
    reservations: zeroReservations, quantumTokens: 1 }));
  try {
    const results = [app.request("first", "fresh"), app.request("second", "fresh"), app.request("third", "fresh")];
    await pump(() => app.starts.length >= 2);
    await new Promise<void>(resolve => setImmediate(resolve));
    assert.equal(app.active.size, 2);
    app.release("first"); await pump(() => app.active.has("third"));
    assert.equal(app.maxActive(), 2);
    app.release("second"); app.release("third");
    assert.ok((await Promise.all(results)).every(result => result.stopReason === "stop"));
  } finally { await app.close(); }
});

test("independent model accounts retain separate resource pools", async () => {
  const app = await openControlled(new InferenceCapacity({ maxConcurrent: 1, reservations: zeroReservations }));
  try {
    const first = app.request("first-account", "backfill", { account: "account-one" });
    const queued = app.request("same-account", "foreground", { account: "account-one" });
    const separate = app.request("other-account", "foreground", { account: "account-two" });
    await pump(() => app.active.has("first-account") && app.active.has("other-account"));
    assert.equal(app.starts.includes("same-account"), false);
    app.release("first-account"); await pump(() => app.active.has("same-account"));
    app.release("same-account"); app.release("other-account");
    assert.ok((await Promise.all([first, queued, separate])).every(result => result.stopReason === "stop"));
  } finally { await app.close(); }
});

test("an oversized retained request drains and runs alone rather than starving", async () => {
  const app = await openControlled(new InferenceCapacity({ maxConcurrent: 3, maxEstimatedTokens: 160,
    reservations: zeroReservations, quantumTokens: 1 }));
  try {
    const first = app.request("small-active", "fresh"); await pump(() => app.active.has("small-active"));
    const giant = app.request("large-original", "backfill", { text: "Original evidence. ".repeat(100) });
    const later = app.request("small-later", "foreground");
    await new Promise<void>(resolve => setImmediate(resolve));
    app.release("small-active");
    await pump(() => app.active.has("large-original") || app.active.has("small-later"));
    if (app.active.has("small-later")) { app.release("small-later"); await later; }
    await pump(() => app.active.has("large-original")); assert.equal(app.active.size, 1);
    app.release("large-original"); await giant;
    if (!app.starts.includes("small-later")) { await pump(() => app.active.has("small-later")); app.release("small-later"); }
    await Promise.all([first, later]);
  } finally { await app.close(); }
});

test("queued cancellation and provider errors return capacity without starting cancelled work", async () => {
  const app = await openControlled(new InferenceCapacity({ maxConcurrent: 1, reservations: zeroReservations }));
  try {
    const first = app.request("active-failure", "backfill"); await pump(() => app.active.has("active-failure"));
    const cancelled = app.request("cancelled", "foreground");
    await new Promise<void>(resolve => setImmediate(resolve)); app.controllers.get("cancelled")?.abort();
    assert.equal((await cancelled).stopReason, "aborted"); assert.equal(app.starts.includes("cancelled"), false);
    const successor = app.request("successor", "cron"); app.active.get("active-failure")?.fail();
    assert.equal((await first).stopReason, "error");
    await pump(() => app.active.has("successor")); app.release("successor");
    assert.equal((await successor).stopReason, "stop"); assert.equal(app.maxActive(), 1);
  } finally { await app.close(); }
});

test("a synchronous provider setup failure releases its resource permit", async () => {
  const app = await openControlled(new InferenceCapacity({ maxConcurrent: 1, reservations: zeroReservations }));
  try {
    app.throws.add("setup-failure");
    assert.equal((await app.request("setup-failure", "backfill")).stopReason, "error");
    const next = app.request("after-setup-failure", "fresh");
    await pump(() => app.active.has("after-setup-failure")); app.release("after-setup-failure");
    assert.equal((await next).stopReason, "stop"); assert.equal(app.maxActive(), 1);
  } finally { await app.close(); }
});

test("active cancellation waits for the provider to stop before releasing its permit", async () => {
  const app = await openControlled(new InferenceCapacity({ maxConcurrent: 1, reservations: zeroReservations }));
  try {
    const active = app.request("active", "fresh"); await pump(() => app.active.has("active"));
    const next = app.request("next", "fresh"); app.controllers.get("active")?.abort();
    assert.equal((await active).stopReason, "aborted"); await pump(() => app.active.has("next"));
    app.release("next"); assert.equal((await next).stopReason, "stop"); assert.equal(app.maxActive(), 1);
  } finally { await app.close(); }
});

test("quota deferral releases capacity and queued work rechecks the exhausted account", async () => {
  const app = await openControlled(new InferenceCapacity({ maxConcurrent: 1, reservations: zeroReservations }));
  try {
    app.failures.add("quota"); const first = app.request("quota", "backfill"); await pump(() => app.active.has("quota"));
    const queued = app.request("queued", "foreground"); app.release("quota");
    const deferred = await first; assert.equal(deferred.stopReason, "deferred"); assert.ok(deferred.deferred);
    assert.equal((await queued).stopReason, "deferred"); assert.deepEqual(app.starts, ["quota"]);
    const healthy = app.request("healthy-account", "cron", { account: "other-account" });
    await pump(() => app.active.has("healthy-account")); app.release("healthy-account"); assert.equal((await healthy).stopReason, "stop");
    app.failures.delete("quota"); app.clock.advance(300_000);
    const resumed = app.models.fetchDeferred(app.model, deferred.deferred, { apiKey: "synthetic-account" });
    await pump(() => app.active.has("quota")); app.release("quota"); assert.equal((await resumed).stopReason, "stop");
  } finally { await app.close(); }
});

test("token-weighted shared admission gives retained backfill progress under a foreground queue", async () => {
  const capacity = new InferenceCapacity({ maxConcurrent: 1, maxEstimatedTokens: 100,
    reservations: zeroReservations, quantumTokens: 1 });
  const occupied = await capacity.acquire("account", "foreground", 1);
  const order: InferenceClass[] = [], releases: (() => void)[] = [];
  const waiting = Array.from({ length: 20 }, () => capacity.acquire("account", "foreground", 4).then(release => {
    order.push("foreground"); releases.push(release);
  }));
  const backfill = capacity.acquire("account", "backfill", 4).then(release => { order.push("backfill"); releases.push(release); });
  occupied();
  for (let turn = 0; turn < 21; turn += 1) {
    await pump(() => releases.length > 0); releases.shift()?.();
  }
  await Promise.all([...waiting, backfill]);
  assert.ok(order.indexOf("backfill") < 10, "A continuously queued foreground class must not starve retained work");
});

test("work classification survives configure-before-model, model edits and native reopen", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-inference-policy-")), path = join(directory, "session.sqlite");
  async function open() {
    const models = createModels(), native = fauxProvider({ provider: "openai", models: [{ id: "fixture" }], tokensPerSecond: 1_000_000 });
    models.setProvider(native.provider); const registry = createRegistry(), inference = new ConversationModels(models, undefined, registry);
    const session = await Harness.open(await SqliteStorage.open(await openNodeSqliteDatabase(path)), { models, registry,
      identity: { attribution: null } }, BACKGROUND_CONTEXT); inference.bind(session);
    return { models, native, inference, session };
  }
  let app = await open(); let closing = false; const active: (() => void)[] = []; let calls: Promise<AssistantMessage>[] = [];
  try {
    const conversation = await app.session.createConversation({ ownership: { kind: "ownerless" } }, BACKGROUND_CONTEXT);
    await app.session.commit(tx => app.inference.assignClassIn(tx, conversation, "backfill"), BACKGROUND_CONTEXT);
    await app.inference.configure(conversation, fixtureModel, {});
    await app.inference.edit(conversation, fixtureModel, { type: "set_reasoning", reasoning: null });
    const connection = await app.session.snapshot(ProviderDoc, conversation.id, BACKGROUND_CONTEXT); assert.ok(connection);
    await app.session.close(BACKGROUND_CONTEXT); app = await open();
    const restored = await app.session.conversation(conversation.id, BACKGROUND_CONTEXT);
    assert.ok(restored);
    await app.inference.configure(restored, fixtureModel, {});
    const starts: string[] = [];
    app.native.setResponses(Array.from({ length: 8 }, () => async (_context, options) => {
      const id = options?.sessionId; assert.ok(id); starts.push(id);
      if (!closing) await new Promise<void>(resolve => { active.push(resolve); });
      return fauxAssistantMessage("Done.");
    }));
    // The restored provider session's persisted class must enforce backfill's
    // four-request ceiling; treating it as foreground would admit a fifth.
    const model = app.models.getModel("openai", "fixture"); assert.ok(model);
    calls = Array.from({ length: 5 }, () => app.models.streamSimple(model, context(), { sessionId: connection.sessionId, maxTokens: 16 }).result());
    await pump(() => starts.length === 4);
    assert.equal(starts.length, 4);
    for (const release of active.splice(0)) release();
    await pump(() => starts.length === 5); for (const release of active.splice(0)) release(); await Promise.all(calls);
  } finally {
    closing = true; for (const release of active.splice(0)) release(); await Promise.allSettled(calls);
    await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true });
  }
});

const LegacyPolicies = defineDoc<{ sessions: Record<string, ModelSelection> }>({ kind: "agent.models", scope: "session", version: 1,
  initial: () => ({ sessions: {} }), checkpointWhen: () => true });
test("legacy model policy survives lazy per-session migration and work-class assignment", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-inference-legacy-policy-")), path = join(directory, "session.sqlite");
  async function open() {
    const models = builtinModels(), registry = createRegistry(), inference = new ConversationModels(models, undefined, registry);
    const session = await Harness.open(await SqliteStorage.open(await openNodeSqliteDatabase(path)), { models, registry,
      identity: { attribution: null } }, BACKGROUND_CONTEXT); inference.bind(session);
    return { inference, session };
  }
  let app = await open();
  try {
    const conversation = await app.session.createConversation({ ownership: { kind: "ownerless" } }, BACKGROUND_CONTEXT);
    const selected = { ...DEFAULT_MODEL, fast: true };
    await app.session.commit(async tx => {
      await configure(tx, conversation.id, { model: { provider: selected.provider, modelId: selected.model_id }, thinkingLevel: selected.reasoning });
      const native = await tx.doc(ProviderDoc, conversation.id);
      (await tx.doc(LegacyPolicies)).sessions[native.sessionId] = selected;
    }, BACKGROUND_CONTEXT);
    const legacy = await app.session.snapshot(LegacyPolicies, BACKGROUND_CONTEXT);
    assert.deepEqual(await app.inference.peek(conversation, DEFAULT_MODEL), selected);
    await app.session.commit(tx => app.inference.assignClassIn(tx, conversation, "cron"), BACKGROUND_CONTEXT);
    assert.deepEqual(await app.inference.configureCurrent(conversation, DEFAULT_MODEL, {}), selected);
    assert.deepEqual(await app.session.snapshot(LegacyPolicies, BACKGROUND_CONTEXT), legacy, "Migration retains the legacy policy evidence");
    await app.session.close(BACKGROUND_CONTEXT); app = await open();
    const restored = await app.session.conversation(conversation.id, BACKGROUND_CONTEXT); assert.ok(restored);
    assert.deepEqual(await app.inference.peek(restored, DEFAULT_MODEL), selected);
  } finally { await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});
