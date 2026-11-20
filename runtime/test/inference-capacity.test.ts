import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { openNodeSqliteDatabase } from "@earendil-works/chord/sqlite/node";
import { createModels, fauxAssistantMessage, fauxProvider, fauxToolCall, type AssistantMessage } from "@earendil-works/pi-ai";
import { normalizeContext } from "@earendil-works/pi-ai/utils/transcript";
import { builtinModels } from "@earendil-works/pi-ai/providers/all";
import { configure, createRegistry, defineDoc, Harness, ProviderDoc } from "@earendil-works/pi-durable";
import { SqliteStorage } from "@earendil-works/pi-durable/storage/sqlite";
import { InferenceCapacity, estimatedInferenceTokens } from "../src/inference-capacity.js";
import type { InferenceClass } from "../src/generated/InferenceClass.js";
import type { ModelSelection } from "../src/generated/ModelSelection.js";
import { ConversationModels, DEFAULT_MODEL } from "../src/models.js";
import { QuotaRecovery } from "../src/quota.js";
import { SourceReviews } from "../src/source-review.js";
import { SourceJournal } from "../src/sources.js";
import { TaskStore } from "../src/tasks.js";
import { Executions } from "../src/tools.js";
import { FakeClock, pump, START } from "./fixtures/background-runtime.js";
import { fixtureModel } from "./model.js";

const zeroReservations = {
  foreground: { requests: 0, tokens: 0 }, critical: { requests: 0, tokens: 0 }, email: { requests: 0, tokens: 0 }, cron: { requests: 0, tokens: 0 },
  review: { requests: 0, tokens: 0 }, fresh: { requests: 0, tokens: 0 }, backfill: { requests: 0, tokens: 0 },
};
const context = (text = "Review the new evidence.") => ({ messages: [{ role: "user", content: text, timestamp: START }] }) satisfies Parameters<ReturnType<typeof createModels>["streamSimple"]>[1];

test("critical workflows, email and source review retain admission beside oversized ordinary work", async () => {
  const capacity = new InferenceCapacity({ maxEstimatedTokens: 800 });
  const ordinary = await capacity.acquire("account", "foreground", 400);
  const first = await capacity.acquire("account", "critical", 100, undefined, "workflow-a");
  let sameStarted = false;
  const same = capacity.acquire("account", "critical", 100, undefined, "workflow-a").then(release => { sameStarted = true; return release; });
  const other = await capacity.acquire("account", "critical", 100, undefined, "workflow-b");
  const email = await capacity.acquire("account", "email", 100), review = await capacity.acquire("account", "review", 100);
  assert.equal(sameStarted, false, "One workflow must not take another workflow's critical request");
  first(); const successor = await same;
  assert.equal(sameStarted, true);
  successor(); other(); email(); review(); ordinary();
});

test("oversized ordinary contexts fail without consuming critical capacity", async () => {
  const capacity = new InferenceCapacity({ maxEstimatedTokens: 800 });
  await assert.rejects(capacity.acquire("account", "backfill", 601), /compact retained work/u);
  const first = await capacity.acquire("account", "critical", 100, undefined, "workflow-a");
  const other = await capacity.acquire("account", "critical", 100, undefined, "workflow-b");
  first(); other();
});

test("oversized ordinary admission accounts for critical borrowing without spending source reservations", async () => {
  const capacity = new InferenceCapacity({ maxEstimatedTokens: 1000 });
  const critical = await capacity.acquire("account", "critical", 400, undefined, "workflow-a");
  let started = false;
  const ordinary = capacity.acquire("account", "foreground", 490).then(release => { started = true; return release; });
  await new Promise<void>(resolve => setImmediate(resolve));
  assert.equal(started, false, "Critical shared-token usage must leave source reservations intact");
  const email = await capacity.acquire("account", "email", 125), review = await capacity.acquire("account", "review", 125);
  critical(); const release = await ordinary; release();
  email(); review();
});

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

test("spawned fresh work cannot consume source review, critical, email or interactive admission", async () => {
  const app = await openControlled(new InferenceCapacity());
  try {
    const workflows = [app.request("workflow-0", "fresh"), app.request("workflow-1", "fresh"), app.request("workflow-2", "fresh")];
    await pump(() => app.active.size === 2);
    const reserved = [app.request("review-0", "review"), app.request("spam", "critical"), app.request("shipments", "critical"),
      app.request("email", "email"), app.request("foreground", "foreground"), app.request("cron", "cron")];
    await pump(() => app.active.size === 8);
    assert.equal(app.starts.includes("workflow-2"), false);
    const nextReview = app.request("review-1", "review");
    app.release("review-0"); await pump(() => app.active.has("review-1"));
    assert.equal(app.starts.includes("workflow-2"), false, "Spawned work cannot borrow an idle review reservation");
    app.release("workflow-0"); await pump(() => app.active.has("workflow-2"));
    for (const id of [...app.active.keys()]) app.release(id);
    assert.ok((await Promise.all([...workflows, ...reserved, nextReview])).every(result => result.stopReason === "stop"));
    assert.equal(app.maxActive(), 8);
  } finally { await app.close(); }
});

test("the native source reviewer finishes while its spawned fresh workflows still hold the provider", { timeout: 10_000 }, async t => {
  const directory = await mkdtemp(join(tmpdir(), "agent-review-admission-")), models = createModels(), registry = createRegistry();
  const native = fauxProvider({ provider: "openai", models: [{ id: "fixture" }], tokensPerSecond: 1_000_000 }); models.setProvider(native.provider);
  const inference = new ConversationModels(models, undefined, registry);
  const session = await Harness.open(await SqliteStorage.open(await openNodeSqliteDatabase(join(directory, "session.sqlite"))),
    { models, registry, identity: { attribution: null }, settings: { retry: { enabled: false }, compaction: { enabled: false } } }, BACKGROUND_CONTEXT);
  const tasks = new TaskStore(session, inference), executions = new Executions(), sources = new SourceJournal(session, tasks, executions);
  const workflows = new Set<string>(), active = new Set<string>(), controllers: AbortController[] = [], calls: Promise<AssistantMessage>[] = [];
  const controller = new AbortController(); let release = () => {}, reviewing: Promise<boolean> | undefined;
  const held = new Promise<void>(resolve => { release = resolve; });
  const reviews = new SourceReviews(session, registry, sources, tasks, executions, {
    enabled: () => true, workflows: async () => [],
    settings: async () => ({ model: fixtureModel, cwd: "/workspace/example", hosts: [], instructions: "Review exact admitted evidence." }),
    async legacy() { throw new Error("This fixture has no legacy review"); },
    bind: (conversation, model) => executions.bind(conversation.id.toString(), { model, cwd: "/workspace/example", hosts: [], role: "review",
      async call() { throw new Error("Source admission cannot perform external effects"); } }),
  }, { workers: 1 });
  registry.install(reviews.tools);
  native.setResponses(Array.from({ length: 16 }, () => async (context, options) => {
    const id = options?.sessionId; assert.ok(id);
    if (workflows.has(id)) {
      active.add(id);
      try { await held; return fauxAssistantMessage("Retained workflow complete."); }
      finally { active.delete(id); }
    }
    assert.ok(active.size > 0, "Review admission must finish before releasing downstream provider work");
    if (context.messages.some(message => message.role === "toolResult" && message.toolName === "review_sources")) return fauxAssistantMessage("");
    const message = context.messages.findLast(message => message.role === "user"); assert.ok(message?.role === "user" && typeof message.content === "string");
    const input: unknown = JSON.parse(message.content);
    assert.ok(typeof input === "object" && input !== null && "groups" in input && Array.isArray(input.groups));
    const groups = input.groups.map((group: unknown) => { assert.ok(typeof group === "object" && group !== null && "id" in group && typeof group.id === "string"); return group.id; });
    return fauxAssistantMessage(fauxToolCall("review_sources", { decisions: [{ type: "dismiss", groups, keys: [], reason: "The exact evidence is accounted for" }] }), { stopReason: "toolUse" });
  }));
  let deadline: ReturnType<typeof setTimeout> | undefined;
  t.signal.addEventListener("abort", () => controller.abort(), { once: true });
  try {
    await tasks.prepare(); await sources.prepare();
    const model = models.getModel("openai", "fixture"); assert.ok(model);
    for (let index = 0; index < 3; index++) {
      const conversation = await session.createConversation({ ownership: { kind: "ownerless" } }, BACKGROUND_CONTEXT);
      await inference.configure(conversation, fixtureModel, {}, "fresh");
      const provider = await session.snapshot(ProviderDoc, conversation.id, BACKGROUND_CONTEXT); assert.ok(provider);
      workflows.add(provider.sessionId); const abort = new AbortController(); controllers.push(abort);
      calls.push(models.streamSimple(model, context(), { sessionId: provider.sessionId, signal: abort.signal, maxTokens: 16 }).result());
    }
    await pump(() => active.size >= 2);
    await sources.admit({ source: { provider: "slack", account: "invented-workspace" }, checkpoint: null, coverage: { status: "complete" }, events: [{
      id: "new-evidence", version: "one", source: { provider: "slack", account: "invented-workspace", thread_id: "new-thread" },
      title: "Invented source evidence", text: "The new evidence needs a review before more work is spawned.", occurred_at: Date.now(),
    }] }, null);
    deadline = setTimeout(() => controller.abort(new Error("Source review admission waited for its spawned workflows")), 5000);
    reviewing = reviews.review(0, controller.signal);
    assert.equal(await reviewing, true); reviewing = undefined;
    assert.equal((await sources.events({ pending: true })).length, 0);
    assert.ok(active.size >= 2, "Downstream work stays held until the source decision commits");
  } finally {
    clearTimeout(deadline); controller.abort(); for (const abort of controllers) abort.abort(); release();
    await Promise.allSettled([...calls, reviewing]); await session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true });
  }
});

test("backfill leaves real wrapped-provider capacity for foreground, source review and cron", async () => {
  const app = await openControlled(new InferenceCapacity());
  try {
    const bulk = Array.from({ length: 6 }, (_, index) => app.request(`bulk-${index}`, "backfill"));
    await pump(() => app.starts.length >= 2);
    await new Promise<void>(resolve => setImmediate(resolve));
    assert.equal(app.starts.length, 2, "Backfill must leave reserved provider capacity for critical and other work");
    const foreground = app.request("interactive", "foreground"), cron = app.request("scheduled", "cron"), review = app.request("new-signal", "review");
    await pump(() => app.active.has("interactive") && app.active.has("scheduled") && app.active.has("new-signal"));
    assert.equal(app.starts.filter(id => id.startsWith("bulk-")).length, 2);
    app.release("interactive"); app.release("scheduled"); app.release("new-signal");
    assert.equal((await foreground).stopReason, "stop"); assert.equal((await cron).stopReason, "stop"); assert.equal((await review).stopReason, "stop");
    for (let completed = 0; completed < 6; completed += 1) {
      const id = `bulk-${completed}`; await pump(() => app.active.has(id)); app.release(id);
    }
    assert.ok((await Promise.all(bulk)).every(result => result.stopReason === "stop"));
    assert.ok(app.maxActive() <= 8);
  } finally { await app.close(); }
});

test("email starts through the real wrapped provider while source review and backfill hold their capacity", async () => {
  const models = createModels(); models.setProvider(fauxProvider({ provider: "openai", models: [{ id: "fixture" }] }).provider);
  const model = models.getModel("openai", "fixture"); assert.ok(model);
  const cost = estimatedInferenceTokens(model, normalizeContext(context()), { maxTokens: 16 });
  const app = await openControlled(new InferenceCapacity({ maxEstimatedTokens: cost * 8 }));
  try {
    const backfill = [app.request("bulk-0", "backfill")];
    const reviews = [app.request("held-review-one", "review")];
    const foreground = [app.request("interactive-one", "foreground"), app.request("interactive-two", "foreground")];
    const cron = app.request("held-cron", "cron");
    await pump(() => app.active.size === 5);
    const queuedFresh = app.request("queued-fresh", "fresh");
    await new Promise<void>(resolve => setImmediate(resolve));
    assert.equal(app.starts.includes("queued-fresh"), false, "Ordinary fresh work must leave the email reservation available");
    const email = app.request("priority-email", "email");
    await pump(() => app.active.has("priority-email"));
    assert.equal(app.active.size, 6, "Email owns its reservation while critical capacity remains available");
    assert.equal(app.starts.includes("queued-fresh"), false);
    assert.equal(app.starts.filter(id => id.startsWith("bulk-")).length, 1);
    app.release("priority-email");
    assert.equal((await email).stopReason, "stop");
    app.release("interactive-one"); app.release("interactive-two"); app.release("held-cron");
    app.release("held-review-one");
    await Promise.all([...foreground, ...reviews, cron]);
    for (let index = 0; index < backfill.length; index++) {
      const id = `bulk-${index}`; await pump(() => app.active.has(id)); app.release(id);
    }
    await pump(() => app.active.has("queued-fresh")); app.release("queued-fresh");
    assert.ok((await Promise.all([...backfill, queuedFresh])).every(result => result.stopReason === "stop"));
    assert.equal(app.maxActive(), 6);
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
    const cancelled = app.request("cancelled", "review");
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
    const next = app.request("after-setup-failure", "review");
    await pump(() => app.active.has("after-setup-failure")); app.release("after-setup-failure");
    assert.equal((await next).stopReason, "stop"); assert.equal(app.maxActive(), 1);
  } finally { await app.close(); }
});

test("active cancellation waits for the provider to stop before releasing its permit", async () => {
  const app = await openControlled(new InferenceCapacity({ maxConcurrent: 1, reservations: zeroReservations }));
  try {
    const active = app.request("active", "review"); await pump(() => app.active.has("active"));
    const next = app.request("next", "review"); app.controllers.get("active")?.abort();
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

for (const queuedClass of ["foreground", "review", "fresh"] satisfies InferenceClass[]) test(`token-weighted shared admission gives retained backfill progress under a ${queuedClass} queue`, async () => {
  const capacity = new InferenceCapacity({ maxConcurrent: 1, maxEstimatedTokens: 100,
    reservations: zeroReservations, quantumTokens: 1 });
  const occupied = await capacity.acquire("account", "foreground", 1);
  const order: InferenceClass[] = [], releases: (() => void)[] = [];
  const waiting = Array.from({ length: 20 }, () => capacity.acquire("account", queuedClass, 4).then(release => {
    order.push(queuedClass); releases.push(release);
  }));
  const backfill = capacity.acquire("account", "backfill", 4).then(release => { order.push("backfill"); releases.push(release); });
  occupied();
  for (let turn = 0; turn < 21; turn += 1) {
    await pump(() => releases.length > 0); releases.shift()?.();
  }
  await Promise.all([...waiting, backfill]);
  assert.ok(order.indexOf("backfill") < 10, "A continuously queued class must not starve retained work");
});

for (const workClass of ["backfill", "review"] satisfies InferenceClass[]) test(`${workClass} classification survives configure-before-model, model edits and native reopen`, async () => {
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
    await app.session.commit(tx => app.inference.assignClassIn(tx, conversation, workClass), BACKGROUND_CONTEXT);
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
    // Review owns its reservation plus shared work. Backfill only owns shared
    // work; both classes must survive native provider-session reopening.
    const model = app.models.getModel("openai", "fixture"); assert.ok(model);
    calls = Array.from({ length: 5 }, () => app.models.streamSimple(model, context(), { sessionId: connection.sessionId, maxTokens: 16 }).result());
    const overlap = workClass === "review" ? 3 : 2;
    await pump(() => starts.length === overlap);
    assert.equal(starts.length, overlap);
    for (let index = 0; index < calls.length; index++) {
      await pump(() => starts.length >= index + 1);
      for (const release of active.splice(0)) release();
    }
    await Promise.all(calls);
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
