import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { openNodeSqliteDatabase } from "@earendil-works/chord/sqlite/node";
import { createModels, fauxAssistantMessage, fauxProvider, type OAuthCredential } from "@earendil-works/pi-ai";
import { createRegistry, Harness } from "@earendil-works/pi-durable";
import { SqliteStorage } from "@earendil-works/pi-durable/storage/sqlite";
import { AuthError, ChatGptAuth } from "../src/auth.js";
import type { AuthStatus } from "../src/generated/AuthStatus.js";
import type { InferenceClass } from "../src/generated/InferenceClass.js";
import { InferenceCapacity } from "../src/inference-capacity.js";
import { ConversationModels } from "../src/models.js";
import { TaskStore } from "../src/tasks.js";
import { FakeClock, pump, START } from "./fixtures/background-runtime.js";
import { fixtureModel } from "./model.js";

function gate() {
  let release = () => {};
  const promise = new Promise<void>(resolve => { release = resolve; });
  return { promise, release };
}
class ControlledAuth extends ChatGptAuth {
  readonly clock = new FakeClock();
  readonly initialExpiry = this.clock.now() + 120_000;
  resolutions = 0;
  refreshes = 0;
  refreshEntered = 0;
  refreshGate: Promise<void> | undefined;
  subject = "synthetic-account";
  refreshFailure = false;
  private access = "synthetic-before-wait";
  private expires = this.initialExpiry;
  get validAccess(): string | undefined { return this.clock.now() < this.expires ? this.access : undefined; }
  override async status(): Promise<AuthStatus> {
    return { state: "connected", account: { issuer: "https://issuer.example.test", subject: this.subject, email: null },
      expires_at: Math.floor(this.expires / 1000) };
  }
  override async credential(): Promise<OAuthCredential> {
    this.resolutions++;
    if (this.expires <= this.clock.now() + 60_000) {
      this.refreshEntered++; await this.refreshGate;
      if (this.refreshFailure) throw new AuthError("refresh", "Synthetic credential refresh is unavailable");
      this.refreshes++; this.access = "synthetic-refreshed-" + this.refreshes; this.expires = this.clock.now() + 3_600_000;
    }
    return { type: "oauth", clientId: "synthetic-client", access: this.access, refresh: "synthetic-refresh", expires: this.expires };
  }
}
class ObservedCapacity extends InferenceCapacity {
  requests = 0;
  active = 0;
  latestAccount: string | undefined;
  override run<T>(account: string, workClass: InferenceClass, tokens: number, operation: () => Promise<T>, signal?: AbortSignal): Promise<T> {
    this.requests++; this.latestAccount = account;
    return super.run(account, workClass, tokens, async () => {
      this.active++;
      try { return await operation(); } finally { this.active--; }
    }, signal);
  }
}
async function open() {
  const directory = await mkdtemp(join(tmpdir(), "agent-auth-admission-"));
  const auth = new ControlledAuth(directory), models = createModels(), registry = createRegistry();
  const faux = fauxProvider({ provider: "openai", models: [{ id: "fixture" }], tokensPerSecond: 1_000_000 });
  const seen: (string | undefined)[] = [];
  models.setProvider({ ...faux.provider, streamSimple: (model, context, options) => {
    seen.push(options?.apiKey); return faux.provider.streamSimple(model, context, options);
  } });
  const capacity = new ObservedCapacity({ maxConcurrent: 1 });
  const inference = new ConversationModels(models, auth, registry, capacity);
  const session = await Harness.open(await SqliteStorage.open(await openNodeSqliteDatabase(join(directory, "session.sqlite"))),
    { models, registry, identity: { attribution: null }, now: auth.clock.now, setTimer: auth.clock.setTimer,
      settings: { retry: { enabled: false }, stream: { maxRetries: 0 }, compaction: { enabled: false } } }, BACKGROUND_CONTEXT);
  const tasks = new TaskStore(session, inference), model = models.getModel("openai", "fixture"); assert.ok(model);
  const controller = new AbortController();
  return { auth, models, faux, inference, session, tasks, model, seen, controller, capacity,
    async close() { controller.abort(); await session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); } };
}
const context = { messages: [{ role: "user", content: "Continue this retained request.", timestamp: 1 }] } satisfies
  Parameters<ReturnType<typeof createModels>["streamSimple"]>[1];

test("a native generation refreshes its configured ChatGPT credential after waiting for inference capacity", { timeout: 10_000 }, async () => {
  const app = await open(), held = gate();
  try {
    const first = await app.session.createConversation({ ownership: { kind: "ownerless" } }, BACKGROUND_CONTEXT);
    const second = await app.session.createConversation({ ownership: { kind: "ownerless" } }, BACKGROUND_CONTEXT);
    await app.inference.configure(first, fixtureModel, {}); await app.inference.configure(second, fixtureModel, {});
    app.faux.setResponses([async () => { await held.promise; return fauxAssistantMessage("The already dispatched request completed."); }, (_input, options) =>
      options?.apiKey !== app.auth.validAccess
        ? fauxAssistantMessage("", { stopReason: "error", errorMessage: "401 token_expired" }) : fauxAssistantMessage("The queued request completed.")]);
    const firstInput = await first.submit({ type: "input", content: "Occupy the local inference permit.", requestId: "auth-held" }, BACKGROUND_CONTEXT);
    await pump(() => app.seen.length === 1);
    const secondInput = await second.submit({ type: "input", content: "Wait for the same account.", requestId: "auth-queued" }, BACKGROUND_CONTEXT);
    await pump(() => app.capacity.requests === 2); assert.equal(app.seen.length, 1);
    app.auth.clock.advance(180_000); held.release();
    assert.equal((await firstInput.wait(BACKGROUND_CONTEXT)).status, "done");
    assert.equal((await secondInput.wait(BACKGROUND_CONTEXT)).status, "done",
      "A credential that expired in the local queue must refresh before dispatch rather than strand the original input with token_expired");
    assert.deepEqual(app.seen, ["synthetic-before-wait", "synthetic-refreshed-1"]); assert.equal(app.auth.refreshes, 1);
    const retained = await app.session.commit(tx => tx.submissionByRequest(second.id, "auth-queued"), BACKGROUND_CONTEXT);
    assert.equal(retained?.id, secondInput.id); assert.equal(retained?.status, "done");
  } finally { held.release(); await app.close(); }
});

test("an explicit OpenAI key remains explicit while a configured ChatGPT account is connected", async () => {
  const app = await open();
  try {
    app.faux.setResponses([fauxAssistantMessage("The explicit key owns this request.")]);
    const result = await app.models.streamSimple(app.model, context, { apiKey: "synthetic-explicit-key", signal: app.controller.signal }).result();
    assert.equal(result.stopReason, "stop"); assert.deepEqual(app.seen, ["synthetic-explicit-key"]);
  } finally { await app.close(); }
});

test("upstream caller hooks cannot rebind an already authenticated request to another ChatGPT account", { timeout: 10_000 }, async () => {
  const app = await open(), authenticated = gate(), held = gate();
  try {
    app.faux.setResponses([fauxAssistantMessage("Unexpected dispatch under a replacement account.")]);
    const request = app.models.streamSimple(app.model, context, { signal: app.controller.signal,
      transformHeaders: async headers => { authenticated.release(); await held.promise; return headers; } }).result();
    await authenticated.promise; app.auth.subject = "another-synthetic-account"; held.release();
    assert.equal((await request).stopReason, "error"); assert.deepEqual(app.seen, [],
      "Owner identity must be captured before Models.applyAuth and preserved across its caller hooks");
  } finally { held.release(); await app.close(); }
});

test("a queued request never switches to another ChatGPT account or sends a stale key after refresh failure", { timeout: 10_000 }, async () => {
  for (const mode of ["account-change", "refresh-failure"]) {
    const app = await open(), held = gate();
    try {
      app.faux.setResponses([async () => { await held.promise; return fauxAssistantMessage("Held request completed."); }, fauxAssistantMessage("Unexpected dispatch.")]);
      const first = app.models.streamSimple(app.model, context, { signal: app.controller.signal }).result();
      await pump(() => app.seen.length === 1);
      const queued = app.models.streamSimple(app.model, context, { signal: app.controller.signal }).result();
      await pump(() => app.capacity.requests === 2); assert.equal(app.seen.length, 1);
      if (mode === "account-change") app.auth.subject = "another-synthetic-account";
      else { app.auth.clock.advance(180_000); app.auth.refreshFailure = true; }
      held.release(); await first;
      const failed = await queued; assert.equal(failed.stopReason, "error"); assert.equal(app.seen.length, 1,
        "Neither credential-owner changes nor refresh failures may dispatch an old or replacement account's bearer");
      app.auth.refreshFailure = false;
      app.faux.setResponses([fauxAssistantMessage("The failed request released its capacity permit.")]);
      assert.equal((await app.models.streamSimple(app.model, context, { signal: app.controller.signal }).result()).stopReason, "stop");
      assert.equal(app.seen.length, 2);
    } finally { held.release(); await app.close(); }
  }
});

for (const cancel of [false, true]) test(cancel
  ? "cancelling a credential refresh releases capacity and the quota probe without discarding retained work"
  : "a retained quota probe refreshes again after capacity wait and leaves cooldown polls quiet", { timeout: 10_000 }, async t => {
  t.mock.timers.enable({ apis: ["Date"], now: START });
  const app = await open(), refreshing = gate(); let releaseCapacity: (() => void) | undefined;
  try {
    app.faux.setResponses([fauxAssistantMessage("", { stopReason: "error", errorMessage: "subscription_sharing_usage_limit_exceeded" }),
      (_input, options) => options?.apiKey === app.auth.validAccess ? fauxAssistantMessage("The retained request completed.")
        : fauxAssistantMessage("", { stopReason: "error", errorMessage: "401 token_expired" })]);
    const deferred = await app.models.streamSimple(app.model, context, { signal: app.controller.signal }).result();
    assert.equal(deferred.stopReason, "deferred"); const handle = deferred.deferred; assert.ok(handle);
    const before = app.auth.resolutions;
    assert.equal((await app.models.fetchDeferred(app.model, handle, { signal: app.controller.signal })).stopReason, "deferred");
    assert.equal(app.auth.resolutions, before + 1, "A cooldown-only poll must not perform an extra dispatch credential refresh");
    assert.equal(app.seen.length, 1); assert.equal(app.auth.refreshes, 0);
    const account = app.capacity.latestAccount; assert.ok(account);
    releaseCapacity = await app.capacity.acquire(account, "foreground", 1);
    app.auth.clock.advance(3_600_000); t.mock.timers.tick(3_600_000);
    const controller = new AbortController();
    const resumed = app.models.fetchDeferred(app.model, handle, { signal: controller.signal });
    await pump(() => app.capacity.requests === 3 && app.auth.refreshes === 1);
    app.auth.clock.advance(3_600_000); t.mock.timers.tick(3_600_000);
    if (cancel) app.auth.refreshGate = refreshing.promise;
    releaseCapacity(); releaseCapacity = undefined;
    if (cancel) {
      await pump(() => app.auth.refreshEntered === 2); controller.abort();
      assert.equal((await resumed).stopReason, "aborted"); assert.equal(app.capacity.active, 0);
      assert.equal(app.seen.length, 1, "An aborted refresh must never reach the model provider");
      refreshing.release(); await pump(() => app.auth.refreshes === 2);
      assert.equal((await app.models.fetchDeferred(app.model, handle, { signal: app.controller.signal })).stopReason, "stop",
        "The cancelled probe must release its lease so the same retained request can finish");
    } else assert.equal((await resumed).stopReason, "stop");
    assert.equal(app.auth.refreshes, 2); assert.deepEqual(app.seen, ["synthetic-before-wait", "synthetic-refreshed-2"]);
    assert.equal(app.capacity.active, 0);
  } finally { releaseCapacity?.(); refreshing.release(); t.mock.timers.reset(); await app.close(); }
});
