import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { openNodeSqliteDatabase } from "@earendil-works/chord/sqlite/node";
import { createModels } from "@earendil-works/pi-ai";
import { createRegistry, Harness } from "@earendil-works/pi-durable";
import { SqliteStorage } from "@earendil-works/pi-durable/storage/sqlite";
import { SourceCollectors, collectorSourceKey, discoverSourceCatalogs, type SourceCollectorOptions } from "../src/source-collectors.js";
import { SourceJournal } from "../src/sources.js";
import { TaskStore } from "../src/tasks.js";
import { HostRequests } from "../src/tools.js";
import type { SourceConfig } from "../src/generated/SourceConfig.js";
import type { SourceRead } from "../src/generated/SourceRead.js";
import type { SourceState } from "../src/generated/SourceState.js";
import { FakeClock, pump } from "./fixtures/background-runtime.js";

function config(account: string, provider: "google" | "github" = "google"): SourceConfig {
  return { provider, enabled: true, namespaces: [account] };
}
function read(source: SourceConfig, state: SourceState | null, next = "complete", more = false): SourceRead {
  const key = collectorSourceKey(source);
  return { expected_checkpoint: state?.checkpoint ?? null, batch: { source: key, checkpoint: { value: next },
    coverage: { status: more ? "more" : "complete" }, events: [{ id: key.account + ":" + next, version: "one",
      source: { ...key, thread_id: next }, title: "Example signal", text: "Retained source original." }] } };
}
function gate() {
  let release = () => {};
  const released = new Promise<void>(resolve => { release = resolve; });
  return { release, async wait(signal: AbortSignal) {
    signal.throwIfAborted(); let abort = () => {};
    const aborted = new Promise<never>((_resolve, reject) => { abort = () => reject(signal.reason); signal.addEventListener("abort", abort, { once: true }); });
    try { await Promise.race([released, aborted]); } finally { signal.removeEventListener("abort", abort); }
  } };
}
type Catalog = { sources: SourceConfig[]; reads: SourceRead[] };
type Publish = (catalog: Catalog) => Promise<void>;
type Fixture = { sources: SourceConfig[]; failures?: SourceRead[];
  discover?(signal: AbortSignal, publish?: Publish): Promise<Catalog>;
  poll(source: SourceConfig, state: SourceState | null, signal: AbortSignal): Promise<SourceRead> };
async function open(directory: string, clock: FakeClock, fixture: Fixture, options: SourceCollectorOptions = {}) {
  const registry = createRegistry(), database = await openNodeSqliteDatabase(join(directory, "session.sqlite"));
  const session = await Harness.open(await SqliteStorage.open(database),
    { models: createModels(), registry, now: clock.now, setTimer: clock.setTimer, identity: { attribution: null } }, BACKGROUND_CONTEXT);
  const journal = new SourceJournal(session, new TaskStore(session)); let discoveries = 0;
  const collectors = new SourceCollectors(session, registry, {
    discover: async (signal, publish?: Publish) => {
      discoveries += 1;
      return fixture.discover === undefined ? { sources: fixture.sources, reads: fixture.failures ?? [] } : fixture.discover(signal, publish);
    },
    states: () => journal.states(), poll: fixture.poll,
    admit: page => journal.admit(page.batch, page.expected_checkpoint),
  }, options);
  return { session, journal, collectors, discoveries: () => discoveries };
}
const signal = () => new AbortController().signal;

test("a discovered source admits consecutive pages while another source catalog is still discovering", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-source-progressive-discovery-")), clock = new FakeClock(), held = gate();
  const slow = config("example.slow"), fast = config("example.fast", "github"), started: string[] = [], seen: (string | null)[] = [];
  const fixture: Fixture = { sources: [slow, fast], async discover(abort, publish) {
    const discoveries = await Promise.allSettled([slow, fast].map(async source => {
      const key = collectorSourceKey(source); started.push(key.account);
      if (key.account === "example.slow") await held.wait(abort);
      const catalog = { sources: [source], reads: [] };
      if (publish !== undefined) await publish(catalog);
      return catalog;
    }));
    const sources: SourceConfig[] = [];
    for (const discovery of discoveries) {
      if (discovery.status === "rejected") throw discovery.reason;
      if (publish === undefined) sources.push(...discovery.value.sources);
    }
    return { sources, reads: [] };
  }, poll: async (source, state) => {
    if (collectorSourceKey(source).account === "example.fast") seen.push(state?.checkpoint?.value ?? null);
    return read(source, state, state === null ? "first" : "second", state === null);
  } };
  const app = await open(directory, clock, fixture); let finished = false;
  const discovering = app.collectors.sync(60, signal()).then(() => { finished = true; });
  try {
    await pump(async () => (await app.journal.states()).some(state => state.source.account === "example.fast" && state.checkpoint?.value === "first"));
    assert.deepEqual(new Set(started), new Set(["example.slow", "example.fast"]));
    assert.equal(finished, false, "The refresh retains ownership of its unfinished discovery operations");
    const initial = (await app.collectors.status()).find(slot => slot.source.account === "example.fast"); assert.ok(initial);
    await pump(async () => (await app.collectors.status()).find(slot => slot.source.account === "example.fast")?.next_at !== null);
    clock.advance(1000);
    await pump(async () => (await app.journal.states()).some(state => state.source.account === "example.fast" && state.checkpoint?.value === "second"));
    assert.equal(finished, false); assert.deepEqual(seen, [null, "first"]);
    assert.equal((await app.journal.events({ account: "example.fast", limit: 10 })).length, 2);
    held.release(); await discovering;
    assert.equal((await app.collectors.status()).find(slot => slot.source.account === "example.fast")?.task, initial.task);
    await pump(async () => (await app.collectors.status()).some(slot => slot.source.account === "example.slow" && slot.pages === 1));
  } finally { held.release(); await discovering.catch(() => {}); await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("failed discovery keeps successful partial readers and never prunes undiscovered accounts", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-source-failed-discovery-")), clock = new FakeClock(), held = gate();
  const retained = config("example.retained"), removed = config("example.removed", "github"), added = config("example.added", "github");
  const fixture: Fixture = { sources: [retained, removed], poll: async (source, state) => read(source, state) };
  const app = await open(directory, clock, fixture); let discovering: Promise<void> | undefined;
  try {
    await app.collectors.sync(60, signal()); await pump(async () => (await app.collectors.status()).every(slot => slot.pages === 1));
    const original = new Map((await app.collectors.status()).map(slot => [slot.source.account, slot.task]));
    fixture.sources = [added]; fixture.discover = async (abort, publish) => {
      if (publish !== undefined) await publish({ sources: [added], reads: [] });
      await held.wait(abort); throw new Error("One configured source catalog is unavailable");
    };
    discovering = app.collectors.sync(60, signal()); const failed = assert.rejects(discovering, /catalog is unavailable/u);
    await pump(async () => (await app.collectors.status()).some(slot => slot.source.account === "example.added" && slot.pages === 1));
    held.release(); await failed;
    const after = new Map((await app.collectors.status()).map(slot => [slot.source.account, slot.task]));
    assert.equal(after.get("example.retained"), original.get("example.retained"));
    assert.equal(after.get("example.removed"), original.get("example.removed"));
    const addedTask = after.get("example.added"); assert.ok(addedTask);
    delete fixture.discover;
    await app.collectors.sync(60, signal());
    assert.deepEqual((await app.collectors.status()).map(slot => [slot.source.account, slot.task]), [["example.added", addedTask]],
      "Only a completed successful refresh prunes omitted accounts, preserving the partial reader's native identity");
  } finally { held.release(); await discovering?.catch(() => {}); await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("large configuration discovery leaves shared host receipt capacity for independent page readers", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-source-discovery-receipts-")), clock = new FakeClock();
  const sources = [...Array.from({ length: 129 }, (_, index) => config("example.google-" + index)), config("example.fast", "github")];
  let held = true, pending = 0, maximum = 0;
  const delayed: { request: number; call: string; source: SourceConfig }[] = [], publications: string[] = [];
  const requests = new HostRequests((request, call, command) => {
    pending++; maximum = Math.max(maximum, pending);
    if (command.type !== "background") throw new Error("Unexpected discovery transport");
    const operation = command.request;
    if (operation.type === "discover_sources") {
      const source = operation.sources[0]; assert.ok(source); assert.equal(operation.sources.length, 1);
      if (held && source.provider === "google") { delayed.push({ request, call, source }); return; }
      queueMicrotask(() => { pending--; requests.reply(request, call, { type: "background",
        response: { type: "source_catalog", sources: [source], reads: [] } }); });
    } else if (operation.type === "poll_source") {
      queueMicrotask(() => { pending--; requests.reply(request, call, { type: "background",
        response: { type: "source_page", read: read(operation.source, operation.state, operation.state === null ? "first" : "second", operation.state === null) } }); });
    } else throw new Error("Unexpected source operation");
  });
  const fixture: Fixture = { sources, async discover(abort, publish) {
    assert.ok(publish);
    await discoverSourceCatalogs(sources, abort, async (source, signal) => {
      const result = await requests.call(1, { type: "background", request: { type: "discover_sources", sources: [source], states: [] } }, signal);
      assert.ok(result.type === "background" && result.response.type === "source_catalog"); return result.response;
    }, async catalog => { publications.push(...catalog.sources.map(source => collectorSourceKey(source).account)); await publish(catalog); });
    return { sources: [], reads: [] };
  }, async poll(source, state, abort) {
    const result = await requests.call(1, { type: "background", request: { type: "poll_source", source, state } }, abort);
    assert.ok(result.type === "background" && result.response.type === "source_page"); return result.response.read;
  } };
  const release = () => {
    held = false;
    for (const item of delayed.splice(0)) { pending--; requests.reply(item.request, item.call,
      { type: "background", response: { type: "source_catalog", sources: [item.source], reads: [] } }); }
  };
  const app = await open(directory, clock, fixture), discovering = app.collectors.sync(60, signal());
  try {
    await pump(async () => (await app.collectors.status()).some(slot => slot.source.account === "example.fast" && slot.pages === 1 && slot.next_at !== null));
    assert.equal(delayed.length, 2, "Provider discovery admission must leave room for independent providers and page reads");
    clock.advance(1000);
    await pump(async () => (await app.journal.states()).some(state => state.source.account === "example.fast" && state.checkpoint?.value === "second"));
    assert.ok(maximum <= 12, "Discovery and polling cannot consume the 128-entry shared host receipt table");
    release(); await discovering;
    assert.equal(new Set(publications).size, sources.length, "Every queued configuration eventually reaches native discovery");
    assert.equal((await app.collectors.status()).length, sources.length);
  } finally { release(); await discovering.catch(() => {}); requests.finish(1); await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

for (const stop of [false, true]) test(`${stop ? "stopping collectors" : "aborting discovery"} fences late catalogs without pruning unknown accounts`, async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-source-cancel-discovery-")), clock = new FakeClock(), held = gate();
  const retained = config("example.retained"), added = config("example.late", "github");
  const fixture: Fixture = { sources: [retained], poll: async (source, state) => read(source, state) };
  const app = await open(directory, clock, fixture), controller = new AbortController(); let discovering: Promise<void> | undefined, started = false, finished = false;
  try {
    await app.collectors.sync(60, signal()); await pump(async () => (await app.collectors.status())[0]?.pages === 1);
    const original = (await app.collectors.status())[0]?.task; assert.ok(original);
    fixture.discover = async (_abort, publish) => {
      started = true;
      // A cancelled native operation may still finish before its reply drains.
      // Publication must validate refresh ownership even if that producer is late.
      await held.wait(signal());
      if (publish !== undefined) await publish({ sources: [added], reads: [] });
      return { sources: publish === undefined ? [added] : [], reads: [] };
    };
    discovering = app.collectors.sync(60, controller.signal).finally(() => { finished = true; });
    const cancelled = assert.rejects(discovering);
    await pump(() => started);
    if (stop) await app.collectors.stop(); else controller.abort();
    assert.equal(finished, false, "Refresh cancellation must drain operations that it already started");
    held.release(); await cancelled;
    assert.deepEqual((await app.collectors.status()).map(slot => [slot.source.account, slot.task]), stop ? [] : [["example.retained", original]]);
    assert.equal((await app.journal.states()).some(state => state.source.account === "example.late"), false);
  } finally { held.release(); controller.abort(); await discovering?.catch(() => {}); await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("a slow source does not block an independent account and each page commits before continuation", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-source-collectors-")), clock = new FakeClock(), held = gate();
  const seen: { account: string; checkpoint: string | null }[] = [];
  const fixture: Fixture = { sources: [config("example.slow"), config("example.fast")], poll: async (source, state, abort) => {
    const key = collectorSourceKey(source); seen.push({ account: key.account, checkpoint: state?.checkpoint?.value ?? null });
    if (key.account === "example.slow") await held.wait(abort);
    return read(source, state, state?.checkpoint === null || state === null ? "first" : "second", state === null);
  } };
  const app = await open(directory, clock, fixture);
  try {
    await app.collectors.sync(60, signal());
    await pump(async () => (await app.journal.states()).some(state => state.source.account === "example.fast" && state.checkpoint?.value === "first"));
    assert.equal((await app.journal.states()).some(state => state.source.account === "example.slow"), false);
    const fast = (await app.collectors.status()).find(slot => slot.source.account === "example.fast"); assert.ok(fast);
    await pump(async () => (await app.collectors.status()).find(slot => slot.source.account === "example.fast")?.next_at !== null);
    clock.advance(1000);
    await pump(async () => (await app.journal.states()).some(state => state.source.account === "example.fast" && state.checkpoint?.value === "second"));
    assert.deepEqual(seen.filter(row => row.account === "example.fast").map(row => row.checkpoint), [null, "first"]);
    assert.equal(app.discoveries(), 1, "Page continuation cannot rediscover the account catalog");
    const originals = await app.journal.events({ provider: "google", account: "example.fast", limit: 10 });
    assert.equal(originals.length, 2); assert.ok(originals.every(record => record.event.text === "Retained source original."));
    held.release();
    await pump(async () => (await app.journal.states()).some(state => state.source.account === "example.slow"));
  } finally { held.release(); await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("global and per-provider capacity allow another provider past queued busy accounts", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-source-capacity-")), clock = new FakeClock(), held = gate();
  let active = 0, maximum = 0; const providers = new Map<string, number>(), maxima = new Map<string, number>();
  const started: string[] = [];
  const fixture: Fixture = { sources: [config("example.a"), config("example.b"), config("example.c"), config("example.github", "github")],
    poll: async (source, state, abort) => {
      const key = collectorSourceKey(source); active += 1; maximum = Math.max(maximum, active);
      const count = (providers.get(key.provider) ?? 0) + 1; providers.set(key.provider, count);
      maxima.set(key.provider, Math.max(maxima.get(key.provider) ?? 0, count)); started.push(key.account);
      try { if (key.provider === "google") await held.wait(abort); return read(source, state); }
      finally { active -= 1; providers.set(key.provider, (providers.get(key.provider) ?? 1) - 1); }
    } };
  const app = await open(directory, clock, fixture, { concurrency: 3, provider_concurrency: 2 });
  try {
    await app.collectors.sync(60, signal());
    await pump(async () => (await app.journal.states()).some(state => state.source.provider === "github"));
    assert.ok(started.includes("example.github"));
    assert.equal(started.filter(account => account !== "example.github").length, 2);
    assert.ok(maximum <= 3); assert.equal(maxima.get("google"), 2);
    held.release(); await pump(async () => (await app.collectors.status()).every(slot => slot.pages === 1));
  } finally { held.release(); await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("partial coverage retains its checkpoint and evidence without stopping other sources", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-source-partial-")), clock = new FakeClock();
  const fixture: Fixture = { sources: [config("example.partial"), config("example.complete", "github")], poll: async (source, state) => {
    const page = read(source, state);
    if (collectorSourceKey(source).account === "example.partial") {
      page.batch.coverage = { status: "partial", message: "Example page remains incomplete" }; page.batch.checkpoint = null;
    }
    return page;
  } };
  const app = await open(directory, clock, fixture);
  try {
    await app.collectors.sync(60, signal()); await pump(async () => (await app.collectors.status()).every(slot => slot.pages === 1));
    const partial = (await app.journal.states()).find(state => state.source.account === "example.partial"); assert.ok(partial);
    assert.equal(partial.checkpoint, null); assert.equal(partial.coverage.status, "partial");
    assert.equal((await app.journal.events({ account: "example.partial", limit: 10 })).length, 1);
    assert.equal((await app.journal.states()).find(state => state.source.account === "example.complete")?.coverage.status, "complete");
    assert.equal((await app.collectors.status()).find(slot => slot.source.account === "example.partial")?.next_at, clock.value + 60_000);
    const before = (await app.collectors.status()).map(slot => slot.task);
    fixture.failures = [{ expected_checkpoint: null, batch: { source: { provider: "google", account: "discovery" }, events: [],
      checkpoint: null, coverage: { status: "unavailable", message: "Discovery unavailable" } } }];
    fixture.sources = [config("example.complete", "github")];
    await app.collectors.sync(60, signal());
    assert.deepEqual((await app.collectors.status()).map(slot => slot.task), before, "Temporary discovery failure preserves independently scoped collectors");
  } finally { await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("collector restart retains native task identity and reads the next committed page", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-source-reopen-")), clock = new FakeClock(), seen: (string | null)[] = [];
  const fixture: Fixture = { sources: [config("example.reopen")], poll: async (source, state) => {
    seen.push(state?.checkpoint?.value ?? null); return read(source, state, state === null ? "first" : "second", state === null);
  } };
  let app = await open(directory, clock, fixture);
  try {
    await app.collectors.sync(60, signal()); await pump(async () => (await app.collectors.status())[0]?.pages === 1);
    const task = (await app.collectors.status())[0]?.task; assert.ok(task);
    await app.session.close(BACKGROUND_CONTEXT); app = await open(directory, clock, fixture);
    await app.collectors.sync(60, signal()); assert.equal((await app.collectors.status())[0]?.task, task);
    clock.advance(1000); await pump(async () => (await app.collectors.status())[0]?.pages === 2);
    assert.deepEqual(seen, [null, "first"]);
    assert.equal((await app.journal.events({ limit: 10 })).length, 2);
  } finally { await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("removed sources abort their readers without admitting a late page or deleting evidence", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-source-remove-")), clock = new FakeClock(), held = gate(); let started = false;
  const fixture: Fixture = { sources: [config("example.removed")], poll: async (source, state, abort) => {
    started = true; await held.wait(abort); return read(source, state);
  } };
  const app = await open(directory, clock, fixture);
  try {
    await app.collectors.sync(60, signal()); await pump(() => started);
    fixture.sources = []; await app.collectors.sync(60, signal()); held.release();
    assert.deepEqual(await app.collectors.status(), []); assert.deepEqual(await app.journal.events({ limit: 10 }), []);
    assert.equal((await app.session.inspect(BACKGROUND_CONTEXT)).tasks.some(task => task.record.kind === "agent.source.collector"), false);
  } finally { held.release(); await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("a source page deadline releases capacity and retains the failed account checkpoint", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-source-timeout-")), clock = new FakeClock(), held = gate();
  const fixture: Fixture = { sources: [config("example.timeout"), config("example.other", "github")], poll: async (source, state, abort) => {
    if (collectorSourceKey(source).account === "example.timeout") await held.wait(abort);
    return read(source, state);
  } };
  const app = await open(directory, clock, fixture, { concurrency: 1, page_timeout_ms: 20 });
  try {
    await app.collectors.sync(60, signal()); await new Promise<void>(resolve => setTimeout(resolve, 40));
    await pump(async () => (await app.journal.states()).some(state => state.source.account === "example.other"));
    const unavailable = (await app.journal.states()).find(state => state.source.account === "example.timeout"); assert.ok(unavailable);
    assert.equal(unavailable.coverage.status, "unavailable"); assert.equal(unavailable.checkpoint, null);
    const timed = (await app.collectors.status()).find(slot => slot.source.account === "example.timeout"); assert.ok(timed);
    assert.equal(timed.error, "Source collection failed; checkpoint retained"); assert.equal(timed.next_at, clock.value + 60_000);
  } finally { held.release(); await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("collector descriptors reject undiscovered account sets and keep local Messages explicit", () => {
  assert.throws(() => collectorSourceKey({ provider: "google", enabled: true, namespaces: null }), /one discovered account/u);
  assert.throws(() => collectorSourceKey({ provider: "github", enabled: true, namespaces: ["example.a", "example.b"] }), /one discovered account/u);
  assert.deepEqual(collectorSourceKey({ provider: "messages", enabled: true, namespaces: [] }), { provider: "messages", account: "local" });
});

test("a provider response cannot admit another account's evidence under the requested collector", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-source-account-fence-")), clock = new FakeClock();
  const fixture: Fixture = { sources: [config("example.owned")], poll: async (_source, state) => read(config("example.foreign"), state) };
  const app = await open(directory, clock, fixture);
  try {
    await app.collectors.sync(60, signal());
    await pump(async () => (await app.collectors.status())[0]?.error !== null);
    assert.equal((await app.journal.states()).some(state => state.source.provider === "google"), false);
    assert.deepEqual(await app.journal.events({ limit: 10 }), []);
    assert.equal((await app.collectors.status())[0]?.pages, 0);
  } finally { await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});
