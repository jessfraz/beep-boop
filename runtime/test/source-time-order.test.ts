import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { openNodeSqliteDatabase } from "@earendil-works/chord/sqlite/node";
import { createModels } from "@earendil-works/pi-ai";
import { createRegistry, Harness } from "@earendil-works/pi-durable";
import { SqliteStorage } from "@earendil-works/pi-durable/storage/sqlite";
import type { SourceEvent } from "../src/generated/SourceEvent.js";
import { ConversationModels } from "../src/models.js";
import { SourceJournal } from "../src/sources.js";
import { TaskStore } from "../src/tasks.js";
import { LegacySources } from "./fixtures/state-storage.js";

async function open(directory: string) {
  const storage = await SqliteStorage.open(await openNodeSqliteDatabase(join(directory, "session.sqlite")));
  const models = createModels();
  const session = await Harness.open(storage, { models, registry: createRegistry(), identity: { attribution: null } }, BACKGROUND_CONTEXT);
  const sources = new SourceJournal(session, new TaskStore(session, new ConversationModels(models)));
  return { session, storage, sources };
}

function event(id: string, account = "alpha", occurred_at?: number): SourceEvent {
  return { id, version: id, source: { provider: "google", account, thread_id: id }, title: "Synthetic mail", text: "Original evidence",
    ...(occurred_at === undefined ? {} : { occurred_at }) };
}

/** Claim through production admission, then stop at the real missing-model
 * preflight boundary. No model/provider implementation or inference is needed. */
async function claim(app: Awaited<ReturnType<typeof open>>) {
  await assert.rejects(app.sources.triage({ type: "triage_sources", instructions: "", cwd: "/", hosts: [],
    model: { provider: "openai", model_id: "no-inference-for-source-order", reasoning: null, fast: false } },
  () => { assert.fail("An empty model registry must stop before execution binding"); }, []), /Unknown model/);
  const record = await app.storage.findDocument({ kind: "agent.source.control", scope: { kind: "session" } }, "current", BACKGROUND_CONTEXT);
  assert.ok(record);
  const document = await app.storage.document(record.id, "current", BACKGROUND_CONTEXT);
  const active = document?.value.active;
  assert.ok(active !== null && typeof active === "object" && !Array.isArray(active));
  const keys = active.events;
  assert.ok(Array.isArray(keys));
  const selected = keys.map((key) => { assert.ok(typeof key === "string"); return key; });
  const records = new Map((await app.sources.events({ keys: selected, limit: 100 })).map((item) => [item.key, item]));
  return selected.map((key) => { const item = records.get(key); assert.ok(item); return item; });
}

test("current provider messages stay beside old work when historical catchup arrives later", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-source-time-")); const app = await open(directory);
  try {
    const current = Date.UTC(2026, 10, 2);
    const events = [
      ...Array.from({ length: 100 }, (_, index) => event(`earlier-${index}`, "alpha", current - 86_400_000)),
      ...Array.from({ length: 3 }, (_, index) => event(`current-${index}`, "alpha", current + index)),
      ...Array.from({ length: 1000 }, (_, index) => event(`historical-${index}`, "alpha", current - 172_800_000)),
    ];
    await app.sources.admit({ source: { provider: "google", account: "alpha" }, events, coverage: { status: "complete" }, checkpoint: null }, null);
    await app.sources.admit({ source: { provider: "google", account: "beta" },
      events: Array.from({ length: 3 }, (_, index) => event(`other-${index}`, "beta", current)),
      coverage: { status: "complete" }, checkpoint: null }, null);
    const selected = await claim(app);
    assert.equal(selected.length, 100);
    assert.equal(new Set(selected.map((item) => item.key)).size, 100);
    assert.equal(selected.filter((item) => item.event.id.startsWith("current-")).length, 3,
      "Current mail must not wait behind later-admitted historical pages");
    assert.equal(selected.filter((item) => item.event.source.account === "beta").length, 1);
    assert.ok(selected.some((item) => item.event.id === "historical-0"), "Old work still advances");
  } finally { await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("rereading legacy mail enriches ordering without changing its original version or payload across reopen", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-source-time-legacy-")); let app = await open(directory);
  try {
    const original = event("legacy-current"), current = Date.UTC(2026, 10, 2);
    const source = { provider: "google", account: "alpha" };
    await app.sources.admit({ source, events: [original], coverage: { status: "complete" }, checkpoint: { value: "one" } }, null);
    const historical = Array.from({ length: 1000 }, (_, index) => event(`historical-${index}`, "alpha", current - 86_400_000));
    await app.sources.admit({ source, events: historical, coverage: { status: "complete" }, checkpoint: { value: "two" } }, { value: "one" });
    const enriched = await app.sources.admit({ source, events: [{ ...original, occurred_at: current }],
      coverage: { status: "complete" }, checkpoint: { value: "three" } }, { value: "two" });
    assert.equal(enriched.new_events, 0);
    const replay = await app.sources.admit({ source, events: [original], coverage: { status: "complete" },
      checkpoint: { value: "four" } }, { value: "three" });
    assert.equal(replay.new_events, 0);
    await app.session.close(BACKGROUND_CONTEXT); app = await open(directory);
    const saved = await app.sources.events({ provider: "google", account: "alpha", thread_id: original.id });
    assert.equal(saved.length, 1);
    assert.deepEqual(saved[0]?.event, original, "An admitted source payload remains immutable");
    assert.equal(saved[0]?.occurred_at, current);
    assert.ok((await claim(app)).some((item) => item.event.id === original.id));
    assert.equal((await app.sources.states()).find((item) => item.source.account === "alpha")?.checkpoint?.value, "four");
  } finally { await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("invalid provider times cannot mutate source evidence or advance its checkpoint", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-source-time-invalid-")); const app = await open(directory);
  try {
    for (const time of [-1, 0.5, Number.MAX_SAFE_INTEGER + 1]) {
      await assert.rejects(app.sources.admit({ source: { provider: "google", account: "alpha" },
        events: [event("invalid", "alpha", time)], coverage: { status: "complete" }, checkpoint: { value: "invalid" } }, null),
      /Source event time/);
    }
    assert.deepEqual(await app.sources.events({ provider: "google" }), []);
    assert.ok((await app.sources.states()).every((state) => state.source.provider !== "google"));
  } finally { await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("receipt freshness is immutable, provider clocks take precedence and legacy undated originals remain backfill", async t => {
  const directory = await mkdtemp(join(tmpdir(), "agent-source-receipt-order-")); let app = await open(directory);
  const now = Date.UTC(2035, 0, 1), source = { provider: "google", account: "alpha" };
  t.mock.timers.enable({ apis: ["Date"], now });
  try {
    const original = event("legacy-undated"), key = createHash("sha256").update(JSON.stringify([source.provider, source.account, original.id, original.version])).digest("hex");
    await app.session.commit(async tx => {
      const legacy = await tx.doc(LegacySources); legacy.events.push({ key, event: original, decision: null });
      legacy.sources.push({ source, checkpoint: null, coverage: { status: "complete" }, checked_at: 1 });
    }, BACKGROUND_CONTEXT);
    const retainedLegacy = await app.session.snapshot(LegacySources, BACKGROUND_CONTEXT);
    await app.sources.prepare();
    const undated = event("received-undated"), old = event("known-old", "alpha", now - 86_400_000), epoch = event("epoch", "alpha", 0);
    await app.sources.admit({ source, events: [undated, old, epoch], coverage: { status: "complete" }, checkpoint: null }, null);
    const admitted = await app.sources.events({ limit: 100 }), received = admitted.find(record => record.event.id === undated.id); assert.ok(received);
    assert.equal(received.received_at, now); assert.equal(admitted.find(record => record.key === key)?.received_at, undefined);
    assert.ok(admitted.filter(record => record.event.occurred_at !== undefined).every(record => record.received_at === undefined));
    await app.session.commit(async tx => {
      const pending = await app.sources.index.streamPendingIn(tx, source, { limit: 10 });
      const ordered: string[] = [];
      for (const item of pending) { const record = await app.sources.sourceRecordIn(tx, item.key); assert.ok(record); ordered.push(record.event.id); }
      assert.deepEqual(ordered, [original.id, epoch.id, old.id, undated.id]);
      assert.deepEqual((await app.sources.index.entitiesIn(tx, source, { fresh: true, now, limit: 10 })).map(entity => entity.source.thread_id), [undated.id]);
    }, BACKGROUND_CONTEXT);
    t.mock.timers.tick(3_600_000);
    await app.sources.admit({ source, events: [original, undated], coverage: { status: "complete" }, checkpoint: null }, null);
    await app.session.close(BACKGROUND_CONTEXT); app = await open(directory); await app.sources.prepare();
    assert.deepEqual(await app.sources.originals([key, received.key]), [received, { key, event: original, decision: null }]);
    await app.session.commit(async tx => {
      assert.deepEqual(await app.sources.index.entitiesIn(tx, source, { fresh: true, now: Date.now(), limit: 10 }), [],
        "A duplicate page must not refresh either a first receipt or an undated legacy original");
    }, BACKGROUND_CONTEXT);
    await app.sources.decide(admitted.filter(record => record.key !== received.key).map(record => record.key), { type: "dismissed", reason: "Retained comparison originals reviewed" });
    assert.equal(await app.session.commit(tx => app.sources.index.countsIn(tx, source), BACKGROUND_CONTEXT).then(counts => counts.oldest_pending_at), null);
    await app.sources.admit({ source, events: [{ ...undated, occurred_at: now }], coverage: { status: "complete" }, checkpoint: null }, null);
    assert.equal(await app.session.commit(tx => app.sources.index.countsIn(tx, source), BACKGROUND_CONTEXT).then(counts => counts.oldest_pending_at), now,
      "Provider-time enrichment must update diagnostics even when its time equals the receipt ordering key");
    const enriched = (await app.sources.originals([received.key]))[0]; assert.ok(enriched);
    assert.deepEqual(enriched.event, undated); assert.equal(enriched.received_at, now); assert.equal(enriched.occurred_at, now);
    assert.deepEqual(await app.session.snapshot(LegacySources, BACKGROUND_CONTEXT), retainedLegacy);
  } finally { t.mock.timers.reset(); await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});
