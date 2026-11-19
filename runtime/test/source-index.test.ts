import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { createModels } from "@earendil-works/pi-ai";
import { createRegistry, Harness } from "@earendil-works/pi-durable";
import { SqliteStorage } from "@earendil-works/pi-durable/storage/sqlite";
import { openConversationDatabase } from "../src/conversation-database.js";
import { PartitionedRecords } from "../src/partitioned-records.js";
import { SourceIndex, sourceEntityId, type SourceIndexFilter, type SourceRecord } from "../src/source-index.js";

const NOW = Date.UTC(2027, 0, 1, 12);
const source = { provider: "slack", account: "example" };
function record(index: number, overrides: { provider?: string; account?: string; thread?: string; at?: number | null; decision?: SourceRecord["decision"] } = {}): SourceRecord {
  const at = overrides.at === undefined ? NOW - 1000 : overrides.at;
  return { key: "event-" + index, event: { id: "message-" + index, version: "v1", title: "Example source",
    text: "Retained original " + index, source: { provider: overrides.provider ?? source.provider, account: overrides.account ?? source.account,
      thread_id: overrides.thread ?? "discussion" }, ...(at === null ? {} : { occurred_at: at }) }, decision: overrides.decision ?? null };
}
async function open(path: string, prepare = true) {
  const storage = await SqliteStorage.open(await openConversationDatabase(path));
  const session = await Harness.open(storage,
    { models: createModels(), registry: createRegistry(), identity: { attribution: null } }, BACKGROUND_CONTEXT);
  const records = new PartitionedRecords<SourceRecord>("agent.source.event", false), index = new SourceIndex(session);
  await records.prepare(session, async () => [], item => item.key);
  if (prepare) await index.prepare(records);
  return { session, storage, records, index };
}
type App = Awaited<ReturnType<typeof open>>;
async function admit(app: App, items: readonly SourceRecord[], project = true): Promise<void> {
  for (let offset = 0; offset < items.length; offset += 32) await app.session.commit(async tx => {
    for (const item of items.slice(offset, offset + 32)) {
      const saved = await app.records.insertIn(tx, item.key, item), slot = await tx.doc(app.records.records, item.key, null);
      if (project) await app.index.projectIn(tx, saved.value, slot.ordinal);
    }
  }, BACKGROUND_CONTEXT);
}
async function decide(app: App, key: string, decision: SourceRecord["decision"], claimId?: string): Promise<void> {
  await app.session.commit(async tx => {
    const slot = await tx.doc(app.records.records, key, null); assert.ok(slot.value);
    slot.value.decision = decision;
    await app.index.projectIn(tx, slot.value, slot.ordinal, claimId);
  }, BACKGROUND_CONTEXT);
}
async function all(app: App, filter: SourceIndexFilter, limit = 2): Promise<string[]> {
  const result: string[] = [];
  let cursor: string | undefined;
  do {
    const page = await app.session.commit(tx => app.index.pageIn(tx, { ...filter, limit, ...(cursor === undefined ? {} : { cursor }) }), BACKGROUND_CONTEXT);
    assert.ok(page.scanned <= limit); result.push(...page.keys); cursor = page.cursor ?? undefined;
    if (page.complete) assert.equal(cursor, undefined);
  } while (cursor !== undefined);
  return result;
}

test("source projection filters retain admission order and bounded cursors across reopen", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-source-index-page-")), path = join(directory, "session.sqlite");
  let app = await open(path);
  try {
    const items = [record(0, { at: NOW - 30 }), record(1, { thread: "other", at: NOW - 10 }), record(2, { account: "second" }),
      record(3, { provider: "mail" }), record(4, { at: NOW - 100 }), record(5, { thread: "undated", at: null })];
    await admit(app, items);
    assert.deepEqual(await all(app, {}), items.map(item => item.key).reverse());
    assert.deepEqual(await all(app, { ...source, thread_id: "discussion" }), ["event-4", "event-0"]);
    assert.deepEqual(await all(app, { provider: "slack", thread_id: "discussion" }), ["event-4", "event-2", "event-0"]);
    assert.deepEqual(await all(app, { account: "example", thread_id: "discussion" }), ["event-4", "event-3", "event-0"]);
    const first = await app.session.commit(tx => app.index.pageIn(tx, { provider: "slack", limit: 1 }), BACKGROUND_CONTEXT);
    assert.ok(first.cursor);
    await assert.rejects(app.session.commit(tx => app.index.pageIn(tx, { provider: "mail", limit: 1, cursor: first.cursor ?? "" }), BACKGROUND_CONTEXT), /another selection/u);
    await decide(app, "event-0", { type: "dismissed", reason: "Reviewed original" });
    await app.session.close(BACKGROUND_CONTEXT); app = await open(path);
    assert.deepEqual(await all(app, { ...source, pending: false }), ["event-0"]);
    assert.deepEqual(await all(app, { ...source, pending: true }), ["event-5", "event-4", "event-1"]);
    const pending = await app.session.commit(tx => app.index.streamPendingIn(tx, source, { limit: 10 }), BACKGROUND_CONTEXT);
    assert.deepEqual(pending.map(item => item.key), ["event-5", "event-4", "event-1"]);
    assert.deepEqual(await app.session.commit(tx => app.index.countsIn(tx, source), BACKGROUND_CONTEXT),
      { retained: 4, pending: 3, dirty_entities: 3, oldest_pending_at: null });
    assert.deepEqual(await app.records.get(app.session, "event-0"), { ...items[0], decision: { type: "dismissed", reason: "Reviewed original" } });
    await app.session.commit(async tx => {
      const before = await app.index.countsIn(tx, source), slot = await tx.doc(app.records.records, "event-0", null); assert.ok(slot.value);
      await app.index.projectIn(tx, slot.value, slot.ordinal);
      assert.deepEqual(await app.index.countsIn(tx, source), before, "Replaying a projection does not double-count it");
      assert.deepEqual(await app.index.pageIn(tx, { provider: "missing", account: "missing", limit: 5 }),
        { keys: [], cursor: null, scanned: 0, complete: true });
    }, BACKGROUND_CONTEXT);
  } finally { await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("entity claims coalesce originals, retain arrivals and expose manual correction fences", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-source-index-claim-")), path = join(directory, "session.sqlite");
  let app = await open(path);
  try {
    const items = [record(0), record(1), record(2)], id = sourceEntityId(items[0]!);
    await admit(app, items);
    await app.session.commit(async tx => {
      const ready = await app.index.entitiesIn(tx, source, { fresh: true, now: NOW, limit: 10 });
      assert.equal(ready.length, 1); assert.equal(ready[0]?.pending, 3);
      const captured = await app.index.claimEntityIn(tx, id, "claim-one"); assert.ok(captured);
      assert.equal(captured.revision, 3); assert.equal(captured.decision_revision, 0);
      assert.equal(await app.index.claimEntityIn(tx, id, "claim-two"), undefined);
      assert.deepEqual(await app.index.entitiesIn(tx, source, { fresh: true, now: NOW, limit: 10 }), []);
      assert.equal((await app.index.countsIn(tx, source)).dirty_entities, 1, "Leased entities remain dirty until their actual pending work is accounted for");
    }, BACKGROUND_CONTEXT);
    await app.session.close(BACKGROUND_CONTEXT); app = await open(path);
    await admit(app, [record(3, { at: NOW })]);
    await decide(app, "event-0", { type: "task", task_id: "task-a" }, "claim-one");
    await decide(app, "event-1", { type: "task", task_id: "task-a" }, "claim-one");
    await app.session.commit(async tx => {
      const current = await app.index.entityIn(tx, id); assert.ok(current);
      assert.equal(current.revision, 4); assert.equal(current.pending, 2); assert.equal(current.decision_revision, 0);
      assert.equal(current.owner_task_id, "task-a"); assert.equal(await app.index.ownerCountIn(tx, id, "task-a"), 2);
      assert.equal(await app.index.releaseEntityIn(tx, id, "wrong-claim"), false);
      assert.equal(await app.index.releaseEntityIn(tx, id, "claim-one"), true);
      const pending = await app.index.pendingIn(tx, id, { limit: 10 });
      assert.deepEqual(pending.map(item => item.key), ["event-2", "event-3"]);
      const next = await app.index.claimEntityIn(tx, id, "claim-two"); assert.ok(next); assert.equal(next.revision, 4);
    }, BACKGROUND_CONTEXT);
    await decide(app, "event-1", { type: "task", task_id: "task-b" });
    await app.session.commit(async tx => {
      const current = await app.index.entityIn(tx, id); assert.ok(current);
      assert.equal(current.decision_revision, 1, "Manual correction invalidates the captured decision revision");
      assert.equal(current.owner_task_id, "task-b"); assert.equal(await app.index.ownerCountIn(tx, id, "task-a"), 1);
      assert.equal(await app.index.ownerCountIn(tx, id, "task-b"), 1);
      assert.equal(await app.index.ownerCountIn(tx, id, "unknown"), 0);
    }, BACKGROUND_CONTEXT);
    await assert.rejects(decide(app, "event-2", { type: "dismissed", reason: "Stale review" }, "claim-one"), /no longer owns/u);
    assert.equal((await app.records.get(app.session, "event-2"))?.decision, null, "A rejected claim rolls back its original decision too");
    await app.session.commit(async tx => {
      await app.index.releaseEntityIn(tx, id, "claim-two");
      const captured = await app.index.claimEntityIn(tx, id, "claim-three"); assert.equal(captured?.decision_revision, 1);
    }, BACKGROUND_CONTEXT);
    await decide(app, "event-2", { type: "dismissed", reason: "Accounted for exact original" }, "claim-three");
    await decide(app, "event-3", { type: "dismissed", reason: "Accounted for later arrival" }, "claim-three");
    await app.session.commit(async tx => {
      assert.equal(await app.index.releaseEntityIn(tx, id, "claim-three"), true);
      assert.equal((await app.index.countsIn(tx, source)).pending, 0);
      assert.equal((await app.index.countsIn(tx, source)).dirty_entities, 0);
      assert.deepEqual((await app.index.relatedIn(tx, id, { reverse: true, limit: 10 })).map(item => item.key),
        ["event-3", "event-2", "event-1", "event-0"]);
    }, BACKGROUND_CONTEXT);
  } finally { await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("fresh and backfill entities use the latest still-pending event without dropping older evidence", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-source-index-age-")), app = await open(join(directory, "session.sqlite"));
  try {
    const items = [record(0, { thread: "active", at: NOW - 86_400_000 }), record(1, { thread: "active", at: NOW }),
      record(2, { thread: "old", at: NOW - 3_600_000 }), record(3, { thread: "boundary", at: NOW - 900_000 }),
      record(4, { thread: "unknown", at: null })];
    await admit(app, items);
    await app.session.commit(async tx => {
      assert.deepEqual((await app.index.entitiesIn(tx, source, { fresh: true, now: NOW, limit: 10 })).map(entity => entity.source.thread_id), ["active", "boundary"]);
      assert.deepEqual((await app.index.entitiesIn(tx, source, { fresh: false, now: NOW, limit: 10 })).map(entity => entity.source.thread_id), ["unknown", "active", "old"]);
      const active = sourceEntityId(items[0]!);
      assert.deepEqual((await app.index.pendingIn(tx, active, { limit: 10 })).map(item => item.key), ["event-0", "event-1"]);
    }, BACKGROUND_CONTEXT);
    await decide(app, "event-1", { type: "dismissed", reason: "Latest event handled" });
    await app.session.commit(async tx => {
      assert.deepEqual((await app.index.entitiesIn(tx, source, { fresh: true, now: NOW, limit: 10 })).map(entity => entity.source.thread_id), ["boundary"]);
      assert.deepEqual((await app.index.entitiesIn(tx, source, { fresh: false, now: NOW, limit: 10 })).map(entity => entity.source.thread_id), ["unknown", "active", "old"]);
      const slot = await tx.doc(app.records.records, "event-4", null); assert.ok(slot.value);
      slot.value.occurred_at = NOW - 500; await app.index.projectIn(tx, slot.value, slot.ordinal);
      assert.deepEqual((await app.index.entitiesIn(tx, source, { fresh: true, now: NOW, limit: 10 })).map(entity => entity.source.thread_id), ["unknown", "boundary"]);
      assert.equal((await app.index.countsIn(tx, source)).oldest_pending_at, NOW - 86_400_000);
    }, BACKGROUND_CONTEXT);
  } finally { await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("resource ownership rebuilds from GitHub originals and follows manual corrections without inferring Slack URL ownership", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-source-resource-owner-")), app = await open(join(directory, "session.sqlite"), false);
  const resource = "github:example/project:42", url = "https://api.github.com/repos/example/project/issues/42";
  try {
    const first = record(0, { provider: "github", decision: { type: "task", task_id: "old-owner" } });
    first.event.text = "Subject: Related discussion https://github.com/example/project/issues/99\nSubject API: " + url;
    const second = record(1, { provider: "slack", decision: { type: "task", task_id: "unrelated-owner" } }); second.event.text = "A related link, not an ownership declaration: " + url;
    await admit(app, [first, second], false); await app.index.prepare(app.records);
    assert.deepEqual(await app.session.commit(tx => app.index.resourceOwnersIn(tx, resource, 2), BACKGROUND_CONTEXT), ["old-owner"]);
    assert.deepEqual(await app.session.commit(tx => app.index.resourceOwnersIn(tx, "github:example/project:99", 2), BACKGROUND_CONTEXT), []);
    await decide(app, first.key, { type: "task", task_id: "current-owner" });
    assert.deepEqual(await app.session.commit(tx => app.index.resourceOwnersIn(tx, resource, 2), BACKGROUND_CONTEXT), ["current-owner"]);
    await decide(app, first.key, { type: "dismissed", reason: "Ownership corrected" });
    assert.deepEqual(await app.session.commit(tx => app.index.resourceOwnersIn(tx, resource, 2), BACKGROUND_CONTEXT), []);
  } finally { await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("task outcome entities retain their task and conversation identity while source reads retain thread scope", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-source-index-outcome-")), app = await open(join(directory, "session.sqlite"));
  try {
    const first = record(0, { provider: "task_outcomes", account: "local", thread: "example-task" });
    first.outcome = { task_id: "example-task", conversation_id: "conversation-one", entry_id: null, tool_call_id: null,
      summary: "Example outcome", sources: [], state: { state: "progress" } };
    const second: SourceRecord = { ...record(1, { provider: "task_outcomes", account: "local", thread: "example-task" }), outcome: { ...first.outcome, conversation_id: "conversation-two" } };
    const third: SourceRecord = { ...record(2, { provider: "task_outcomes", account: "local", thread: "example-task" }), outcome: first.outcome };
    await admit(app, [first, second, third]);
    assert.deepEqual(await all(app, { provider: "task_outcomes", account: "local", thread_id: "example-task" }), [third.key, second.key, first.key]);
    await app.session.commit(async tx => {
      const ready = await app.index.entitiesIn(tx, { provider: "task_outcomes", account: "local" }, { fresh: true, now: NOW, limit: 10 });
      assert.equal(ready.length, 2);
      assert.deepEqual((await app.index.relatedIn(tx, sourceEntityId(first), { limit: 10 })).map(item => item.key), [first.key, third.key]);
      assert.deepEqual((await app.index.relatedIn(tx, sourceEntityId(second), { limit: 10 })).map(item => item.key), [second.key]);
      assert.equal((await app.index.entityIn(tx, sourceEntityId(first)))?.owner_task_id, "example-task");
    }, BACKGROUND_CONTEXT);
    const mismatched: SourceRecord = { ...record(3, { provider: "task_outcomes", account: "another-account", thread: "example-task" }), outcome: first.outcome };
    await assert.rejects(admit(app, [mismatched]), /source identity changed/u);
    assert.equal(await app.records.get(app.session, mismatched.key), undefined, "A cross-account outcome collision rolls back original admission");
  } finally { await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("projection migration resumes its exact native position and preserves originals already projected ahead", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-source-index-migration-")), path = join(directory, "session.sqlite");
  let app = await open(path, false);
  try {
    const items = Array.from({ length: 137 }, (_, index) => record(index, { thread: "discussion-" + index % 11,
      ...(index === 129 ? { decision: { type: "dismissed", reason: "Already accounted for" } } : {}) }));
    await admit(app, items, false);
    const originalDirectory = await app.session.snapshot(app.records.directory, BACKGROUND_CONTEXT);
    await app.session.commit(async tx => {
      const slot = await tx.doc(app.records.records, "event-129", null); assert.ok(slot.value);
      await app.index.projectIn(tx, slot.value, slot.ordinal);
      assert.equal(await app.index.readyIn(tx), false);
      assert.equal(await app.index.claimEntityIn(tx, sourceEntityId(items[0]!), "incomplete-migration"), undefined);
    }, BACKGROUND_CONTEXT);
    const commit = app.session.commit.bind(app.session); let attempts = 0;
    app.session.commit = async (change, context) => {
      if (++attempts === 3) throw new Error("Stop before the next migration commit");
      return commit(change, context);
    };
    await assert.rejects(app.index.prepare(app.records), /Stop before/u);
    app.session.commit = commit;
    await app.session.close(BACKGROUND_CONTEXT); app = await open(path, false);
    await app.session.commit(async tx => {
      assert.equal(await app.index.readyIn(tx), false);
      assert.deepEqual(await app.index.lookupIn(tx, "event-63"), { ordinal: 63 });
      assert.equal(await app.index.lookupIn(tx, "event-64"), undefined);
      assert.deepEqual(await app.index.lookupIn(tx, "event-129"), { ordinal: 129 });
    }, BACKGROUND_CONTEXT);
    await app.index.prepare(app.records);
    await app.session.commit(async tx => {
      assert.equal(await app.index.readyIn(tx), true);
      assert.deepEqual(await app.index.countsIn(tx, source), { retained: 137, pending: 136, dirty_entities: 11, oldest_pending_at: NOW - 1000 });
    }, BACKGROUND_CONTEXT);
    await app.index.prepare(app.records);
    assert.deepEqual(await app.session.snapshot(app.records.directory, BACKGROUND_CONTEXT), originalDirectory);
    for (const original of items) assert.deepEqual(await app.records.get(app.session, original.key), original);
  } finally { await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("cold projection lookups hydrate no unrelated originals and preserve bounded candidate pages", { timeout: 120_000 }, async t => {
  const directory = await mkdtemp(join(tmpdir(), "agent-source-index-bounded-")), path = join(directory, "session.sqlite");
  let app = await open(path);
  try {
    const target = [record(0, { thread: "target" }), record(1, { thread: "target" })];
    await admit(app, target);
    await admit(app, Array.from({ length: 768 }, (_, index) => record(index + 2, { thread: "unrelated-" + index, at: NOW - 86_400_000 })));
    await app.session.close(BACKGROUND_CONTEXT); app = await open(path);
    const originals: string[] = [], pages: string[] = [], find = app.storage.findDocument.bind(app.storage);
    app.storage.findDocument = async (address, at, context) => {
      if (address.kind === "agent.source.event.records" && address.key !== undefined) originals.push(address.key);
      if (address.kind.startsWith("agent.source.projection.") && address.kind.endsWith(".pages") && address.key !== undefined) pages.push(address.kind + ":" + address.key);
      return find(address, at, context);
    };
    const result = await app.session.commit(async tx => {
      const page = await app.index.pageIn(tx, { ...source, thread_id: "target", limit: 2 });
      assert.deepEqual(page, { keys: ["event-1", "event-0"], cursor: null, scanned: 2, complete: true });
      assert.deepEqual(await app.index.lookupIn(tx, "event-1"), { ordinal: 1 });
      assert.equal(await app.index.lookupIn(tx, "absent-original"), undefined);
      assert.deepEqual((await app.index.entitiesIn(tx, source, { fresh: true, now: NOW, limit: 5 })).map(entity => entity.source.thread_id), ["target"]);
      return page;
    }, BACKGROUND_CONTEXT);
    assert.deepEqual(originals, [], "Projection selection must not hydrate originals before the caller requests its selected keys");
    assert.ok(new Set(pages).size < 40, "A narrow projection lookup must traverse bounded metadata pages beside the retained backlog");
    t.diagnostic(JSON.stringify({ retained: 770, selected: result.keys.length, original_reads: originals.length, projection_pages: new Set(pages).size }));
  } finally { await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});
