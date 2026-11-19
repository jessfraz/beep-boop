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
import type { SourceEvent } from "../src/generated/SourceEvent.js";
import type { TaskOutcome } from "../src/generated/TaskOutcome.js";
import { SourceJournal } from "../src/sources.js";
import { TaskStore } from "../src/tasks.js";

async function open(path: string) {
  const storage = await SqliteStorage.open(await openConversationDatabase(path));
  const session = await Harness.open(storage,
    { models: createModels(), registry: createRegistry(), identity: { attribution: null } }, BACKGROUND_CONTEXT);
  return { storage, session, sources: new SourceJournal(session, new TaskStore(session)) };
}

function event(id: string, thread: string, text: string,
  options: { provider?: string; account?: string; version?: string; occurred_at?: number } = {}): SourceEvent {
  return { id, version: options.version ?? "v1", source: {
    provider: options.provider ?? "slack", account: options.account ?? "alpha", thread_id: thread },
    title: "Example message", text,
    ...(options.occurred_at === undefined ? {} : { occurred_at: options.occurred_at }) };
}

async function admit(sources: SourceJournal, events: SourceEvent[]): Promise<void> {
  const first = events[0]; assert.ok(first);
  await sources.admit({ source: { provider: first.source.provider, account: first.source.account },
    events, coverage: { status: "complete" }, checkpoint: null }, null);
}

async function seed(sources: SourceJournal) {
  const first = event("original", "target", "Original retained text", { occurred_at: 300 });
  const middle = event("reply", "target", "Needle reply", { occurred_at: 200 });
  const latest = event("original", "target", "Updated NEEDLE text", { version: "v2", occurred_at: 100 });
  const otherThread = event("elsewhere", "other", "Needle in another thread");
  const otherAccount = event("other-account", "target", "Needle in another account", { account: "beta" });
  const otherProvider = event("other-provider", "target", "Needle from another provider", { provider: "mail" });
  await admit(sources, [first, middle, latest, otherThread]);
  await admit(sources, [otherAccount]);
  await admit(sources, [otherProvider]);
  return { first, middle, latest, otherThread, otherAccount, otherProvider };
}

const thread = { provider: "slack", account: "alpha", thread_id: "target" };

test("exact task outcome reads retain the outcome index's conversation scope", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-source-outcome-thread-")), app = await open(join(directory, "session.sqlite"));
  try {
    const task = await app.sources.tasks.create({ title: "Retained work", outcome: "Keep exact lifecycle evidence" });
    const outcome: TaskOutcome = { task_id: task.id, conversation_id: task.conversation_id, entry_id: null, tool_call_id: null,
      summary: "Verified progress", sources: [], state: { state: "progress" } };
    await app.session.commit(tx => app.sources.recordIn(tx, outcome, false), BACKGROUND_CONTEXT);
    const partial = await app.sources.events({ provider: "task_outcomes", thread_id: task.id });
    assert.equal(partial.length, 1);
    assert.deepEqual(await app.sources.events({ provider: "task_outcomes", account: "local", thread_id: task.id }), partial);
  } finally { await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("exact source thread reads retain version order, filters and decisions across reopen", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-source-thread-read-")), path = join(directory, "session.sqlite");
  let app = await open(path);
  try {
    const originals = await seed(app.sources);
    const records = await app.sources.events(thread);
    assert.deepEqual(records.map(record => record.event), [originals.latest, originals.middle, originals.first],
      "Thread results follow admission order, retaining distinct versions rather than sorting by provider time");
    const middle = records.find(record => record.event.id === originals.middle.id); assert.ok(middle);
    await app.sources.decide([middle.key], { type: "dismissed", reason: "Reviewed this reply" });
    await app.session.close(BACKGROUND_CONTEXT); app = await open(path);

    assert.deepEqual((await app.sources.events(thread)).map(record => record.event),
      [originals.latest, originals.middle, originals.first]);
    assert.deepEqual((await app.sources.events({ ...thread, limit: 2 })).map(record => record.event),
      [originals.latest, originals.middle]);
    assert.deepEqual((await app.sources.events({ ...thread, query: "nEeDlE" })).map(record => record.event),
      [originals.latest, originals.middle]);
    assert.deepEqual((await app.sources.events({ ...thread, pending: true })).map(record => record.event),
      [originals.latest, originals.first]);
    assert.deepEqual((await app.sources.events({ ...thread, query: "needle", pending: true, limit: 1 })).map(record => record.event),
      [originals.latest]);
    assert.deepEqual((await app.sources.events({ ...thread, thread_id: "absent" })), []);
  } finally { await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("partial source filters and explicit keys retain their selection scope", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-source-thread-filters-")), app = await open(join(directory, "session.sqlite"));
  try {
    const originals = await seed(app.sources);
    assert.deepEqual((await app.sources.events({ provider: "slack", thread_id: "target" })).map(record => record.event),
      [originals.otherAccount, originals.latest, originals.middle, originals.first]);
    assert.deepEqual((await app.sources.events({ account: "alpha", thread_id: "target" })).map(record => record.event),
      [originals.otherProvider, originals.latest, originals.middle, originals.first]);
    assert.deepEqual((await app.sources.events({ provider: "slack", account: "alpha" })).map(record => record.event),
      [originals.otherThread, originals.latest, originals.middle, originals.first]);

    const records = await app.sources.events({ limit: 100 });
    const first = records.find(record => record.event.id === originals.first.id && record.event.version === "v1"); assert.ok(first);
    const latest = records.find(record => record.event.id === originals.latest.id && record.event.version === "v2"); assert.ok(latest);
    const other = records.find(record => record.event.id === originals.otherAccount.id); assert.ok(other);
    assert.deepEqual((await app.sources.events({ ...thread, keys: [first.key, other.key, latest.key] })).map(record => record.event),
      [originals.latest, originals.first], "Explicit keys must not expand to all indexed thread records");
    assert.deepEqual((await app.sources.events({ ...thread, keys: [] })), []);
  } finally { await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("cold exact thread reads hydrate only indexed originals beside unrelated retained records", { timeout: 30_000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-source-thread-bound-")), path = join(directory, "session.sqlite");
  let app = await open(path);
  try {
    const first = event("target-first", "target", "Retained first message");
    const latest = event("target-latest", "target", "Retained latest message");
    await admit(app.sources, [first, latest]);
    const targetKeys = new Set((await app.sources.events(thread)).map(record => record.key));
    await admit(app.sources, Array.from({ length: 512 }, (_, index) =>
      event("unrelated-" + index, "unrelated-thread-" + index, "Unrelated original evidence. ".repeat(128))));
    await app.session.close(BACKGROUND_CONTEXT); app = await open(path);

    // Observe the real cold SQLite storage boundary. An in-memory journal can
    // mask the full-corpus hydration that made exact thread reads expensive.
    const reads: string[] = [], findDocument = app.storage.findDocument.bind(app.storage);
    app.storage.findDocument = async (address, at, context) => {
      if (address.kind === "agent.source.event.records" && typeof address.key === "string") reads.push(address.key);
      return findDocument(address, at, context);
    };
    const selected = await app.sources.events(thread);
    assert.deepEqual(selected.map(record => record.event), [latest, first]);
    const hydrated = new Set(reads);
    assert.equal(hydrated.size, targetKeys.size, "Exact thread lookup must hydrate only its indexed source originals");
    assert.deepEqual(hydrated, targetKeys, "Exact thread lookup must not hydrate unrelated source originals");
    reads.length = 0;
    assert.deepEqual(await app.sources.events({ ...thread, thread_id: "absent" }), []);
    assert.deepEqual(reads, [], "An unknown exact thread must not fall back to scanning the retained corpus");
  } finally { await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("provider and account misses hydrate no retained originals across cold reopen", { timeout: 30_000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-source-filter-bound-")), path = join(directory, "session.sqlite");
  let app = await open(path);
  try {
    await admit(app.sources, Array.from({ length: 128 }, (_, index) => event(`slack-${index}`, `slack-thread-${index}`, "Retained Slack evidence")));
    await admit(app.sources, Array.from({ length: 128 }, (_, index) =>
      event(`mail-${index}`, `mail-thread-${index}`, "Retained mail evidence", { provider: "mail", account: "beta" })));
    await app.session.close(BACKGROUND_CONTEXT); app = await open(path);
    const hydrated: string[] = [], findDocument = app.storage.findDocument.bind(app.storage);
    app.storage.findDocument = async (address, at, context) => {
      if (address.kind === "agent.source.event.records" && typeof address.key === "string") hydrated.push(address.key);
      return findDocument(address, at, context);
    };
    assert.deepEqual(await app.sources.events({ provider: "unknown" }), []);
    assert.deepEqual(await app.sources.events({ account: "unknown" }), []);
    assert.deepEqual(await app.sources.events({ provider: "slack", account: "beta" }), []);
    assert.deepEqual(hydrated, [], "Negative indexed selections must not scan unrelated originals");
  } finally { await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("substring pages bound candidate hydration and retain continuation through empty matches", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-source-query-page-")), path = join(directory, "session.sqlite");
  let app = await open(path);
  try {
    await admit(app.sources, [event("first", "first", "The older NEEDLE remains searchable"),
      ...Array.from({ length: 8 }, (_, index) => event(`newer-${index}`, `newer-${index}`, "Unrelated retained evidence"))]);
    await app.session.close(BACKGROUND_CONTEXT); app = await open(path);
    const hydrated: string[] = [], findDocument = app.storage.findDocument.bind(app.storage);
    app.storage.findDocument = async (address, at, context) => {
      if (address.kind === "agent.source.event.records" && typeof address.key === "string") hydrated.push(address.key);
      return findDocument(address, at, context);
    };
    const filter = { provider: "slack", account: "alpha", query: "needle", limit: 3 };
    let page = await app.sources.eventPage(filter);
    assert.deepEqual(page.events, []); assert.equal(page.scanned, 3); assert.equal(page.complete, false); assert.ok(page.cursor);
    assert.equal(new Set(hydrated).size, 3, "A substring miss hydrates only its bounded indexed candidates");
    await assert.rejects(app.sources.eventPage({ ...filter, query: "different", cursor: page.cursor }), /another selection/u);
    const matches: string[] = [];
    while (page.cursor !== null) {
      page = await app.sources.eventPage({ ...filter, cursor: page.cursor });
      assert.ok(page.scanned <= 3); matches.push(...page.events.map(record => record.event.id));
    }
    assert.equal(page.complete, true); assert.deepEqual(matches, ["first"]);
  } finally { await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("new source admission and identity lookup bypass retained legacy membership buckets", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-source-membership-bound-")), path = join(directory, "session.sqlite");
  let app = await open(path);
  try {
    await app.sources.prepare();
    const membershipReads: string[] = [], findDocument = app.storage.findDocument.bind(app.storage);
    app.storage.findDocument = async (address, at, context) => {
      if (address.kind === "agent.source.event.membership" && typeof address.key === "string") membershipReads.push(address.key);
      return findDocument(address, at, context);
    };
    const original = event("new-original", "new-thread", "Original evidence remains intact");
    await admit(app.sources, [original]);
    const record = (await app.sources.events({ provider: "slack", account: "alpha" }))[0]; assert.ok(record);
    await app.sources.decide([record.key], { type: "dismissed", reason: "Verified no action needed" });
    assert.deepEqual(membershipReads, []);
    await app.session.close(BACKGROUND_CONTEXT); app = await open(path);
    await app.session.commit(async tx => {
      const restored = await app.sources.sourceRecordIn(tx, record.key);
      assert.deepEqual(restored?.event, original);
      assert.deepEqual(restored?.decision, { type: "dismissed", reason: "Verified no action needed" });
    }, BACKGROUND_CONTEXT);
  } finally { await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});
