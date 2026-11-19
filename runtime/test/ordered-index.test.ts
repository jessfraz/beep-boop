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
import { OrderedIndex } from "../src/ordered-index.js";

type Value = { ordinal: number; text: string; evidence: { versions: string[] } };
function key(index: number): string { return String(index).padStart(8, "0"); }
function value(index: number): Value { return { ordinal: index, text: "Original " + index, evidence: { versions: ["v1", "v2"] } }; }
async function open(path: string) {
  const storage = await SqliteStorage.open(await openConversationDatabase(path));
  const session = await Harness.open(storage,
    { models: createModels(), registry: createRegistry(), identity: { attribution: null } }, BACKGROUND_CONTEXT);
  return { session, storage, index: new OrderedIndex<Value>("test.ordered") };
}
async function insert(app: Awaited<ReturnType<typeof open>>, entries: readonly number[], index = "work") {
  for (let offset = 0; offset < entries.length; offset += 128) {
    await app.session.commit(async tx => {
      for (const entry of entries.slice(offset, offset + 128)) await app.index.setIn(tx, index, key(entry), value(entry));
    }, BACKGROUND_CONTEXT);
  }
}

test("native ordered indexes retain exact values, scope, strict ranges and updates through reopen", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-ordered-values-")), path = join(directory, "session.sqlite");
  let app = await open(path);
  try {
    const entries = Array.from({ length: 1153 }, (_, index) => index);
    await insert(app, entries.map(index => index * 977 % entries.length));
    await app.session.commit(async tx => {
      await app.index.setIn(tx, "other", key(500), value(-500));
      const supplied = value(5000);
      await app.index.setIn(tx, "work", key(500), supplied);
      supplied.evidence.versions.push("outside the native draft");
      assert.equal(await app.index.countIn(tx, "work"), entries.length);
      assert.deepEqual(await app.index.getIn(tx, "work", key(500)), value(5000));
      assert.deepEqual(await app.index.rangeIn(tx, "work", { after: key(126), before: key(132), limit: 100 }),
        [127, 128, 129, 130, 131].map(index => ({ key: key(index), value: value(index) })));
    }, BACKGROUND_CONTEXT);
    await app.session.close(BACKGROUND_CONTEXT); app = await open(path);
    assert.equal(await app.index.count(app.session, "work"), entries.length);
    assert.deepEqual(await app.index.get(app.session, "work", key(500)), value(5000));
    assert.deepEqual(await app.index.get(app.session, "other", key(500)), value(-500));
    assert.deepEqual(await app.index.range(app.session, "work", { after: key(126), before: key(132), reverse: true, limit: 3 }),
      [131, 130, 129].map(index => ({ key: key(index), value: value(index) })));
    assert.deepEqual((await app.index.range(app.session, "work", { after: key(1149), limit: 10 })).map(entry => entry.key),
      [1150, 1151, 1152].map(key));
    assert.deepEqual((await app.index.range(app.session, "work", { before: key(3), reverse: true, limit: 10 })).map(entry => entry.key),
      [2, 1, 0].map(key));
    assert.equal(await app.index.get(app.session, "work", "not-present"), undefined);
    const detached = await app.index.get(app.session, "work", key(10)); assert.ok(detached);
    detached.evidence.versions.push("client mutation");
    assert.deepEqual(await app.index.get(app.session, "work", key(10)), value(10), "Index reads do not expose mutable native drafts");
    assert.deepEqual(await app.index.range(app.session, "work", { after: key(100), before: key(100), limit: 1 }), []);
    assert.deepEqual(await app.index.range(app.session, "work", { limit: 0 }), []);
    await assert.rejects(app.index.range(app.session, "work", { limit: -1 }), /nonnegative safe integer/u);
    await assert.rejects(app.index.range(app.session, "work", { limit: Number.NaN }), /nonnegative safe integer/u);
    assert.equal(await app.index.get(app.session, "never-created", "missing"), undefined);
    assert.deepEqual(await app.index.range(app.session, "never-created", { limit: 5 }), []);
    assert.equal(await app.index.count(app.session, "never-created"), 0);
    assert.equal(await app.storage.findDocument({ kind: "test.ordered.roots", scope: { kind: "session" }, key: "never-created" },
      "current", BACKGROUND_CONTEXT), undefined, "Read-only misses do not materialize index roots");
  } finally { await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("native ordered mutations roll back together and completed deletions survive reopen", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-ordered-rollback-")), path = join(directory, "session.sqlite");
  let app = await open(path);
  try {
    await insert(app, Array.from({ length: 257 }, (_, index) => index));
    await assert.rejects(app.session.commit(async tx => {
      await app.index.setIn(tx, "work", "new", value(1000));
      assert.equal(await app.index.deleteIn(tx, "work", key(128)), true);
      throw new Error("Reject the whole change");
    }, BACKGROUND_CONTEXT), /Reject the whole change/u);
    assert.equal(await app.index.get(app.session, "work", "new"), undefined);
    assert.deepEqual(await app.index.get(app.session, "work", key(128)), value(128));
    assert.equal(await app.index.count(app.session, "work"), 257);
    await app.session.commit(async tx => {
      assert.equal(await app.index.deleteIn(tx, "work", "missing"), false);
      for (let index = 0; index < 257; index += 2) assert.equal(await app.index.deleteIn(tx, "work", key(index)), true);
      assert.equal(await app.index.deleteIn(tx, "work", key(128)), false);
    }, BACKGROUND_CONTEXT);
    await app.session.close(BACKGROUND_CONTEXT); app = await open(path);
    assert.equal(await app.index.count(app.session, "work"), 128);
    assert.deepEqual((await app.index.range(app.session, "work", { limit: 300 })).map(entry => entry.key),
      Array.from({ length: 128 }, (_, index) => key(index * 2 + 1)));
    await app.session.commit(async tx => {
      for (let index = 1; index < 257; index += 2) await app.index.deleteIn(tx, "work", key(index));
      assert.equal(await app.index.countIn(tx, "work"), 0);
      assert.deepEqual(await app.index.rangeIn(tx, "work", { limit: 1 }), []);
      await app.index.setIn(tx, "work", "", value(42));
      await app.index.setIn(tx, "work", "b", value(43));
      await app.index.setIn(tx, "work", "a", value(44));
    }, BACKGROUND_CONTEXT);
    await app.session.close(BACKGROUND_CONTEXT); app = await open(path);
    assert.deepEqual((await app.index.range(app.session, "work", { limit: 3 })).map(entry => entry.key), ["", "a", "b"]);
    assert.deepEqual((await app.index.range(app.session, "work", { after: "", before: "b", limit: 3 })).map(entry => entry.key), ["a"]);
    assert.deepEqual((await app.index.range(app.session, "work", { before: "", reverse: true, limit: 3 })).map(entry => entry.key), []);
  } finally { await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("cold fixed-size native claims visit only tree-height pages beside a large retained index", { timeout: 120_000 }, async t => {
  const directory = await mkdtemp(join(tmpdir(), "agent-ordered-bound-")), path = join(directory, "session.sqlite");
  let app = await open(path);
  try {
    const count = 17_000;
    await insert(app, Array.from({ length: count }, (_, index) => index));
    await app.session.close(BACKGROUND_CONTEXT); app = await open(path);
    const pages: string[] = [], findDocument = app.storage.findDocument.bind(app.storage);
    app.storage.findDocument = async (address, at, context) => {
      if (address.kind === "test.ordered.pages" && address.key !== undefined) pages.push(address.key);
      return findDocument(address, at, context);
    };
    const result = await app.index.range(app.session, "work", { after: key(8498), limit: 5 });
    assert.deepEqual(result.map(entry => entry.key), [8499, 8500, 8501, 8502, 8503].map(key));
    assert.ok(pages.length <= 5, "A five-item claim must follow only its branch path and selected leaves");
    t.diagnostic(JSON.stringify({ retained: count, selected: result.length, cold_page_reads: pages.length }));
    pages.length = 0;
    assert.equal(await app.index.get(app.session, "work", key(count + 1)), undefined);
    assert.ok(pages.length <= 3, "A negative exact lookup must not scan the retained index");
    for (const boundary of [1, 64, 4096, 8192, 16_384, count]) {
      const after = Math.max(0, boundary - 5), before = Math.min(count, boundary + 5);
      const expected = Array.from({ length: before - after - 1 }, (_, index) => key(after + index + 1));
      assert.deepEqual((await app.index.range(app.session, "work", { after: key(after), before: key(before), limit: 20 })).map(entry => entry.key), expected);
      assert.deepEqual((await app.index.range(app.session, "work", { after: key(after), before: key(before), reverse: true, limit: 20 })).map(entry => entry.key), expected.reverse());
    }
    for (let offset = 0; offset < count - 5; offset += 128) {
      await app.session.commit(async tx => {
        for (let index = offset; index < Math.min(offset + 128, count - 5); index += 1) {
          assert.equal(await app.index.deleteIn(tx, "work", key(index)), true);
        }
      }, BACKGROUND_CONTEXT);
    }
    await app.session.close(BACKGROUND_CONTEXT); app = await open(path);
    const retainedPages: string[] = [], findAfterDrain = app.storage.findDocument.bind(app.storage);
    app.storage.findDocument = async (address, at, context) => {
      if (address.kind === "test.ordered.pages" && address.key !== undefined) retainedPages.push(address.key);
      return findAfterDrain(address, at, context);
    };
    assert.equal(await app.index.count(app.session, "work"), 5);
    assert.deepEqual((await app.index.range(app.session, "work", { reverse: true, limit: 5 })).map(entry => entry.key),
      [count - 1, count - 2, count - 3, count - 4, count - 5].map(key));
    assert.equal(retainedPages.length, 1, "A drained index collapses to its retained leaf instead of visiting retired history");
  } finally { await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("a range observes one committed native index while another transaction replaces its contents", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-ordered-concurrent-")), app = await open(join(directory, "session.sqlite"));
  try {
    await insert(app, Array.from({ length: 300 }, (_, index) => index));
    const reading = app.index.range(app.session, "work", { limit: 1000 });
    const writing = app.session.commit(async tx => {
      for (let index = 0; index < 300; index += 1) await app.index.deleteIn(tx, "work", key(index));
      for (let index = 300; index < 600; index += 1) await app.index.setIn(tx, "work", key(index), value(index));
    }, BACKGROUND_CONTEXT);
    const [read] = await Promise.all([reading, writing]);
    const expectedStart = read[0]?.value.ordinal; assert.ok(expectedStart === 0 || expectedStart === 300);
    assert.deepEqual(read.map(entry => entry.value.ordinal), Array.from({ length: 300 }, (_, index) => expectedStart + index));
    assert.deepEqual((await app.index.range(app.session, "work", { limit: 1000 })).map(entry => entry.value.ordinal),
      Array.from({ length: 300 }, (_, index) => index + 300));
  } finally { await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("sparse leaf updates stay incremental with bounded native replay checkpoints", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-ordered-checkpoint-")), path = join(directory, "session.sqlite");
  let app = await open(path);
  try {
    await insert(app, Array.from({ length: 64 }, (_, index) => index));
    const pages = await app.storage.scanDocuments({ scope: { kind: "session" }, at: "current", kind: "test.ordered.pages" }, 10, undefined, BACKGROUND_CONTEXT);
    assert.equal(pages.items.length, 1); const page = pages.items[0]; assert.ok(page);
    for (let update = 1; update <= 70; update++) {
      await app.session.commit(tx => app.index.setIn(tx, "work", key(0), value(update)), BACKGROUND_CONTEXT);
    }
    const stored = await app.storage.document(page.id, "current", BACKGROUND_CONTEXT); assert.ok(stored);
    assert.ok(stored.deltasSinceBase > 0, "An ordinary one-value update must not rewrite a whole index page");
    assert.ok(stored.deltasSinceBase < 32, "Cold native page reconstruction must have a bounded delta tail");
    await app.session.close(BACKGROUND_CONTEXT); app = await open(path);
    assert.deepEqual(await app.index.get(app.session, "work", key(0)), value(70));
    assert.equal(await app.index.count(app.session, "work"), 64);
  } finally { await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});
