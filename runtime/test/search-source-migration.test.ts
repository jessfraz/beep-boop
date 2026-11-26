import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { constants, DatabaseSync, type StatementSync } from "node:sqlite";
import { setTimeout as delay } from "node:timers/promises";
import { test } from "node:test";
import { SearchIndex, readPublished, type Embed, type SearchRecord } from "../src/search.js";
import { backfillSourceSets, prepareSourceSets, sourceSetsPending } from "../src/search-sources.js";

const references = Object.freeze(Array.from({ length: 128 }, (_, ordinal) => `invented:thread-${ordinal}`));
const embed: Embed = async texts => ({ space: { id: "legacy-source-sets", model_revision: "fixture", model_digest: "fixture",
  dimensions: 3, pooling: "last", normalization: "l2", instruction_version: 1 }, vectors: texts.map(() => [1, 0, 0]) });
async function* records(prefix: string, count: number, refs: readonly string[] = references): AsyncIterable<SearchRecord> {
  for (let ordinal = 0; ordinal < count; ordinal++) yield { entry_id: `${prefix}-${ordinal}`, ordinal,
    text: `Invented ${prefix} automobile service evidence ${ordinal}`, source_refs: ordinal === 95 ? [...refs, "invented:tail"] : refs };
}
function open(path: string): DatabaseSync {
  const db = new DatabaseSync(path, { allowExtension: true }), extension = process.env.AGENT_SQLITE_VEC; assert.ok(extension);
  db.loadExtension(extension); db.enableLoadExtension(false); db.exec("PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000"); return db;
}

/** The prior storage format, populated from real published and interrupted producers. */
async function legacy(path: string, options: { count?: number; refs?: readonly string[]; staged?: number } = {}): Promise<void> {
  const index = new SearchIndex(path);
  try {
    await index.upsert("invented", "published", records("published", options.count ?? 96, options.refs), embed);
    async function* interrupted(): AsyncIterable<SearchRecord> { yield* records("interrupted", options.staged ?? 16, options.refs); throw new Error("Interrupted source"); }
    await assert.rejects(index.upsert("invented", "unfinished", interrupted(), embed), /Interrupted/);
  } finally { await index.close(); }
  const db = open(path);
  try {
    db.exec(`PRAGMA foreign_keys=OFF; BEGIN;
      DROP INDEX staging_source_set;
      UPDATE chunks SET source_set=(SELECT refs FROM source_sets WHERE id=chunks.source_set);
      UPDATE staging SET source_set=(SELECT refs FROM source_sets WHERE id=staging.source_set);
      DROP TABLE chunk_source_sets; DROP TABLE source_set_refs; DROP TABLE source_sets;
      ALTER TABLE chunks RENAME COLUMN source_set TO source_refs;
      ALTER TABLE staging RENAME COLUMN source_set TO source_refs;
      CREATE TABLE source_refs(ref TEXT NOT NULL, chunk_id INTEGER NOT NULL REFERENCES chunks(id) ON DELETE CASCADE, PRIMARY KEY(ref,chunk_id));
      CREATE INDEX refs_chunk ON source_refs(chunk_id);
      INSERT INTO source_refs SELECT j.value,c.id FROM chunks c,json_each(c.source_refs) j;
      DELETE FROM metadata WHERE key='source_sets_v1'; COMMIT; PRAGMA foreign_keys=ON;`);
  } finally { db.close(); }
}

function snapshot(db: DatabaseSync, old = false) {
  const value = old ? "c.source_refs" : "CASE WHEN substr(c.source_set,1,1)='[' THEN c.source_set ELSE s.refs END";
  const join = old ? "" : "LEFT JOIN source_sets s ON s.id=c.source_set";
  return {
    chunks: db.prepare(`SELECT c.id,c.conversation_id,c.entry_id,c.ordinal,c.part,c.hash,c.text,${value} AS refs,v.embedding
      FROM chunks c ${join} JOIN chunk_vectors v ON v.rowid=c.id ORDER BY c.id`).all(),
    staging: db.prepare(`SELECT c.conversation_id,c.run,c.entry_id,c.ordinal,c.part,c.hash,c.text,${value} AS refs,c.embedding
      FROM staging c ${join} ORDER BY c.conversation_id,c.entry_id,c.part`).all(),
    conversations: db.prepare("SELECT * FROM conversations ORDER BY id").all(),
  };
}

class ObservedDatabase extends DatabaseSync {
  readonly statements: StatementSync[] = [];
  override prepare(sql: string): StatementSync { const statement = super.prepare(sql); this.statements.push(statement); return statement; }
}

test("legacy source sets migrate in resumable bounded transactions while exact provenance and all cached vectors remain intact", async t => {
  const directory = await mkdtemp(join(tmpdir(), "agent-source-migration-")), path = join(directory, "search.sqlite");
  let db: DatabaseSync | undefined;
  try {
    await legacy(path); db = open(path);
    const original = snapshot(db, true);
    const before = db.prepare("EXPLAIN QUERY PLAN INSERT OR IGNORE INTO source_refs SELECT j.value,c.id FROM chunks c,json_each(c.source_refs) j WHERE c.conversation_id=?").all("invented");
    prepareSourceSets(db); assert.ok(sourceSetsPending(db));
    assert.equal(backfillSourceSets(db, () => false), true);
    assert.equal(db.prepare("SELECT value FROM metadata WHERE key='source_sets_v1'").get()?.value, "chunks:32");
    assert.equal(db.prepare("SELECT count(*) AS n FROM chunk_source_sets").get()?.n, 32);
    assert.deepEqual(snapshot(db), original);
    const pending = readPublished(db, { query: "", vector: null, space: null, source_refs: ["invented:tail"], limit: 8, mode: "keyword", gaps: [] });
    assert.equal(pending.candidates[0]?.matches[0]?.entry_id, "published-95");
    assert.deepEqual(pending.candidates[0]?.matches[0]?.source_refs, [...references, "invented:tail"].sort());
    assert.ok(pending.gaps.includes("History source provenance migration is in progress"));
    let canceled = false;
    db.setAuthorizer((action, table) => { if (action === constants.SQLITE_UPDATE && table === "metadata") canceled = true; return constants.SQLITE_OK; });
    assert.throws(() => backfillSourceSets(db!, () => canceled), /closed/);
    db.setAuthorizer(() => constants.SQLITE_OK);
    assert.equal(db.prepare("SELECT count(*) AS n FROM chunk_source_sets").get()?.n, 32);
    assert.equal(db.prepare("SELECT value FROM metadata WHERE key='source_sets_v1'").get()?.value, "chunks:32");
    assert.deepEqual(snapshot(db), original);
    db.close(); db = undefined;
    const observed = new ObservedDatabase(path, { allowExtension: true }); db = observed;
    const extension = process.env.AGENT_SQLITE_VEC; assert.ok(extension); observed.loadExtension(extension); observed.exec("PRAGMA foreign_keys=ON");
    const plans = new Map<string, { sql: string; plan: unknown }>();
    const capturePlans = () => {
      for (const statement of [...observed.statements]) {
        if (!/^(SELECT|INSERT|UPDATE|DELETE)/u.test(statement.sourceSQL) || plans.has(statement.sourceSQL)) continue;
        plans.set(statement.sourceSQL, { sql: statement.sourceSQL, plan: observed.prepare("EXPLAIN QUERY PLAN " + statement.expandedSQL).all() });
      }
    };
    let more: boolean;
    do { more = backfillSourceSets(observed, () => false); capturePlans(); } while (more);
    assert.equal(sourceSetsPending(observed), false);
    assert.deepEqual(snapshot(observed), original);
    assert.equal(observed.prepare("SELECT count(*) AS n FROM source_sets").get()?.n, 2);
    assert.equal(observed.prepare("SELECT count(*) AS n FROM source_set_refs").get()?.n, 257);
    assert.equal(observed.prepare("PRAGMA foreign_key_check").all().length, 0);
    const result = readPublished(observed, { query: "", vector: null, space: null, source_refs: [references.at(-1)!], limit: 8, mode: "keyword", gaps: [] });
    assert.equal(result.gaps.includes("History source provenance migration is in progress"), false);
    capturePlans();
    t.diagnostic(JSON.stringify({ legacyPublicationPlan: before, actualMigrationAndReadPlans: [...plans.values()] }));
  } finally { db?.close(); await rm(directory, { recursive: true, force: true }); }
});

test("a contended startup migration keeps old exact reads available and close preserves resumable staged ownership", { timeout: 15_000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-source-migration-close-")), path = join(directory, "search.sqlite");
  let index: SearchIndex | undefined, writer: DatabaseSync | undefined, locked = false;
  try {
    await legacy(path); writer = open(path); const original = snapshot(writer, true);
    index = new SearchIndex(path);
    writer.exec("BEGIN IMMEDIATE"); locked = true;
    const pending = await index.search("", undefined, ["invented:tail"], 8, "keyword");
    assert.equal(pending.candidates[0]?.matches[0]?.entry_id, "published-95");
    assert.ok(pending.gaps.includes("History source provenance migration is in progress"));
    const closing = index.close();
    writer.exec("ROLLBACK"); locked = false; await closing;
    assert.deepEqual(snapshot(writer), original);
    index = new SearchIndex(path);
    await index.upsert("invented", "resumed", records("interrupted", 16), async () => { throw new Error("Migration must retain durable staged embeddings"); });
    assert.deepEqual(index.checkpoint("invented"), { watermark: "resumed", dirty: false });
    const deadline = performance.now() + 5000;
    while (sourceSetsPending(writer) && performance.now() < deadline) await delay(10);
    assert.equal(sourceSetsPending(writer), false);
    assert.deepEqual((await index.search("", undefined, [references[0]!], 1, "keyword")).gaps, []);
    assert.equal(writer.prepare("SELECT count(*) AS n FROM source_sets").get()?.n, 1);
  } finally {
    if (locked) writer?.exec("ROLLBACK");
    await index?.close(); writer?.close(); await rm(directory, { recursive: true, force: true });
  }
});

test("migration yields before the next large provenance payload exceeds its byte budget", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-source-migration-bytes-")), path = join(directory, "search.sqlite");
  const refs = Object.freeze(Array.from({ length: 32337 }, (_, ordinal) => `invented:fixture-thread-${String(ordinal).padStart(6, "0")}-${"x".repeat(24)}`));
  let db: DatabaseSync | undefined;
  try {
    await legacy(path, { count: 4, refs, staged: 0 }); db = open(path); prepareSourceSets(db);
    assert.equal(backfillSourceSets(db, () => false), true);
    const expected = Math.max(1, Math.floor(4 * 1024 * 1024 / Buffer.byteLength(JSON.stringify([...refs].sort()))));
    assert.ok(expected < 4);
    assert.equal(db.prepare("SELECT count(*) AS n FROM chunk_source_sets").get()?.n, expected);
    assert.equal(db.prepare("SELECT value FROM metadata WHERE key='source_sets_v1'").get()?.value, `chunks:${expected}`);
    while (backfillSourceSets(db, () => false)) { /* Finish the same native source rows. */ }
    assert.equal(db.prepare("SELECT count(*) AS n FROM source_set_refs").get()?.n, refs.length);
  } finally { db?.close(); await rm(directory, { recursive: true, force: true }); }
});
