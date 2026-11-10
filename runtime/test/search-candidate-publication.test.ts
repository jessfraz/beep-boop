import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { constants, DatabaseSync, type StatementSync } from "node:sqlite";
import { test } from "node:test";
import { backfillCandidates, candidateRanks } from "../src/search-candidates.js";
import { mutatePublished } from "../src/search-mutations.js";
import { SearchIndex, type Embed, type SearchRecord } from "../src/search.js";

class ObservedDatabase extends DatabaseSync {
  readonly statements: StatementSync[] = [];
  override prepare(sql: string): StatementSync {
    const statement = super.prepare(sql); this.statements.push(statement); return statement;
  }
}

test("candidate batches resume their durable cursor, roll back cancellation and preserve publication ownership", { timeout: 60_000 }, async t => {
  const directory = await mkdtemp(join(tmpdir(), "agent-candidate-checkpoint-")), path = join(directory, "search.sqlite");
  const index = new SearchIndex(path), vector = Array<number>(256).fill(1 / 16);
  const embed: Embed = async texts => ({ space: { id: "candidate-checkpoint", model_revision: "synthetic", model_digest: "synthetic",
    dimensions: 256, pooling: "last", normalization: "l2", instruction_version: 1 }, vectors: texts.map(() => vector) });
  async function* records(): AsyncIterable<SearchRecord> {
    for (let ordinal = 0; ordinal < 32768; ordinal++) yield { entry_id: `entry-${ordinal}`, ordinal, text: `Synthetic record ${ordinal}`, source_refs: [] };
  }
  let db: ObservedDatabase | undefined;
  try {
    await index.upsert("retained", "one", records(), embed); await index.close();
    db = new ObservedDatabase(path, { allowExtension: true });
    const database = db, extension = process.env.AGENT_SQLITE_VEC; assert.ok(extension); database.loadExtension(extension);
    database.exec("DROP TABLE IF EXISTS chunk_candidates; DELETE FROM metadata WHERE key='binary_candidates_v1'");
    const cursor = () => database.prepare("SELECT value FROM metadata WHERE key='binary_candidates_v1'").get()?.value;
    const count = () => database.prepare("SELECT count(*) AS n FROM chunk_candidates").get()?.n;
    assert.equal(backfillCandidates(database, () => false), true);
    assert.equal(cursor(), "256"); assert.equal(count(), 256);
    let canceled = false;
    database.setAuthorizer((action, table) => {
      if (action === constants.SQLITE_UPDATE && table === "metadata") canceled = true;
      return constants.SQLITE_OK;
    });
    assert.throws(() => backfillCandidates(database, () => canceled), /closed/);
    database.setAuthorizer(() => constants.SQLITE_OK);
    assert.equal(cursor(), "256"); assert.equal(count(), 256, "Canceled vector inserts must roll back with their cursor");
    while (backfillCandidates(database, () => false)) { /* Real retained vectors only. */ }
    assert.equal(cursor(), "ready"); assert.equal(count(), 32768);
    assert.deepEqual({ ...database.prepare("SELECT watermark,dirty FROM conversations WHERE id='retained'").get() }, { watermark: "one", dirty: 0 });
    const query = vector.map(value => -value);
    const queryBytes = new Uint8Array(new Float32Array(query).buffer);
    mutatePublished(database, { type: "dirty", conversation_id: "replacement" });
    mutatePublished(database, { type: "stage", conversation_id: "replacement", run: "two", chunks: [0, 1].map(ordinal => ({
      entry_id: `replacement-${ordinal}`, ordinal, part: 0, hash: `replacement-${ordinal}`, text: `Replacement ${ordinal}`, source_refs: ["synthetic:replacement"],
      embedding: new Uint8Array(new Float32Array(query.map((value, position) => ordinal === 1 && position === 0 ? -value : value)).buffer),
    })) });
    mutatePublished(database, { type: "publish", conversation_id: "replacement", run: "two", watermark: "two", revision: 0, gaps: [], vectors: true });
    assert.equal(count(), 32770);
    const expected = database.prepare("SELECT rowid AS id,distance AS value FROM chunk_vectors WHERE embedding MATCH ? AND k=2 ORDER BY distance").all(queryBytes);
    const ranked = candidateRanks(database, queryBytes, 2);
    assert.deepEqual(ranked, expected, "The real binary candidate query must rerank retained floats exactly for both separated semantic matches");
    assert.deepEqual(ranked.map(row => {
      const id = row.id; assert.ok(typeof id === "number");
      return { ...database.prepare("SELECT entry_id,text,source_refs FROM chunks WHERE id=?").get(id) };
    }), [
      { entry_id: "replacement-0", text: "Replacement 0", source_refs: '["synthetic:replacement"]' },
      { entry_id: "replacement-1", text: "Replacement 1", source_refs: '["synthetic:replacement"]' },
    ]);
    mutatePublished(database, { type: "delete", conversation_id: "replacement", vectors: true });
    assert.ok(candidateRanks(database, queryBytes, 2).every(row => !ranked.some(previous => previous.id === row.id)));
    assert.equal(count(), 32768);
    const statements = new Map(database.statements.map(statement => [statement.sourceSQL, statement]));
    const plans = [...statements.values()].filter(statement => /^(SELECT|INSERT|UPDATE|DELETE)/u.test(statement.sourceSQL)).map(statement => ({
      sql: statement.sourceSQL, plan: database.prepare("EXPLAIN QUERY PLAN " + statement.expandedSQL).all(),
    }));
    t.diagnostic(JSON.stringify({ candidatePublicationPlans: plans }));
  } finally { db?.close(); await index.close(); await rm(directory, { recursive: true, force: true }); }
});
