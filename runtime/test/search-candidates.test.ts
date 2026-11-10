import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { setTimeout } from "node:timers/promises";
import { test } from "node:test";
import { SearchIndex, type Embed, type SearchRecord } from "../src/search.js";

const dimensions = 1024;
const vector = Array<number>(dimensions).fill(1 / Math.sqrt(dimensions));
const embed: Embed = async texts => ({ space: { id: "candidate-lifecycle", model_revision: "synthetic", model_digest: "synthetic",
  dimensions, pooling: "last", normalization: "l2", instruction_version: 1 }, vectors: texts.map(text => {
    if (text.startsWith("Target")) return vector;
    let state = Number(/\d+/u.exec(text)?.[0] ?? "0") + 1;
    return Array.from({ length: dimensions }, () => { state = Math.imul(state, 1664525) + 1013904223 | 0; return state < 0 ? -1 / 32 : 1 / 32; });
  }) });
async function* target(version: string): AsyncIterable<SearchRecord> {
  yield { entry_id: version, ordinal: 0, text: `Target maintenance evidence ${version}`, source_refs: [`synthetic:${version}`], gaps: [`Coverage ${version}`] };
}
async function* background(): AsyncIterable<SearchRecord> {
  for (let ordinal = 0; ordinal < 32768; ordinal++) yield { entry_id: `entry-${ordinal}`, ordinal, text: `Background ${ordinal}`, source_refs: [] };
}

// The retained vectors, publisher, read worker and publication lock are real.
// A failed/closed migration must never turn a partial candidate set into search.
test("large projections close blocked candidate preparation, reopen retained vectors and atomically publish updates, deletion and reset", { timeout: 90_000 }, async t => {
  const directory = await mkdtemp(join(tmpdir(), "agent-search-candidates-")), path = join(directory, "search.sqlite");
  let index = new SearchIndex(path), locked = false;
  const db = new DatabaseSync(path, { allowExtension: true });
  const extension = process.env.AGENT_SQLITE_VEC; assert.ok(extension); db.loadExtension(extension); db.enableLoadExtension(false);
  db.exec("PRAGMA busy_timeout=5000");
  const state = () => db.prepare("SELECT value FROM metadata WHERE key='binary_candidates_v1'").get()?.value;
  try {
    await index.upsert("target", "one", target("original"), embed);
    const expected = await index.search("Target", embed, [], 1);
    await index.upsert("background", "one", background(), embed);
    await index.close();
    // Reopen the retained float-only format from before this optimization.
    db.exec("DROP TABLE IF EXISTS chunk_candidates; DELETE FROM metadata WHERE key='binary_candidates_v1'");
    index = new SearchIndex(path);
    db.exec("BEGIN IMMEDIATE"); locked = true;
    assert.notEqual(state(), "ready");
    assert.deepEqual(await index.search("Target", embed, [], 1), expected, "Incomplete candidates must retain exact retrieval and source coverage");
    const closing = index.close();
    db.exec("ROLLBACK"); locked = false; await closing;
    index = new SearchIndex(path);
    assert.deepEqual(index.checkpoint("target"), { watermark: "one", dirty: false });
    const deadline = performance.now() + 20_000;
    while (state() !== "ready" && performance.now() < deadline) await setTimeout(20);
    assert.equal(state(), "ready", "Retained vectors must finish candidate preparation after reopening");
    assert.deepEqual(await index.search("Target", embed, [], 1), expected);
    t.diagnostic("Retained candidate preparation completed");
    await index.delete("target");
    t.diagnostic("Candidate deletion completed");
    const absent = await index.search("Target", embed, [], 1);
    assert.equal(absent.candidates.some(candidate => candidate.conversation_id === "target"), false);
    await index.upsert("target", "two", target("replacement"), embed);
    t.diagnostic("Candidate replacement completed");
    const replaced = await index.search("Target", embed, [], 1);
    assert.equal(replaced.candidates[0]?.conversation_id, "target");
    assert.deepEqual(replaced.candidates[0]?.matches, [{ entry_id: "replacement", text: "Target maintenance evidence replacement", source_refs: ["synthetic:replacement"] }]);
    assert.deepEqual(replaced.gaps, ["Coverage replacement"]);
    await index.upsert("target", "cached", target("replacement"), async () => { throw new Error("Candidate publication must reuse the retained vector"); });
    assert.deepEqual(index.checkpoint("target"), { watermark: "cached", dirty: false });
    await index.rebuild((async function* () { yield { conversation_id: "target", watermark: "three" }; })(), () => target("rebuilt"), embed);
    assert.equal(state(), undefined);
    assert.equal((await index.search("Target", embed, [], 1)).candidates[0]?.matches[0]?.entry_id, "rebuilt");
  } finally { if (locked) db.exec("ROLLBACK"); db.close(); await index.close(); await rm(directory, { recursive: true, force: true }); }
});
