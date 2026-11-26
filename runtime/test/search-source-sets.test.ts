import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { test } from "node:test";
import { SearchIndex, type Embed, type SearchRecord } from "../src/search.js";

const embed: Embed = async texts => ({ space: { id: "source-sets", model_revision: "fixture", model_digest: "fixture",
  dimensions: 3, pooling: "last", normalization: "l2", instruction_version: 1 }, vectors: texts.map(() => [1, 0, 0]) });

test("shared provenance is stored once across chunks and cached publications without losing exact source lookup", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-source-sets-")), path = join(directory, "search.sqlite");
  const references = Object.freeze(Array.from({ length: 4096 }, (_, n) => `invented:thread-${String(n).padStart(5, "0")}`));
  async function* records(): AsyncIterable<SearchRecord> {
    for (let ordinal = 0; ordinal < 64; ordinal++) yield { entry_id: `entry-${ordinal}`, ordinal,
      text: `Invented automobile maintenance decision ${ordinal}`, source_refs: [...references] };
  }
  let index = new SearchIndex(path);
  try {
    await index.upsert("invented", "one", records(), embed);
    await index.upsert("invented", "two", records(), async () => { throw new Error("Cached publication must not embed again"); });
    await index.close(); index = new SearchIndex(path);
    for (const ref of [references[0]!, references.at(-1)!]) {
      const found = await index.search("", undefined, [ref], 1, "keyword");
      assert.equal(found.candidates[0]?.conversation_id, "invented");
      assert.deepEqual(found.candidates[0]?.matches[0]?.source_refs, references);
    }
    const db = new DatabaseSync(path, { readOnly: true });
    try {
      assert.equal(db.prepare("SELECT count(*) AS n FROM source_sets").get()?.n, 1, "Every shared reference set has one durable owner");
      assert.equal(db.prepare("SELECT count(*) AS n FROM source_set_refs").get()?.n, references.length);
      assert.equal(db.prepare("SELECT count(*) AS n FROM chunk_source_sets").get()?.n, 64);
    } finally { db.close(); }
  } finally { await index.close(); await rm(directory, { recursive: true, force: true }); }
});

test("mutable producer arrays retain per-entry provenance and shared sets survive until their last staged or published owner", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-source-set-owners-")), path = join(directory, "search.sqlite");
  const index = new SearchIndex(path), refs = ["invented:first"];
  async function* changing(): AsyncIterable<SearchRecord> {
    yield { entry_id: "first", ordinal: 0, text: "First unique evidence", source_refs: refs };
    refs[0] = "invented:second";
    yield { entry_id: "second", ordinal: 1, text: "Second unique evidence", source_refs: refs };
  }
  try {
    await index.upsert("first-owner", "one", changing(), embed);
    for (const entry of ["first", "second"]) {
      const found = await index.search("", undefined, ["invented:" + entry], 1, "keyword");
      assert.deepEqual(found.candidates[0]?.matches.map(match => [match.entry_id, match.source_refs]), [[entry, ["invented:" + entry]]]);
    }
    const shared = Object.freeze(["invented:second"]);
    async function* interrupted(): AsyncIterable<SearchRecord> {
      for (let ordinal = 0; ordinal < 16; ordinal++) yield { entry_id: `staged-${ordinal}`, ordinal,
        text: `Staged reusable evidence ${ordinal}`, source_refs: shared };
      throw new Error("Producer interrupted");
    }
    await assert.rejects(index.upsert("staged-owner", "unfinished", interrupted(), embed), /interrupted/);
    await index.delete("first-owner");
    const db = new DatabaseSync(path, { readOnly: true });
    try {
      assert.equal(db.prepare("SELECT count(*) AS n FROM source_sets").get()?.n, 1);
      assert.equal(db.prepare("SELECT count(*) AS n FROM source_set_refs").get()?.n, 1);
      assert.equal(db.prepare("SELECT count(*) AS n FROM chunk_source_sets").get()?.n, 0);
    } finally { db.close(); }
    await index.delete("staged-owner");
    const empty = new DatabaseSync(path, { readOnly: true });
    try { assert.equal(empty.prepare("SELECT count(*) AS n FROM source_sets").get()?.n, 0); }
    finally { empty.close(); }
  } finally { await index.close(); await rm(directory, { recursive: true, force: true }); }
});
