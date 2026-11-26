import type { DatabaseSync } from "node:sqlite";
import { deleteCandidates, hasCandidates, publishCandidates } from "./search-candidates.js";
import type { EmbeddingSpace } from "./generated/EmbeddingSpace.js";
import { collectSourceSets, internSourceSet, type SourceSet } from "./search-sources.js";

export type StagedChunk = { entry_id: string; ordinal: number; part: number; hash: string; text: string; source_set: string; embedding: Uint8Array };

export type SearchMutation =
  | { type: "dirty"; conversation_id: string }
  | { type: "space"; space: EmbeddingSpace }
  | { type: "stage"; conversation_id: string; run: string; chunks: StagedChunk[]; sources: SourceSet[] }
  | { type: "publish"; conversation_id: string; run: string; watermark: string; revision: number; gaps: string[]; vectors: boolean }
  | { type: "delete"; conversation_id: string; vectors: boolean }
  | { type: "reset" };

/** A complete generation is visible only after its single transaction commits. */
export function mutatePublished(database: DatabaseSync, mutation: SearchMutation, canceled: () => boolean = () => false): void {
  const requireOpen = () => { if (canceled()) throw new Error("Search index is closed"); };
  requireOpen();
  database.exec("BEGIN IMMEDIATE");
  try {
    // BEGIN can wait inside SQLite while its owner closes the worker.
    requireOpen();
    if (mutation.type === "dirty") {
      database.prepare("INSERT INTO conversations(id) VALUES(?) ON CONFLICT(id) DO UPDATE SET dirty=1,revision=revision+1").run(mutation.conversation_id);
    } else if (mutation.type === "space") {
      database.exec(`CREATE VIRTUAL TABLE chunk_vectors USING vec0(embedding float[${mutation.space.dimensions}] distance_metric=cosine)`);
      database.prepare("INSERT INTO metadata VALUES('space',?)").run(JSON.stringify(mutation.space));
    } else if (mutation.type === "stage") {
      for (const set of mutation.sources) internSourceSet(database, set);
      const sources = [...new Set(mutation.chunks.map(chunk => chunk.source_set))];
      if (sources.length > 0 && database.prepare(`SELECT count(*) AS count FROM source_sets WHERE id IN (${sources.map(() => "?").join(",")})`).get(...sources)?.count !== sources.length) {
        throw new Error("Search chunk has no source set");
      }
      const insert = database.prepare(`INSERT INTO staging(conversation_id,run,entry_id,ordinal,part,hash,text,source_set,embedding) VALUES(?,?,?,?,?,?,?,?,?)
        ON CONFLICT(conversation_id,entry_id,part) DO UPDATE SET run=excluded.run,ordinal=excluded.ordinal,hash=excluded.hash,
          text=excluded.text,source_set=excluded.source_set,embedding=excluded.embedding WHERE staging.run<>excluded.run`);
      for (const chunk of mutation.chunks) {
        const written = insert.run(mutation.conversation_id, mutation.run, chunk.entry_id, chunk.ordinal, chunk.part, chunk.hash, chunk.text,
          chunk.source_set, chunk.embedding);
        if (written.changes !== 1) throw new Error("Search source repeated an entry part");
      }
    } else if (mutation.type === "reset") {
      database.exec("DELETE FROM conversations; DROP TABLE IF EXISTS chunk_vectors; DROP TABLE IF EXISTS chunk_candidates; DELETE FROM metadata WHERE key IN ('space','binary_candidates_v1')");
      collectSourceSets(database);
    } else {
      const { conversation_id } = mutation;
      const candidates = hasCandidates(database);
      if (mutation.type === "delete") {
        if (candidates) deleteCandidates(database, database.prepare("SELECT id FROM chunks WHERE conversation_id=?").iterate(conversation_id));
        if (mutation.vectors) database.prepare("DELETE FROM chunk_vectors WHERE rowid IN (SELECT id FROM chunks WHERE conversation_id=?)").run(conversation_id);
        database.prepare("DELETE FROM conversations WHERE id=?").run(conversation_id);
        collectSourceSets(database);
      } else {
        // A generation replaces logical contents, not every physical vector.
        // Retaining unchanged rowids avoids rewriting large vector blobs when
        // a long conversation gains a message or only its provenance changes.
        const obsolete = `SELECT c.id FROM chunks c WHERE c.conversation_id=? AND NOT EXISTS (
          SELECT 1 FROM staging s WHERE s.conversation_id=c.conversation_id AND s.run=?
          AND s.entry_id=c.entry_id AND s.part=c.part AND s.hash=c.hash)`;
        if (candidates) deleteCandidates(database, database.prepare(obsolete).iterate(conversation_id, mutation.run));
        if (mutation.vectors) database.prepare(`DELETE FROM chunk_vectors WHERE rowid IN (${obsolete})`).run(conversation_id, mutation.run);
        database.prepare(`DELETE FROM chunks WHERE id IN (${obsolete})`).run(conversation_id, mutation.run);
        database.prepare(`INSERT INTO chunks(conversation_id,entry_id,ordinal,part,hash,text,source_set)
          SELECT s.conversation_id,s.entry_id,s.ordinal,s.part,s.hash,s.text,s.source_set FROM staging s
          WHERE s.conversation_id=? AND s.run=? AND NOT EXISTS (SELECT 1 FROM chunks c
          WHERE c.conversation_id=s.conversation_id AND c.entry_id=s.entry_id AND c.part=s.part)
          ORDER BY s.ordinal,s.part`).run(conversation_id, mutation.run);
        if (mutation.vectors) database.prepare(`INSERT INTO chunk_vectors(rowid,embedding)
          SELECT c.id,s.embedding FROM staging s JOIN chunks c ON c.conversation_id=s.conversation_id AND c.entry_id=s.entry_id AND c.part=s.part
          WHERE s.conversation_id=? AND s.run=? AND NOT EXISTS (SELECT 1 FROM chunk_vectors v WHERE v.rowid=c.id)`).run(conversation_id, mutation.run);
        if (candidates) publishCandidates(database, conversation_id, mutation.run);
        database.prepare(`UPDATE chunks SET ordinal=s.ordinal,source_set=s.source_set FROM staging s
          WHERE s.conversation_id=? AND s.run=? AND chunks.conversation_id=s.conversation_id
          AND chunks.entry_id=s.entry_id AND chunks.part=s.part
          AND (chunks.ordinal<>s.ordinal OR chunks.source_set<>s.source_set)`).run(conversation_id, mutation.run);
        database.prepare(`INSERT INTO chunk_source_sets(chunk_id,set_id)
          SELECT c.id,s.source_set FROM staging s JOIN chunks c ON c.conversation_id=s.conversation_id AND c.entry_id=s.entry_id AND c.part=s.part
          WHERE s.conversation_id=? AND s.run=? ON CONFLICT(chunk_id) DO UPDATE SET set_id=excluded.set_id WHERE set_id<>excluded.set_id`).run(conversation_id, mutation.run);
        database.prepare("UPDATE conversations SET watermark=?,dirty=CASE WHEN revision=? THEN 0 ELSE 1 END,gaps=? WHERE id=?")
          .run(mutation.watermark, mutation.revision, JSON.stringify(mutation.gaps), conversation_id);
        database.prepare("DELETE FROM staging WHERE conversation_id=?").run(conversation_id);
        collectSourceSets(database);
      }
    }
    requireOpen();
    database.exec("COMMIT");
  } catch (error) {
    database.exec("ROLLBACK");
    throw error;
  }
}
