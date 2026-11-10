import type { DatabaseSync, SQLOutputValue } from "node:sqlite";

const STATE = "binary_candidates_v1";
const BATCH = 256;

export function hasCandidates(database: DatabaseSync): boolean {
  return database.prepare("SELECT 1 FROM metadata WHERE key=?").get(STATE) !== undefined;
}

export function useCandidates(database: DatabaseSync, vector: Uint8Array): boolean {
  if (database.prepare("SELECT value FROM metadata WHERE key=?").get(STATE)?.value !== "ready") return false;
  // Point hydration costs more than a warm contiguous scan on small indexes.
  // Retain that path until the float projection reaches 4 GiB.
  const offset = Math.ceil(4 * 1024 ** 3 / vector.byteLength) - 1;
  return database.prepare("SELECT id FROM chunks LIMIT 1 OFFSET ?").get(offset) !== undefined;
}

/** Binary candidates reduce scan bytes; original float vectors own the final distances. */
export function candidateRanks(database: DatabaseSync, vector: Uint8Array, take: number): Record<string, SQLOutputValue>[] {
  const candidates = database.prepare("SELECT rowid FROM chunk_candidates WHERE embedding MATCH vec_quantize_binary(?) AND k=? ORDER BY distance")
    .all(vector, 4096);
  if (candidates.length === 0) return [];
  const ids = candidates.map(row => {
    if (typeof row.rowid !== "number" || !Number.isSafeInteger(row.rowid)) throw new Error("Invalid search candidate identity");
    return row.rowid;
  });
  // vec0 does not turn rowid IN (...) into point lookups without MATCH.
  return database.prepare(`SELECT v.rowid AS id,vec_distance_cosine(v.embedding,?) AS value
    FROM json_each(?) c CROSS JOIN chunk_vectors v WHERE v.rowid=c.value ORDER BY value LIMIT ?`).all(vector, JSON.stringify(ids), take);
}

/** One bounded transaction, scheduled by the existing publisher between its messages. */
export function backfillCandidates(database: DatabaseSync, canceled: () => boolean): boolean {
  const requireOpen = () => { if (canceled()) throw new Error("Search index is closed"); };
  requireOpen(); database.exec("BEGIN IMMEDIATE");
  try {
    requireOpen();
    const saved = database.prepare("SELECT value FROM metadata WHERE key=?").get(STATE);
    if (saved?.value === "ready") { database.exec("ROLLBACK"); return false; }
    if (saved === undefined) {
      const dimensions = database.prepare("SELECT json_extract(value,'$.dimensions') AS dimensions FROM metadata WHERE key='space'").get()?.dimensions;
      // Small projections already scan cheaply. Low-dimensional test/custom
      // spaces also need not support binary quantization.
      if (typeof dimensions !== "number" || dimensions < 256 || dimensions % 8 !== 0 ||
          database.prepare("SELECT id FROM chunks LIMIT 1 OFFSET 32767").get() === undefined) {
        database.exec("ROLLBACK"); return false;
      }
      database.exec(`CREATE VIRTUAL TABLE chunk_candidates USING vec0(embedding bit[${dimensions}])`);
      database.prepare("INSERT INTO metadata(key,value) VALUES(?,?)").run(STATE, "0");
    }
    const cursor = saved === undefined ? 0 : Number(saved.value);
    if (!Number.isSafeInteger(cursor) || cursor < 0) throw new Error("Invalid search candidate checkpoint");
    const rows = database.prepare(`SELECT c.id,v.embedding,EXISTS(SELECT 1 FROM chunk_candidates b WHERE b.rowid=c.id) AS present FROM
      (SELECT id FROM chunks WHERE id>? ORDER BY id LIMIT ?) c CROSS JOIN chunk_vectors v WHERE v.rowid=c.id`).all(cursor, BATCH);
    const insert = database.prepare("INSERT INTO chunk_candidates(rowid,embedding) VALUES(?,vec_quantize_binary(?))");
    let next = cursor;
    for (const row of rows) {
      if (typeof row.id !== "number" || !Number.isSafeInteger(row.id) || !(row.embedding instanceof Uint8Array)) throw new Error("Invalid retained search vector");
      if (row.present === 0) insert.run(BigInt(row.id), row.embedding);
      next = row.id;
    }
    database.prepare("UPDATE metadata SET value=? WHERE key=?").run(rows.length < BATCH ? "ready" : String(next), STATE);
    requireOpen(); database.exec("COMMIT"); return rows.length === BATCH;
  } catch (error) { database.exec("ROLLBACK"); throw error; }
}

/** Keep quantization at the actual insert: SQLite temporary rows erase subtypes. */
export function publishCandidates(database: DatabaseSync, conversation_id: string, run: string): void {
  const rows = database.prepare(`SELECT c.id,s.embedding FROM staging s JOIN chunks c
    ON c.conversation_id=s.conversation_id AND c.entry_id=s.entry_id AND c.part=s.part
    WHERE s.conversation_id=? AND s.run=? AND NOT EXISTS (SELECT 1 FROM chunk_candidates v WHERE v.rowid=c.id)`);
  const insert = database.prepare("INSERT INTO chunk_candidates(rowid,embedding) VALUES(?,vec_quantize_binary(?))");
  for (const row of rows.iterate(conversation_id, run)) {
    if (typeof row.id !== "number" || !Number.isSafeInteger(row.id) || !(row.embedding instanceof Uint8Array)) throw new Error("Invalid staged search vector");
    insert.run(BigInt(row.id), row.embedding);
  }
}

export function deleteCandidates(database: DatabaseSync, rows: Iterable<Record<string, SQLOutputValue>>): void {
  const remove = database.prepare("DELETE FROM chunk_candidates WHERE rowid=?");
  for (const row of rows) {
    if (typeof row.id !== "number" || !Number.isSafeInteger(row.id)) throw new Error("Invalid obsolete search identity");
    remove.run(BigInt(row.id));
  }
}
