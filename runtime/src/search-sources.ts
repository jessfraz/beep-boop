import { createHash } from "node:crypto";
import type { DatabaseSync } from "node:sqlite";

const STATE = "source_sets_v1";
const BATCH = 32;
const BYTES = 4 * 1024 * 1024;
export type SourceSet = { id: string; refs: string };

export function sourceReferences(value: string): string[] {
  const decoded: unknown = JSON.parse(value);
  if (!Array.isArray(decoded) || !decoded.every((item): item is string => typeof item === "string")) throw new Error("Invalid search provenance");
  return decoded;
}

export function sourceSet(references: readonly string[]): SourceSet {
  const refs = [...new Set(references)].sort();
  if (refs.some(ref => ref.length === 0 || ref.length > 2048)) throw new Error("Invalid search source references");
  return serializedSet(JSON.stringify(refs));
}

function serializedSet(refs: string): SourceSet {
  return { id: createHash("sha256").update(refs).digest("hex"), refs };
}

export function internSourceSet(database: DatabaseSync, set: SourceSet): void {
  if (database.prepare("INSERT OR IGNORE INTO source_sets(id,refs) VALUES(?,?)").run(set.id, set.refs).changes === 0) return;
  const insert = database.prepare("INSERT INTO source_set_refs(ref,set_id) VALUES(?,?)");
  for (const ref of sourceReferences(set.refs)) insert.run(ref, set.id);
}

/** Schema changes are small; legacy payload conversion belongs to the writer. */
export function prepareSourceSets(database: DatabaseSync): void {
  database.exec("BEGIN IMMEDIATE");
  try {
    let legacy = false;
    for (const table of ["chunks", "staging"]) {
      if (!database.prepare(`PRAGMA table_info(${table})`).all().some(row => row.name === "source_refs")) continue;
      database.exec(`ALTER TABLE ${table} RENAME COLUMN source_refs TO source_set`);
      legacy = true;
    }
    database.exec(`CREATE TABLE IF NOT EXISTS source_sets(id TEXT PRIMARY KEY, refs TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS source_set_refs(ref TEXT NOT NULL, set_id TEXT NOT NULL REFERENCES source_sets(id) ON DELETE CASCADE, PRIMARY KEY(ref,set_id));
      CREATE INDEX IF NOT EXISTS source_set_ref_set ON source_set_refs(set_id);
      CREATE TABLE IF NOT EXISTS chunk_source_sets(chunk_id INTEGER PRIMARY KEY REFERENCES chunks(id) ON DELETE CASCADE,
        set_id TEXT NOT NULL REFERENCES source_sets(id));
      CREATE INDEX IF NOT EXISTS chunk_source_set ON chunk_source_sets(set_id,chunk_id);`);
    database.prepare("INSERT OR IGNORE INTO metadata(key,value) VALUES(?,?)").run(STATE, legacy ? "chunks:0" : "ready");
    if (!sourceSetsPending(database)) database.exec("CREATE INDEX IF NOT EXISTS staging_source_set ON staging(source_set)");
    database.exec("COMMIT");
  } catch (error) { database.exec("ROLLBACK"); throw error; }
}

export function sourceSetsPending(database: DatabaseSync): boolean {
  return database.prepare("SELECT value FROM metadata WHERE key=?").get(STATE)?.value !== "ready";
}

/** Pending staged generations own sets as well as published chunks. */
export function collectSourceSets(database: DatabaseSync): void {
  if (sourceSetsPending(database)) return;
  database.prepare(`DELETE FROM source_sets WHERE NOT EXISTS (SELECT 1 FROM chunk_source_sets c WHERE c.set_id=source_sets.id)
    AND NOT EXISTS (SELECT 1 FROM staging s WHERE s.source_set=source_sets.id)`).run();
}

/** One resumable transaction, bounded by rows and original JSON bytes. */
export function backfillSourceSets(database: DatabaseSync, canceled: () => boolean): boolean {
  const saved = database.prepare("SELECT value FROM metadata WHERE key=?").get(STATE)?.value;
  if (saved === "ready") return false;
  if (typeof saved !== "string" || !/^(chunks|staging):\d+$/u.test(saved)) throw new Error("Invalid search provenance checkpoint");
  const [phase, position] = saved.split(":"), cursor = Number(position);
  if (!Number.isSafeInteger(cursor) || cursor < 0) throw new Error("Invalid search provenance checkpoint");
  const table = phase === "chunks" ? "chunks" : "staging", key = phase === "chunks" ? "id" : "rowid";
  const requireOpen = () => { if (canceled()) throw new Error("Search index is closed"); };
  requireOpen(); database.exec("BEGIN IMMEDIATE");
  try {
    requireOpen();
    const update = database.prepare(`UPDATE ${table} SET source_set=? WHERE ${key}=?`);
    const owner = database.prepare("INSERT OR REPLACE INTO chunk_source_sets(chunk_id,set_id) VALUES(?,?)");
    // This table exists only while upgrading the former expanded projection.
    const legacyRefs = table === "chunks" && database.prepare("SELECT 1 FROM sqlite_schema WHERE name='source_refs'").get() !== undefined
      ? database.prepare("DELETE FROM source_refs WHERE chunk_id=?") : undefined;
    let next = cursor, bytes = 0;
    for (const row of database.prepare(`SELECT ${key} AS id,source_set FROM ${table} WHERE ${key}>? ORDER BY ${key} LIMIT ?`).iterate(cursor, BATCH)) {
      requireOpen();
      if (typeof row.id !== "number" || !Number.isSafeInteger(row.id) || typeof row.source_set !== "string") throw new Error("Invalid legacy search provenance");
      let set = row.source_set;
      if (set.startsWith("[")) {
        const size = Buffer.byteLength(set);
        // A single oversized source set must still make progress on its own.
        if (bytes > 0 && bytes + size > BYTES) break;
        bytes += size;
        const interned = serializedSet(set);
        internSourceSet(database, interned);
        set = interned.id; update.run(set, row.id);
      }
      if (table === "chunks") { owner.run(row.id, set); legacyRefs?.run(row.id); }
      next = row.id;
      if (bytes >= BYTES) break;
    }
    const more = database.prepare(`SELECT 1 FROM ${table} WHERE ${key}>? LIMIT 1`).get(next) !== undefined;
    const state = more ? `${table}:${next}` : table === "chunks" ? "staging:0" : "ready";
    if (state === "ready") {
      // Converted rows now contain short IDs; this index never copies legacy
      // megabyte arrays. Expanded associations were removed with each owner.
      database.exec("DROP TABLE IF EXISTS source_refs; CREATE INDEX IF NOT EXISTS staging_source_set ON staging(source_set)");
    }
    database.prepare("UPDATE metadata SET value=? WHERE key=?").run(state, STATE);
    if (state === "ready") collectSourceSets(database);
    requireOpen(); database.exec("COMMIT"); return state !== "ready";
  } catch (error) { database.exec("ROLLBACK"); throw error; }
}
