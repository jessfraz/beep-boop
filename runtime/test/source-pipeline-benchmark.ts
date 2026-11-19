/** Explicit, disposable native replay qualification. This is deliberately not a
 * *.test.ts file: the default test suite must not silently admit ten million
 * originals. Example, from the repository root:
 * nix develop -c node runtime/dist/test/source-pipeline-benchmark.js \
 *   --sizes=100000,1000000,10000000 --max-ms=14400000 --keep-database
 * Continue the same native database with --directory=PATH --resume. Fixture
 * identity is checked against its receipt; native source checkpoints and
 * projection counts determine the next original, including after interruption.
 *
 * The harness uses normal FULL-synchronous SQLite admission and native claim,
 * reopen, exact-original read and decision APIs. No model/provider calls or SQL
 * writes are substituted for the production producer/consumer. "Cold" means a
 * reopened native harness, not a purged operating-system page cache.
 */
import assert from "node:assert/strict";
import { appendFile, mkdir, mkdtemp, readFile, rm, stat, statfs } from "node:fs/promises";
import { join } from "node:path";
import { setImmediate } from "node:timers/promises";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import type { SqliteDatabase, SqliteExecutor, SqliteValue } from "@earendil-works/chord/sqlite";
import { createModels } from "@earendil-works/pi-ai";
import { createRegistry, Harness } from "@earendil-works/pi-durable";
import { SqliteStorage } from "@earendil-works/pi-durable/storage/sqlite";
import { openConversationDatabase } from "../src/conversation-database.js";
import type { SourceBatch } from "../src/generated/SourceBatch.js";
import type { SourceEvent } from "../src/generated/SourceEvent.js";
import type { SourceKey } from "../src/generated/SourceKey.js";
import { ConversationModels } from "../src/models.js";
import { SourceJournal } from "../src/sources.js";
import { sourceReference, TaskStore } from "../src/tasks.js";

const DOCUMENT_RETENTION_MS = 1000;

type Query = { sql: string; params: SqliteValue[]; calls: number; rows: number; elapsed_ms: number };
class Observation {
  reads = new Map<string, Query>(); writes = 0; written_parameter_bytes = 0;
  documents = new Map<string, number>();
  read(sql: string, params: SqliteValue[], rows: number, elapsed: number): void {
    const query = this.reads.get(sql);
    if (query === undefined) this.reads.set(sql, { sql, params, calls: 1, rows, elapsed_ms: elapsed });
    else { query.calls += 1; query.rows += rows; query.elapsed_ms += elapsed; }
  }
  write(params: SqliteValue[]): void {
    this.writes += 1;
    for (const value of params) this.written_parameter_bytes += typeof value === "string" ? Buffer.byteLength(value) : value instanceof Uint8Array ? value.byteLength : 8;
  }
  summary() {
    return { sql_reads: [...this.reads.values()].reduce((sum, query) => sum + query.calls, 0),
      sql_rows: [...this.reads.values()].reduce((sum, query) => sum + query.rows, 0),
      sql_read_ms: [...this.reads.values()].reduce((sum, query) => sum + query.elapsed_ms, 0),
      sql_writes: this.writes, written_parameter_bytes: this.written_parameter_bytes,
      document_lookups: Object.fromEntries(this.documents) };
  }
}
/** Observe the actual Pi driver statements, including its transaction executor.
 * SQL text and binding shapes stay owned by upstream storage. */
class ObservedDatabase implements SqliteDatabase {
  observation: Observation | undefined;
  constructor(readonly base: SqliteDatabase) {}
  private executor(base: SqliteExecutor): SqliteExecutor {
    return {
      exec: sql => base.exec(sql),
      run: async (sql, ...params) => { this.observation?.write(params); await base.run(sql, ...params); },
      get: async <T extends object>(sql: string, ...params: SqliteValue[]) => {
        const began = performance.now(), result = await base.get<T>(sql, ...params);
        this.observation?.read(sql, params, result === undefined ? 0 : 1, performance.now() - began); return result;
      },
      all: async <T extends object>(sql: string, ...params: SqliteValue[]) => {
        const began = performance.now(), result = await base.all<T>(sql, ...params);
        this.observation?.read(sql, params, result.length, performance.now() - began); return result;
      },
    };
  }
  exec(sql: string): Promise<void> { return this.base.exec(sql); }
  run(sql: string, ...params: SqliteValue[]): Promise<void> { return this.executor(this.base).run(sql, ...params); }
  get<T extends object>(sql: string, ...params: SqliteValue[]): Promise<T | undefined> { return this.executor(this.base).get<T>(sql, ...params); }
  all<T extends object>(sql: string, ...params: SqliteValue[]): Promise<T[]> { return this.executor(this.base).all<T>(sql, ...params); }
  transaction<T>(callback: (transaction: SqliteExecutor) => Promise<T>): Promise<T> {
    return this.base.transaction(transaction => callback(this.executor(transaction)));
  }
  close(): Promise<void> { return this.base.close(); }
}
async function open(path: string) {
  const database = new ObservedDatabase(await openConversationDatabase(path));
  const storage = await SqliteStorage.open(database);
  const findDocument = storage.findDocument.bind(storage);
  storage.findDocument = async (address, at, context) => {
    const observation = database.observation;
    if (observation !== undefined) observation.documents.set(address.kind, (observation.documents.get(address.kind) ?? 0) + 1);
    return findDocument(address, at, context);
  };
  const session = await Harness.open(storage,
    { models: createModels(), registry: createRegistry(), identity: { attribution: null },
      settings: { documentRetentionMs: DOCUMENT_RETENTION_MS } }, BACKGROUND_CONTEXT);
  const tasks = new TaskStore(session, new ConversationModels());
  const journal = new SourceJournal(session, tasks);
  await journal.prepare();
  return { database, storage, session, journal };
}
type App = Awaited<ReturnType<typeof open>>;
const NOW = 1_800_000_000_000;
const STREAMS: SourceKey[] = ["slack", "google", "github", "google_calendar", "messages"].map(provider => ({ provider, account: "benchmark" }));
function event(source: SourceKey, sequence: number, threads: number, bodyBytes: number): SourceEvent {
  const id = Math.floor(sequence / 2), version = sequence % 2 + 1;
  const thread = id % 5 === 0 ? 0 : 1 + id % threads;
  const text = `Invented benchmark message ${sequence}, version ${version}. `;
  return { source: { ...source, thread_id: `thread-${thread}` }, id: `message-${id}`, version: `v${version}`,
    title: `Invented message ${id}`, text: text + "x".repeat(Math.max(0, bodyBytes - Buffer.byteLength(text))),
    occurred_at: NOW - (thread % 2 === 0 ? 60_000 : 86_400_000) + Math.floor(sequence / 2) };
}
function integer(value: string | undefined, fallback: number, name: string): number {
  const parsed = value === undefined ? fallback : Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < 1) throw new Error(`${name} must be a positive safe integer`);
  return parsed;
}
function options() {
  const args = new Map<string, string>();
  for (const argument of process.argv.slice(2)) {
    const match = /^--([a-z-]+)(?:=(.*))?$/u.exec(argument);
    if (match?.[1] === undefined) throw new Error(`Invalid argument: ${argument}`);
    args.set(match[1], match[2] ?? "true");
  }
  for (const name of args.keys()) if (!["sizes", "directory", "resume", "batch", "threads", "body-bytes", "max-ms", "max-estimated-ms", "max-bytes", "min-free-bytes", "keep-database"].includes(name)) throw new Error(`Unknown option: ${name}`);
  const sizes = [...new Set((args.get("sizes") ?? "100000,1000000,10000000").split(",").map(value => integer(value, 0, "size")))].sort((left, right) => left - right);
  const smallest = sizes[0]; assert.ok(smallest !== undefined);
  return { sizes, stages: [...new Set([Math.min(10_000, smallest), ...sizes])], directory: args.get("directory"),
    batch: integer(args.get("batch"), 128, "batch"), threads: integer(args.get("threads"), 1024, "threads"),
    bodyBytes: integer(args.get("body-bytes"), 512, "body-bytes"), maxMs: integer(args.get("max-ms"), 900_000, "max-ms"),
    maxEstimatedMs: integer(args.get("max-estimated-ms"), 3_600_000, "max-estimated-ms"),
    maxBytes: integer(args.get("max-bytes"), 8 * 1024 ** 3, "max-bytes"),
    minFreeBytes: integer(args.get("min-free-bytes"), 4 * 1024 ** 3, "min-free-bytes"), keep: args.has("keep-database"), resume: args.has("resume") };
}
async function bytes(path: string): Promise<number> {
  try { return (await stat(path)).size; }
  catch (error) { if (error instanceof Error && "code" in error && error.code === "ENOENT") return 0; throw error; }
}
async function disk(path: string) {
  const [database, wal, shm, available] = await Promise.all([bytes(path), bytes(path + "-wal"), bytes(path + "-shm"), statfs(path)]);
  return { database_bytes: database, wal_bytes: wal, shm_bytes: shm, total_bytes: database + wal + shm,
    free_bytes: available.bavail * available.bsize };
}
async function sample<T>(app: App, action: () => Promise<T>) {
  const observation = new Observation(); app.database.observation = observation;
  const began = performance.now();
  try { return { result: await action(), elapsed_ms: performance.now() - began, observation }; }
  finally { app.database.observation = undefined; }
}
async function coldSample<T>(path: string, action: (app: App) => Promise<T>) {
  const app = await open(path);
  try { return await sample(app, () => action(app)); }
  finally { await app.session.close(BACKGROUND_CONTEXT); }
}
async function plans(app: App, observations: Observation[]) {
  const queries = new Map<string, Query>();
  for (const observation of observations) for (const query of observation.reads.values()) {
    if (/^SELECT /u.test(query.sql) && /FROM (?:documents|document_revisions)\b/u.test(query.sql)) queries.set(query.sql, query);
  }
  const result = [];
  for (const query of queries.values()) result.push({ sql: query.sql, bindings: query.params,
    plan: await app.database.all<{ detail: string }>("EXPLAIN QUERY PLAN " + query.sql, ...query.params) });
  return result;
}
async function consume(path: string, retained: number, report: (value: object) => Promise<void>) {
  const source = STREAMS[0]; assert.ok(source !== undefined);
  const page = await coldSample(path, app => app.journal.eventPage({ ...source, limit: 5 }));
  assert.equal(page.result.events.length, 5); assert.ok(page.result.scanned <= 5);
  const selected = page.result.events[0]; assert.ok(selected !== undefined);
  const hotId = "source:" + sourceReference({ ...source, thread_id: "thread-0" });
  const hotThread = await coldSample(path, app => app.session.commit(async tx => {
    const entity = await app.journal.index.entityIn(tx, hotId); assert.ok(entity !== undefined);
    const oldest = await app.journal.index.pendingIn(tx, hotId, { limit: 5 });
    const newest = await app.journal.index.pendingIn(tx, hotId, { reverse: true, limit: 5 });
    const retained = await app.journal.index.relatedIn(tx, hotId, { reverse: true, limit: 5 });
    assert.equal(await app.journal.index.ownerCountIn(tx, hotId, "benchmark-unassigned-task"), 0);
    assert.ok(oldest.length > 0 && newest.length > 0 && retained.length > 0);
    return { pending: entity.pending, oldest: oldest.length, newest: newest.length, related: retained.length };
  }, BACKGROUND_CONTEXT));
  const exact = await coldSample(path, app => app.session.commit(async tx => {
    const original = await app.journal.sourceRecordIn(tx, selected.key);
    assert.deepEqual(original?.event, selected.event); return original?.key;
  }, BACKGROUND_CONTEXT));
  const missing = await coldSample(path, app => app.session.commit(tx => app.journal.sourceRecordIn(tx, "benchmark-missing-original"), BACKGROUND_CONTEXT));
  assert.equal(missing.result, undefined);
  const claimId = `benchmark-${retained}`;
  const claim = await coldSample(path, app => app.session.commit(async tx => {
    const entities = await app.journal.index.entitiesIn(tx, source, { fresh: true, now: NOW, limit: 1 });
    const entity = entities[0]; assert.ok(entity !== undefined);
    const leased = await app.journal.index.claimEntityIn(tx, entity.id, claimId); assert.ok(leased !== undefined);
    const pending = await app.journal.index.pendingIn(tx, entity.id, { limit: 5 }); assert.ok(pending.length > 0);
    return { id: entity.id, keys: pending.map(position => position.key), revision: leased.revision };
  }, BACKGROUND_CONTEXT));
  const originalRead = await coldSample(path, app => app.session.commit(async tx => {
    const entity = await app.journal.index.entityIn(tx, claim.result.id); assert.equal(entity?.claim_id, claimId);
    for (const key of claim.result.keys) {
      const record = await app.journal.sourceRecordIn(tx, key); assert.ok(record !== undefined); assert.equal(record.decision, null);
    }
    return claim.result.keys.length;
  }, BACKGROUND_CONTEXT));
  const decision = await coldSample(path, app => app.session.commit(async tx => {
    await app.journal.decideBatchIn(tx, claim.result.keys, { type: "dismissed", reason: "Benchmark exact claim completed" }, claimId);
    assert.equal(await app.journal.index.releaseEntityIn(tx, claim.result.id, claimId), true);
  }, BACKGROUND_CONTEXT));
  await report({ type: "hot_thread", retained, ...hotThread.result });
  for (const [name, measured] of Object.entries({ page, hotThread, exact, missing, claim, originalRead, decision })) {
    await report({ type: "consumer", retained, operation: name, elapsed_ms: measured.elapsed_ms, ...measured.observation.summary() });
  }
  assert.equal(page.observation.documents.get("agent.source.event.records"), 5, "List hydrates only selected originals");
  assert.equal(missing.observation.documents.get("agent.source.event.records") ?? 0, 0, "Missing lookup hydrates no originals");
  assert.equal(originalRead.observation.documents.get("agent.source.event.records"), claim.result.keys.length);
  const app = await open(path);
  try {
    await report({ type: "query_plans", retained, queries: await plans(app,
      [page.observation, hotThread.observation, exact.observation, missing.observation, claim.observation, originalRead.observation]) });
  } finally { await app.session.close(BACKGROUND_CONTEXT); }
  return claim.result.keys.length;
}

async function main(): Promise<void> {
  if (process.argv.includes("--help")) {
    process.stdout.write("Native source replay: --sizes=100000,1000000,10000000 --directory=NEW_PATH --keep-database\n" +
      "Resume: --directory=EXISTING_PATH --resume --keep-database\n" +
      "Limits: --max-ms=900000 --max-estimated-ms=3600000 --max-bytes=8589934592 --min-free-bytes=4294967296\n" +
      "Fixture: --batch=128 --threads=1024 --body-bytes=512. Idle native document cache: 1000ms.\n");
    return;
  }
  const config = options();
  if (config.resume && config.directory === undefined) throw new Error("Resume requires the existing --directory");
  // Nix sets TMPDIR to its own shell lifetime. Keep the small receipt after that
  // shell exits while disposing of this script's database below.
  const directory = config.directory ?? await mkdtemp(join("/tmp", "agent-source-replay-"));
  if (config.directory !== undefined && !config.resume) await mkdir(directory, { mode: 0o700 });
  const path = join(directory, "session.sqlite"), receipt = join(directory, "receipt.jsonl");
  if (config.resume) {
    const first = (await readFile(receipt, "utf8")).split("\n")[0]; assert.ok(first !== undefined);
    const saved: unknown = JSON.parse(first);
    assert.ok(typeof saved === "object" && saved !== null && "config" in saved && "seed" in saved);
    assert.equal(saved.seed, "deterministic-paired-versions-hot-thread-v1");
    const fixture = saved.config;
    assert.ok(typeof fixture === "object" && fixture !== null && "batch" in fixture && "threads" in fixture && "bodyBytes" in fixture);
    assert.equal(fixture.batch, config.batch); assert.equal(fixture.threads, config.threads); assert.equal(fixture.bodyBytes, config.bodyBytes);
    assert.ok((await stat(path)).isFile(), "Resume requires the retained native database");
  }
  const report = async (value: object) => { const line = JSON.stringify(value); process.stdout.write(line + "\n"); await appendFile(receipt, line + "\n", { mode: 0o600 }); };
  let app = await open(path), retained = 0, decided = 0, producerMs = 0, nativeWrites = 0, writeBytes = 0, stopped: string | undefined;
  const sequence = STREAMS.map(() => 0);
  // A stream checkpoint is the producer's CAS baseline, never a seeded native counter.
  const expected = new Map<string, { value: string }>();
  const began = performance.now();
  try {
    if (config.resume) {
      const states = await app.journal.states();
      await app.session.commit(async tx => {
        for (const [index, source] of STREAMS.entries()) {
          const counts = await app.journal.index.countsIn(tx, source);
          const state = states.find(state => state.source.provider === source.provider && state.source.account === source.account);
          const offset = state?.checkpoint?.value;
          const match = offset === undefined ? undefined : /^offset-(\d+)$/u.exec(offset);
          assert.ok(offset === undefined || match?.[1] !== undefined, "Fixture source checkpoint has an unknown format");
          const count = match?.[1] === undefined ? 0 : Number(match[1]);
          assert.ok(Number.isSafeInteger(count)); assert.equal(counts.retained, count, "Native checkpoint must match actual unique originals");
          sequence[index] = count; retained += counts.retained; decided += counts.retained - counts.pending;
          if (offset !== undefined) expected.set(source.provider, { value: offset });
        }
      }, BACKGROUND_CONTEXT);
    }
    const startingRetained = retained;
    const pragmas = { sqlite: await app.database.get<{ version: string }>("SELECT sqlite_version() AS version"),
      journal_mode: await app.database.get<{ journal_mode: string }>("PRAGMA journal_mode"),
      synchronous: await app.database.get<{ synchronous: number }>("PRAGMA synchronous"),
      fullfsync: await app.database.get<{ fullfsync: number }>("PRAGMA fullfsync") };
    await report({ type: "configuration", directory, receipt, config, pragmas, starting_retained: startingRetained,
      document_retention_ms: DOCUMENT_RETENTION_MS, admission_yield: "setImmediate between native batches",
      cold_definition: "Reopened native harness; OS page cache is retained", seed: "deterministic-paired-versions-hot-thread-v1" });
    let previousBytes = (await disk(path)).total_bytes, previousRetained = startingRetained, previousProducerMs = 0;
    for (const target of config.stages) {
      if (target <= startingRetained) continue;
      if (retained > startingRetained) {
        const projectedMs = producerMs / (retained - startingRetained) * target, projectedBytes = previousBytes / retained * target;
        await report({ type: "estimate", retained, target, projected_total_producer_ms: projectedMs, projected_total_bytes: projectedBytes, qualified: false });
        if (projectedMs > config.maxEstimatedMs) { stopped = "estimated_runtime_limit"; break; }
        if (projectedBytes > config.maxBytes) { stopped = "estimated_disk_limit"; break; }
      }
      let lastProgress = performance.now();
      while (retained < target) {
        if (performance.now() - began > config.maxMs) { stopped = "runtime_limit"; break; }
        const streamIndex = Math.floor(retained / config.batch) % STREAMS.length, source = STREAMS[streamIndex]; assert.ok(source !== undefined);
        const first = sequence[streamIndex]; assert.ok(first !== undefined);
        const count = Math.min(config.batch, target - retained), next = { value: `offset-${first + count}` };
        const events = Array.from({ length: count }, (_, index) => event(source, first + index, config.threads, config.bodyBytes));
        const batch: SourceBatch = { source, events, checkpoint: next, coverage: { status: "complete" } };
        const admission = await sample(app, () => app.journal.admit(batch, expected.get(source.provider) ?? null));
        assert.equal(admission.result.new_events, count); assert.equal(admission.result.checkpoint_advanced, true);
        assert.deepEqual(admission.result.state.checkpoint, next);
        retained += count; sequence[streamIndex] = first + count; expected.set(source.provider, next);
        producerMs += admission.elapsed_ms; nativeWrites += admission.observation.writes; writeBytes += admission.observation.written_parameter_bytes;
        // The node SQLite facade resolves through microtasks. Give timers,
        // native cache eviction and garbage collection an ordinary scheduler
        // turn between provider batches, as real I/O ingress naturally does.
        await setImmediate();
        if (performance.now() - lastProgress >= 15_000) {
          const usage = await disk(path);
          await report({ type: "progress", retained, target, starting_retained: startingRetained, producer_ms: producerMs,
            events_per_second: (retained - startingRetained) * 1000 / producerMs, elapsed_ms: performance.now() - began,
            memory: process.memoryUsage(), peak_rss_bytes: process.resourceUsage().maxRSS * 1024,
            native_cache: "documentCacheSizes" in app.session ? app.session.documentCacheSizes : null, disk: usage });
          lastProgress = performance.now();
          if (usage.total_bytes > config.maxBytes || usage.free_bytes < config.minFreeBytes) { stopped = "disk_limit"; break; }
        }
      }
      if (stopped !== undefined) break;
      // Duplicate delivery traverses the real idempotency lookup and preserves
      // every original while repeating the current stream checkpoint.
      const source = STREAMS[0], first = sequence[0]; assert.ok(source !== undefined && first !== undefined && first >= 2);
      const duplicate = await sample(app, () => app.journal.admit({ source,
        events: [event(source, first - 2, config.threads, config.bodyBytes), event(source, first - 1, config.threads, config.bodyBytes)],
        checkpoint: expected.get(source.provider) ?? null, coverage: { status: "complete" } }, expected.get(source.provider) ?? null));
      assert.equal(duplicate.result.new_events, 0);
      await report({ type: "duplicate", retained, elapsed_ms: duplicate.elapsed_ms, ...duplicate.observation.summary() });
      await app.session.close(BACKGROUND_CONTEXT);
      const reopen = performance.now(); app = await open(path);
      await report({ type: "reopen", retained, elapsed_ms: performance.now() - reopen });
      const counts = await app.session.commit(async tx => {
        const result = [];
        for (const source of STREAMS) result.push({ source, ...await app.journal.index.countsIn(tx, source) });
        return result;
      }, BACKGROUND_CONTEXT);
      assert.equal(counts.reduce((sum, source) => sum + source.retained, 0), retained);
      assert.equal(counts.reduce((sum, source) => sum + source.pending, 0), retained - decided);
      const usage = await disk(path);
      await report({ type: "stage", retained, qualified: true, starting_retained: startingRetained, producer_ms: producerMs,
        stage_events_per_second: (retained - previousRetained) * 1000 / (producerMs - previousProducerMs),
        native_write_statements: nativeWrites, written_parameter_bytes: writeBytes,
        stage_disk_bytes_per_original: (usage.total_bytes - previousBytes) / (retained - previousRetained),
        disk: usage, counts, peak_rss_bytes: process.resourceUsage().maxRSS * 1024 });
      previousBytes = usage.total_bytes; previousRetained = retained; previousProducerMs = producerMs;
      await app.session.close(BACKGROUND_CONTEXT);
      decided += await consume(path, retained, report);
      app = await open(path);
      // Projection-page plans come from the exact driver reads captured above.
      // A separate original/revision census is intentionally outside timings.
      await report({ type: "storage_census", retained,
        documents: await app.database.get<{ count: number }>("SELECT count(*) AS count FROM documents"),
        revisions: await app.database.get<{ count: number }>("SELECT count(*) AS count FROM document_revisions"),
        plans: { documents: await app.database.all<{ detail: string }>("EXPLAIN QUERY PLAN SELECT count(*) AS count FROM documents"),
          revisions: await app.database.all<{ detail: string }>("EXPLAIN QUERY PLAN SELECT count(*) AS count FROM document_revisions") } });
    }
    await report({ type: "result", retained, decided, completed_targets: config.sizes.filter(target => target <= retained),
      unqualified_targets: config.sizes.filter(target => target > retained), stopped: stopped ?? null,
      starting_retained: startingRetained, producer_ms: producerMs, elapsed_ms: performance.now() - began,
      estimates: config.sizes.map(target => ({ target, producer_ms: producerMs / Math.max(1, retained - startingRetained) * target,
        bytes: (previousBytes / Math.max(1, previousRetained)) * target })),
      disk: await disk(path), receipt, database_retained: config.keep });
  } finally {
    await app.session.close(BACKGROUND_CONTEXT);
    if (!config.keep) for (const suffix of ["", "-wal", "-shm"]) await rm(path + suffix, { force: true });
  }
}
await main();
