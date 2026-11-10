import { createHash } from "node:crypto";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import type { SqliteDatabase, SqliteExecutor, SqliteValue } from "@earendil-works/chord/sqlite";
import { createModels } from "@earendil-works/pi-ai";
import { createRegistry, defineDoc, Harness } from "@earendil-works/pi-durable";
import { SqliteStorage } from "@earendil-works/pi-durable/storage/sqlite";
import { openConversationDatabase } from "../../src/conversation-database.js";
import { ConversationModels } from "../../src/models.js";
import { TaskStore } from "../../src/tasks.js";
import { SourceJournal } from "../../src/sources.js";
import type { SourceEvent } from "../../src/generated/SourceEvent.js";
import type { SourceState } from "../../src/generated/SourceState.js";
import type { TaskRecord } from "../../src/generated/TaskRecord.js";
import type { TaskOutcome } from "../../src/generated/TaskOutcome.js";
import { fixtureModel } from "../model.js";

export type LegacyEvent = { key: string; event: SourceEvent; decision: { type: "task"; task_id: string } | { type: "dismissed"; reason: string } | null; occurred_at?: number; outcome?: TaskOutcome };
export const LegacyTasks = defineDoc<{ tasks: TaskRecord[] }>({ kind: "agent.tasks", scope: "session", version: 1,
  initial: () => ({ tasks: [] }), checkpointWhen: () => true });
export const LegacySources = defineDoc<{ sources: SourceState[]; events: LegacyEvent[]; conversation_id: string | null;
  active: { id: string; events: string[]; due: { id: string; next_check: string }[]; attempt: number; content: string | null } | null;
  last_claimed_source?: { provider: string; account: string }; next_event?: Record<string, "oldest" | "newest"> }>({
  kind: "agent.sources", scope: "session", version: 1, initial: () => ({ sources: [], events: [], conversation_id: null, active: null }), checkpointWhen: () => false,
});

type Sample = { sql: string; params: SqliteValue[]; calls: number; returned_rows: number; content_bytes: number; bound_bytes: number };
/** Observe the SQL the pinned driver actually executes, without replacing storage. */
export class SqlTrace {
  enabled = false;
  readonly samples = new Map<string, Sample>();
  wrap(executor: SqliteExecutor): SqliteExecutor {
    const record = (sql: string, params: SqliteValue[]) => {
      if (!this.enabled) return undefined;
      let sample = this.samples.get(sql);
      if (sample === undefined) {
        sample = { sql, params, calls: 0, returned_rows: 0, content_bytes: 0, bound_bytes: 0 }; this.samples.set(sql, sample);
      }
      sample.calls += 1;
      sample.bound_bytes += params.reduce<number>((sum, value) => sum + (typeof value === "string" ? Buffer.byteLength(value) : value instanceof Uint8Array ? value.byteLength : 0), 0);
      return sample;
    };
    const rows = (sample: Sample | undefined, values: object[]) => {
      if (sample === undefined) return;
      sample.returned_rows += values.length;
      for (const value of values) if ("content" in value && typeof value.content === "string") sample.content_bytes += Buffer.byteLength(value.content);
    };
    return {
      async exec(sql) { record(sql, []); return executor.exec(sql); },
      async run(sql, ...params) { record(sql, params); return executor.run(sql, ...params); },
      async get<T extends object>(sql: string, ...params: SqliteValue[]): Promise<T | undefined> {
        const sample = record(sql, params), value = await executor.get<T>(sql, ...params); rows(sample, value === undefined ? [] : [value]); return value;
      },
      async all<T extends object>(sql: string, ...params: SqliteValue[]): Promise<T[]> {
        const sample = record(sql, params), values = await executor.all<T>(sql, ...params); rows(sample, values); return values;
      },
    };
  }
  database(database: SqliteDatabase): SqliteDatabase {
    return { ...this.wrap(database), transaction: change => database.transaction(tx => change(this.wrap(tx))), close: () => database.close() };
  }
  async report(database: SqliteDatabase) {
    this.enabled = false;
    const plans = [];
    for (const { sql, params, calls, returned_rows, content_bytes, bound_bytes } of this.samples.values()) {
      if (!/^(SELECT|UPDATE|INSERT|DELETE)/i.test(sql.trim())) continue;
      const plan = await database.all<{ detail: string }>("EXPLAIN QUERY PLAN " + sql, ...params);
      plans.push({ sql, calls, returned_rows, content_bytes, bound_bytes, plan: plan.map(row => row.detail) });
    }
    return plans;
  }
}

export async function openStateStorage(path: string, trace = new SqlTrace()) {
  const database = await openConversationDatabase(path), storage = await SqliteStorage.open(trace.database(database));
  const models = createModels(), session = await Harness.open(storage,
    { models, registry: createRegistry(), identity: { attribution: null } }, BACKGROUND_CONTEXT);
  const tasks = new TaskStore(session, new ConversationModels(models)), sources = new SourceJournal(session, tasks);
  return { session, tasks, sources, database, storage, trace };
}

/** An actual old Pi document layout, retained as the migration's input. */
export async function seedLegacy(app: Awaited<ReturnType<typeof openStateStorage>>, count: number, sourceBytes = 4096, taskBytes = 1024) {
  const tasks: TaskRecord[] = [], events: LegacyEvent[] = [];
  const source = { provider: "mail", account: "fixture" };
  const state: SourceState = { source, checkpoint: { value: "legacy" }, coverage: { status: "complete" }, checked_at: 1 };
  await app.session.commit(async tx => {
    for (let index = 0; index < count; index += 1) {
      const conversation = await tx.createConversation({ ownership: { kind: "ownerless" } });
      tasks.push({ id: `11111111-1111-4111-8111-${String(index).padStart(12, "0")}`, conversation_id: String(conversation.id),
        model: fixtureModel, title: `Retained work ${index}`, outcome: "Exact legacy ownership", summary: "T".repeat(taskBytes), state: { state: "open" },
        worker: { state: "unassigned" }, sources: [], pending: [], agents: [], updated_at: index });
      const event: SourceEvent = { id: `message-${Math.floor(index / 2)}`, version: `v${index % 2 + 1}`, source: { ...source, thread_id: `thread-${Math.floor(index / 2)}` },
        title: `Retained evidence ${index}`, text: "S".repeat(sourceBytes), occurred_at: 1_800_000_000_000 + index };
      const key = createHash("sha256").update(JSON.stringify([event.source.provider, event.source.account, event.id, event.version])).digest("hex");
      events.push({ key, event, decision: index % 3 === 0 ? { type: "dismissed", reason: "Previously handled" } : null });
    }
    const first = tasks[0], event = events[0];
    if (first !== undefined && event !== undefined) { first.sources = [event.event.source]; event.decision = { type: "task", task_id: first.id }; }
    (await tx.doc(LegacyTasks)).tasks = tasks;
    const journal = await tx.doc(LegacySources); journal.events = events; journal.sources = [state];
    journal.last_claimed_source = source; journal.next_event = { [JSON.stringify([source.provider, source.account])]: "newest" };
  }, BACKGROUND_CONTEXT);
  return { tasks, events, state };
}
