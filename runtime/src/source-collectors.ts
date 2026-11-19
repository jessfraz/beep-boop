import { createHash } from "node:crypto";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { defineDoc, defineDocFamily, defineTask, ROOT_CONVERSATION_ID,
  type Harness, type Registry, type Task, type TaskId } from "@earendil-works/pi-durable";
import type { SourceConfig } from "./generated/SourceConfig.js";
import type { SourceKey } from "./generated/SourceKey.js";
import type { SourceRead } from "./generated/SourceRead.js";
import type { SourceState } from "./generated/SourceState.js";

type Input = { source: SourceConfig; key: SourceKey; generation: number; interval_ms: number };
type Checkpoint = { phase: "poll"; attempt: number };
type CollectorTask = Task<Input, Checkpoint, null, object>;
type Slot = { source: SourceKey; revision: string; generation: number; task: TaskId<null> | null; enabled: boolean;
  pages: number; events: number; admitted_events: number; wait_ms: number | null; poll_ms: number | null; admit_ms: number | null;
  started_at: number | null; finished_at: number | null; duration_ms: number | null;
  next_at: number | null; error: string | null };
const Catalog = defineDoc<{ members: string[] }>({ kind: "agent.source.collectors", scope: "session", version: 1,
  initial: () => ({ members: [] }), checkpointWhen: () => true });
const Slots = defineDocFamily<Slot, null>({ kind: "agent.source.collector", scope: "session", family: true, version: 1,
  initial: () => ({ source: { provider: "", account: "" }, revision: "", generation: 0, task: null, enabled: false,
    pages: 0, events: 0, admitted_events: 0, wait_ms: null, poll_ms: null, admit_ms: null,
    started_at: null, finished_at: null, duration_ms: null, next_at: null, error: null }), checkpointWhen: () => true });

export type SourceCollectorCallbacks = {
  ready?(signal: AbortSignal): Promise<void>;
  enabled?(): boolean;
  discover(signal: AbortSignal): Promise<{ sources: SourceConfig[]; reads: SourceRead[] }>;
  states(): Promise<SourceState[]>;
  poll(source: SourceConfig, state: SourceState | null, signal: AbortSignal): Promise<SourceRead>;
  admit(read: SourceRead): Promise<{ checkpoint_advanced: boolean; new_events?: number; state?: SourceState }>;
};
export type SourceCollectorOptions = { concurrency?: number; provider_concurrency?: number; page_timeout_ms?: number;
  page_interval_ms?: number; provider_intervals_ms?: Readonly<Record<string, number>> };

/** These descriptors come from Rust discovery. An empty Messages namespace list
 * explicitly selects its discovered local reader; normal page reads own one
 * namespace and never invoke account discovery or credential fallback. */
export function collectorSourceKey(source: SourceConfig): SourceKey {
  if (source.enabled === false) throw new Error("Disabled source has no collector");
  if (source.provider === "slack") {
    if (!source.account.trim() || source.command.length === 0) throw new Error("Invalid Slack collector descriptor");
    return { provider: source.provider, account: source.account };
  }
  if (source.provider === "messages" && source.namespaces?.length === 0) return { provider: "messages", account: "local" };
  if (source.namespaces?.length !== 1 || !source.namespaces[0]?.trim()) throw new Error("A collector owns exactly one discovered account");
  return { provider: source.provider, account: source.namespaces[0] };
}
function identity(source: SourceKey): string { return JSON.stringify([source.provider, source.account]); }
function same(left: SourceKey, right: SourceKey): boolean { return identity(left) === identity(right); }
function valid(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value <= 0) throw new Error(`${name} must be a positive integer`);
  return value;
}

/** Capacity is ephemeral admission control around native task invocations, never
 * another durable scheduler. A blocked provider cannot head-block other queued
 * providers; native Pi checkpoints recover every waiting invocation on restart. */
class Capacity {
  private active = 0;
  private readonly providers = new Map<string, number>();
  private readonly waiting: { provider: string; signal: AbortSignal; grant(release: () => void): void; reject(error: unknown): void; abort(): void }[] = [];
  constructor(private readonly maximum: number, private readonly perProvider: number) {}
  async acquire(provider: string, signal: AbortSignal): Promise<() => void> {
    signal.throwIfAborted();
    return new Promise((resolve, reject) => {
      const request = { provider, signal, grant: resolve, reject, abort: () => {
        const index = this.waiting.indexOf(request);
        if (index >= 0) this.waiting.splice(index, 1);
        reject(signal.reason); this.drain();
      } };
      signal.addEventListener("abort", request.abort, { once: true });
      this.waiting.push(request); this.drain();
    });
  }
  private drain(): void {
    while (this.active < this.maximum) {
      const index = this.waiting.findIndex(request => (this.providers.get(request.provider) ?? 0) < this.perProvider);
      if (index < 0) return;
      const request = this.waiting.splice(index, 1)[0];
      if (request === undefined) return;
      request.signal.removeEventListener("abort", request.abort);
      if (request.signal.aborted) { request.reject(request.signal.reason); continue; }
      this.active += 1; this.providers.set(request.provider, (this.providers.get(request.provider) ?? 0) + 1);
      let released = false;
      request.grant(() => {
        if (released) return; released = true;
        this.active -= 1; const count = (this.providers.get(request.provider) ?? 1) - 1;
        if (count === 0) this.providers.delete(request.provider); else this.providers.set(request.provider, count);
        this.drain();
      });
    }
  }
}

/** A native task per discovered account owns one bounded page per phase. Source
 * originals and cursor admission remain the SourceJournal's atomic commit. */
export class SourceCollectors {
  private readonly task: CollectorTask;
  private readonly capacity: Capacity;
  private readonly timeout: number;
  private readonly spacing: number;
  private readonly intervals: Readonly<Record<string, number>>;
  private syncing: Promise<void> | undefined;
  constructor(private readonly session: Harness, registry: Registry, private readonly callbacks: SourceCollectorCallbacks,
    options: SourceCollectorOptions = {}) {
    this.capacity = new Capacity(valid(options.concurrency ?? 4, "Collector concurrency"),
      valid(options.provider_concurrency ?? 2, "Provider concurrency"));
    this.timeout = valid(options.page_timeout_ms ?? 30_000, "Source page timeout");
    this.spacing = valid(options.page_interval_ms ?? 1000, "Source page interval");
    this.intervals = { slack: 3000, ...options.provider_intervals_ms };
    for (const value of Object.values(this.intervals)) valid(value, "Provider page interval");
    this.task = defineTask<Input, Checkpoint, null>({ name: "agent.source.collector", version: 1,
      initial: () => ({ phase: "poll", attempt: 0 }),
      phases: { poll: async (task, runtime, context) => {
        await this.callbacks.ready?.(runtime.signal); runtime.signal.throwIfAborted();
        const input = task.input, id = identity(input.key);
        const owns = async () => {
          const slot = await this.session.snapshot(Slots, id, BACKGROUND_CONTEXT);
          return slot?.enabled && slot.task === runtime.taskId && slot.generation === input.generation;
        };
        if (this.callbacks.enabled?.() === false || !await owns()) {
          await runtime.commit(async tx => {
            const slot = await tx.doc(Slots, id, null);
            if (slot.task === runtime.taskId && slot.generation === input.generation) {
              slot.enabled = false; slot.task = null; slot.next_at = null;
            }
            return { status: "terminal", outcome: { status: "completed", result: null } };
          }, context); return;
        }
        let release: (() => void) | undefined;
        const queued = runtime.now(); let started = queued;
        try {
          release = await this.capacity.acquire(input.key.provider, runtime.signal);
          if (!await owns()) {
            await runtime.commit(() => ({ status: "terminal", outcome: { status: "completed", result: null } }), context); return;
          }
          started = runtime.now();
          await this.session.commit(async tx => { (await tx.doc(Slots, id, null)).started_at = runtime.now(); }, context);
          const state = (await this.callbacks.states()).find(state => same(state.source, input.key)) ?? null;
          const signal = AbortSignal.any([runtime.signal, AbortSignal.timeout(this.timeout)]);
          let read: SourceRead;
          try {
            read = await this.callbacks.poll(input.source, state, signal); signal.throwIfAborted();
          } catch (error) {
            if (runtime.signal.aborted) throw error;
            // Failed transport is explicit unknown coverage, not an empty page
            // or a forgotten account. Admission retains the committed cursor.
            read = { expected_checkpoint: state?.checkpoint ?? null, batch: { source: input.key, events: [], checkpoint: null,
              coverage: { status: "unavailable", message: "Source collection failed; checkpoint retained" } } };
          }
          const polled = runtime.now();
          if (!same(read.batch.source, input.key) || (read.expected_checkpoint?.value ?? null) !== (state?.checkpoint?.value ?? null)) {
            throw new Error("A source page changed its account or checkpoint identity");
          }
          if (Buffer.byteLength(JSON.stringify(read.batch)) > 2 * 1024 * 1024) throw new Error("Source page exceeds its byte allowance");
          if (!await owns()) {
            await runtime.commit(() => ({ status: "terminal", outcome: { status: "completed", result: null } }), context); return;
          }
          const admitted = await this.callbacks.admit(read);
          runtime.signal.throwIfAborted();
          const coverage = admitted.state?.coverage ?? read.batch.coverage;
          const failure = coverage.status === "partial" || coverage.status === "unavailable" ? coverage.message : null;
          const failed = failure !== null;
          const attempt = failed ? task.state.checkpoint.attempt + 1 : 0;
          const delay = failed ? Math.min(900_000, 60_000 * 2 ** Math.min(attempt - 1, 4))
            : read.batch.coverage.status === "more" && admitted.checkpoint_advanced
              ? this.intervals[input.key.provider] ?? this.spacing : input.interval_ms;
          await runtime.commit(async tx => {
            const slot = await tx.doc(Slots, id, null);
            if (!slot.enabled || slot.task !== runtime.taskId || slot.generation !== input.generation) {
              return { status: "terminal", outcome: { status: "completed", result: null } };
            }
            slot.pages += 1; slot.events += read.batch.events.length; slot.admitted_events += admitted.new_events ?? 0;
            slot.wait_ms = Math.max(0, started - queued); slot.poll_ms = Math.max(0, polled - started);
            slot.admit_ms = Math.max(0, runtime.now() - polled); slot.finished_at = runtime.now();
            slot.duration_ms = Math.max(0, runtime.now() - started); slot.next_at = runtime.now() + delay;
            slot.error = failure;
            return { status: "sleeping", until: slot.next_at, checkpoint: { phase: "poll", attempt } };
          }, context);
        } catch (error) {
          if (runtime.signal.aborted) throw error;
          await runtime.commit(async tx => {
            const slot = await tx.doc(Slots, id, null);
            if (!slot.enabled || slot.task !== runtime.taskId || slot.generation !== input.generation) {
              return { status: "terminal", outcome: { status: "completed", result: null } };
            }
            slot.finished_at = runtime.now(); slot.duration_ms = Math.max(0, runtime.now() - started);
            slot.error = "Source collection failed; checkpoint retained";
            slot.next_at = runtime.now() + Math.min(900_000, 60_000 * 2 ** Math.min(task.state.checkpoint.attempt, 4));
            return { status: "sleeping", until: slot.next_at,
              checkpoint: { phase: "poll", attempt: task.state.checkpoint.attempt + 1 } };
          }, context);
        } finally { release?.(); }
      } },
      abort: async (_task, runtime, context) => {
        await runtime.commit(() => ({ status: "terminal", outcome: { status: "aborted" } }), context);
      },
    });
    registry.install({ name: "source-collectors", tasks: [this.task] });
  }

  sync(intervalSeconds: number, signal: AbortSignal): Promise<void> {
    return this.syncing ??= this.reconcile(valid(intervalSeconds, "Source poll interval") * 1000, signal)
      .finally(() => { this.syncing = undefined; });
  }
  private async reconcile(intervalMs: number, signal: AbortSignal): Promise<void> {
    signal.throwIfAborted();
    await this.session.root(BACKGROUND_CONTEXT);
    const catalog = await this.callbacks.discover(signal); signal.throwIfAborted();
    if (this.callbacks.enabled?.() === false) { await this.stop(); return; }
    const sources = new Map<string, { source: SourceConfig; key: SourceKey }>();
    for (const source of catalog.sources) {
      const key = collectorSourceKey(source), id = identity(key);
      if (sources.has(id)) throw new Error("Duplicate discovered source account");
      sources.set(id, { source, key });
    }
    for (const read of catalog.reads) { signal.throwIfAborted(); await this.callbacks.admit(read); }
    const cancelled = await this.session.commit(async tx => {
      const saved = await tx.doc(Catalog), cancelled: TaskId<null>[] = [];
      const members = [...saved.members];
      for (const id of members) {
        if (sources.has(id)) continue;
        const slot = await tx.doc(Slots, id, null);
        // A temporary catalog failure must not revoke a previously discovered
        // account. Its independently scoped reader can keep making progress.
        if (catalog.reads.some(read => read.batch.source.provider === slot.source.provider &&
          (read.batch.source.account === "discovery" || read.batch.source.account === slot.source.account))) continue;
        slot.enabled = false; slot.generation += 1;
        if (slot.task !== null) cancelled.push(slot.task);
        slot.task = null; slot.next_at = null;
        saved.members.splice(saved.members.indexOf(id), 1);
      }
      for (const [id, { source, key }] of sources) {
        const slot = await tx.doc(Slots, id, null);
        const revision = createHash("sha256").update(JSON.stringify({ source, intervalMs })).digest("hex");
        if (slot.enabled && slot.task !== null && slot.revision === revision) continue;
        if (slot.task !== null) cancelled.push(slot.task);
        slot.source = key; slot.revision = revision; slot.enabled = true; slot.generation += 1; slot.error = null;
        const task = await tx.createTask(this.task, { source, key, generation: slot.generation, interval_ms: intervalMs },
          { ownership: { kind: "conversation" }, conversationId: ROOT_CONVERSATION_ID, background: true });
        slot.task = task; slot.next_at = null;
        if (!saved.members.includes(id)) saved.members.push(id);
      }
      return cancelled;
    }, BACKGROUND_CONTEXT);
    for (const task of cancelled) await this.session.abortTask(task, BACKGROUND_CONTEXT);
    const members = (await this.session.snapshot(Catalog, BACKGROUND_CONTEXT))?.members ?? [];
    const owned = new Set<TaskId>();
    for (const id of members) {
      const slot = await this.session.snapshot(Slots, id, BACKGROUND_CONTEXT);
      if (slot?.enabled && slot.task !== null && slot.task !== undefined) owned.add(slot.task);
    }
    // Pi checks its restart fence for the exact owned task; no global native
    // task inventory is needed to recover a collector.
    for (const task of owned) await this.session.retryTask(task, BACKGROUND_CONTEXT);
    this.session.resume();
  }
  async stop(): Promise<void> {
    const cancelled = await this.session.commit(async tx => {
      const catalog = await tx.doc(Catalog), cancelled: TaskId<null>[] = [];
      for (const id of catalog.members) {
        const slot = await tx.doc(Slots, id, null); slot.enabled = false; slot.generation += 1; slot.next_at = null;
        if (slot.task !== null) cancelled.push(slot.task); slot.task = null;
      }
      catalog.members = []; return cancelled;
    }, BACKGROUND_CONTEXT);
    for (const task of cancelled) await this.session.abortTask(task, BACKGROUND_CONTEXT);
  }
  async status(): Promise<Readonly<Slot>[]> {
    const result: Readonly<Slot>[] = [];
    for (const id of (await this.session.snapshot(Catalog, BACKGROUND_CONTEXT))?.members ?? []) {
      const slot = await this.session.snapshot(Slots, id, BACKGROUND_CONTEXT); if (slot !== undefined) result.push(slot);
    }
    return result;
  }
}
