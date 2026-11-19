import { createHash } from "node:crypto";
import { setImmediate } from "node:timers/promises";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { defineDoc, type Harness, type Tx } from "@earendil-works/pi-durable";
import type { SourceEvent } from "./generated/SourceEvent.js";
import type { SourceKey } from "./generated/SourceKey.js";
import type { SourceRef } from "./generated/SourceRef.js";
import type { TaskOutcome } from "./generated/TaskOutcome.js";
import { OrderedIndex, type IndexRange } from "./ordered-index.js";
import type { PartitionedRecords } from "./partitioned-records.js";
import { sourceReference } from "./tasks.js";
import { legacySourceWorkResources } from "./source-work.js";

export type SourceRecord = { key: string; event: SourceEvent;
  decision: { type: "task"; task_id: string } | { type: "dismissed"; reason: string } | null;
  outcome?: TaskOutcome; occurred_at?: number; work_resource?: string };
export type SourceEntity = { source: SourceRef; revision: number; decision_revision: number; pending: number;
  owner_task_id: string | null; claim_id: string | null; next_newest: boolean };
type Entity = SourceEntity & { ready_order: string | null; oldest_order: string | null; outcome_owner: string | null; owner_links: number };
type Projection = { ordinal: number; entity: string; source: SourceRef; order: string; pending: boolean;
  decision: string; owner_task_id: string | null; resources: string[] };
type Reference = SourceRef & { key: string; pending: boolean };
type Position = { key: string; at: number | null };
type Counts = { retained: number; pending: number; dirty_entities: number };
export type SourceIndexFilter = { provider?: string; account?: string; thread_id?: string; pending?: boolean };
export type SourceIndexPage = { keys: string[]; cursor: string | null; scanned: number; complete: boolean };
type PageCursor = { selection: string; scope: string; position: string };
const Migration = defineDoc<{ next: number; target: number | null; complete: boolean }>({
  kind: "agent.source.projection.migration", scope: "session", version: 1,
  initial: () => ({ next: 0, target: null, complete: false }), checkpointWhen: () => true,
});
const WIDTH = 16;
const BATCH = 32;
const PAGE_SIZE = 128;
const MIGRATION_BYTES = 4 * 1024 * 1024;
const FRESH_WINDOW = 15 * 60_000;

function digest(value: string): string { return createHash("sha256").update(value).digest("hex"); }
function number(value: number): string {
  if (!Number.isSafeInteger(value) || value < 0) throw new Error("Source projection position must be a nonnegative safe integer");
  return String(value).padStart(WIDTH, "0");
}
function stream(source: SourceKey): string { return JSON.stringify(["stream", source.provider, source.account]); }
function scopes(source: SourceRef): string[] {
  return [JSON.stringify(["all"]), JSON.stringify(["provider", source.provider]), JSON.stringify(["account", source.account]),
    JSON.stringify(["thread", source.thread_id]), stream(source), JSON.stringify(["ref", sourceReference(source)])];
}
function candidates(filter: SourceIndexFilter): string[] {
  const result = [JSON.stringify(["all"])];
  if (filter.provider !== undefined) result.push(JSON.stringify(["provider", filter.provider]));
  if (filter.account !== undefined) result.push(JSON.stringify(["account", filter.account]));
  if (filter.thread_id !== undefined) result.push(JSON.stringify(["thread", filter.thread_id]));
  if (filter.provider !== undefined && filter.account !== undefined) result.push(stream({ provider: filter.provider, account: filter.account }));
  if (filter.provider !== undefined && filter.account !== undefined && filter.thread_id !== undefined) result.push(JSON.stringify(["ref",
    sourceReference({ provider: filter.provider, account: filter.account, thread_id: filter.thread_id })]));
  return result;
}
function cursorValue(value: unknown): value is PageCursor {
  return typeof value === "object" && value !== null && "selection" in value && typeof value.selection === "string" &&
    "scope" in value && typeof value.scope === "string" && "position" in value && typeof value.position === "string";
}
function selection(filter: SourceIndexFilter): string {
  return JSON.stringify([filter.provider ?? null, filter.account ?? null, filter.thread_id ?? null, filter.pending ?? null]);
}
function decision(record: SourceRecord): string {
  return digest(JSON.stringify([record.decision, record.work_resource ?? null]));
}
function retainedOrder(key: string, ordinal: number): string { return number(ordinal) + ":" + key; }
function publicEntity(value: Entity): SourceEntity {
  return { source: { ...value.source }, revision: value.revision, decision_revision: value.decision_revision, pending: value.pending,
    owner_task_id: value.owner_task_id, claim_id: value.claim_id, next_newest: value.next_newest };
}
export function sourceEntityId(record: SourceRecord): string {
  return record.outcome === undefined ? "source:" + sourceReference(record.event.source)
    : "outcome:" + JSON.stringify([record.outcome.task_id, record.outcome.conversation_id]);
}

/** Rebuildable work projections reference immutable originals. Every update runs
 * on Pi's native mutation line with the owning original/decision transaction. */
export class SourceIndex {
  private readonly lookup = new OrderedIndex<Projection>("agent.source.projection.lookup");
  private readonly references = new OrderedIndex<Reference>("agent.source.projection.references");
  private readonly positions = new OrderedIndex<Position>("agent.source.projection.pending");
  private readonly entities = new OrderedIndex<Entity>("agent.source.projection.entities");
  private readonly owners = new OrderedIndex<{ task_id: string }>("agent.source.projection.owners");
  private readonly ownerCounts = new OrderedIndex<{ count: number }>("agent.source.projection.owner-counts");
  private readonly resourceOwners = new OrderedIndex<{ count: number }>("agent.source.projection.resource-owners");
  private readonly dirty = new OrderedIndex<{ id: string }>("agent.source.projection.dirty");
  private readonly scopeCounts = new OrderedIndex<Counts>("agent.source.projection.counts");
  private preparing: Promise<void> | undefined;
  constructor(private readonly session: Harness) {}

  async readyIn(tx: Tx): Promise<boolean> { return (await tx.doc(Migration)).complete; }
  async lookupIn(tx: Tx, eventKey: string): Promise<{ ordinal: number } | undefined> {
    const value = await this.lookup.getIn(tx, "events", eventKey);
    return value === undefined ? undefined : { ordinal: value.ordinal };
  }
  private async counts(tx: Tx, scope: string): Promise<Counts> {
    return await this.scopeCounts.getIn(tx, "scopes", scope) ?? { retained: 0, pending: 0, dirty_entities: 0 };
  }
  private async adjust(tx: Tx, source: SourceRef, retained: number, pending: number, dirty: number): Promise<void> {
    if (retained === 0 && pending === 0 && dirty === 0) return;
    for (const scope of scopes(source)) {
      const value = await this.counts(tx, scope);
      value.retained += retained; value.pending += pending; value.dirty_entities += dirty;
      if (value.retained < 0 || value.pending < 0 || value.pending > value.retained || value.dirty_entities < 0 || value.dirty_entities > value.pending) {
        throw new Error("Source projection counts are inconsistent");
      }
      await this.scopeCounts.setIn(tx, "scopes", scope, value);
    }
  }
  private async adjustOwner(tx: Tx, entityId: string, taskId: string, delta: number): Promise<void> {
    const key = JSON.stringify([entityId, taskId]), current = await this.ownerCounts.getIn(tx, "owners", key);
    const count = (current?.count ?? 0) + delta;
    if (count < 0) throw new Error("Source task ownership count is inconsistent");
    if (count === 0) await this.ownerCounts.deleteIn(tx, "owners", key);
    else await this.ownerCounts.setIn(tx, "owners", key, { count });
  }
  private async adjustResource(tx: Tx, resource: string, taskId: string, delta: number): Promise<void> {
    const before = await this.resourceOwners.getIn(tx, resource, taskId), count = (before?.count ?? 0) + delta;
    if (count < 0) throw new Error("Source resource ownership count is inconsistent");
    if (count === 0) await this.resourceOwners.deleteIn(tx, resource, taskId);
    else await this.resourceOwners.setIn(tx, resource, taskId, { count });
  }
  async resourceOwnersIn(tx: Tx, resource: string, limit: number): Promise<string[]> {
    return (await this.resourceOwners.rangeIn(tx, resource, { limit })).map(entry => entry.key);
  }
  private async queue(tx: Tx, id: string, entity: Entity): Promise<void> {
    const source = stream(entity.source);
    let order: string | null = null, oldest: string | null = null;
    if (entity.pending > 0 && entity.claim_id === null) {
      const last = (await this.positions.rangeIn(tx, id, { reverse: true, limit: 1 }))[0];
      if (last === undefined) throw new Error("Pending source entity has no indexed originals");
      order = last.key + ":" + digest(id);
      const first = (await this.positions.rangeIn(tx, id, { limit: 1 }))[0];
      if (first === undefined) throw new Error("Pending source entity has no oldest original");
      oldest = first.key + ":" + digest(id);
    }
    if (entity.ready_order !== order) {
      // Replace a hot entity's position without emptying/reallocating a
      // singleton ready tree for every new message in that entity.
      if (order !== null) await this.dirty.setIn(tx, source, order, { id });
      if (entity.ready_order !== null) await this.dirty.deleteIn(tx, source, entity.ready_order);
      entity.ready_order = order;
    }
    const backfill = JSON.stringify(["backfill", source]);
    if (entity.oldest_order !== oldest) {
      if (oldest !== null) await this.dirty.setIn(tx, backfill, oldest, { id });
      if (entity.oldest_order !== null) await this.dirty.deleteIn(tx, backfill, entity.oldest_order);
      entity.oldest_order = oldest;
    }
    await this.entities.setIn(tx, "entities", id, entity);
  }
  async projectIn(tx: Tx, record: SourceRecord, ordinal: number, claimId?: string): Promise<void> {
    const ordered = retainedOrder(record.key, ordinal), id = sourceEntityId(record), source = record.event.source;
    if (ordinal >= Number.MAX_SAFE_INTEGER) throw new Error("Source projection revision exceeds safe integer range");
    const at = record.occurred_at ?? record.event.occurred_at ?? null;
    const order = number(at ?? 0) + ":" + ordered, pending = record.decision === null, signature = decision(record);
    const owner = record.decision?.type === "task" ? record.decision.task_id : null;
    const previous = await this.lookup.getIn(tx, "events", record.key);
    if (previous !== undefined && (previous.ordinal !== ordinal || previous.entity !== id || sourceReference(previous.source) !== sourceReference(source))) {
      throw new Error("An immutable source projection identity changed");
    }
    if (previous?.order === order && previous.decision === signature) return;
    const entity = await this.entities.getIn(tx, "entities", id) ?? { source: { ...source }, revision: 0, decision_revision: 0,
      pending: 0, owner_task_id: null, claim_id: null, next_newest: true, ready_order: null, oldest_order: null,
      outcome_owner: record.outcome?.task_id ?? null, owner_links: 0 };
    if (sourceReference(entity.source) !== sourceReference(source)) throw new Error("Source entity source identity changed");
    if (claimId !== undefined && entity.claim_id !== claimId) throw new Error("Source review no longer owns this entity");
    if (previous !== undefined && previous.decision !== signature && claimId !== entity.claim_id) entity.decision_revision += 1;
    entity.revision = Math.max(entity.revision, ordinal + 1);
    const beforePending = entity.pending;
    const pendingDelta = Number(pending) - Number(previous?.pending ?? false);
    entity.pending += pendingDelta;
    if (entity.pending < 0) throw new Error("Source entity pending count is inconsistent");
    const reference: Reference = { ...source, key: record.key, pending };
    for (const scope of scopes(source)) {
      if (previous === undefined || previous.pending !== pending) await this.references.setIn(tx, JSON.stringify(["retained", scope]), ordered, reference);
      if (previous?.pending === true && !pending) await this.references.deleteIn(tx, JSON.stringify(["pending", scope]), ordered);
      if (pending && previous?.pending !== true) await this.references.setIn(tx, JSON.stringify(["pending", scope]), ordered, reference);
    }
    for (const position of [stream(source), id]) {
      if (previous?.pending === true && (!pending || previous.order !== order)) await this.positions.deleteIn(tx, position, previous.order);
      if (pending && (previous?.pending !== true || previous.order !== order)) await this.positions.setIn(tx, position, order, { key: record.key, at });
    }
    if (previous === undefined) await this.positions.setIn(tx, JSON.stringify(["retained", id]), ordered, { key: record.key, at });
    if (previous?.owner_task_id !== null && previous?.owner_task_id !== undefined && previous.owner_task_id !== owner) {
      await this.owners.deleteIn(tx, id, ordered); entity.owner_links -= 1;
      await this.adjustOwner(tx, id, previous.owner_task_id, -1);
    }
    if (owner !== null && previous?.owner_task_id !== owner) {
      await this.owners.setIn(tx, id, ordered, { task_id: owner }); entity.owner_links += 1;
      await this.adjustOwner(tx, id, owner, 1);
    }
    // A reviewer records its validated identity explicitly. Legacy GitHub
    // subjects can supply that identity; arbitrary URL mentions cannot.
    const resources = owner === null ? [] : record.work_resource === undefined
      ? legacySourceWorkResources(record.event) : [record.work_resource];
    for (const resource of previous?.resources ?? []) if (previous?.owner_task_id !== null && previous?.owner_task_id !== undefined &&
      (previous.owner_task_id !== owner || !resources.includes(resource))) await this.adjustResource(tx, resource, previous.owner_task_id, -1);
    for (const resource of resources) if (owner !== null && (previous?.owner_task_id !== owner || !previous.resources.includes(resource))) {
      await this.adjustResource(tx, resource, owner, 1);
    }
    entity.owner_task_id = entity.owner_links > 0
      ? (await this.owners.rangeIn(tx, id, { reverse: true, limit: 1 }))[0]?.value.task_id ?? null : entity.outcome_owner;
    if (entity.owner_links < 0 || entity.owner_links > 0 && entity.owner_task_id === null) throw new Error("Source entity ownership is inconsistent");
    await this.lookup.setIn(tx, "events", record.key, { ordinal, entity: id, source: { ...source }, order, pending,
      decision: signature, owner_task_id: owner, resources });
    await this.adjust(tx, source, previous === undefined ? 1 : 0, pendingDelta, Number(entity.pending > 0) - Number(beforePending > 0));
    await this.queue(tx, id, entity);
  }
  async pageIn(tx: Tx, filter: SourceIndexFilter & { cursor?: string; limit: number }): Promise<SourceIndexPage> {
    if (!Number.isSafeInteger(filter.limit) || filter.limit < 1 || filter.limit > 10_000) throw new Error("Source projection page limit must be between 1 and 10000");
    const available = candidates(filter), selected = selection(filter);
    let scope = available[0], position: string | undefined;
    if (scope === undefined) throw new Error("Source projection scope is unavailable");
    if (filter.cursor !== undefined) {
      let parsed: unknown;
      try { parsed = JSON.parse(filter.cursor); } catch { throw new Error("Source projection cursor is invalid"); }
      if (!cursorValue(parsed) || parsed.selection !== selected || !available.includes(parsed.scope)) throw new Error("Source projection cursor belongs to another selection");
      scope = parsed.scope; position = parsed.position;
    } else {
      let size = Number.POSITIVE_INFINITY;
      for (const candidate of available) {
        const count = await this.counts(tx, candidate), length = filter.pending === true ? count.pending : count.retained;
        if (length < size) { size = length; scope = candidate; }
      }
      if (size === 0) return { keys: [], cursor: null, scanned: 0, complete: true };
    }
    const options: IndexRange = { limit: filter.limit + 1, reverse: true, ...(position === undefined ? {} : { before: position }) };
    const rows = await this.references.rangeIn(tx, JSON.stringify([filter.pending === true ? "pending" : "retained", scope]), options);
    const page = rows.slice(0, filter.limit), complete = rows.length <= filter.limit;
    const keys = page.filter(({ value }) => (filter.provider === undefined || value.provider === filter.provider) &&
      (filter.account === undefined || value.account === filter.account) && (filter.thread_id === undefined || value.thread_id === filter.thread_id) &&
      (filter.pending === undefined || value.pending === filter.pending)).map(({ value }) => value.key);
    const last = page.at(-1);
    return { keys, scanned: page.length, complete,
      cursor: complete || last === undefined ? null : JSON.stringify({ selection: selected, scope, position: last.key }) };
  }
  async streamPendingIn(tx: Tx, source: SourceKey, options: IndexRange): Promise<{ key: string; order: string }[]> {
    if ((await this.counts(tx, stream(source))).pending === 0) return [];
    return (await this.positions.rangeIn(tx, stream(source), options)).map(entry => ({ key: entry.value.key, order: entry.key }));
  }
  async countsIn(tx: Tx, source: SourceKey): Promise<Counts & { oldest_pending_at: number | null }> {
    const counts = await this.counts(tx, stream(source));
    const first = counts.pending === 0 ? undefined : (await this.positions.rangeIn(tx, stream(source), { limit: 1 }))[0];
    return { ...counts, oldest_pending_at: first?.value.at ?? null };
  }
  async entityIn(tx: Tx, id: string): Promise<SourceEntity | undefined> {
    const entity = await this.entities.getIn(tx, "entities", id);
    return entity === undefined ? undefined : publicEntity(entity);
  }
  async entitiesIn(tx: Tx, source: SourceKey, options: { fresh: boolean; now: number; limit: number }): Promise<({ id: string } & SourceEntity)[]> {
    if ((await this.counts(tx, stream(source))).dirty_entities === 0) return [];
    const boundary = number(Math.max(0, options.now - FRESH_WINDOW)) + ":";
    const rows = await this.dirty.rangeIn(tx, options.fresh ? stream(source) : JSON.stringify(["backfill", stream(source)]), { limit: options.limit, reverse: options.fresh,
      ...(options.fresh ? { after: boundary } : { before: boundary }) });
    const result: ({ id: string } & SourceEntity)[] = [];
    for (const row of rows) {
      const entity = await this.entities.getIn(tx, "entities", row.value.id);
      if (entity === undefined || (options.fresh ? entity.ready_order : entity.oldest_order) !== row.key || entity.claim_id !== null || entity.pending === 0) throw new Error("Source ready queue is inconsistent");
      result.push({ id: row.value.id, ...publicEntity(entity) });
    }
    return result;
  }
  async pendingIn(tx: Tx, entityId: string, options: IndexRange): Promise<{ key: string; order: string }[]> {
    const entity = await this.entities.getIn(tx, "entities", entityId);
    if (entity === undefined || entity.pending === 0) return [];
    return (await this.positions.rangeIn(tx, entityId, options)).map(entry => ({ key: entry.value.key, order: entry.key }));
  }
  async relatedIn(tx: Tx, entityId: string, options: IndexRange): Promise<{ key: string; order: string }[]> {
    if (await this.entities.getIn(tx, "entities", entityId) === undefined) return [];
    return (await this.positions.rangeIn(tx, JSON.stringify(["retained", entityId]), options)).map(entry => ({ key: entry.value.key, order: entry.key }));
  }
  async ownerCountIn(tx: Tx, entityId: string, taskId: string): Promise<number> {
    return (await this.ownerCounts.getIn(tx, "owners", JSON.stringify([entityId, taskId])))?.count ?? 0;
  }
  async claimEntityIn(tx: Tx, id: string, claimId: string, newest?: boolean): Promise<SourceEntity | undefined> {
    if (claimId.trim().length === 0) throw new Error("Source review claim identity is empty");
    if (!await this.readyIn(tx)) return undefined;
    const entity = await this.entities.getIn(tx, "entities", id);
    if (entity === undefined || entity.pending === 0 || entity.claim_id !== null) return undefined;
    if (entity.ready_order === null || entity.oldest_order === null) throw new Error("Source entity has no ready queue entry");
    await this.dirty.deleteIn(tx, stream(entity.source), entity.ready_order);
    await this.dirty.deleteIn(tx, JSON.stringify(["backfill", stream(entity.source)]), entity.oldest_order);
    entity.claim_id = claimId; entity.ready_order = null; entity.oldest_order = null;
    if (newest !== undefined) entity.next_newest = !newest;
    await this.entities.setIn(tx, "entities", id, entity);
    return publicEntity(entity);
  }
  async releaseEntityIn(tx: Tx, id: string, claimId: string): Promise<boolean> {
    const entity = await this.entities.getIn(tx, "entities", id);
    if (entity === undefined || entity.claim_id !== claimId) return false;
    entity.claim_id = null; await this.queue(tx, id, entity); return true;
  }
  prepare(records: PartitionedRecords<SourceRecord>): Promise<void> {
    return this.preparing ??= this.migrate(records).catch((error: unknown) => { this.preparing = undefined; throw error; });
  }
  private async migrate(records: PartitionedRecords<SourceRecord>): Promise<void> {
    if ((await this.session.snapshot(Migration, BACKGROUND_CONTEXT))?.complete === true) return;
    const target = (await this.session.snapshot(records.directory, BACKGROUND_CONTEXT))?.count ?? 0;
    for (;;) {
      const complete = await this.session.commit(async tx => {
        const migration = await tx.doc(Migration);
        if (migration.complete) return true;
        migration.target ??= target;
        const end = Math.min(migration.target, migration.next + BATCH, (Math.floor(migration.next / PAGE_SIZE) + 1) * PAGE_SIZE);
        let bytes = 0;
        const page = end > migration.next ? await tx.doc(records.pages, String(Math.floor(migration.next / PAGE_SIZE)), null) : undefined;
        while (migration.next < end) {
          const key = page?.keys[migration.next % PAGE_SIZE];
          if (key === undefined) throw new Error("Source original directory is incomplete");
          const slot = await tx.doc(records.records, key, null);
          if (slot.value === null || slot.ordinal !== migration.next) throw new Error("Source original ordinal is unavailable");
          const size = Buffer.byteLength(JSON.stringify(slot.value));
          if (bytes > 0 && bytes + size > MIGRATION_BYTES) break;
          await this.projectIn(tx, slot.value, slot.ordinal);
          bytes += size; migration.next += 1;
        }
        migration.complete = migration.next >= migration.target;
        return migration.complete;
      }, BACKGROUND_CONTEXT);
      if (complete) return;
      await setImmediate();
    }
  }
}
