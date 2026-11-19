import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { defineDoc, defineDocFamily, type Harness, type JsonObject, type Tx } from "@earendil-works/pi-durable";
import { setImmediate } from "node:timers/promises";
import { createHash } from "node:crypto";
import type { Draft } from "@earendil-works/chord";
import { OrderedIndex } from "./ordered-index.js";

const PAGE_SIZE = 128;
const MIGRATION_BATCH = 32;
const MIGRATION_BYTES = 4 * 1024 * 1024;

/** Small directory pages locate independently checkpointed Pi records. */
export class PartitionedRecords<T extends JsonObject> {
  readonly directory;
  readonly pages;
  readonly records;
  private readonly membership;
  private readonly orderedMembership: OrderedIndex<{ key: string }> | undefined;
  private readonly membershipState;
  private preparing: Promise<void> | undefined;
  private emptyLegacy = false;
  constructor(kind: string, checkpointRecords = true, orderedMembership = false) {
    this.directory = defineDoc<{ count: number; migrated: number; complete: boolean }>({ kind: kind + ".directory", scope: "session", version: 1,
      initial: () => ({ count: 0, migrated: 0, complete: false }), checkpointWhen: () => true });
    this.pages = defineDocFamily<{ keys: string[] }, null>({ kind: kind + ".pages", scope: "session", family: true, version: 1,
      initial: () => ({ keys: [] }), checkpointWhen: () => true });
    this.records = defineDocFamily<{ value: T | null; ordinal: number }, null>({ kind: kind + ".records", scope: "session", family: true, version: 1,
      initial: () => ({ value: null, ordinal: -1 }), checkpointWhen: () => checkpointRecords });
    this.membership = defineDocFamily<{ keys: string[] }, null>({ kind: kind + ".membership", scope: "session", family: true, version: 1,
      initial: () => ({ keys: [] }), checkpointWhen: () => false });
    this.orderedMembership = orderedMembership ? new OrderedIndex(kind + ".members") : undefined;
    this.membershipState = defineDoc<{ migrated: number; complete: boolean }>({ kind: kind + ".members.migration", scope: "session", version: 1,
      initial: () => ({ migrated: 0, complete: false }), checkpointWhen: () => true });
  }
  private bucket(key: string): string { return createHash("sha256").update(key).digest("hex").slice(0, 2); }
  async valueIn(tx: Tx, key: string): Promise<Draft<T> | undefined> {
    if (this.orderedMembership !== undefined) {
      if (await this.orderedMembership.getIn(tx, "members", key) === undefined) {
        if ((await tx.doc(this.membershipState)).complete || !(await tx.doc(this.membership, this.bucket(key), null)).keys.includes(key)) return undefined;
      }
    } else if (!(await tx.doc(this.membership, this.bucket(key), null)).keys.includes(key)) return undefined;
    return (await tx.doc(this.records, key, null)).value ?? undefined;
  }
  async insertIn(tx: Tx, key: string, value: T): Promise<{ value: Draft<T>; inserted: boolean }> {
    const slot = await tx.doc(this.records, key, null);
    const inserted = slot.value === null;
    if (inserted) {
      const directory = await tx.doc(this.directory);
      if (this.emptyLegacy) directory.complete = true;
      (await tx.doc(this.pages, String(Math.floor(directory.count / PAGE_SIZE)), null)).keys.push(key);
      if (this.orderedMembership === undefined || !(await tx.doc(this.membershipState)).complete) {
        (await tx.doc(this.membership, this.bucket(key), null)).keys.push(key);
      }
      slot.ordinal = directory.count; directory.count += 1;
      Object.assign(slot, { value });
    }
    if (this.orderedMembership !== undefined) await this.orderedMembership.setIn(tx, "members", key, { key });
    if (slot.value === null) throw new Error("Partitioned record was not admitted");
    return { value: slot.value, inserted };
  }
  async get(session: Harness, key: string): Promise<T | undefined> {
    return (await session.snapshot(this.records, key, BACKGROUND_CONTEXT))?.value ?? undefined;
  }
  async selected(session: Harness, keys: readonly string[]): Promise<T[]> {
    const slots = await Promise.all([...new Set(keys)].map(key => session.snapshot(this.records, key, BACKGROUND_CONTEXT)));
    const selected: { ordinal: number; value: T }[] = [];
    for (const slot of slots) if (slot !== undefined && slot.value !== null) selected.push({ ordinal: slot.ordinal, value: slot.value });
    return selected.sort((left, right) => right.ordinal - left.ordinal).map(slot => slot.value);
  }
  async *values(session: Harness, reverse = false): AsyncIterable<T> {
    const directory = await session.snapshot(this.directory, BACKGROUND_CONTEXT);
    const count = directory?.count ?? 0, pages = Math.ceil(count / PAGE_SIZE);
    for (let offset = 0; offset < pages; offset += 1) {
      const page = reverse ? pages - 1 - offset : offset;
      const saved = await session.snapshot(this.pages, String(page), BACKGROUND_CONTEXT);
      const keys = [...saved?.keys ?? []];
      if (reverse) keys.reverse();
      for (const key of keys) {
        const value = await this.get(session, key);
        if (value === undefined) throw new Error("Partitioned record directory is incomplete");
        yield value;
      }
    }
  }
  prepare(session: Harness, legacy: () => Promise<readonly T[]>, key: (value: T) => string,
    indexed?: (tx: Tx, value: Draft<T>, inserted: boolean) => Promise<void>): Promise<void> {
    return this.preparing ??= (async () => {
      await this.migrate(session, legacy, key, indexed);
      await this.migrateMembership(session);
    })().catch((error: unknown) => { this.preparing = undefined; throw error; });
  }
  private async migrateMembership(session: Harness): Promise<void> {
    const ordered = this.orderedMembership;
    if (ordered === undefined || (await session.snapshot(this.membershipState, BACKGROUND_CONTEXT))?.complete) return;
    for (;;) {
      const complete = await session.commit(async tx => {
        const progress = await tx.doc(this.membershipState);
        if (progress.complete) return true;
        const directory = await tx.doc(this.directory);
        if (this.emptyLegacy) directory.complete = true;
        let ordinal = progress.migrated, bytes = 0;
        for (; ordinal < directory.count && ordinal - progress.migrated < MIGRATION_BATCH; ordinal += 1) {
          const page = await tx.doc(this.pages, String(Math.floor(ordinal / PAGE_SIZE)), null), key = page.keys[ordinal % PAGE_SIZE];
          if (key === undefined) throw new Error("Partitioned membership migration is missing an indexed key");
          const original = (await tx.doc(this.records, key, null)).value;
          if (original === null) throw new Error("Partitioned membership migration is missing an original");
          const size = Buffer.byteLength(JSON.stringify(original));
          if (ordinal > progress.migrated && bytes + size > MIGRATION_BYTES) break;
          bytes += size;
          await ordered.setIn(tx, "members", key, { key });
        }
        progress.migrated = ordinal; progress.complete = ordinal === directory.count;
        return progress.complete;
      }, BACKGROUND_CONTEXT);
      if (complete) return;
      await setImmediate();
    }
  }
  private async migrate(session: Harness, legacy: () => Promise<readonly T[]>, key: (value: T) => string,
    indexed?: (tx: Tx, value: Draft<T>, inserted: boolean) => Promise<void>): Promise<void> {
    const saved = await session.snapshot(this.directory, BACKGROUND_CONTEXT);
    if (saved?.complete) return;
    const values = await legacy();
    if (values.length === 0 && saved === undefined) { this.emptyLegacy = true; return; }
    for (;;) {
      const finished = await session.commit(async (tx) => {
        const directory = await tx.doc(this.directory);
        if (directory.complete) return true;
        let end = directory.migrated, bytes = 0;
        while (end < values.length && end - directory.migrated < MIGRATION_BATCH) {
          const value = values[end], size = value === undefined ? 0 : Buffer.byteLength(JSON.stringify(value));
          // An old oversized original still makes progress as its own batch.
          if (end > directory.migrated && bytes + size > MIGRATION_BYTES) break;
          bytes += size; end += 1;
        }
        for (let index = directory.migrated; index < end; index += 1) {
          const value = values[index];
          if (value !== undefined) {
            const saved = await this.insertIn(tx, key(value), structuredClone(value));
            await indexed?.(tx, saved.value, saved.inserted);
          }
        }
        directory.migrated = end; directory.complete = end === values.length;
        return directory.complete;
      }, BACKGROUND_CONTEXT);
      if (finished) return;
      await setImmediate();
    }
  }
}
