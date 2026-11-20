import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { copyJson } from "@earendil-works/chord/json";
import type { Draft } from "@earendil-works/chord/delta";
import { defineDocFamily, type Harness, type JsonObject, type Tx } from "@earendil-works/pi-durable";

const CAPACITY = 128;
type Root = { node: string | null; next: number; count: number };
type Node<T extends JsonObject> = { leaf: boolean; keys: string[]; values: T[]; children: string[] };
type Reader<T extends JsonObject> = (id: string) => Promise<Draft<Node<T>>>;
/** Bounds are exclusive lexical bounds in either traversal direction. */
export type IndexRange = { after?: string; before?: string; reverse?: boolean; limit: number };
export type IndexEntry<T extends JsonObject> = { key: string; value: T };
type Insert = { inserted: boolean; maximum: string; right: { id: string; maximum: string } | null };
type Removal = { removed: boolean; maximum: string | null; empty: boolean };

function lower(keys: readonly string[], key: string): number {
  let start = 0, end = keys.length;
  while (start < end) {
    const middle = Math.floor((start + end) / 2);
    if (keys[middle]! < key) start = middle + 1; else end = middle;
  }
  return start;
}
function upper(keys: readonly string[], key: string): number {
  const position = lower(keys, key);
  return keys[position] === key ? position + 1 : position;
}
function maximum<T extends JsonObject>(node: Readonly<Node<T>>): string {
  const key = node.keys.at(-1);
  if (key === undefined) throw new Error("Ordered index has an empty linked page");
  return key;
}
function rangeValid(options: IndexRange): boolean {
  if (!Number.isSafeInteger(options.limit) || options.limit < 0) throw new Error("Ordered index limit must be a nonnegative safe integer");
  return options.limit > 0 && !(options.after !== undefined && options.before !== undefined && options.after >= options.before);
}
function detached<T extends JsonObject>(value: JsonObject): T {
  const result: { value: T | null } = { value: null };
  // The native definition owns this schema. Copy the selected value through its
  // typed envelope instead of copying every value in the leaf for each lookup.
  Object.assign(result, copyJson({ value }));
  if (result.value === null) throw new Error("Ordered index value is unavailable");
  return result.value;
}
function writeValue(values: JsonObject[], position: number, value: JsonObject, insert: boolean): void {
  // JsonObject is the common native draft boundary; the enclosing typed token
  // owns T. Splice the tracked array directly so one insert stays one operation.
  const owned = copyJson(value);
  if (typeof owned !== "object" || owned === null || Array.isArray(owned)) throw new Error("Ordered index values must be JSON objects");
  if (insert) values.splice(position, 0, owned); else values[position] = owned;
}

/** A rebuildable ordered projection held in bounded Pi documents. Branch keys
 * are the inclusive maxima of their child pages; values live only in leaves.
 * Native transactions own mutations, rollback, checkpointing and recovery. */
export class OrderedIndex<T extends JsonObject> {
  private readonly roots;
  private readonly pages;
  constructor(kind: string) {
    this.roots = defineDocFamily<Root, null>({ kind: kind + ".roots", scope: "session", family: true, version: 1,
      initial: () => ({ node: null, next: 0, count: 0 }), checkpointWhen: () => true });
    this.pages = defineDocFamily<{ node: Node<T> | null }, null>({ kind: kind + ".pages", scope: "session", family: true, version: 1,
      initial: () => ({ node: null }), checkpointWhen: (_value, _ops, info) => info.deltasSinceBase >= 31 });
  }
  private pageKey(index: string, id: string): string { return JSON.stringify([index, id]); }
  private async pageIn(tx: Tx, index: string, id: string): Promise<Draft<Node<T>>> {
    const page = await tx.doc(this.pages, this.pageKey(index, id), null);
    if (page.node === null) throw new Error("Ordered index page is unavailable");
    return page.node;
  }
  private async allocate(tx: Tx, index: string, root: Draft<Root>, node: Node<T>): Promise<string> {
    const id = String(root.next++);
    Object.assign(await tx.doc(this.pages, this.pageKey(index, id), null), { node: copyJson(node) });
    return id;
  }
  private readerIn(tx: Tx, index: string): Reader<T> { return id => this.pageIn(tx, index, id); }
  private async lookup(id: string | null, key: string, read: Reader<T>): Promise<T | undefined> {
    if (id === null) return undefined;
    const node = await read(id), position = lower(node.keys, key);
    if (node.leaf) {
      const value = node.values[position];
      return node.keys[position] === key && value !== undefined ? detached<T>(value) : undefined;
    }
    const child = node.children[position];
    return child === undefined ? undefined : this.lookup(child, key, read);
  }
  async getIn(tx: Tx, index: string, key: string): Promise<T | undefined> {
    return this.lookup((await tx.doc(this.roots, index, null)).node, key, this.readerIn(tx, index));
  }
  async get(session: Harness, index: string, key: string): Promise<T | undefined> {
    if ((await session.snapshot(this.roots, index, BACKGROUND_CONTEXT))?.node == null) return undefined;
    return session.commit(tx => this.getIn(tx, index, key), BACKGROUND_CONTEXT);
  }
  async countIn(tx: Tx, index: string): Promise<number> { return (await tx.doc(this.roots, index, null)).count; }
  async count(session: Harness, index: string): Promise<number> {
    return (await session.snapshot(this.roots, index, BACKGROUND_CONTEXT))?.count ?? 0;
  }
  private async insert(tx: Tx, index: string, root: Draft<Root>, id: string, key: string, value: T): Promise<Insert> {
    const node = await this.pageIn(tx, index, id);
    let inserted: boolean;
    if (node.leaf) {
      const position = lower(node.keys, key);
      inserted = node.keys[position] !== key;
      if (inserted) node.keys.splice(position, 0, key);
      writeValue(node.values, position, value, inserted);
    } else {
      const position = Math.min(lower(node.keys, key), node.children.length - 1), child = node.children[position];
      if (child === undefined) throw new Error("Ordered index branch is empty");
      const result = await this.insert(tx, index, root, child, key, value);
      inserted = result.inserted; node.keys[position] = result.maximum;
      if (result.right !== null) { node.keys.splice(position + 1, 0, result.right.maximum); node.children.splice(position + 1, 0, result.right.id); }
    }
    let right: Insert["right"] = null;
    if (node.keys.length > CAPACITY) {
      const position = Math.floor(node.keys.length / 2);
      const sibling: Node<T> = { leaf: node.leaf, keys: node.keys.splice(position), values: [], children: [] };
      if (node.leaf) Object.assign(sibling, { values: node.values.splice(position) });
      else sibling.children = node.children.splice(position);
      right = { id: await this.allocate(tx, index, root, sibling), maximum: maximum(sibling) };
    }
    return { inserted, maximum: maximum(node), right };
  }
  async setIn(tx: Tx, index: string, key: string, value: T): Promise<void> {
    const root = await tx.doc(this.roots, index, null);
    if (root.node === null) {
      root.node = await this.allocate(tx, index, root, { leaf: true, keys: [key], values: [value], children: [] });
      root.count = 1; return;
    }
    const result = await this.insert(tx, index, root, root.node, key, value);
    if (result.inserted) root.count += 1;
    if (result.right !== null) root.node = await this.allocate(tx, index, root, { leaf: false,
      keys: [result.maximum, result.right.maximum], values: [], children: [root.node, result.right.id] });
  }
  private async remove(tx: Tx, index: string, id: string, key: string): Promise<Removal> {
    const node = await this.pageIn(tx, index, id), position = lower(node.keys, key);
    if (node.leaf) {
      if (node.keys[position] !== key) return { removed: false, maximum: maximum(node), empty: false };
      node.keys.splice(position, 1); node.values.splice(position, 1);
    } else {
      const child = node.children[position];
      if (child === undefined) return { removed: false, maximum: maximum(node), empty: false };
      const result = await this.remove(tx, index, child, key);
      if (!result.removed) return { removed: false, maximum: maximum(node), empty: false };
      if (result.empty) { node.keys.splice(position, 1); node.children.splice(position, 1); }
      else {
        if (result.maximum === null) throw new Error("Ordered index branch maximum is unavailable");
        node.keys[position] = result.maximum;
      }
    }
    if (node.keys.length === 0) {
      await tx.retireDoc(this.pages, this.pageKey(index, id));
      return { removed: true, maximum: null, empty: true };
    }
    return { removed: true, maximum: maximum(node), empty: false };
  }
  async deleteIn(tx: Tx, index: string, key: string): Promise<boolean> {
    const root = await tx.doc(this.roots, index, null);
    if (root.node === null) return false;
    const result = await this.remove(tx, index, root.node, key);
    if (!result.removed) return false;
    root.count -= 1;
    if (result.empty) root.node = null;
    // Removing old work unlinks empty pages immediately. Collapse unary roots
    // so draining a formerly large queue never scans its retired page history.
    while (root.node !== null) {
      const node = await this.pageIn(tx, index, root.node);
      if (node.leaf || node.children.length !== 1) break;
      const child = node.children[0];
      if (child === undefined) throw new Error("Ordered index root child is unavailable");
      await tx.retireDoc(this.pages, this.pageKey(index, root.node)); root.node = child;
    }
    return true;
  }
  private async scan(id: string | null, options: IndexRange, read: Reader<T>, output: IndexEntry<T>[]): Promise<void> {
    if (id === null || output.length >= options.limit) return;
    const node = await read(id), reverse = options.reverse === true;
    if (node.leaf) {
      let position = reverse ? options.before === undefined ? node.keys.length - 1 : lower(node.keys, options.before) - 1
        : options.after === undefined ? 0 : upper(node.keys, options.after);
      for (; position >= 0 && position < node.keys.length && output.length < options.limit; position += reverse ? -1 : 1) {
        const key = node.keys[position], value = node.values[position];
        if (key === undefined || value === undefined) throw new Error("Ordered index leaf entry is unavailable");
        if ((options.after !== undefined && key <= options.after) || (options.before !== undefined && key >= options.before)) break;
        output.push({ key, value: detached<T>(value) });
      }
      return;
    }
    let position = reverse ? options.before === undefined ? node.children.length - 1 : Math.min(lower(node.keys, options.before), node.children.length - 1)
      : options.after === undefined ? 0 : upper(node.keys, options.after);
    for (; position >= 0 && position < node.children.length && output.length < options.limit; position += reverse ? -1 : 1) {
      const key = node.keys[position], previous = node.keys[position - 1], child = node.children[position];
      if (key === undefined || child === undefined) throw new Error("Ordered index branch entry is unavailable");
      if (options.after !== undefined && key <= options.after) break;
      if (options.before !== undefined && previous !== undefined && previous >= options.before) break;
      await this.scan(child, options, read, output);
    }
  }
  async rangeIn(tx: Tx, index: string, options: IndexRange): Promise<IndexEntry<T>[]> {
    if (!rangeValid(options)) return [];
    const output: IndexEntry<T>[] = [];
    await this.scan((await tx.doc(this.roots, index, null)).node, options, this.readerIn(tx, index), output);
    return output;
  }
  async range(session: Harness, index: string, options: IndexRange): Promise<IndexEntry<T>[]> {
    if (!rangeValid(options)) return [];
    if ((await session.snapshot(this.roots, index, BACKGROUND_CONTEXT))?.node == null) return [];
    return session.commit(tx => this.rangeIn(tx, index, options), BACKGROUND_CONTEXT);
  }
}
