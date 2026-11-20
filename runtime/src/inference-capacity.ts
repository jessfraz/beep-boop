import type { Api, Model, SimpleStreamOptions, TranscriptContext } from "@earendil-works/pi-ai";
import type { InferenceClass } from "./generated/InferenceClass.js";

type Reservation = { requests: number; tokens: number };
export type InferenceCapacityOptions = {
  maxConcurrent?: number;
  maxEstimatedTokens?: number;
  maxBackfillConcurrent?: number;
  quantumTokens?: number;
  reservations?: Partial<Record<InferenceClass, Reservation>>;
};
type Counters = Record<InferenceClass, number>;
type Waiter = {
  workClass: InferenceClass; tokens: number; resolve(release: () => void): void;
  group?: string;
  signal: AbortSignal | undefined; abort(): void;
};
type Account = {
  active: Counters; tokens: Counters; queues: Record<InferenceClass, Waiter[]>; deficits: Counters;
  cursor: number; enter: boolean; exclusive: boolean; waiting: Waiter | undefined; pumping: boolean;
  criticalGroups: Map<string, number>;
};
const classes: readonly InferenceClass[] = ["foreground", "critical", "email", "cron", "fresh", "backfill"];
const weights: Counters = { foreground: 4, critical: 5, email: 4, cron: 3, fresh: 3, backfill: 1 };
function counters(): Counters { return { foreground: 0, critical: 0, email: 0, cron: 0, fresh: 0, backfill: 0 }; }
function integer(value: number, name: string, minimum = 1): number {
  if (!Number.isSafeInteger(value) || value < minimum) throw new RangeError(`${name} must be a safe integer >= ${minimum}`);
  return value;
}

/** A conservative local working-set estimate, not a prediction of provider billing. */
export function estimatedInferenceTokens(model: Model<Api>, context: TranscriptContext, options: SimpleStreamOptions): number {
  const input = Math.ceil(Buffer.byteLength(JSON.stringify(context.messages)) / 3);
  const output = options.maxTokens ?? model.maxTokens;
  const reasoning = options.reasoning;
  const thinking = reasoning === "minimal" || reasoning === "low" || reasoning === "medium" || reasoning === "high"
    ? options.thinkingBudgets?.[reasoning] : undefined;
  return integer(Math.max(1, input + Math.max(output, thinking ?? 0)), "Estimated inference tokens");
}

/** Ephemeral resource admission only. Pi owns every queued/running generation
 * and recreates requests after restart; no execution receipt lives here.
 *
 * Each class owns reserved requests and tokens. Excess uses a shared pool. A
 * fair turn waiting for shared capacity holds its place while other classes
 * may still use their unoccupied reservations. This prevents small requests
 * from continually stealing the capacity needed by a larger pending request.
 */
export class InferenceCapacity {
  private readonly accounts = new Map<string, Account>();
  private readonly maxConcurrent: number;
  private readonly maxTokens: number;
  private readonly maxBackfill: number;
  private readonly quantum: number;
  private readonly reserved: Record<InferenceClass, Reservation>;
  private readonly shared: Reservation;
  constructor(options: InferenceCapacityOptions = {}) {
    this.maxConcurrent = integer(options.maxConcurrent ?? 8, "Concurrent inference requests");
    this.maxTokens = integer(options.maxEstimatedTokens ?? 2_097_152, "Inference token working set");
    this.maxBackfill = integer(options.maxBackfillConcurrent ?? Math.min(4, this.maxConcurrent), "Concurrent backfill requests");
    this.quantum = integer(options.quantumTokens ?? 4096, "Inference fairness quantum");
    if (this.maxBackfill > this.maxConcurrent) throw new RangeError("Backfill capacity exceeds total inference capacity");
    const reservation = (workClass: InferenceClass, divisor: number, tokenDivisor = divisor): Reservation => {
      const value = options.reservations?.[workClass] ?? {
        requests: Math.floor(this.maxConcurrent / divisor), tokens: Math.floor(this.maxTokens / tokenDivisor),
      };
      return { requests: integer(value.requests, "Reserved inference requests", 0), tokens: integer(value.tokens, "Reserved inference tokens", 0) };
    };
    this.reserved = { foreground: reservation("foreground", 8), critical: reservation("critical", 4), email: reservation("email", 8), cron: reservation("cron", 8, 16),
      fresh: reservation("fresh", 8), backfill: reservation("backfill", Infinity) };
    this.shared = {
      requests: this.maxConcurrent - classes.reduce((sum, workClass) => sum + this.reserved[workClass].requests, 0),
      tokens: this.maxTokens - classes.reduce((sum, workClass) => sum + this.reserved[workClass].tokens, 0),
    };
    if (this.shared.requests < 0 || this.shared.tokens < 0 || classes.some(workClass =>
      this.reserved[workClass].requests + this.shared.requests < 1 || this.reserved[workClass].tokens + this.shared.tokens < 1)) {
      throw new RangeError("Inference reservations must leave runnable capacity for every work class");
    }
  }
  async run<T>(account: string, workClass: InferenceClass, tokens: number, operation: () => Promise<T>, signal?: AbortSignal, group?: string): Promise<T> {
    const release = await this.acquire(account, workClass, tokens, signal, group);
    try { signal?.throwIfAborted(); return await operation(); }
    finally { release(); }
  }
  async acquire(accountId: string, workClass: InferenceClass, tokens: number, signal?: AbortSignal, group?: string): Promise<() => void> {
    signal?.throwIfAborted(); integer(tokens, "Estimated inference tokens");
    // Ordinary oversized work may borrow ordinary reservations, never the
    // critical reserve. Impossible requests fail visibly instead of waiting.
    if (this.reserved.critical.requests > 0 && workClass !== "critical" && tokens > this.maxTokens - this.reserved.critical.tokens) {
      throw new RangeError("Model context exceeds ordinary capacity; compact retained work before retrying");
    }
    const otherCriticalReserve = group === undefined || this.reserved.critical.requests < 2 ? 0
      : Math.floor(this.reserved.critical.tokens / this.reserved.critical.requests);
    if (workClass === "critical" && tokens > this.reserved.critical.tokens + this.shared.tokens - otherCriticalReserve) {
      throw new RangeError("Workflow context exceeds critical capacity; compact the current work packet before retrying");
    }
    let account = this.accounts.get(accountId);
    if (account === undefined) {
      account = { active: counters(), tokens: counters(), queues: { foreground: [], critical: [], email: [], cron: [], fresh: [], backfill: [] },
        deficits: counters(), cursor: 0, enter: true, exclusive: false, waiting: undefined, pumping: false, criticalGroups: new Map() };
      this.accounts.set(accountId, account);
    }
    const owned = account;
    return new Promise((resolve, reject) => {
      const waiter: Waiter = { workClass, tokens, resolve, signal, ...(group === undefined ? {} : { group }),
        abort: () => {
          const queue = owned.queues[workClass], index = queue.indexOf(waiter);
          if (index < 0) return;
          queue.splice(index, 1); signal?.removeEventListener("abort", waiter.abort);
          if (owned.waiting === waiter) owned.waiting = undefined;
          reject(signal?.reason ?? new Error("Inference admission aborted"));
          this.pump(accountId, owned);
        },
      };
      owned.queues[workClass].push(waiter);
      signal?.addEventListener("abort", waiter.abort, { once: true });
      this.pump(accountId, owned);
    });
  }
  private oversized(waiter: Waiter): boolean {
    return waiter.tokens > this.reserved[waiter.workClass].tokens + this.shared.tokens;
  }
  private sharedUse(account: Account, waiter?: Waiter): Reservation {
    return classes.reduce((total, workClass) => {
      const add = waiter?.workClass === workClass;
      return {
        requests: total.requests + Math.max(0, account.active[workClass] + (add ? 1 : 0) - this.reserved[workClass].requests),
        tokens: total.tokens + Math.max(0, account.tokens[workClass] + (add ? waiter.tokens : 0) - this.reserved[workClass].tokens),
      };
    }, { requests: 0, tokens: 0 });
  }
  private fits(account: Account, waiter: Waiter): boolean {
    if (account.exclusive && waiter.workClass !== "critical") return false;
    // One request per critical workflow leaves the other workflow's reserved
    // request available. Other items still execute tools concurrently.
    if (waiter.workClass === "critical" && waiter.group !== undefined && account.criticalGroups.has(waiter.group)) return false;
    const active = classes.reduce((sum, workClass) => sum + account.active[workClass], 0);
    if (active >= this.maxConcurrent) return false;
    if (this.oversized(waiter)) return classes.every(workClass => workClass === "critical" || account.active[workClass] === 0) &&
      (active === 0 || classes.reduce((sum, workClass) => sum + account.tokens[workClass], waiter.tokens) <= this.maxTokens);
    if (active >= this.maxConcurrent || waiter.workClass === "backfill" && account.active.backfill >= this.maxBackfill) return false;
    if (account.exclusive) return account.active.critical < this.reserved.critical.requests &&
      account.tokens.critical + waiter.tokens <= this.reserved.critical.tokens;
    const shared = this.sharedUse(account, waiter);
    return shared.requests <= this.shared.requests && shared.tokens <= this.shared.tokens;
  }
  private advance(account: Account): void { account.cursor = (account.cursor + 1) % classes.length; account.enter = true; }
  /** Deficit round robin charges estimated tokens rather than request count.
   * Jump empty-credit rounds arithmetically, including for a giant original.
   */
  private choose(account: Account): Waiter | undefined {
    for (;;) {
      for (let turn = 0; turn < classes.length; turn += 1) {
        const workClass = classes[account.cursor];
        if (workClass === undefined) throw new Error("Inference fairness cursor is invalid");
        const first = account.queues[workClass][0];
        if (first === undefined) account.deficits[workClass] = 0;
        else {
          if (account.enter) account.deficits[workClass] = Math.min(Number.MAX_SAFE_INTEGER, account.deficits[workClass] + this.quantum * weights[workClass]);
          account.enter = false;
          if (first.tokens <= account.deficits[workClass]) return first;
        }
        this.advance(account);
      }
      let rounds = Infinity;
      for (const workClass of classes) {
        const first = account.queues[workClass][0];
        if (first !== undefined) rounds = Math.min(rounds, Math.ceil((first.tokens - account.deficits[workClass]) / (this.quantum * weights[workClass])));
      }
      if (!Number.isFinite(rounds)) return undefined;
      if (rounds > 1) for (const workClass of classes) if (account.queues[workClass].length > 0) {
        account.deficits[workClass] = Math.min(Number.MAX_SAFE_INTEGER, account.deficits[workClass] + (rounds - 1) * this.quantum * weights[workClass]);
      }
    }
  }
  private grant(accountId: string, account: Account, waiter: Waiter, charge: boolean): void {
    const position = account.queues[waiter.workClass].indexOf(waiter);
    if (position < 0 || position !== 0 && waiter.workClass !== "critical") throw new Error("Inference admission lost its queue owner");
    account.queues[waiter.workClass].splice(position, 1); waiter.signal?.removeEventListener("abort", waiter.abort);
    if (charge) account.deficits[waiter.workClass] -= waiter.tokens;
    if (account.queues[waiter.workClass].length === 0) {
      account.deficits[waiter.workClass] = 0;
      if (classes[account.cursor] === waiter.workClass) this.advance(account);
    }
    account.active[waiter.workClass] += 1; account.tokens[waiter.workClass] += waiter.tokens;
    if (waiter.workClass === "critical" && waiter.group !== undefined) account.criticalGroups.set(waiter.group, 1);
    if (this.oversized(waiter)) account.exclusive = true;
    let released = false;
    waiter.resolve(() => {
      if (released) return;
      released = true; account.active[waiter.workClass] -= 1; account.tokens[waiter.workClass] -= waiter.tokens;
      if (waiter.workClass === "critical" && waiter.group !== undefined) account.criticalGroups.delete(waiter.group);
      if (this.oversized(waiter)) account.exclusive = false;
      this.pump(accountId, account);
    });
  }
  private pump(accountId: string, account: Account): void {
    if (account.pumping) return;
    account.pumping = true;
    try {
      for (;;) {
        account.waiting ??= this.choose(account);
        const selected = account.waiting;
        if (selected === undefined) break;
        if (this.fits(account, selected)) {
          account.waiting = undefined; this.grant(accountId, account, selected, true); continue;
        }
        // Reservations remain usable while another class waits for the shared
        // pool. An oversized head drains the account before running alone.
        const used = this.sharedUse(account);
        const bypass = classes.flatMap(workClass => {
          const first = account.queues[workClass][0];
          const available = workClass === "critical" ? account.queues.critical.find(waiter => this.fits(account, waiter)) : first;
          if (available === undefined || available === selected || !this.fits(account, available)) return [];
          if (workClass === "critical") return [available];
          if (this.oversized(selected)) return [];
          const next = this.sharedUse(account, available);
          return next.requests <= used.requests && next.tokens <= used.tokens ? [available] : [];
        })[0];
        if (bypass === undefined) break;
        this.grant(accountId, account, bypass, false);
      }
    } finally {
      account.pumping = false;
      if (classes.every(workClass => account.active[workClass] === 0 && account.queues[workClass].length === 0)) this.accounts.delete(accountId);
    }
  }
}
