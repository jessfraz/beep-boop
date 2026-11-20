import type { TaskWorkflow } from "./generated/TaskWorkflow.js";

type Waiter = { workflow: string; resource: string; resolve(release: () => void): void;
  reject(reason: unknown): void; signal: AbortSignal | undefined; abort(): void };

/** The task outbox owns durable work. This bounded, process-local admission
 * only limits live actors and serializes effects for the same work identity. */
export class WorkflowCapacity {
  private readonly counts = new Map<string, number>();
  private readonly resources = new Set<string>();
  private readonly waiters: Waiter[] = [];
  private readonly running = new Map<string, Promise<void>>();
  constructor(private readonly maximum = 8) {
    if (!Number.isSafeInteger(maximum) || maximum < 2) throw new RangeError("Workflow concurrency must allow independent work");
  }
  async run(id: string, work: TaskWorkflow, operation: () => Promise<void>, signal?: AbortSignal): Promise<void> {
    const existing = this.running.get(id);
    if (existing !== undefined) return existing;
    const pending = (async () => {
      const release = await this.acquire(work.job.id, JSON.stringify([work.job.id, work.work_key]), signal);
      try { signal?.throwIfAborted(); await operation(); }
      finally { release(); }
    })();
    this.running.set(id, pending);
    try { await pending; }
    finally { if (this.running.get(id) === pending) this.running.delete(id); }
  }
  private async acquire(workflow: string, resource: string, signal?: AbortSignal): Promise<() => void> {
    signal?.throwIfAborted();
    return new Promise((resolve, reject) => {
      const waiter: Waiter = { workflow, resource, resolve, reject, signal,
        abort: () => {
          const position = this.waiters.indexOf(waiter);
          if (position < 0) return;
          this.waiters.splice(position, 1); signal?.removeEventListener("abort", waiter.abort);
          reject(signal?.reason ?? new Error("Workflow admission interrupted")); this.pump();
        } };
      this.waiters.push(waiter); signal?.addEventListener("abort", waiter.abort, { once: true }); this.pump();
    });
  }
  private pump(): void {
    for (let index = 0; index < this.waiters.length;) {
      const waiter = this.waiters[index];
      if (waiter === undefined) break;
      const count = this.counts.get(waiter.workflow) ?? 0;
      if (count >= this.maximum || this.resources.has(waiter.resource)) { index++; continue; }
      this.waiters.splice(index, 1); waiter.signal?.removeEventListener("abort", waiter.abort);
      this.counts.set(waiter.workflow, count + 1); this.resources.add(waiter.resource);
      let released = false;
      waiter.resolve(() => {
        if (released) return;
        released = true; this.resources.delete(waiter.resource);
        const remaining = (this.counts.get(waiter.workflow) ?? 1) - 1;
        if (remaining === 0) this.counts.delete(waiter.workflow); else this.counts.set(waiter.workflow, remaining);
        this.pump();
      });
    }
  }
}
