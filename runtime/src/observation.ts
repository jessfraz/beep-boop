import type { RuntimeEventKind } from "./generated/RuntimeEventKind.js";

type BufferedEvent = { event: RuntimeEventKind; bytes: number };
type ObservationState =
  | { type: "buffering" | "flushing"; events: BufferedEvent[]; bytes: number }
  | { type: "live" }
  | { type: "closed" };

/** Own the snapshot/live boundary. Subscribers cannot access or replace its queue. */
export class Observation {
  #state: ObservationState = { type: "buffering", events: [], bytes: 0 };
  readonly finished: Promise<void>;
  #finish = () => {};
  constructor(private readonly emit: (event: RuntimeEventKind) => void) {
    this.finished = new Promise<void>((resolve) => { this.#finish = resolve; });
  }
  get closed(): boolean { return this.#state.type === "closed"; }

  /** Snapshot owners publish ready/history/receipts before releasing live events. */
  snapshot(event: RuntimeEventKind): void { if (!this.closed) this.emit(event); }

  publish(event: RuntimeEventKind): void {
    const state = this.#state;
    if (state.type === "closed") return;
    if (state.type === "live") { this.emit(event); return; }
    const bytes = Buffer.byteLength(JSON.stringify(event), "utf8");
    if (state.events.length >= 512 || state.bytes + bytes > 4 * 1024 * 1024) {
      this.close();
      this.emit({ type: "failure", code: "internal",
        message: "Observation fell behind during history load. Reconnect to recover saved history." });
      return;
    }
    state.events.push({ event, bytes }); state.bytes += bytes;
  }

  open(): boolean {
    const state = this.#state;
    if (state.type === "closed") return false;
    if (state.type === "live" || state.type === "flushing") return true;
    state.type = "flushing";
    while (this.#state === state) {
      const next = state.events.shift();
      if (next === undefined) { this.#state = { type: "live" }; break; }
      state.bytes -= next.bytes;
      this.emit(next.event);
    }
    return !this.closed;
  }

  close(): void {
    if (this.#state.type === "buffering" || this.#state.type === "flushing") this.#state.events.length = 0;
    this.#state = { type: "closed" };
    this.#finish();
  }
}
