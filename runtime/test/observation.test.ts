import assert from "node:assert/strict";
import { test } from "node:test";
import { Observation } from "../src/observation.js";
import type { RuntimeEventKind } from "../src/generated/RuntimeEventKind.js";

test("observation publishes snapshots before bounded live events and releases its flushed queue", () => {
  const events: RuntimeEventKind[] = [], observation = new Observation((event) => events.push(event));
  observation.publish({ type: "input_placed", input_id: "saved", entry_id: "1" });
  observation.snapshot({ type: "input_accepted", input_id: "saved" });
  observation.snapshot({ type: "history", conversation_id: "saved", messages: [], next_cursor: null, page_complete: true });
  assert.deepEqual(events.map((event) => event.type), ["input_accepted", "history"]);
  assert.equal(observation.open(), true);
  observation.publish({ type: "worker_idle" });
  assert.equal(observation.open(), true);
  assert.deepEqual(events.map((event) => event.type), ["input_accepted", "history", "input_placed", "worker_idle"]);
  observation.close();
});

test("observation accepts exactly 512 buffered frames and closes once on overflow", async () => {
  const events: RuntimeEventKind[] = [], observation = new Observation((event) => events.push(event));
  for (let index = 0; index < 512; index += 1) observation.publish({ type: "worker_idle" });
  assert.equal(observation.closed, false);
  observation.publish({ type: "worker_idle" });
  await observation.finished;
  assert.equal(observation.closed, true);
  observation.publish({ type: "worker_idle" }); observation.snapshot({ type: "worker_idle" });
  assert.equal(observation.open(), false);
  assert.deepEqual(events.map((event) => event.type), ["failure"]);
});

test("observation counts serialized UTF-8 bytes and accepts exactly 4 MiB", () => {
  const empty: RuntimeEventKind = { type: "text_delta", text: "" };
  const remaining = 4 * 1024 * 1024 - Buffer.byteLength(JSON.stringify(empty), "utf8");
  const text = "界".repeat(Math.floor(remaining / 3)) + "x".repeat(remaining % 3);
  const events: RuntimeEventKind[] = [], observation = new Observation((event) => events.push(event));
  observation.publish({ type: "text_delta", text });
  assert.equal(observation.closed, false);
  observation.publish({ type: "text_delta", text: "" });
  assert.equal(observation.closed, true);
  assert.deepEqual(events.map((event) => event.type), ["failure"]);
  const exact: RuntimeEventKind[] = [], boundary = new Observation((event) => exact.push(event));
  boundary.publish({ type: "text_delta", text });
  assert.equal(boundary.open(), true);
  assert.deepEqual(exact, [{ type: "text_delta", text }]); boundary.close();
});

test("detaching while buffering or flushing discards queued and future events", async () => {
  const buffered: RuntimeEventKind[] = [], waiting = new Observation((event) => buffered.push(event));
  waiting.publish({ type: "worker_idle" }); waiting.close(); await waiting.finished;
  waiting.snapshot({ type: "worker_idle" }); waiting.publish({ type: "worker_idle" });
  assert.equal(waiting.open(), false); assert.deepEqual(buffered, []);
  const events: RuntimeEventKind[] = [];
  const flushing = new Observation((event) => { events.push(event); flushing.close(); });
  flushing.publish({ type: "worker_idle" }); flushing.publish({ type: "interrupted" });
  assert.equal(flushing.open(), false); await flushing.finished;
  assert.deepEqual(events, [{ type: "worker_idle" }]);
});

test("events published during a flush retain order behind the existing queue", () => {
  const events: RuntimeEventKind[] = [];
  const observation = new Observation((event) => {
    events.push(event);
    if (event.type === "worker_idle") observation.publish({ type: "interrupted" });
  });
  observation.publish({ type: "worker_idle" });
  observation.publish({ type: "input_accepted", input_id: "saved" });
  assert.equal(observation.open(), true);
  assert.deepEqual(events.map((event) => event.type), ["worker_idle", "input_accepted", "interrupted"]);
  observation.close();
});
