import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createInterface } from "node:readline";
import { test } from "node:test";
import { fileURLToPath } from "node:url";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { createModels } from "@earendil-works/pi-ai";
import { createRegistry, Harness, UserEntry } from "@earendil-works/pi-durable";
import { SqliteStorage } from "@earendil-works/pi-durable/storage/sqlite";
import { openConversationDatabase } from "../src/conversation-database.js";
import { ConversationModels } from "../src/models.js";
import { validEvent } from "../src/protocol.js";
import { TaskStore } from "../src/tasks.js";
import type { RuntimeCommand } from "../src/generated/RuntimeCommand.js";
import type { RuntimeEventKind } from "../src/generated/RuntimeEventKind.js";
import type { TaskRecord } from "../src/generated/TaskRecord.js";

test("retained native history scans keep the real bridge responsive", { timeout: 60_000 }, async t => {
  const directory = await mkdtemp(join(tmpdir(), "agent-history-responsive-"));
  t.after(() => rm(directory, { recursive: true, force: true }));
  const models = createModels(), session = await Harness.open(await SqliteStorage.open(await openConversationDatabase(join(directory, "session.sqlite"))),
    { models, registry: createRegistry(), identity: { attribution: null } }, BACKGROUND_CONTEXT);
  const store = new TaskStore(session, new ConversationModels(models)), tasks: TaskRecord[] = [];
  const entries = 10_000, oldest = "ancientneedlealpha", newest = "Latest retained note", absent = "quasarzyxabsent";
  try {
    for (let index = 0; index < 2; index += 1) {
      const task = await store.create({ title: `Retained work ${index}`, outcome: "Read the same durable evidence" }); tasks.push(task);
      const conversation = await store.conversation(task.conversation_id);
      for (let start = 0; start < entries; start += 500) await session.commit(async tx => {
        for (let ordinal = start; ordinal < start + 500; ordinal += 1) await tx.appendEntry(UserEntry, conversation.id, {
          model: [{ role: "user", content: ordinal === 0 && index === 0 ? oldest : ordinal === entries - 1 ? newest : "note", timestamp: ordinal }],
        });
      }, BACKGROUND_CONTEXT);
    }
  } finally { await session.close(BACKGROUND_CONTEXT); }
  const first = tasks[0]; assert.ok(first);
  const child = spawn(process.execPath, ["--no-warnings", fileURLToPath(new URL("../src/bridge.js", import.meta.url)), directory],
    { stdio: ["pipe", "pipe", "pipe"], env: { ...process.env, OPENAI_API_KEY: "" } });
  const closed = new Promise<number | null>((resolve, reject) => { child.once("close", resolve); child.once("error", reject); });
  const lines = createInterface({ input: child.stdout }), arrivals: { id: number; type: RuntimeEventKind["type"] }[] = [];
  const pending = new Map<number, { events: RuntimeEventKind[]; resolve(events: RuntimeEventKind[]): void; reject(error: Error): void }>();
  let sequence = 0, errors = "";
  child.stderr.setEncoding("utf8").on("data", (chunk: string) => { errors += chunk; });
  const stopped = () => { for (const reply of pending.values()) reply.reject(new Error("Bridge stopped before its native read completed: " + errors)); pending.clear(); };
  child.once("close", stopped); t.signal.addEventListener("abort", () => child.kill(), { once: true });
  lines.on("line", line => {
    const frame: unknown = JSON.parse(line); assert.ok(validEvent(frame));
    arrivals.push({ id: frame.id, type: frame.event.type });
    const reply = pending.get(frame.id); if (reply === undefined) return;
    reply.events.push(frame.event);
    if (frame.event.type === "failure") { pending.delete(frame.id); reply.reject(new Error(frame.event.message)); }
    else if (frame.event.type === "done") { pending.delete(frame.id); reply.resolve(reply.events); }
  });
  const prepare = (command: RuntimeCommand) => {
    const id = ++sequence;
    const done = new Promise<RuntimeEventKind[]>((resolve, reject) => { pending.set(id, { events: [], resolve, reject }); });
    return { id, done, line: JSON.stringify({ version: 1, id, command }) + "\n" };
  };
  const request = (command: RuntimeCommand) => { const reply = prepare(command); child.stdin.write(reply.line); return reply.done; };
  const concurrent = async (command: Extract<RuntimeCommand, { type: "search_tasks" | "read_task" }>) => {
    const scan = prepare(command), control = prepare({ type: "status" }), began = performance.now();
    // Both real wire requests arrive together. The auth read needs an event-loop
    // turn while retained-history pages can otherwise resolve entirely in microtasks.
    child.stdin.write(scan.line + control.line);
    const [result, status] = await Promise.all([scan.done, control.done]);
    assert.deepEqual(status[0], { type: "auth_status", status: { state: "signed_out" } });
    const controlDone = arrivals.findIndex(frame => frame.id === control.id && frame.type === "done");
    const scanResult = arrivals.findIndex(frame => frame.id === scan.id && frame.type === (command.type === "search_tasks" ? "tasks" : "task"));
    assert.ok(controlDone >= 0 && scanResult >= 0 && controlDone < scanResult,
      `${command.type} must let an independent status finish before its exhaustive native history scan`);
    t.diagnostic(JSON.stringify({ command: command.type, native_entries: command.type === "read_task" ? entries : entries * tasks.length, elapsed_ms: performance.now() - began }));
    return result;
  };
  try {
    await request({ type: "list_tasks" }); await request({ type: "status" });
    const warmed = await request({ type: "search_tasks", query: absent });
    assert.deepEqual(warmed[0], { type: "tasks", tasks: [] });
    await request({ type: "read_task", task_id: first.id });
    await t.test("an absent-token search yields while preserving exhaustive results", async () => {
      const response = await concurrent({ type: "search_tasks", query: absent });
      assert.deepEqual(response[0], { type: "tasks", tasks: [] });
      const found = (await request({ type: "search_tasks", query: oldest }))[0]; assert.ok(found?.type === "tasks");
      assert.deepEqual(found.tasks.map(task => task.id), [first.id], "An oldest retained entry remains searchable");
    });
    await t.test("a full task-history read yields without truncating its oldest evidence", async () => {
      const response = (await concurrent({ type: "read_task", task_id: first.id }))[0]; assert.ok(response?.type === "task");
      assert.equal(response.task.id, first.id); assert.equal(response.history.split("\n").length, entries);
      assert.ok(response.history.startsWith(oldest)); assert.ok(response.history.endsWith(newest));
    });
    child.stdin.end(); assert.equal(await closed, 0, errors);
  } finally { lines.close(); child.kill(); await closed; }
});
