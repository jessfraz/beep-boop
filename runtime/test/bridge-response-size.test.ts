import assert from "node:assert/strict";
import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { mkdir, mkdtemp, rm, stat, symlink, writeFile } from "node:fs/promises";
import { createConnection } from "node:net";
import { join } from "node:path";
import { createInterface } from "node:readline";
import { setTimeout as delay } from "node:timers/promises";
import { test } from "node:test";
import { fileURLToPath } from "node:url";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { createModels } from "@earendil-works/pi-ai";
import { createRegistry, Harness, UserEntry } from "@earendil-works/pi-durable";
import { SqliteStorage } from "@earendil-works/pi-durable/storage/sqlite";
import { openConversationDatabase } from "../src/conversation-database.js";
import type { RuntimeCommand } from "../src/generated/RuntimeCommand.js";
import type { RuntimeEvent } from "../src/generated/RuntimeEvent.js";
import type { RuntimeEventKind } from "../src/generated/RuntimeEventKind.js";
import { ConversationModels } from "../src/models.js";
import { protocolLines, validEvent } from "../src/protocol.js";
import { TaskStore } from "../src/tasks.js";

const MAX_FRAME_BYTES = 4 * 1024 * 1024;
const ids = { exact: "11111111-1111-4111-8111-111111111111", overflow: "22222222-2222-4222-8222-222222222222",
  unicode: "33333333-3333-4333-8333-333333333333", small: "44444444-4444-4444-8444-444444444444",
  view: "55555555-5555-4555-8555-555555555555" };
const runtime = fileURLToPath(new URL("../src/bridge.js", import.meta.url));
type Running = { child: ChildProcessWithoutNullStreams; closed: Promise<number | null>; errors(): string };

async function open(state: string) {
  const models = createModels();
  const session = await Harness.open(await SqliteStorage.open(await openConversationDatabase(join(state, "session.sqlite"))),
    { models, registry: createRegistry(), identity: { attribution: null } }, BACKGROUND_CONTEXT);
  return { session, tasks: new TaskStore(session, new ConversationModels(models)) };
}
function track(child: ChildProcessWithoutNullStreams): Running {
  let errors = "";
  child.stderr.setEncoding("utf8").on("data", (chunk: string) => { errors += chunk; });
  const closed = new Promise<number | null>((resolve, reject) => { child.once("close", resolve); child.once("error", reject); });
  void closed.catch(() => {});
  return { child, closed, errors: () => errors };
}
async function stop(running: Running): Promise<void> {
  if (running.child.exitCode === null && running.child.signalCode === null) running.child.kill("SIGTERM");
  running.child.stdout.destroy();
  await running.closed;
}
function assertTooLarge(events: RuntimeEventKind[]): void {
  assert.deepEqual(events.map(event => event.type), ["failure", "done"],
    "An oversized response must fail only its request instead of emitting a partial task or stopping the coordinator");
  const failure = events[0]; assert.ok(failure?.type === "failure");
  assert.equal(failure.code, "invalid_request"); assert.match(failure.message, /size limit/i);
}

test("bridge response framing counts UTF-8 and the final LF without changing retained tasks", { timeout: 45_000 }, async t => {
  const state = await mkdtemp("/tmp/agent-response-boundary-");
  let running: Running | undefined;
  const originals = new Map<string, string>();
  try {
    const app = await open(state);
    try {
      for (const [id, request, excess, multibyte] of [[ids.exact, 1, 0, false], [ids.overflow, 2, 1, false], [ids.unicode, 3, 1, true]] satisfies [string, number, number, boolean][]) {
        const task = await app.tasks.create({ id, title: "Invented retained task", outcome: "Placeholder" });
        const empty: RuntimeEvent = { version: 1, id: request, event: { type: "task", task: { ...task, outcome: "" }, history: await app.tasks.history(id) } };
        const available = MAX_FRAME_BYTES - Buffer.byteLength(JSON.stringify(empty) + "\n");
        const outcome = multibyte ? "🌲".repeat(Math.ceil((available + excess) / 4)) : "x".repeat(available + excess);
        await app.tasks.update(id, saved => { saved.outcome = outcome; }); originals.set(id, outcome);
        const expected: RuntimeEvent = { ...empty, event: { type: "task", task: await app.tasks.get(id), history: await app.tasks.history(id) } };
        const encoded = JSON.stringify(expected) + "\n";
        if (multibyte) { assert.ok(encoded.length < MAX_FRAME_BYTES); assert.ok(Buffer.byteLength(encoded) > MAX_FRAME_BYTES); }
        else assert.equal(Buffer.byteLength(encoded), MAX_FRAME_BYTES + excess);
      }
    } finally { await app.session.close(BACKGROUND_CONTEXT); }
    const bridge = track(spawn(process.execPath, ["--no-warnings", runtime, state],
      { stdio: ["pipe", "pipe", "pipe"], env: { ...process.env, OPENAI_API_KEY: "" } }));
    running = bridge;
    const lines = createInterface({ input: bridge.child.stdout });
    t.signal.addEventListener("abort", () => bridge.child.kill("SIGTERM"), { once: true });
    const request = async (id: number, command: RuntimeCommand) => {
      const frames: { frame: RuntimeEvent; bytes: number }[] = [];
      let receive = (_line: string) => {};
      try {
        return await Promise.race([new Promise<typeof frames>((resolve, reject) => {
          receive = line => {
            try {
              const value: unknown = JSON.parse(line); assert.ok(validEvent(value)); assert.equal(value.id, id);
              frames.push({ frame: value, bytes: Buffer.byteLength(line + "\n") });
              if (value.event.type === "done") resolve(frames);
            } catch (error) { reject(error); }
          };
          lines.on("line", receive);
          bridge.child.stdin.write(JSON.stringify({ version: 1, id, command }) + "\n");
        }), bridge.closed.then(() => { throw new Error("Bridge stopped before its response: " + bridge.errors()); })]);
      } finally { lines.off("line", receive); }
    };
    const exact = await request(1, { type: "read_task", task_id: ids.exact });
    const task = exact[0]; assert.ok(task?.frame.event.type === "task");
    assert.equal(task.bytes, MAX_FRAME_BYTES); assert.equal(task.frame.event.task.outcome, originals.get(ids.exact));
    for (const [id, task_id] of [[2, ids.overflow], [3, ids.unicode]] satisfies [number, string][]) {
      const response = await request(id, { type: "read_task", task_id });
      assertTooLarge(response.map(item => item.frame.event));
      assert.ok(response.every(item => item.bytes <= MAX_FRAME_BYTES));
    }
    assert.deepEqual((await request(4, { type: "status" })).map(item => item.frame.event.type), ["auth_status", "done"]);
    bridge.child.stdin.end(); assert.equal(await bridge.closed, 0, bridge.errors()); lines.close();
    const reopened = await open(state);
    try { for (const [id, outcome] of originals) assert.equal((await reopened.tasks.get(id)).outcome, outcome); }
    finally { await reopened.session.close(BACKGROUND_CONTEXT); }
  } finally { if (running !== undefined) await stop(running); await rm(state, { recursive: true, force: true }); }
});

test("an oversized task read leaves the real Rust coordinator and its other requests alive", { timeout: 90_000 }, async t => {
  const directory = await mkdtemp("/tmp/agent-response-coordinator-");
  const state = join(directory, "state"), bin = join(directory, "bin"), socket = join(state, "agent.sock");
  let running: Running | undefined;
  try {
    await mkdir(state, { mode: 0o700 }); await mkdir(bin); await symlink(process.execPath, join(bin, "node"));
    await writeFile(join(directory, "agent.toml"), 'node_name = "response-fixture"\njob_directories = []\n');
    await writeFile(join(directory, "AGENTS.md"), "Use invented test data.\n");
    const app = await open(state), outcome = "🌲".repeat(MAX_FRAME_BYTES / 4);
    let viewConversation = "", retainedReceiptId = "";
    try {
      await app.tasks.create({ id: ids.unicode, title: "Large retained original", outcome });
      await app.tasks.create({ id: ids.small, title: "Independent retained task", outcome: "Still readable" });
      viewConversation = (await app.tasks.create({ id: ids.view, title: outcome, outcome: "Keep its saved title" })).conversation_id;
      const conversation = await app.tasks.conversation(viewConversation);
      retainedReceiptId = await conversation.commit(async tx => {
        const entry = await tx.appendEntry(UserEntry, conversation.id, { model: [{ role: "user", content: "Retain this placed input", timestamp: 1 }] });
        return (await tx.createSubmission({ type: "input", conversationId: conversation.id,
          requestId: "retained-before-large-view", status: "placed", entry: entry.id })).id.toString();
      }, BACKGROUND_CONTEXT);
    } finally { await app.session.close(BACKGROUND_CONTEXT); }
    const binary = process.env.AGENT_TEST_BINARY ?? fileURLToPath(new URL("../../../target/debug/beep-boop", import.meta.url));
    const server = track(spawn(binary, ["--state-dir", state, "--config", join(directory, "agent.toml"), "--runtime", runtime,
      "--instructions", join(directory, "AGENTS.md"), "serve", "--local-only", "--no-sources"],
    { stdio: ["pipe", "pipe", "pipe"], env: { PATH: bin, HOME: directory, XDG_CONFIG_HOME: join(directory, "config"),
      XDG_DATA_HOME: join(directory, "data"), TMPDIR: directory, AGENT_SQLITE_VEC: process.env.AGENT_SQLITE_VEC } }));
    running = server; server.child.stdout.resume();
    for (;;) {
      assert.equal(server.child.exitCode, null, server.errors()); assert.equal(server.child.signalCode, null, server.errors());
      t.signal.throwIfAborted();
      if (await stat(socket).then(value => value.isSocket(), () => false)) break;
      await delay(10, undefined, { signal: t.signal });
    }
    const request = async (command: RuntimeCommand) => {
      const channel = createConnection({ path: socket, signal: t.signal });
      const events: RuntimeEventKind[] = [];
      try {
        channel.write(JSON.stringify(command) + "\n");
        for await (const line of protocolLines(channel)) {
          const event: unknown = JSON.parse(line), frame = { version: 1, id: 1, event }; assert.ok(validEvent(frame));
          events.push(frame.event); if (frame.event.type === "done") return events;
        }
        throw new Error("The real coordinator closed before completing the request: " + server.errors());
      } finally { channel.destroy(); }
    };
    assertTooLarge(await request({ type: "read_task", task_id: ids.unicode }));
    assertTooLarge(await request({ type: "list_tasks" }));
    assertTooLarge(await request({ type: "attach_conversation", conversation_id: viewConversation, input_ids: [] }));
    assert.deepEqual((await request({ type: "status" })).map(event => event.type), ["auth_status", "done"]);
    const small = (await request({ type: "read_task", task_id: ids.small }))[0]; assert.ok(small?.type === "task");
    assert.equal(small.task.outcome, "Still readable"); assert.equal(server.child.exitCode, null); assert.equal(server.child.signalCode, null);
    server.child.kill("SIGTERM"); assert.equal(await server.closed, 0, server.errors());
    const reopened = await open(state);
    try {
      assert.equal((await reopened.tasks.get(ids.unicode)).outcome, outcome);
      assert.equal((await reopened.tasks.get(ids.view)).title, outcome);
      const conversation = await reopened.tasks.conversation(viewConversation);
      const receipt = await conversation.commit(tx => tx.submissionByRequest(conversation.id, "retained-before-large-view"), BACKGROUND_CONTEXT);
      assert.equal(receipt?.id.toString(), retainedReceiptId); assert.equal(receipt?.status, "placed");
    }
    finally { await reopened.session.close(BACKGROUND_CONTEXT); }
  } finally { if (running !== undefined) await stop(running); await rm(directory, { recursive: true, force: true }); }
});
