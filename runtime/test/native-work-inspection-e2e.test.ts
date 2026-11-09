import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { mkdir, mkdtemp, rm, symlink, writeFile } from "node:fs/promises";
import { createConnection } from "node:net";
import { join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import { test } from "node:test";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { openNodeSqliteDatabase } from "@earendil-works/chord/sqlite/node";
import { builtinModels } from "@earendil-works/pi-ai/providers/all";
import { createRegistry, GenerationTask, Harness, LiveDoc, UserEntry, type TaskId } from "@earendil-works/pi-durable";
import { SqliteStorage } from "@earendil-works/pi-durable/storage/sqlite";
import type { RuntimeCommand } from "../src/generated/RuntimeCommand.js";
import type { RuntimeEventKind } from "../src/generated/RuntimeEventKind.js";
import type { SavedConversation } from "../src/generated/SavedConversation.js";
import { ConversationModels, DEFAULT_MODEL } from "../src/models.js";
import { protocolLines, validEvent } from "../src/protocol.js";
import { TaskStore } from "../src/tasks.js";
import { scheduledTaskId } from "../src/scheduled.js";

test("native job and saved-work inspection uses the production coordinator without claiming execution", { timeout: 180_000 }, async t => {
  const directory = await mkdtemp("/tmp/agent-work-inspection-"), state = join(directory, "state"), bin = join(directory, "bin");
  t.after(() => rm(directory, { recursive: true, force: true }));
  const socket = join(state, "agent.sock"), runtime = fileURLToPath(new URL("../src/bridge.js", import.meta.url));
  const binary = process.env.AGENT_TEST_BINARY ?? fileURLToPath(new URL("../../../target/debug/beep-boop", import.meta.url));
  const generationIDs: TaskId[] = [];
  // Inspection remains available without an execution host or provider auth.
  // Startup/preflight and owner shutdown must retain these parked native runs.
  await mkdir(state, { mode: 0o700 }); await mkdir(bin, { mode: 0o700 }); await symlink(process.execPath, join(bin, "node"));
  await writeFile(join(directory, "config.toml"), 'node_name = "work-inspection-fixture"\njob_directories = []\n');
  await writeFile(join(directory, "AGENTS.md"), "Read only the invented retained evidence.\n");
  const models = builtinModels();
  async function open() {
    const storage = await SqliteStorage.open(await openNodeSqliteDatabase(join(state, "session.sqlite")));
    const harness = await Harness.open(storage, { models, registry: createRegistry(), identity: { attribution: null } }, BACKGROUND_CONTEXT);
    return { harness, storage, tasks: new TaskStore(harness, new ConversationModels(models)) };
  }
  async function generations() {
    // Read the running coordinator's durable state without another Harness that
    // could acquire execution or recover its tasks.
    const storage = await SqliteStorage.open(await openNodeSqliteDatabase(join(state, "session.sqlite")));
    try {
      return await Promise.all(generationIDs.map(async id => {
        const record = await storage.task(id, BACKGROUND_CONTEXT); assert.ok(record); return record;
      }));
    } finally { await storage.close(BACKGROUND_CONTEXT); }
  }
  const seeded = await open();
  const jobID = "native-work-inspection";
  let parentID: string, childID: string;
  try {
    const task = await seeded.tasks.create({ id: scheduledTaskId("job:" + jobID), title: "Native work inspection", outcome: "Retain the original work", model: DEFAULT_MODEL,
      sources: [{ provider: "scheduled_jobs", account: "local", thread_id: jobID }] });
    const child = await seeded.tasks.createAgent(task.id, { id: "review", title: "Bounded review", outcome: "Read the existing evidence",
      input: "Retain the pending child assignment", inputId: "pending-child-assignment", model: DEFAULT_MODEL });
    parentID = task.conversation_id; childID = child.conversation_id;
    const root = await seeded.harness.root(BACKGROUND_CONTEXT);
    for (const id of [parentID, childID, root.id.toString()]) {
      const conversation = await seeded.tasks.conversation(id);
      const generation = await conversation.commit(async tx => {
        if (id === parentID) {
          for (let index = 0; index < 150; index += 1) await tx.appendEntry(UserEntry, conversation.id,
            { model: [{ role: "user", content: `Earlier parent evidence ${index}`, timestamp: index + 1 }] });
        }
        const entry = await tx.appendEntry(UserEntry, conversation.id,
          { model: [{ role: "user", content: `Retained ${id} request`, timestamp: 1 }] });
        const input = await tx.createSubmission({ type: "input", conversationId: conversation.id, requestId: `retained-${id}`,
          status: "placed", entry: entry.id });
        const generation = await tx.createTask(GenerationTask, {}, { ownership: { kind: "conversation" } });
        const live = await tx.doc(LiveDoc, conversation.id);
        live.run = { taskId: generation, inputs: [input.id] }; live.generation = { attempt: 1 };
        return generation;
      }, BACKGROUND_CONTEXT);
      const record = await seeded.harness.getTask(generation, BACKGROUND_CONTEXT); assert.ok(record);
      const until = Date.now() + 3_600_000;
      await seeded.storage.commit([{ type: "task", value: { ...record, state: { status: "sleeping", until,
        checkpoint: { phase: "retry", attempt: 1, until } } } }], BACKGROUND_CONTEXT);
      generationIDs.push(generation);
    }
  } finally { await seeded.harness.close(BACKGROUND_CONTEXT); }
  const child = spawn(binary, ["--state-dir", state, "--config", join(directory, "config.toml"), "--runtime", runtime,
    "--instructions", join(directory, "AGENTS.md"), "serve", "--local-only", "--no-sources"], {
    cwd: directory, stdio: ["pipe", "pipe", "pipe"], env: { PATH: bin, HOME: directory, TMPDIR: directory,
      XDG_CONFIG_HOME: join(directory, "config"), XDG_DATA_HOME: join(directory, "data"), AGENT_SQLITE_VEC: process.env.AGENT_SQLITE_VEC },
  });
  let stderr = ""; child.stderr.setEncoding("utf8").on("data", (value: string) => { stderr += value; }); child.stdout.resume();
  const closed = new Promise<number | null>((resolve, reject) => { child.once("close", resolve); child.once("error", reject); });
  void closed.catch(() => {});
  async function control(command: RuntimeCommand,
    complete: (event: RuntimeEventKind) => boolean = event => event.type === "done"): Promise<RuntimeEventKind[]> {
    const channel = createConnection({ path: socket, signal: t.signal }), result: RuntimeEventKind[] = [];
    try {
      channel.write(JSON.stringify(command) + "\n");
      for await (const line of protocolLines(channel)) {
        const event: unknown = JSON.parse(line), frame = { version: 1, id: 1, event }; assert.ok(validEvent(frame));
        result.push(frame.event); assert.notEqual(frame.event.type, "failure", JSON.stringify(frame.event));
        assert.notEqual(frame.event.type, "host_call", "Read-only inspection must not execute host effects");
        if (complete(frame.event)) return result;
      }
      assert.fail("The production coordinator ended before its control receipt");
    } finally { channel.destroy(); }
  }
  try {
    for (;;) {
      assert.equal(child.exitCode, null, stderr); t.signal.throwIfAborted();
      try { await control({ type: "list_jobs" }); break; }
      catch (error) {
        if (!(error instanceof Error && "code" in error && ["ENOENT", "ECONNREFUSED"].includes(String(error.code)))) throw error;
        await delay(10, undefined, { signal: t.signal });
      }
    }
    const beforeInspection = await generations();
    for (const record of beforeInspection) {
      assert.equal(record.state.status, "sleeping", "The execution owner must retain the parked generation before any inspector attaches");
      assert.equal(record.abortRequested, false);
    }
    await control({ type: "sync_jobs", jobs: [{ id: jobID, title: "Native work inspection", prompt: "Read existing evidence", revision: "original",
      cwd: directory, timezone: "Etc/UTC", model: DEFAULT_MODEL, schedules: [{ id: "daily",
        recurrence: "DTSTART:20270101T090000Z\nRRULE:FREQ=DAILY", focus: null, prompt: null }] }] });
    await control({ type: "run_job", job_id: jobID, schedule_id: null, request_id: "existing-job-admission" });
    // Exercise the production inspection wire on every platform. Closing each
    // observer must leave the same native generations and controls available.
    const listing = (await control({ type: "list_jobs" })).find(event => event.type === "scheduled_jobs");
    assert.ok(listing);
    const listed = listing.jobs.find(job => job.job.id === jobID); assert.ok(listed);
    const taskID = scheduledTaskId("job:" + jobID); assert.equal(listed.task_id, taskID);
    const detail = (await control({ type: "read_job", job_id: jobID })).find(event => event.type === "scheduled_job");
    assert.ok(detail); assert.equal(detail.job.task_id, taskID); assert.ok(detail.job.last_run);
    const page = (await control({ type: "list_conversations", query: taskID, cursor: null, limit: 50 }))
      .find(event => event.type === "conversations"); assert.ok(page);
    for (const { conversationID, agentID } of [{ conversationID: parentID, agentID: null }, { conversationID: childID, agentID: "review" }]) {
      const saved: SavedConversation | undefined = page.items.find(item => item.conversation_id === conversationID); assert.ok(saved);
      assert.equal(saved.task_id, taskID); assert.equal(saved.agent_id, agentID);
      const attached = (await control({ type: "attach_conversation", conversation_id: conversationID, input_ids: [] },
        event => event.type === "conversation_ready")).find(event => event.type === "conversation_ready");
      assert.ok(attached); assert.deepEqual(attached.conversation, saved);
      const history = (await control({ type: "read_conversation", conversation_id: conversationID, cursor: null }))
        .filter(event => event.type === "history");
      assert.ok(history.length > 0); assert.ok(history.every(event => event.conversation_id === conversationID));
      assert.ok(history.some(event => event.messages.some(message => message.text === `Retained ${conversationID} request`)));
      if (conversationID === parentID) {
        const cursor = history.find(event => event.page_complete)?.next_cursor; assert.ok(cursor);
        const earlier = (await control({ type: "read_conversation", conversation_id: conversationID, cursor }))
          .filter(event => event.type === "history");
        assert.ok(earlier.every(event => event.conversation_id === conversationID));
        const messages = earlier.flatMap(event => event.messages); assert.ok(messages.length > 0);
        assert.ok(messages.every(message => message.text.startsWith("Earlier parent evidence ")));
      }
    }
    const retained = (await control({ type: "list_conversations", query: taskID, cursor: null, limit: 50 }))
      .find(event => event.type === "conversations"); assert.ok(retained); assert.deepEqual(retained.items, page.items);
    // macOS also exercises the actual Swift session, outbox and reducer against
    // this coordinator. Linux retains all production wire and Pi checks above.
    if (process.platform === "darwin") {
      const project = fileURLToPath(new URL("../../../ios", import.meta.url));
      const scratch = process.env.AGENT_NATIVE_SWIFT_SCRATCH ?? fileURLToPath(new URL("../../../target/native-work-integration", import.meta.url));
      // Apple's Swift toolchain must use its own SDK, without Nix's overrides.
      const swift = spawn("/usr/bin/xcrun", ["--sdk", "macosx", "swift", "test", "--package-path", project, "--scratch-path",
        scratch, "--filter", "nativeWorkInspectionReadsTheRealCoordinatorAndDetachesWithoutStoppingWork"], {
        signal: t.signal, env: { PATH: "/usr/bin:/bin:/usr/sbin:/sbin", HOME: directory, TMPDIR: directory,
          AGENT_WORK_E2E_SOCKET: socket, AGENT_WORK_E2E_DIRECTORY: directory, AGENT_WORK_E2E_JOB: jobID,
          AGENT_WORK_E2E_PARENT: parentID, AGENT_WORK_E2E_CHILD: childID }, stdio: ["ignore", "pipe", "pipe"],
      });
      let output = ""; swift.stdout.setEncoding("utf8").on("data", (value: string) => { output += value; });
      swift.stderr.setEncoding("utf8").on("data", (value: string) => { output += value; });
      const swiftExit = await new Promise<number | null>((resolve, reject) => { swift.once("close", resolve); swift.once("error", reject); });
      assert.equal(swiftExit, 0, output); assert.match(output, /1 test passed|1 tests? passed|with 1 test/);
    }
    const afterInspection = await generations();
    assert.deepEqual(afterInspection.map(record => ({ id: record.id, state: record.state, abortRequested: record.abortRequested })),
      beforeInspection.map(record => ({ id: record.id, state: record.state, abortRequested: record.abortRequested })),
      "Attaching and detaching the production inspectors must neither stop nor resume the same parked generations");
    child.kill("SIGINT"); assert.equal(await closed, 0, stderr);
    const reopened = await open();
    try {
      for (const id of generationIDs) {
        const record = await reopened.harness.getTask(id, BACKGROUND_CONTEXT); assert.ok(record);
        assert.equal(record.state.status, "sleeping", "Graceful coordinator shutdown must retain the same parked generation for restart");
        assert.equal(record.abortRequested, false);
      }
    } finally { await reopened.harness.close(BACKGROUND_CONTEXT); }
  } finally {
    if (child.exitCode === null && child.signalCode === null) { child.kill("SIGINT"); await closed; }
  }
});
