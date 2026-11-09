import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createInterface } from "node:readline";
import { test } from "node:test";
import { setImmediate } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { fauxAssistantMessage, fauxToolCall, type FauxResponseFactory } from "@earendil-works/pi-ai";
import { getCurrentSystemPrompt } from "@earendil-works/pi-ai/utils/transcript";
import { LiveDoc } from "@earendil-works/pi-durable";
import { runScheduled } from "../src/scheduled.js";
import type { RuntimeEventKind } from "../src/generated/RuntimeEventKind.js";
import { pump } from "./fixtures/background-runtime.js";
import { binding, node, openScheduled, scheduledOccurrence } from "./fixtures/scheduled-runtime.js";
import { fixtureModel } from "./model.js";

test("background children execute, report and continue through their original saved conversations after reopen", { timeout: 10_000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-background-children-")); let app = await openScheduled(directory);
  try {
    const respond: FauxResponseFactory = (input) => {
      const done = (name: string) => input.messages.some((message) => message.role === "toolResult" && message.toolName === name && !message.isError);
      if (getCurrentSystemPrompt(input.messages).includes("CHILD_HOST_WORK")) {
        if (done("report_result")) return fauxAssistantMessage("");
        if (done("exec")) return fauxAssistantMessage(fauxToolCall("report_result", { summary: "The child verified its owned host result." }), { stopReason: "toolUse" });
        return fauxAssistantMessage(fauxToolCall("exec", { program: "example", args: [], timeout_seconds: 1 }), { stopReason: "toolUse" });
      }
      if (done("record_task")) return fauxAssistantMessage("The parent verified the child result.");
      if (done("wait_agents")) return fauxAssistantMessage(fauxToolCall("record_task", { summary: "The child result is verified.",
        state: { state: "resolved", evidence: "The child finished its host tool and reported its result." } }), { stopReason: "toolUse" });
      if (done("spawn_agent")) return fauxAssistantMessage(fauxToolCall("wait_agents", { timeout_seconds: 2 }), { stopReason: "toolUse" });
      return fauxAssistantMessage(fauxToolCall("spawn_agent", { title: "Inspect the host result", outcome: "CHILD_HOST_WORK",
        input: "Run the bounded host check and report the result." }), { stopReason: "toolUse" });
    };
    app.faux.setResponses(Array.from({ length: 12 }, () => respond));
    await runScheduled(scheduledOccurrence, new AbortController().signal, app);
    await pump(async () => { const tasks = await app.tasks.list(); return tasks.length === 1 && !app.tasks.isRunning(tasks[0]?.id ?? ""); });
    const parent = (await app.tasks.list())[0]; assert.ok(parent);
    assert.equal(parent.state.state, "resolved"); assert.equal(parent.summary, "The parent verified the child result.");
    const child = parent.agents[0]; assert.ok(child); assert.equal(child.state.state, "done");
    assert.equal(child.summary, "The child verified its owned host result."); assert.equal(child.worker.state, "unassigned");
    assert.equal(app.commands.filter((command) => command.type === "exec").length, 1);
    const conversation = await app.tasks.conversation(child.conversation_id);
    const original = (await conversation.entries({ order: "ascending" }, 100, undefined, BACKGROUND_CONTEXT)).items;
    const history = await app.tasks.history(parent.id, 120_000, child.id);
    assert.match(history, /owned host tool output/); assert.match(history, /The child verified its owned host result/);
    await app.close(); app = await openScheduled(directory, "restored");
    app.faux.setResponses([
      fauxAssistantMessage(fauxToolCall("report_result", { summary: "The same child checked the follow-up." }), { stopReason: "toolUse" }),
      fauxAssistantMessage(""), fauxAssistantMessage("The parent retained the child follow-up."),
    ]);
    const frames: RuntimeEventKind[] = [];
    await app.views.input({ type: "conversation_input", conversation_id: child.conversation_id, input_id: "child-followup",
      input: "Continue this exact child assignment.", mode: "follow_up" }, app.call, (event) => frames.push(event));
    await pump(async () => (await app.tasks.get(parent.id)).agents[0]?.summary === "The same child checked the follow-up." && !app.tasks.isRunning(parent.id));
    const current = await app.tasks.get(parent.id);
    assert.equal(current.conversation_id, parent.conversation_id); assert.equal(current.agents[0]?.conversation_id, child.conversation_id);
    assert.ok(frames.some((event) => event.type === "input_accepted" && event.input_id === "child-followup"));
    const retained = await app.tasks.conversation(child.conversation_id);
    assert.deepEqual((await retained.entries({ order: "ascending" }, 100, undefined, BACKGROUND_CONTEXT)).items.slice(0, original.length), original);
    assert.equal((await app.session.commit((tx) => tx.submissionByRequest(retained.id, "child-followup"), BACKGROUND_CONTEXT))?.status, "done");
    assert.equal(app.commands.some((command) => command.type === "spawn_worker" || command.type === "spawn_subagent" || command.type === "resume_agent_worker"), false);
  } finally { await app.close(); await rm(directory, { recursive: true, force: true }); }
});

test("an offline registered host retains the scheduled input and resumes it once without a fallback execution", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-background-offline-")), app = await openScheduled(directory);
  try {
    app.environment.setConnected([]);
    await assert.rejects(runScheduled(scheduledOccurrence, new AbortController().signal, app), /host is offline/);
    const original = (await app.tasks.list())[0]; assert.ok(original);
    assert.equal(original.pending[0]?.id, scheduledOccurrence.id); assert.equal(app.faux.state.callCount, 0);
    assert.deepEqual(app.commands, []);
    app.environment.setConnected([node.host_id]);
    app.faux.setResponses([fauxAssistantMessage("The restored owning host completed the original input.")]);
    await runScheduled(scheduledOccurrence, new AbortController().signal, app);
    assert.equal((await app.tasks.list()).length, 1);
    const restored = await app.tasks.get(original.id); assert.equal(restored.conversation_id, original.conversation_id);
    assert.deepEqual(restored.pending, []); assert.equal(app.faux.state.callCount, 1);
  } finally { await app.close(); await rm(directory, { recursive: true, force: true }); }
});

test("an existing interactive job actor drains a scheduled occurrence without waiting for its terminal to close", { timeout: 10_000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-background-existing-owner-")), app = await openScheduled(directory);
  const controller = new AbortController(); let running: Promise<unknown> | undefined, release = () => {};
  try {
    const task = await app.tasks.create({ title: "Existing job actor", outcome: "Keep the original job conversation", model: fixtureModel,
      sources: [{ provider: "scheduled_jobs", account: "local", thread_id: scheduledOccurrence.job.id }] });
    const host = await app.tasks.workerHost(task.id);
    const allocation = "existing-allocation";
    await app.tasks.update(task.id, (saved) => { saved.worker = { state: "bound", allocation_id: allocation, host, binding }; });
    await app.tasks.enqueue(task.id, "Existing native work", "existing-input");
    let entered = () => {}; const started = new Promise<void>((resolve) => { entered = resolve; });
    const held = new Promise<void>((resolve) => { release = resolve; });
    app.faux.setResponses([async () => { entered(); await held; return fauxAssistantMessage("Original work completed."); },
      fauxAssistantMessage("The next scheduled occurrence completed.")]);
    let closed = false;
    running = app.tasks.run({ type: "run_task", task_id: task.id, allocation_id: allocation, binding, instructions: "", model: fixtureModel,
      cwd: host.cwd, interactive: true }, (conversation, executionHost, model) => app.executions.bind(conversation.id.toString(), {
        cwd: executionHost.cwd, hosts: [executionHost], model, role: "worker", call: app.call,
      }), [], app.call, undefined, { signal: controller.signal, async ready() {}, answer() {}, idle() {} }).finally(() => { closed = true; });
    await started;
    const scheduled = runScheduled(scheduledOccurrence, controller.signal, app, task.id);
    await pump(async () => (await app.tasks.get(task.id)).pending.some((input) => input.id === scheduledOccurrence.id));
    release(); await scheduled;
    assert.equal(closed, false, "The old interactive transport remains attached after its scheduled answer");
    const conversation = await app.tasks.conversation(task.conversation_id);
    assert.equal((await app.session.commit((tx) => tx.submissionByRequest(conversation.id, scheduledOccurrence.id), BACKGROUND_CONTEXT))?.status, "done");
    assert.equal(app.commands.some((command) => command.type === "spawn_worker" || command.type === "resume_worker"), false);
  } finally { release(); controller.abort(); await running?.catch(() => {}); await app.close(); await rm(directory, { recursive: true, force: true }); }
});

test("a completed occurrence settles while a later independent follow-up remains active", { timeout: 10_000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-background-exact-occurrence-")), app = await openScheduled(directory);
  let releaseFirst = () => {}, releaseSecond = () => {}, scheduled: Promise<void> | undefined;
  try {
    let enterFirst = () => {}, enterSecond = () => {};
    const firstStarted = new Promise<void>((resolve) => { enterFirst = resolve; });
    const secondStarted = new Promise<void>((resolve) => { enterSecond = resolve; });
    const firstGate = new Promise<void>((resolve) => { releaseFirst = resolve; });
    const secondGate = new Promise<void>((resolve) => { releaseSecond = resolve; });
    app.faux.setResponses([
      async () => { enterFirst(); await firstGate; return fauxAssistantMessage("Occurrence completed."); },
      async () => { enterSecond(); await secondGate; return fauxAssistantMessage("Later independent request completed."); },
    ]);
    let settled = false;
    scheduled = runScheduled(scheduledOccurrence, new AbortController().signal, app).then(() => { settled = true; });
    await firstStarted;
    const task = (await app.tasks.list())[0]; assert.ok(task);
    await app.views.input({ type: "conversation_input", conversation_id: task.conversation_id, input_id: "later-independent",
      input: "Continue with a separate request.", mode: "follow_up" }, app.call, () => {});
    releaseFirst(); await secondStarted;
    const conversation = await app.tasks.conversation(task.conversation_id);
    const occurrence = await app.session.commit((tx) => tx.submissionByRequest(conversation.id, scheduledOccurrence.id), BACKGROUND_CONTEXT);
    assert.equal(occurrence?.status, "done");
    await pump(() => settled);
    assert.equal(app.tasks.isRunning(task.id), true, "The later input keeps its own execution after the occurrence settles");
    assert.equal((await app.session.commit((tx) => tx.submissionByRequest(conversation.id, "later-independent"), BACKGROUND_CONTEXT))?.status, "placed");
  } finally { releaseFirst(); releaseSecond(); await scheduled?.catch(() => {}); await app.close(); await rm(directory, { recursive: true, force: true }); }
});

test("stop during asynchronous preflight fences the admitted input before inference", { timeout: 10_000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-background-stop-preflight-")); let app = await openScheduled(directory);
  let release = () => {}, running: Promise<void> | undefined;
  try {
    let entered = () => {}; const preparing = new Promise<void>((resolve) => { entered = resolve; });
    const held = new Promise<void>((resolve) => { release = resolve; });
    app.tasks.useBackgroundExecution({ available: () => true, extensions: () => [], prepare: async () => { entered(); await held; },
      call: (command) => app.call(command), bind: (conversation, host, model) => app.executions.bind(conversation.id.toString(), {
        cwd: host.cwd, hosts: [host], model, role: "worker", call: app.call,
      }) });
    const task = await app.tasks.create({ title: "Preparing job", outcome: "Stop before inference", model: scheduledOccurrence.job.model,
      sources: [{ provider: "scheduled_jobs", account: "local", thread_id: scheduledOccurrence.job.id }] });
    await app.tasks.enqueue(task.id, "Stop this queued turn", "preparing-input");
    app.faux.setResponses([fauxAssistantMessage("Must never execute after the interrupted receipt.")]);
    running = app.tasks.runBackground(task.id);
    const stopped = assert.rejects(running, /Background execution stopped/);
    await preparing;
    const interrupt = (async () => {
      const matched = await app.tasks.interruptBackground(task.conversation_id);
      if (!matched) await (await app.tasks.conversation(task.conversation_id)).abort(BACKGROUND_CONTEXT);
      return matched;
    })();
    await setImmediate(); release();
    assert.equal(await interrupt, true, "The controller is owned before preflight completes");
    await stopped;
    assert.equal(app.faux.state.callCount, 0); assert.equal((await app.tasks.get(task.id)).summary, "");
    assert.deepEqual(app.commands, []);
    const conversation = await app.tasks.conversation(task.conversation_id);
    const receipt = await app.session.commit((tx) => tx.submissionByRequest(conversation.id, "preparing-input"), BACKGROUND_CONTEXT);
    assert.ok(receipt?.type === "input" && receipt.status === "unanswered");
    assert.equal(receipt.reason, "aborted"); assert.equal(receipt.entry, undefined);
    await app.close(); app = await openScheduled(directory, "restored");
    assert.deepEqual((await app.tasks.get(task.id)).pending, []);
    const frames: RuntimeEventKind[] = [];
    await app.views.input({ type: "conversation_input", conversation_id: task.conversation_id, input_id: "preparing-input",
      input: "Stop this queued turn", mode: "steer" }, app.call, (event) => frames.push(event));
    assert.equal(frames[0]?.type, "input_rejected");
    await app.tasks.resumeBackground(); await setImmediate();
    assert.equal(app.faux.state.callCount, 0, "Owner recovery cannot revive a stopped preflight input");
  } finally { release(); await running?.catch(() => {}); await app.close(); await rm(directory, { recursive: true, force: true }); }
});

for (const shutdown of ["SIGKILL", "graceful"]) for (const phase of ["child", "continuation"]) {
  test(`background owner startup rebinds a placed ${phase} after ${shutdown} without replaying its host tool`, { timeout: 15_000 }, async () => {
    const directory = await mkdtemp(join(tmpdir(), "agent-background-resume-"));
    const childProcess = spawn(process.execPath, ["--no-warnings", fileURLToPath(new URL("./fixtures/scheduled-admission-child.js", import.meta.url)), directory, phase, shutdown],
      { stdio: ["pipe", "pipe", "pipe"] });
    let app: Awaited<ReturnType<typeof openScheduled>> | undefined;
    try {
      let stderr = ""; childProcess.stderr.on("data", (data: Buffer) => { stderr += data.toString(); });
      const lines = createInterface({ input: childProcess.stdout });
      assert.equal((await lines[Symbol.asyncIterator]().next()).value, "PLACED", stderr);
      const exited = new Promise<number | null>((resolve) => childProcess.once("close", resolve));
      if (shutdown === "SIGKILL") childProcess.kill("SIGKILL"); else childProcess.stdin.end();
      const exit = await exited; if (shutdown === "graceful") assert.equal(exit, 0, stderr);
      lines.close();
      app = await openScheduled(directory, "restored");
      const task = (await app.tasks.list())[0]; assert.ok(task);
      const actor = phase === "child" ? task.agents[0] : task; assert.ok(actor);
      const conversation = await app.tasks.conversation(actor.conversation_id);
      const inputId = phase === "child" ? "child-input" : "continuation-input";
      const original = await app.session.commit((tx) => tx.submissionByRequest(conversation.id, inputId), BACKGROUND_CONTEXT);
      assert.equal(original?.status, "placed");
      const history = await app.tasks.history(task.id, 120_000, phase === "child" ? actor.id : undefined);
      if (shutdown === "graceful") {
        const live = await app.session.snapshot(LiveDoc, conversation.id, BACKGROUND_CONTEXT); assert.ok(live?.run);
        const generation = await app.session.getTask(live.run.taskId, BACKGROUND_CONTEXT); assert.ok(generation);
        let bindings = 0;
        app.tasks.useBackgroundExecution({ available: () => true, extensions: () => [],
          prepare: async () => { throw new Error("Fixture credentials are unavailable"); }, call: app.call,
          bind: () => { bindings += 1; return () => {}; } });
        await assert.rejects(app.tasks.runBackground(task.id, undefined, phase === "child" ? actor.id : undefined), /credentials are unavailable/);
        const retained = await app.session.getTask(generation.id, BACKGROUND_CONTEXT); assert.ok(retained);
        assert.deepEqual(retained.state, generation.state); assert.equal(retained.abortRequested, false);
        assert.deepEqual(await app.session.commit((tx) => tx.submissionByRequest(conversation.id, inputId), BACKGROUND_CONTEXT), original);
        assert.equal(bindings, 0); assert.equal(app.faux.state.callCount, 0);
        app.configureBackground();
      }
      app.faux.setResponses([(context) => {
        assert.ok(context.messages.some((message) => message.role === "toolResult" && message.content.some((part) =>
          part.type === "text" && part.text.includes("owned host tool output"))));
        return fauxAssistantMessage("Recovered the original placed work.");
      }, fauxAssistantMessage("The parent retained its recovered child result.")]);
      await app.tasks.resumeBackground();
      const restored = app;
      await pump(async () => !restored.tasks.isRunning(task.id) &&
        (await restored.session.commit((tx) => tx.submissionByRequest(conversation.id, inputId), BACKGROUND_CONTEXT))?.status === "done");
      const current = await app.tasks.get(task.id);
      assert.equal(current.conversation_id, task.conversation_id);
      if (phase === "child") { assert.equal(current.agents[0]?.state.state, "done"); assert.equal(current.agents[0]?.conversation_id, actor.conversation_id); }
      const recovered = await app.session.commit((tx) => tx.submissionByRequest(conversation.id, inputId), BACKGROUND_CONTEXT);
      assert.equal(recovered?.id, original?.id);
      const full = await app.tasks.history(task.id, 120_000, phase === "child" ? actor.id : undefined);
      assert.ok(full.startsWith(history)); assert.match(full, /Recovered the original placed work/);
      assert.equal(app.commands.filter((command) => command.type === "exec").length, 0, "The prior host result is consumed, not executed again");
    } finally { childProcess.kill("SIGKILL"); await app?.close(); await rm(directory, { recursive: true, force: true }); }
  });
}
