import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { appendFile, mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createInterface } from "node:readline";
import { test } from "node:test";
import { fileURLToPath } from "node:url";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { fauxAssistantMessage, fauxToolCall } from "@earendil-works/pi-ai";
import { getCurrentSystemPrompt } from "@earendil-works/pi-ai/utils/transcript";
import { BackgroundScheduler } from "../src/background.js";
import { openJobActor, runScheduled } from "../src/scheduled.js";
import type { ScheduledOccurrence } from "../src/generated/ScheduledOccurrence.js";
import { createAppTools } from "../src/tools.js";
import { DAY, FakeClock, exampleJob, planned, pump } from "./fixtures/background-runtime.js";
import { binding, openScheduled, scheduledOccurrence } from "./fixtures/scheduled-runtime.js";

test("scheduled work uses the existing task actor, current guidance and exact native input", { timeout: 10_000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-scheduled-task-")), app = await openScheduled(directory);
  try {
    app.faux.setResponses([(context) => {
      assert.match(getCurrentSystemPrompt(context.messages), /FRESH_GLOBAL_POLICY/);
      assert.match(getCurrentSystemPrompt(context.messages), /FRESH_PROJECT_POLICY/);
      return fauxAssistantMessage(fauxToolCall("exec", { program: "example", args: [], timeout_seconds: 1 }), { stopReason: "toolUse" });
    }, fauxAssistantMessage("The scheduled review is complete.")]);
    await runScheduled(scheduledOccurrence, new AbortController().signal, app);
    const task = (await app.tasks.list())[0]; assert.ok(task);
    const conversation = await app.tasks.conversation(task.conversation_id);
    assert.deepEqual(task.pending, []); assert.deepEqual(task.model, scheduledOccurrence.job.model);
    assert.equal(task.worker.state, "unassigned");
    const input = await app.session.commit((tx) => tx.submissionByRequest(conversation.id, scheduledOccurrence.id), BACKGROUND_CONTEXT);
    assert.ok(input); assert.equal(input.status, "done");
    const page = await conversation.entries({}, 100, undefined, BACKGROUND_CONTEXT);
    assert.equal(page.items.filter((entry) => entry.model?.some((message) => message.role === "user")).length, 1);
    assert.equal(app.commands.filter((command) => command.type === "exec").length, 1);
    await runScheduled(scheduledOccurrence, new AbortController().signal, app);
    assert.equal((await app.tasks.list()).length, 1); assert.equal(app.faux.state.callCount, 2);
    assert.equal(app.commands.filter((command) => command.type === "spawn_worker" || command.type === "resume_worker").length, 0);
  } finally { await app.close(); await rm(directory, { recursive: true, force: true }); }
});

for (const scenario of ["absent", "unknown", "wrong", "failed", "foreign"]) {
  test(`recovery with ${scenario} receipt cannot settle an unanswered scheduled input`, { timeout: 10_000 }, async () => {
    const directory = await mkdtemp(join(tmpdir(), "agent-scheduled-receipt-")), app = await openScheduled(directory);
    const controller = new AbortController();
    try {
      app.faux.setResponses([() => { throw new Error("The original generation failed before any tool"); }]);
      await assert.rejects(runScheduled(scheduledOccurrence, controller.signal, app), /scheduled turn/);
      const task = (await app.tasks.list())[0]; assert.ok(task);
      if (scenario === "wrong") {
        await app.tasks.enqueue(task.id, "An independent request.", "other-unanswered", undefined, "follow_up");
        app.faux.setResponses([() => { throw new Error("The independent request also failed before tools"); }]);
        await assert.rejects(app.tasks.runBackground(task.id), /did not complete/);
      }
      app.faux.setResponses([
        fauxAssistantMessage(fauxToolCall("record_task", {
          summary: "Reviewed the earlier failure.",
          ...(scenario === "foreign" ? { task_id: task.id } : {}),
          state: { state: "waiting", actor: "Upstream check", next_check: null },
          ...(scenario === "absent" ? {} : { reconciles_input: scenario === "wrong" ? "other-unanswered"
            : scenario === "unknown" ? "unknown-request" : scheduledOccurrence.id }),
        }), { stopReason: "toolUse" }),
        (context) => {
          if (scenario === "unknown" || scenario === "foreign") assert.ok(context.messages.some((message) => message.role === "toolResult" && message.isError));
          if (scenario === "failed") throw new Error("The recovery failed after claiming reconciliation");
          return fauxAssistantMessage("The later request finished.");
        },
      ]);
      if (scenario === "foreign") {
        const root = await app.session.root(BACKGROUND_CONTEXT);
        const unbind = app.executions.bind(root.id.toString(), { cwd: "/workspace/example", hosts: [], call: app.call });
        try {
          await root.configure({ model: { provider: "openai", modelId: "fixture" }, extensions: [createAppTools(app.tasks, app.executions).main] }, BACKGROUND_CONTEXT);
          const submission = await root.submit({ type: "input", content: "Review another conversation's failure.", requestId: "foreign-recovery" }, BACKGROUND_CONTEXT);
          assert.equal((await submission.wait(BACKGROUND_CONTEXT)).status, "done");
        } finally { unbind(); }
      } else {
        await app.tasks.enqueue(task.id, "Review the previous failure.", "recovery", undefined, "follow_up");
        if (scenario === "failed") await assert.rejects(app.tasks.runBackground(task.id), /did not complete/);
        else await app.tasks.runBackground(task.id);
      }
      await assert.rejects(runScheduled(scheduledOccurrence, controller.signal, app, task.id), /scheduled turn/);
      const claimed = (await app.sources.events({ provider: "task_outcomes", thread_id: task.id, limit: 100 }))
        .find((event) => event.outcome?.reconciliation !== undefined)?.outcome;
      assert.equal(claimed !== undefined, scenario === "failed" || scenario === "wrong");
      if (claimed?.tool_call_id !== null && claimed?.tool_call_id !== undefined) {
        await assert.rejects(app.tasks.record(task.id, { summary: "Stale invocation", state: { state: "open" },
          reconciles_input: scheduledOccurrence.id }, { conversation_id: task.conversation_id,
          call_id: claimed.tool_call_id, settled: false }), /current native recovery turn/);
      }
    } finally {
      controller.abort(); await app.close(); await rm(directory, { recursive: true, force: true });
    }
  });
}

for (const recoveredState of ["waiting", "resolved"]) {
  test(`explicit ${recoveredState} recovery settles the same occurrence after reopen without replaying its effect`, { timeout: 15_000 }, async () => {
    const directory = await mkdtemp(join(tmpdir(), "agent-scheduled-reconcile-")), clock = new FakeClock();
    let app = await openScheduled(directory, "original", clock), release = () => {}, allowVerification = () => {};
    const controller = new AbortController(), effectPath = join(directory, "effect.txt");
    let first: ScheduledOccurrence | undefined;
    const background = () => {
      const current = app;
      return new BackgroundScheduler(current.session, current.registry, current.tasks, {
        plan: async (cadence, after, now) => planned(cadence, after, now),
        refreshSources: async () => {}, triageSources: async () => {}, collectResources: async () => {}, indexHistory: async () => {},
        runJob: async (occurrence, signal, actor) => { first ??= occurrence; await runScheduled(occurrence, signal, current, actor); },
      });
    };
    app.configureBackground(async (request) => {
      const command = request.type === "on_node" ? request.command : request;
      if (command.type === "exec" && command.program === "perform-effect") {
        await appendFile(effectPath, "performed once\n");
        return { type: "process", exit_code: 0, stdout: "Effect recorded", stderr: "", truncated: false };
      }
      if (command.type === "exec" && command.program === "verify-effect") {
        return { type: "process", exit_code: 0, stdout: await readFile(effectPath, "utf8"), stderr: "", truncated: false };
      }
      return app.call(request);
    });
    let scheduler = background();
    try {
      let started = () => {}; const streaming = new Promise<void>((resolve) => { started = resolve; });
      app.faux.setResponses([
        fauxAssistantMessage(fauxToolCall("exec", { program: "perform-effect", args: [] }), { stopReason: "toolUse" }),
        async (_context, options) => {
          const signal = options?.signal; assert.ok(signal); started();
          await new Promise<void>((resolve) => { if (signal.aborted) resolve(); else signal.addEventListener("abort", () => resolve(), { once: true }); });
          return fauxAssistantMessage("Discarded after interruption");
        },
      ]);
      await scheduler.start({ poll_interval_seconds: 60, sources: false, jobs: [exampleJob()] });
      await pump(async () => (await scheduler.getJob("example-review")).schedules[0]?.next_at !== null);
      clock.advance(DAY); await streaming;
      const task = (await app.tasks.list())[0]; assert.ok(task); assert.ok(first); const occurrence = first;
      const conversation = await app.tasks.conversation(task.conversation_id);
      assert.equal(await readFile(effectPath, "utf8"), "performed once\n");
      assert.equal(await app.tasks.interruptBackground(task.conversation_id), true);
      const original = await app.session.commit((tx) => tx.submissionByRequest(conversation.id, occurrence.id), BACKGROUND_CONTEXT);
      assert.ok(original); assert.equal(original.status, "unanswered");
      await pump(async () => (await scheduler.status()).errors.some((error) => /scheduled turn/.test(error.message)));
      clock.advance(DAY); await pump(async () => (await scheduler.status()).pending === 2);

      let verified = () => {}; const verifying = new Promise<void>((resolve) => { verified = resolve; });
      const verificationGate = new Promise<void>((resolve) => { allowVerification = resolve; });
      let recorded = () => {}; const resolving = new Promise<void>((resolve) => { recorded = resolve; });
      const finalGate = new Promise<void>((resolve) => { release = resolve; });
      app.faux.setResponses([
        async () => { verified(); await verificationGate;
          return fauxAssistantMessage(fauxToolCall("exec", { program: "verify-effect", args: [] }), { stopReason: "toolUse" }); },
        (context) => {
          assert.ok(context.messages.some((message) => message.role === "toolResult" &&
            message.content.some((part) => part.type === "text" && part.text.includes("performed once"))));
          return fauxAssistantMessage(fauxToolCall("record_task", { summary: "Verified the original effect occurred once.",
            ...(recoveredState === "waiting" ? { reconciles_input: occurrence.id } : {}),
            state: recoveredState === "waiting" ? { state: "waiting", actor: "Upstream check", next_check: null }
              : { state: "resolved", evidence: "Read back the original effect receipt; no replay is needed." } }), { stopReason: "toolUse" });
        },
        async () => { recorded(); await finalGate; return fauxAssistantMessage("The original effect is verified."); },
      ]);
      await app.views.input({ type: "conversation_input", conversation_id: task.conversation_id, input_id: "reconcile-effect",
        input: "Verify the original effect before resolving this same task.", mode: "follow_up" }, app.call, () => {});
      await verifying;
      await app.views.input({ type: "conversation_input", conversation_id: task.conversation_id, input_id: "recovery-steering",
        input: "Keep the existing upstream wait after verifying the effect.", mode: "steer" }, app.call, () => {});
      allowVerification(); await resolving;
      assert.equal((await app.tasks.get(task.id)).state.state, recoveredState);
      await assert.rejects(runScheduled(occurrence, controller.signal, app, task.id), /scheduled turn/);
      release(); await pump(async () => !app.tasks.isRunning(task.id));
      const receipt = (await app.sources.events({ provider: "task_outcomes", thread_id: task.id, limit: 100 }))
        .find((event) => event.outcome?.reconciliation !== undefined)?.outcome?.reconciliation;
      assert.equal(await app.session.commit((tx) => app.tasks.canReleaseIn(tx, task.id), BACKGROUND_CONTEXT), recoveredState === "resolved");
      await app.close(); app = await openScheduled(directory, "restored", clock); scheduler = background();
      await scheduler.start({ poll_interval_seconds: 60, sources: false, jobs: [exampleJob()] });
      await runScheduled(occurrence, controller.signal, app, task.id);
      assert.equal((await app.tasks.get(task.id)).state.state, recoveredState);
      if (recoveredState === "waiting") {
        assert.equal(receipt?.original.submission_id, original.id.toString());
        assert.deepEqual(receipt?.recovery_inputs.map((input) => input.request_id), ["reconcile-effect", "recovery-steering"]);
      } else assert.equal(receipt, undefined);
      app.faux.setResponses([fauxAssistantMessage("The next queued review completed.")]);
      clock.advance(3_600_000); await pump(async () => (await scheduler.status()).pending === 0);
      assert.deepEqual((await scheduler.status()).errors, []);
      assert.equal(await readFile(effectPath, "utf8"), "performed once\n"); assert.equal((await app.tasks.list()).length, 1);
      const restored = await app.tasks.conversation((await app.tasks.get(task.id)).conversation_id);
      const retained = await app.session.commit((tx) => tx.submissionByRequest(restored.id, occurrence.id), BACKGROUND_CONTEXT);
      assert.equal(retained?.id, original.id); assert.equal(retained?.status, "unanswered");
      assert.equal(app.commands.filter((command) => command.type === "exec" && command.program === "perform-effect").length, 0);
    } finally { release(); allowVerification(); controller.abort(); await app.close(); await rm(directory, { recursive: true, force: true }); }
  });
}

test("silent scheduled completion does not produce a main-chat response or receipt", { timeout: 10_000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-scheduled-quiet-")), app = await openScheduled(directory);
  try {
    app.faux.setResponses([fauxAssistantMessage("")]);
    await runScheduled(scheduledOccurrence, new AbortController().signal, app);
    const root = await app.session.root(BACKGROUND_CONTEXT);
    assert.deepEqual((await root.entries({}, 100, undefined, BACKGROUND_CONTEXT)).items, []);
    assert.equal(app.faux.state.callCount, 1);
  } finally { await app.close(); await rm(directory, { recursive: true, force: true }); }
});

test("SIGKILL after scheduled input placement restores its actor without duplicating task, input or host work", { timeout: 15_000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-scheduled-kill-"));
  const child = spawn(process.execPath, ["--no-warnings", fileURLToPath(new URL("./fixtures/scheduled-admission-child.js", import.meta.url)), directory],
    { stdio: ["pipe", "pipe", "pipe"] });
  let app: Awaited<ReturnType<typeof openScheduled>> | undefined, scheduled: Promise<void> | undefined;
  const controller = new AbortController();
  try {
    let stderr = ""; child.stderr.on("data", (data: Buffer) => { stderr += data.toString(); });
    const lines = createInterface({ input: child.stdout });
    const signal = await lines[Symbol.asyncIterator]().next();
    assert.equal(signal.value, "PLACED", stderr);
    const closed = new Promise<void>((resolve) => child.once("close", () => resolve())); child.kill("SIGKILL"); await closed; lines.close();
    app = await openScheduled(directory, "restored");
    const tasks = await app.tasks.list(); assert.equal(tasks.length, 1);
    const original = tasks[0]; assert.ok(original); assert.deepEqual(original.pending, []);
    const originalConversation = await app.tasks.conversation(original.conversation_id);
    const prior = await app.session.commit((tx) => tx.submissionByRequest(originalConversation.id, scheduledOccurrence.id), BACKGROUND_CONTEXT);
    assert.ok(prior);
    app.faux.setResponses([async (context) => {
      assert.match(getCurrentSystemPrompt(context.messages), /FRESH_PROJECT_POLICY_restored/, "A restored native input must use its replacement client's current project guidance");
      return fauxAssistantMessage(fauxToolCall("exec", { program: "example", args: [], cwd: scheduledOccurrence.job.cwd, timeout_seconds: 1 }), { stopReason: "toolUse" });
    }, fauxAssistantMessage("Completed the original scheduled request after recovery.")]);
    scheduled = runScheduled(scheduledOccurrence, controller.signal, app);
    await scheduled;
    const current = await app.tasks.get(original.id), conversation = await app.tasks.conversation(current.conversation_id);
    assert.equal((await app.tasks.list()).length, 1); assert.equal(current.conversation_id, original.conversation_id);
    assert.deepEqual(current.pending, []);
    const recovered = await app.session.commit((tx) => tx.submissionByRequest(conversation.id, scheduledOccurrence.id), BACKGROUND_CONTEXT);
    assert.equal(recovered?.id, prior.id);
    const page = await conversation.entries({}, 100, undefined, BACKGROUND_CONTEXT);
    assert.equal(page.items.filter((entry) => entry.model?.some((message) => message.role === "user")).length, 1);
    assert.equal(app.commands.filter((command) => command.type === "spawn_worker").length, 0);
    assert.equal(app.commands.filter((command) => command.type === "resume_worker").length, 0);
    assert.equal(app.commands.filter((command) => command.type === "exec").length, 1,
      JSON.stringify(page.items.flatMap((entry) => entry.model?.filter((message) => message.role === "toolResult") ?? [])));
  } finally {
    child.kill("SIGKILL"); controller.abort(); await Promise.allSettled([scheduled]);
    await app?.close(); await rm(directory, { recursive: true, force: true });
  }
});

test("job migration preserves only a job-owned conversation independently of its old terminal", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-scheduled-adopt-")), app = await openScheduled(directory);
  try {
    const job = exampleJob();
    const host = await app.environment.defaultHost(await app.environment.hosts(job.cwd, null, [])); assert.ok(host);
    const unrelated = await app.tasks.create({ title: job.title, outcome: "An ordinary interactive task" });
    await app.tasks.dispatch(unrelated.id, host, app.call);
    assert.equal(await openJobActor(job, app.tasks), undefined);
    const owned = await app.tasks.create({ title: job.title, outcome: job.prompt,
      sources: [{ provider: "scheduled_jobs", account: "local", thread_id: job.id }] });
    await app.tasks.update(owned.id, (saved) => { saved.worker = { state: "bound", allocation_id: "old-allocation", host, binding }; });
    assert.equal(await openJobActor(job, app.tasks), owned.id);
    assert.equal(app.commands.filter((command) => command.type === "spawn_worker").length, 1);
    app.faux.setResponses([fauxAssistantMessage("Continued the original job conversation.")]);
    await runScheduled(scheduledOccurrence, new AbortController().signal, app, owned.id);
    assert.equal((await app.tasks.get(owned.id)).conversation_id, owned.conversation_id);
    assert.equal(app.commands.filter((command) => command.type === "spawn_worker").length, 1);
    assert.equal(app.commands.filter((command) => command.type === "resume_worker" || command.type === "inspect_worker").length, 0);
  } finally { await app.close(); await rm(directory, { recursive: true, force: true }); }
});
