import { copyJson } from "@earendil-works/chord/json";
import { commitWrites } from "@earendil-works/pi-durable/testing";
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { fauxAssistantMessage, fauxToolCall, type FauxResponseFactory, type TranscriptContext } from "@earendil-works/pi-ai";
import { getCurrentSystemPrompt } from "@earendil-works/pi-ai/utils/transcript";
import { AgentDoc, GenerationTask, InboxDoc, LiveDoc, UserEntry, type GenerationCheckpoint, type Submission } from "@earendil-works/pi-durable";
import { occurrencePrompt, runScheduled } from "../src/scheduled.js";
import { NoticeLineage } from "../src/notice_origin.js";
import { UserNoticeEntry } from "../src/transcript.js";
import type { ScheduledOccurrence } from "../src/generated/ScheduledOccurrence.js";
import { pump } from "./fixtures/background-runtime.js";
import { binding, node, openScheduled, scheduledOccurrence } from "./fixtures/scheduled-runtime.js";

function gate() {
  let release = () => {};
  return { promise: new Promise<void>(resolve => { release = resolve; }), release: () => release() };
}

test("an unchanged scheduled occurrence reaches a busy job before its retained source follow-ups without replacing them", { timeout: 15_000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-scheduled-priority-"));
  let app = await openScheduled(directory);
  const first = gate(), following = gate(), controller = new AbortController();
  let actor: Promise<void> | undefined, scheduled: Promise<void> | undefined;
  try {
    const occurrence = scheduledOccurrence;
    const task = await app.tasks.create({ title: occurrence.job.title, outcome: occurrence.job.prompt, model: occurrence.job.model,
      sources: [{ provider: "scheduled_jobs", account: "local", thread_id: occurrence.job.id }] });
    const conversation = await app.tasks.conversation(task.conversation_id);
    await app.tasks.inference.configure(conversation, occurrence.job.model, { cwd: occurrence.job.cwd });
    const receipt = (id: string) => app.session.commit(tx => tx.submissionByRequest(conversation.id, id), BACKGROUND_CONTEXT);
    const sourceIds = Array.from({ length: 5 }, (_, index) => `source-${index}`);
    const observed: string[] = [];
    app.faux.setResponses([
      async () => { await first.promise; return fauxAssistantMessage(fauxToolCall("read_source_events", { limit: 1 }), { stopReason: "toolUse" }); },
      context => {
        const user = context.messages.findLast(message => message.role === "user");
        assert.match(JSON.stringify(user?.content), /Scheduled occurrence: example-occurrence/);
        return fauxAssistantMessage(fauxToolCall("notify_user", { text: "A scheduled review needs a concrete decision." }), { stopReason: "toolUse" });
      },
      fauxAssistantMessage("The scheduled review is complete."),
      ...sourceIds.map((id, index) => async (context: TranscriptContext) => {
        const user = context.messages.findLast(message => message.role === "user");
        assert.equal(user?.content, `Retained ${id}`); observed.push(id);
        if (index === 0) await following.promise;
        return fauxAssistantMessage(`Completed ${id}`);
      }),
    ]);
    await app.tasks.enqueue(task.id, "Current source work", "source-current", undefined, "follow_up");
    actor = app.tasks.runBackground(task.id);
    void actor.catch(() => {});
    await pump(() => app.faux.state.callCount === 1);
    for (const id of sourceIds) await app.tasks.enqueue(task.id, `Retained ${id}`, id, undefined, "follow_up");
    const retained = await app.session.snapshot(InboxDoc, conversation.id, BACKGROUND_CONTEXT); assert.ok(retained);
    assert.equal(retained.items.length, sourceIds.length);
    assert.ok(retained.items.every(item => item.mode === "followUp"));
    const configuration = await app.session.snapshot(AgentDoc, conversation.id, BACKGROUND_CONTEXT);
    assert.equal((await app.session.snapshot(NoticeLineage, conversation.id, BACKGROUND_CONTEXT))?.origin ?? null, null);
    scheduled = runScheduled(occurrence, controller.signal, app, task.id);
    void scheduled.catch(() => {});
    await pump(async () => await receipt(occurrence.id) !== undefined);
    const admitted = await receipt(occurrence.id); assert.ok(admitted); assert.equal(admitted.status, "queued");
    const inbox = await app.session.snapshot(InboxDoc, conversation.id, BACKGROUND_CONTEXT); assert.ok(inbox);
    assert.deepEqual(inbox.items.slice(0, -1), retained.items, "Already-native source receipts and their order must stay intact");
    const urgent = inbox.items.at(-1); assert.ok(urgent); assert.equal(urgent.mode, "steer");
    assert.equal(urgent.id, admitted.id);
    assert.deepEqual(urgent.entry?.data, { text: occurrencePrompt(occurrence), notice_origin: { job_id: occurrence.job.id, title: occurrence.job.title } });
    const outbox = (await app.tasks.get(task.id)).pending;
    assert.deepEqual(outbox.map(input => input.id), [...sourceIds, occurrence.id]);
    assert.equal(outbox.at(-1)?.mode, undefined, "Native priority must preserve the original durable app input identity");
    assert.deepEqual(await app.session.snapshot(AgentDoc, conversation.id, BACKGROUND_CONTEXT), configuration);

    first.release();
    await scheduled; scheduled = undefined;
    await pump(() => app.faux.state.callCount === 4);
    assert.deepEqual((await app.session.snapshot(InboxDoc, conversation.id, BACKGROUND_CONTEXT))?.items, retained.items.slice(1));
    const completed = await receipt(occurrence.id); assert.equal(completed?.id, admitted.id); assert.equal(completed?.status, "done");
    const root = await app.session.root(BACKGROUND_CONTEXT);
    const notices = (await root.entries({}, 100, undefined, BACKGROUND_CONTEXT)).items.filter(entry => UserNoticeEntry.is(entry));
    assert.equal(notices.length, 1);
    assert.deepEqual(notices[0]?.data.notice_origin, { job_id: occurrence.job.id, title: occurrence.job.title });
    assert.equal((await receipt(sourceIds.at(-1)!))?.status, "queued", "The cron receipt settles before the source backlog drains");
    await runScheduled(occurrence, controller.signal, app, task.id);
    assert.equal(app.faux.state.callCount, 4, "Exact occurrence replay cannot start another model turn");
    following.release(); await actor; actor = undefined;
    assert.deepEqual(observed, sourceIds);
    assert.deepEqual((await app.tasks.get(task.id)).pending, []);
    assert.equal((await app.tasks.list()).length, 1);
    assert.equal(app.commands.some(command => command.type === "spawn_worker" || command.type === "resume_worker"), false);
    await app.close(); app = await openScheduled(directory);
    await runScheduled(occurrence, controller.signal, app, task.id);
    assert.equal(app.faux.state.callCount, 0, "Reopening must retain the exact completed occurrence receipt");
    assert.equal((await receipt(occurrence.id))?.id, admitted.id);
  } finally {
    controller.abort(); first.release(); following.release();
    await scheduled?.catch(() => {}); await app.close(); await actor?.catch(() => {});
    await rm(directory, { recursive: true, force: true });
  }
});

for (const changed of ["prompt", "cwd", "model", "native-model", "concurrent-model", "ownership"]) {
  test(`a scheduled ${changed} change retains the idle configuration boundary behind native source inputs`, { timeout: 10_000 }, async () => {
    const directory = await mkdtemp(join(tmpdir(), "agent-scheduled-configuration-")), app = await openScheduled(directory), first = gate();
    let actor: Promise<void> | undefined;
    try {
      const original = scheduledOccurrence, job = { ...original.job,
        ...(changed === "prompt" ? { prompt: "An updated captured workflow policy." } : {}),
        ...(changed === "cwd" ? { cwd: "/workspace/updated-example" } : {}),
        ...(changed === "ownership" ? { id: "another-example-workflow" } : {}),
        ...(changed === "model" ? { model: { ...original.job.model, model_id: "fixture-updated" } } : {}) };
      const occurrence: ScheduledOccurrence = { ...original, id: `changed-${changed}`, job };
      const task = await app.tasks.create({ title: original.job.title, outcome: original.job.prompt, model: original.job.model,
        sources: [{ provider: "scheduled_jobs", account: "local", thread_id: original.job.id }] });
      const conversation = await app.tasks.conversation(task.conversation_id);
      await app.tasks.inference.configure(conversation, original.job.model, { cwd: original.job.cwd });
      app.faux.setResponses([
        async () => { await first.promise; return fauxAssistantMessage("Current source complete"); },
        fauxAssistantMessage("Retained source complete"),
        (context, _options, _state, model) => {
          assert.match(JSON.stringify(context.messages.findLast(message => message.role === "user")?.content), new RegExp(`Scheduled occurrence: changed-${changed}`));
          assert.ok(getCurrentSystemPrompt(context.messages).includes(job.prompt)); assert.equal(model.id, job.model.model_id);
          return fauxAssistantMessage("Changed occurrence complete");
        },
      ]);
      await app.tasks.enqueue(task.id, "Current source", "source-current", undefined, "follow_up");
      actor = app.tasks.runBackground(task.id); void actor.catch(() => {}); await pump(() => app.faux.state.callCount === 1);
      await app.tasks.enqueue(task.id, "Retained source", "source-retained", undefined, "follow_up");
      if (changed === "native-model") await app.tasks.inference.edit(conversation, original.job.model,
        { type: "set_selection", model: { ...original.job.model, model_id: "fixture-updated" } });
      const inbox = await app.session.snapshot(InboxDoc, conversation.id, BACKGROUND_CONTEXT);
      const configuration = await app.session.snapshot(AgentDoc, conversation.id, BACKGROUND_CONTEXT);
      let concurrentlyChanged = false;
      if (changed === "concurrent-model") {
        const peek = app.tasks.inference.peek.bind(app.tasks.inference);
        app.tasks.inference.peek = async (...args) => {
          const selected = await peek(...args);
          if (!concurrentlyChanged) {
            concurrentlyChanged = true;
            await app.tasks.inference.configure(conversation, { ...original.job.model, model_id: "fixture-updated" }, {});
          }
          return selected;
        };
      }
      await app.tasks.enqueueScheduled(task.id, occurrencePrompt(occurrence), occurrence);
      assert.equal(await app.session.commit(tx => tx.submissionByRequest(conversation.id, occurrence.id), BACKGROUND_CONTEXT), undefined);
      assert.deepEqual(await app.session.snapshot(InboxDoc, conversation.id, BACKGROUND_CONTEXT), inbox);
      if (changed === "concurrent-model") assert.equal(concurrentlyChanged, true, "The supported model edit must commit during admission");
      assert.deepEqual(await app.session.snapshot(AgentDoc, conversation.id, BACKGROUND_CONTEXT), changed === "concurrent-model"
        ? { ...configuration, model: { provider: "openai", modelId: "fixture-updated" } } : configuration);
      first.release(); await actor; actor = undefined;
      const receipt = await app.session.commit(tx => tx.submissionByRequest(conversation.id, occurrence.id), BACKGROUND_CONTEXT);
      assert.equal(receipt?.status, "done"); assert.equal(app.faux.state.callCount, 3);
      const current = await app.tasks.get(task.id);
      assert.equal(current.outcome, job.prompt); assert.deepEqual(current.model, job.model);
      assert.equal((await app.session.snapshot(AgentDoc, conversation.id, BACKGROUND_CONTEXT))?.cwd, job.cwd);
    } finally {
      first.release(); await app.close(); await actor?.catch(() => {}); await rm(directory, { recursive: true, force: true });
    }
  });
}

for (const scenario of ["unsubmitted", "arrived-after-binding", "already-native", "changed-cwd"]) {
  test(`reopened scheduled admission preserves a retained native backlog with the ${scenario} occurrence`, { timeout: 10_000 }, async () => {
    const directory = await mkdtemp(join(tmpdir(), "agent-scheduled-priority-reopen-"));
    let app = await openScheduled(directory);
    const controller = new AbortController();
    let actor: Promise<unknown> | undefined;
    let abortNative = async () => {};
    const bindingReady = gate(), admit = gate();
    try {
      const original = scheduledOccurrence;
      const occurrence: ScheduledOccurrence = { ...original,
        ...(scenario === "changed-cwd" ? { job: { ...original.job, cwd: "/workspace/updated-example" } } : {}) };
      const task = await app.tasks.create({ title: original.job.title, outcome: original.job.prompt, model: original.job.model,
        sources: [{ provider: "scheduled_jobs", account: "local", thread_id: original.job.id }] });
      let conversation = await app.tasks.conversation(task.conversation_id);
      abortNative = () => conversation.abort(BACKGROUND_CONTEXT);
      await app.tasks.inference.configure(conversation, original.job.model, { instructions: original.job.prompt, cwd: original.job.cwd });
      // Retain a real native run at its retry checkpoint, as the process would
      // after a restart. The future retry prevents an external model request.
      const native = await conversation.commit(async tx => {
        const entry = await tx.appendEntry(UserEntry, conversation.id,
          { model: [{ role: "user", content: "Retained current source", timestamp: 1 }] });
        const submission = await tx.createSubmission({ type: "input", conversationId: conversation.id,
          requestId: "source-current", status: "placed", entry: entry.id });
        const generation = await tx.createTask(GenerationTask, {}, { ownership: { kind: "conversation" } });
        const live = await tx.doc(LiveDoc, conversation.id);
        live.run = { turns: 0, taskId: generation, inputs: [submission.id] }; live.generation = { attempt: 1 };
        return generation;
      }, BACKGROUND_CONTEXT);
      const generation = await app.session.getTask(native, BACKGROUND_CONTEXT); assert.ok(generation);
      const until = Date.now() + 3_600_000;
      const checkpoint: GenerationCheckpoint = { phase: "retry", attempt: 1, until };
      await commitWrites(app.storage, [{ type: "task", value: { ...generation, state: { status: "sleeping", until, checkpoint: copyJson(checkpoint) } } }], BACKGROUND_CONTEXT);
      await app.close(); app = await openScheduled(directory);
      conversation = await app.tasks.conversation(task.conversation_id);
      const retained = await conversation.submit({ type: "input", content: "Retained source follow-up", requestId: "source-retained", whenBusy: "followUp" }, BACKGROUND_CONTEXT);
      if (scenario !== "arrived-after-binding") await app.tasks.enqueueScheduled(task.id, occurrencePrompt(occurrence), occurrence);
      let existing: Submission | undefined;
      if (scenario === "already-native") existing = await conversation.submit({ type: "input", content: occurrencePrompt(occurrence), requestId: occurrence.id, whenBusy: "followUp" }, BACKGROUND_CONTEXT);
      if (scenario === "arrived-after-binding") {
        const allocation = "55555555-5555-4555-8555-555555555555";
        const host = { host_id: node.host_id, name: node.name, rex_server: node.rex_server, coordinator: node.coordinator,
          agent_program: node.agent_program, cwd: original.job.cwd };
        await app.tasks.update(task.id, task => { task.worker = { state: "bound", allocation_id: allocation, host, binding }; });
        actor = app.tasks.run({ type: "run_task", task_id: task.id, allocation_id: allocation, binding,
          cwd: original.job.cwd, instructions: "Current worker guidance", model: original.job.model, interactive: true },
        () => () => {}, [], async () => { assert.fail("Retained native admission cannot launch a host command"); }, undefined,
        { signal: controller.signal, async ready() {}, answer() {}, idle() {},
          async beforeTurn() { bindingReady.release(); await admit.promise; } });
        void actor.catch(() => {}); await bindingReady.promise;
        await app.tasks.enqueueScheduled(task.id, occurrencePrompt(occurrence), occurrence);
        admit.release();
      } else actor = app.tasks.runBackground(task.id, controller.signal);
      void actor.catch(() => {});
      const receipt = () => app.session.commit(tx => tx.submissionByRequest(conversation.id, occurrence.id), BACKGROUND_CONTEXT);
      if (scenario === "unsubmitted" || scenario === "arrived-after-binding") await pump(async () => await receipt() !== undefined);
      else await pump(async () => (await app.session.snapshot(AgentDoc, conversation.id, BACKGROUND_CONTEXT))?.instructions?.includes("Task outcome:") === true);
      const inbox = await app.session.snapshot(InboxDoc, conversation.id, BACKGROUND_CONTEXT); assert.ok(inbox);
      const admitted = await receipt();
      assert.deepEqual(inbox.items.map(item => ({ id: item.id, mode: item.mode })), [
        { id: retained.id, mode: "followUp" },
        ...(scenario === "changed-cwd" ? [] : [{ id: admitted?.id, mode: scenario === "already-native" ? "followUp" : "steer" }]),
      ]);
      if (scenario === "changed-cwd") assert.equal(admitted, undefined);
      if (existing !== undefined) assert.equal(admitted?.id, existing.id);
      assert.equal((await app.session.snapshot(AgentDoc, conversation.id, BACKGROUND_CONTEXT))?.cwd, original.job.cwd,
        "A queued job edit cannot reconfigure the retained source run during recovery");
      assert.equal((await app.session.getTask(native, BACKGROUND_CONTEXT))?.state.status, "sleeping");
      assert.equal(app.faux.state.callCount, 0);
      controller.abort(); await conversation.abort(BACKGROUND_CONTEXT); await actor.catch(() => {}); actor = undefined;
      const stopped = { ...occurrence, id: "after-stop" };
      await app.tasks.enqueueScheduled(task.id, occurrencePrompt(stopped), stopped);
      assert.equal(await app.session.commit(tx => tx.submissionByRequest(conversation.id, stopped.id), BACKGROUND_CONTEXT), undefined);
    } finally {
      controller.abort(); admit.release(); await abortNative().catch(() => {}); await actor?.catch(() => {});
      await app.close(); await rm(directory, { recursive: true, force: true });
    }
  });
}

test("a priority scheduled child inherits its actual placed input while the old source invocation retains its lineage", { timeout: 15_000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-scheduled-priority-child-"));
  let app = await openScheduled(directory);
  const first = gate();
  let actor: Promise<void> | undefined;
  try {
    const occurrence = scheduledOccurrence, oldOrigin = { job_id: occurrence.job.id, title: "Previous workflow title" };
    const task = await app.tasks.create({ title: oldOrigin.title, outcome: occurrence.job.prompt, model: occurrence.job.model,
      sources: [{ provider: "scheduled_jobs", account: "local", thread_id: occurrence.job.id }] });
    const conversation = await app.tasks.conversation(task.conversation_id);
    await app.tasks.inference.configure(conversation, occurrence.job.model, { cwd: occurrence.job.cwd });
    await app.session.commit(async tx => { (await tx.doc(NoticeLineage, conversation.id)).origin = oldOrigin; }, BACKGROUND_CONTEXT);
    const respond: FauxResponseFactory = context => {
      const done = (name: string) => context.messages.some(message => message.role === "toolResult" && message.toolName === name);
      if (getCurrentSystemPrompt(context.messages).includes("Task outcome: Inspect the priority signal.")) {
        if (done("report_result")) return fauxAssistantMessage("");
        if (done("notify_user")) return fauxAssistantMessage(fauxToolCall("report_result", { summary: "Verified the priority signal." }), { stopReason: "toolUse" });
        return fauxAssistantMessage(fauxToolCall("notify_user", { text: "The priority child needs a concrete decision." }), { stopReason: "toolUse" });
      }
      if (done("spawn_agent")) return fauxAssistantMessage("The scheduled delegation is retained.");
      return fauxAssistantMessage(fauxToolCall("spawn_agent", { title: "Priority signal", outcome: "Inspect the priority signal.", input: "Inspect the admitted signal." }), { stopReason: "toolUse" });
    };
    app.faux.setResponses([
      async () => { await first.promise; return fauxAssistantMessage(fauxToolCall("read_source_events", { limit: 1 }), { stopReason: "toolUse" }); },
      ...Array.from({ length: 14 }, () => respond),
    ]);
    await app.tasks.enqueue(task.id, "Current source work", "source-current", undefined, "follow_up");
    actor = app.tasks.runBackground(task.id); void actor.catch(() => {}); await pump(() => app.faux.state.callCount === 1);
    await app.tasks.enqueue(task.id, "Retained source work", "source-retained", undefined, "follow_up");
    await app.tasks.enqueueScheduled(task.id, occurrencePrompt(occurrence), occurrence);
    const scheduled = await app.session.commit(tx => tx.submissionByRequest(conversation.id, occurrence.id), BACKGROUND_CONTEXT);
    assert.equal(scheduled?.status, "queued");
    first.release();
    const root = await app.session.root(BACKGROUND_CONTEXT);
    await pump(async () => (await root.entries({}, 100, undefined, BACKGROUND_CONTEXT)).items.some(entry => UserNoticeEntry.is(entry)));
    const notices = (await root.entries({}, 100, undefined, BACKGROUND_CONTEXT)).items.filter(entry => UserNoticeEntry.is(entry));
    assert.equal(notices.length, 1);
    assert.deepEqual(notices[0]?.data.notice_origin, { job_id: occurrence.job.id, title: occurrence.job.title });
    const child = (await app.tasks.get(task.id)).agents[0]; assert.ok(child);
    const childConversation = await app.tasks.conversation(child.conversation_id);
    assert.deepEqual((await app.session.snapshot(NoticeLineage, childConversation.id, BACKGROUND_CONTEXT))?.origin,
      { job_id: occurrence.job.id, title: occurrence.job.title });
    assert.deepEqual((await app.session.snapshot(NoticeLineage, conversation.id, BACKGROUND_CONTEXT))?.origin, oldOrigin,
      "Priority admission must not retag the actor default before placement");
    const original = await app.session.commit(async tx => {
      const source = await tx.submissionByRequest(conversation.id, "source-current");
      return source?.entry === undefined ? undefined : tx.entry(source.entry);
    }, BACKGROUND_CONTEXT);
    assert.deepEqual(original?.data, { text: "Current source work", notice_origin: oldOrigin });
    await actor; actor = undefined;
    await pump(() => !app.tasks.isRunning(task.id));
    const childInput = { id: "direct-child", inputId: "direct-child-input", title: "Direct child", outcome: "Direct bounded work", input: "Inspect a direct assignment." };
    await assert.rejects(app.tasks.createAgent(task.id, childInput, { conversation_id: root.id.toString(), call_id: "foreign" }), /owning parent's/);
    await assert.rejects(app.tasks.createAgent(task.id, childInput, { conversation_id: conversation.id.toString(), call_id: "unknown-native-tool" }), /provenance/);
    const direct = await app.tasks.createAgent(task.id, childInput);
    assert.deepEqual((await app.session.snapshot(NoticeLineage, (await app.tasks.conversation(direct.conversation_id)).id, BACKGROUND_CONTEXT))?.origin, oldOrigin,
      "Direct legacy creation retains its established actor default");
    await app.close(); app = await openScheduled(directory);
    assert.deepEqual((await app.session.snapshot(NoticeLineage, childConversation.id, BACKGROUND_CONTEXT))?.origin,
      { job_id: occurrence.job.id, title: occurrence.job.title });
    await runScheduled(occurrence, new AbortController().signal, app, task.id);
    assert.equal(app.faux.state.callCount, 0);
  } finally {
    first.release(); await app.close(); await actor?.catch(() => {}); await rm(directory, { recursive: true, force: true });
  }
});
