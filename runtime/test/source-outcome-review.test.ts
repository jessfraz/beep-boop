import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { fauxAssistantMessage, fauxToolCall, type TranscriptContext } from "@earendil-works/pi-ai";
import { InboxDoc } from "@earendil-works/pi-durable";
import type { SourceReviewDecision } from "../src/generated/SourceReviewDecision.js";
import type { TaskRecord } from "../src/generated/TaskRecord.js";
import { SourceReviews } from "../src/source-review.js";
import { createAppTools } from "../src/tools.js";
import { UserNoticeEntry } from "../src/transcript.js";
import { pump } from "./fixtures/background-runtime.js";
import { openScheduled } from "./fixtures/scheduled-runtime.js";
import { fixtureModel } from "./model.js";

type App = Awaited<ReturnType<typeof openScheduled>>;
function gate() { let release = () => {}; const promise = new Promise<void>(resolve => { release = resolve; }); return { promise, release }; }
function object(value: unknown): value is Record<string, unknown> { return typeof value === "object" && value !== null && !Array.isArray(value); }
function parsed(text: string) { const value: unknown = JSON.parse(text.slice(text.indexOf("{"))); assert.ok(object(value)); return value; }
function userText(context: TranscriptContext) {
  const message = context.messages.findLast(item => item.role === "user"); assert.ok(message?.role === "user");
  return typeof message.content === "string" ? message.content : message.content.flatMap(part => part.type === "text" ? [part.text] : []).join("\n");
}
function groupIds(context: TranscriptContext): string[] {
  const value = parsed(userText(context)); assert.ok(Array.isArray(value.groups));
  return value.groups.map((group: unknown) => { assert.ok(object(group) && typeof group.id === "string"); return group.id; });
}
function toolText(context: TranscriptContext, name: string, error = false) {
  const item = context.messages.findLast(message => message.role === "toolResult" && message.toolName === name);
  assert.ok(item?.role === "toolResult"); assert.equal(item.isError, error);
  return item.content.flatMap(part => part.type === "text" ? [part.text] : []).join("\n");
}
function proposal(context: TranscriptContext, decision: Omit<Extract<SourceReviewDecision, { type: "notify" }>, "groups" | "keys">, id = "notify-outcome") {
  return fauxAssistantMessage(fauxToolCall("review_sources", { decisions: [{ ...decision, groups: groupIds(context), keys: [] }] }, { id }), { stopReason: "toolUse" });
}
function reviews(app: App, includeTaskRead = false) {
  const tools = createAppTools(app.tasks, app.executions);
  const reader = new SourceReviews(app.session, app.registry, app.sources, app.tasks, app.executions, {
    ...(includeTaskRead ? { extensions: [tools.main] } : {}),
    enabled: () => true, workflows: async () => [],
    settings: async () => ({ model: fixtureModel, cwd: "/workspace/example", hosts: [], instructions: "Review published results and preserve their actual owners." }),
    async legacy() { throw new Error("No legacy claim belongs to this fixture"); },
    bind: (conversation, model) => app.executions.bind(conversation.id.toString(), { role: "review", cwd: "/workspace/example", hosts: [], model,
      async call() { throw new Error("Review cannot execute external effects"); } }),
  }, { workers: 1 });
  app.registry.install(reader.tools); return { reader, tools };
}
async function notices(app: App) {
  const root = await app.session.root(BACKGROUND_CONTEXT);
  return (await root.entries({ order: "ascending" }, 100, undefined, BACKGROUND_CONTEXT)).items.filter(UserNoticeEntry.is);
}
async function publish(app: App, task: TaskRecord, summary: string, callId: string, state: TaskRecord["state"] = { state: "open" }) {
  const conversation = await app.tasks.conversation(task.conversation_id);
  await conversation.submit({ type: "write", entry: { kind: "fixture.published_progress", model: [
    fauxAssistantMessage(fauxToolCall("record_task", { summary, state }, { id: callId }), { stopReason: "toolUse" }),
  ] } }, BACKGROUND_CONTEXT);
  await app.tasks.record(task.id, { summary, state }, { conversation_id: task.conversation_id, call_id: callId, settled: false });
  const records = await app.sources.events({ provider: "task_outcomes", pending: true });
  const result = records.find(record => record.outcome?.task_id === task.id); assert.ok(result?.outcome?.entry_id); return result;
}

for (const mode of ["completed", "recorded"]) test(`${mode} entry-bound outcomes publish while the actor continues without feeding its own progress back`, { timeout: 20_000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-active-outcome-review-")), app = await openScheduled(directory);
  const held = gate(), started = gate(), controller = new AbortController(); let running: Promise<void> | undefined;
  try {
    const { reader } = reviews(app), task = await app.tasks.create({ title: "Continuing owned work", outcome: "Finish the authorized work", execution: "coordinator", model: fixtureModel });
    app.faux.setResponses([
      mode === "completed" ? fauxAssistantMessage("A verified useful result is ready.")
        : fauxAssistantMessage(fauxToolCall("record_task", { summary: "A verified useful result is ready.", state: { state: "open" } }, { id: "worker-progress" }), { stopReason: "toolUse" }),
      async () => { started.release(); await held.promise; return fauxAssistantMessage("The continuing turn finished."); },
      input => fauxAssistantMessage(fauxToolCall("review_sources", { decisions: [{ type: "task", groups: groupIds(input), keys: [], task_id: task.id,
        job_id: null, work_url: null, title: task.title, outcome: task.outcome, input: "Repeat this actor's own progress." }] }, { id: "prevent-feedback" }), { stopReason: "toolUse" }),
      input => { assert.match(toolText(input, "review_sources", true), /already owns its published progress/u);
        return proposal(input, { type: "notify", task_id: task.id, text: "The verified result is ready while the remaining work continues." }); },
      input => { toolText(input, "review_sources"); return fauxAssistantMessage(""); },
    ]);
    await app.tasks.enqueue(task.id, "Produce the first verified result.", "first-input");
    if (mode === "completed") await app.tasks.enqueue(task.id, "Continue the independent remaining work.", "second-input");
    running = app.tasks.runBackground(task.id); await started.promise;
    const published = (await app.sources.events({ provider: "task_outcomes", pending: true }))[0]; assert.ok(published?.outcome?.entry_id);
    assert.equal(app.tasks.isRunning(task.id), true);
    const owner = await app.tasks.conversation(task.conversation_id), inbox = await app.session.snapshot(InboxDoc, owner.id, BACKGROUND_CONTEXT);
    const queued = (await app.tasks.get(task.id)).pending;
    assert.equal(await reader.review(0, controller.signal), true, "Published entry-bound progress must not wait for every later actor input to drain");
    await pump(async () => (await notices(app)).length === 1);
    assert.equal(app.tasks.isRunning(task.id), true); assert.deepEqual((await app.tasks.get(task.id)).pending, queued);
    assert.deepEqual(await app.session.snapshot(InboxDoc, owner.id, BACKGROUND_CONTEXT), inbox, "Review must not create a self-generated actor input");
    assert.equal(await reader.review(0, controller.signal), false);
    assert.equal((await notices(app))[0]?.data.text, "The verified result is ready while the remaining work continues.");
    assert.deepEqual((await app.sources.originals([published.key]))[0]?.event, published.event);
    held.release(); await running; running = undefined;
  } finally { held.release(); controller.abort(); await running?.catch(() => {}); await app.close(); await rm(directory, { recursive: true, force: true }); }
});

test("newer real actor outcomes fence a held review before stale progress can notify", { timeout: 20_000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-outcome-review-fence-")), app = await openScheduled(directory);
  const ownerHeld = gate(), ownerStarted = gate(), reviewHeld = gate(), reviewStarted = gate(), controller = new AbortController();
  let worker: Promise<void> | undefined, reviewing: Promise<boolean> | undefined;
  try {
    const { reader } = reviews(app), task = await app.tasks.create({ title: "Changing owned progress", outcome: "Finish current work", execution: "coordinator", model: fixtureModel });
    app.faux.setResponses([fauxAssistantMessage("The first result is ready."), async () => { ownerStarted.release(); await ownerHeld.promise; return fauxAssistantMessage("The newer result supersedes the first."); },
      async input => { reviewStarted.release(); await reviewHeld.promise; return proposal(input, { type: "notify", task_id: task.id, text: "Stale result must not publish." }); },
      input => { const result = parsed(toolText(input, "review_sources")); assert.equal(result.superseded, 1); return fauxAssistantMessage(""); }]);
    await app.tasks.enqueue(task.id, "First input", "first"); await app.tasks.enqueue(task.id, "Second input", "second");
    worker = app.tasks.runBackground(task.id); await ownerStarted.promise;
    reviewing = reader.review(0, controller.signal); await reviewStarted.promise;
    ownerHeld.release(); await worker; worker = undefined;
    reviewHeld.release(); assert.equal(await reviewing, true); reviewing = undefined;
    assert.deepEqual(await notices(app), []);
    const pending = await app.sources.events({ provider: "task_outcomes", pending: true }); assert.equal(pending.length, 1);
    assert.match(pending[0]?.outcome?.summary ?? "", /newer result/u);
  } finally { ownerHeld.release(); reviewHeld.release(); controller.abort(); await Promise.allSettled([worker, reviewing]); await app.close(); await rm(directory, { recursive: true, force: true }); }
});

test("large task outcomes and all review status fallbacks retain bounded views and complete canonical evidence", { timeout: 60_000 }, async t => {
  const directory = await mkdtemp(join(tmpdir(), "agent-bounded-outcome-review-")), app = await openScheduled(directory), controller = new AbortController();
  try {
    const { reader, tools } = reviews(app, true);
    const refs = Array.from({ length: 592 }, (_, index) => ({ provider: "google", account: "mail-example",
      thread_id: `retained-${index}-` + (index < 80 ? "short" : "x".repeat(4000)) }));
    const retainedDetail = "Retained task details. ".repeat(40_000), state = { state: "resolved", evidence: retainedDetail } satisfies TaskRecord["state"];
    const task = await app.tasks.create({ title: retainedDetail, outcome: retainedDetail, sources: refs, model: fixtureModel });
    for (let index = 0; index < 40; index++) await app.tasks.enqueue(task.id, "Retained queued input. ".repeat(1000), `queued-${index}`);
    const original = await publish(app, task, "A bounded verified result is ready.", "large-outcome-proof", state);
    const originalEntry = original.outcome?.entry_id; assert.ok(originalEntry);
    assert.ok(Buffer.byteLength(original.event.text) > 2_000_000);
    const canonical = await app.tasks.get(task.id), observed: number[] = [];
    const bounded = (value: string) => { observed.push(Buffer.byteLength(value)); if (Buffer.byteLength(value) >= 40_000) t.diagnostic(JSON.stringify({ oversized_review_view_bytes: Buffer.byteLength(value) })); assert.ok(Buffer.byteLength(value) < 40_000, "A reviewer must not hydrate retained reference or queued-input corpora into its model context"); };
    const outcome = (context: TranscriptContext) => { const value = parsed(userText(context)); assert.ok(Array.isArray(value.groups));
      const group = value.groups[0]; assert.ok(object(group) && Array.isArray(group.events)); const item = group.events[0];
      assert.ok(object(item) && object(item.event) && typeof item.event.text === "string"); return parsed(item.event.text); };
    app.faux.setResponses([
      input => { bounded(userText(input)); const view = outcome(input); assert.equal(view.source_count, refs.length); assert.equal(view.sources_complete, false);
        assert.equal(view.entry_id, original.outcome?.entry_id); return fauxAssistantMessage(fauxToolCall("read_source_events", { keys: [original.key], limit: 1 }), { stopReason: "toolUse" }); },
      input => { const text = toolText(input, "read_source_events"); bounded(text); const view = parsed(text); assert.ok(Array.isArray(view.events));
        const saved = view.events[0]; assert.ok(object(saved) && object(saved.outcome)); assert.equal(saved.outcome.source_count, refs.length);
        return fauxAssistantMessage(fauxToolCall("read_source_task", { task_id: task.id }), { stopReason: "toolUse" }); },
      input => { const text = toolText(input, "read_source_task"); bounded(text); const view = parsed(text);
        assert.equal(view.pending_count, 40); assert.equal(view.pending_complete, false); assert.equal(view.source_count, refs.length); assert.equal(view.title_complete, false); assert.equal(view.outcome_complete, false); assert.equal(view.state_complete, false);
        return fauxAssistantMessage(fauxToolCall("read_task", { task_id: task.id, include_history: false }), { stopReason: "toolUse" }); },
      input => { const text = toolText(input, "read_task"); bounded(text); const value = parsed(text); assert.ok(object(value.task));
        assert.equal(value.task.source_count, refs.length); assert.equal(value.task.pending_count, 40); assert.equal(value.task.sources_complete, false); assert.equal(value.task.title_complete, false); assert.equal(value.task.outcome_complete, false); assert.equal(value.task.state_complete, false);
        return fauxAssistantMessage(fauxToolCall("review_sources", { decisions: [{ type: "dismiss", groups: groupIds(input), keys: [], reason: "The published result remains with its owner" }] }), { stopReason: "toolUse" }); },
      input => { toolText(input, "review_sources"); return fauxAssistantMessage(""); },
    ]);
    assert.equal(await reader.review(0, controller.signal), true);
    const retained = (await app.sources.originals([original.key]))[0]; assert.deepEqual(retained?.event, original.event); assert.deepEqual(retained?.outcome, original.outcome);
    assert.deepEqual((await app.tasks.get(task.id)).sources, canonical.sources); assert.deepEqual((await app.tasks.get(task.id)).pending, canonical.pending);
    assert.equal((await app.tasks.get(task.id)).title, canonical.title); assert.equal((await app.tasks.get(task.id)).outcome, canonical.outcome);
    assert.deepEqual((await app.tasks.get(task.id)).state, canonical.state);

    const legacy = await publish(app, task, "A later verified result is ready.", "legacy-outcome-proof", state);
    app.faux.setResponses([
      input => { bounded(userText(input)); return fauxAssistantMessage(fauxToolCall("read_source_events", { keys: [legacy.key], limit: 1 }), { stopReason: "toolUse" }); },
      input => { bounded(toolText(input, "read_source_events")); return fauxAssistantMessage(fauxToolCall("read_task", { task_id: task.id, include_history: false }), { stopReason: "toolUse" }); },
      input => { bounded(toolText(input, "read_task")); return fauxAssistantMessage(fauxToolCall("dismiss_source_events", { keys: [legacy.key], reason: "The exact published evidence remains retained" }), { stopReason: "toolUse" }); },
      input => { toolText(input, "dismiss_source_events"); return fauxAssistantMessage(""); },
    ]);
    const command = { type: "triage_sources", instructions: "Review retained progress.", model: fixtureModel, cwd: "/workspace/example", hosts: [] } satisfies Parameters<App["sources"]["triage"]>[0];
    await app.sources.triage(command, conversation => app.executions.bind(conversation.id.toString(), { role: "triage", cwd: command.cwd, hosts: [], model: fixtureModel,
      async call() { throw new Error("A status read cannot execute host effects"); } }), [tools.main, app.sourceTools.main], controller.signal);
    assert.deepEqual((await app.sources.originals([legacy.key]))[0]?.event, legacy.event);
    assert.deepEqual((await app.tasks.get(task.id)).sources, canonical.sources); assert.deepEqual((await app.tasks.get(task.id)).pending, canonical.pending);
    assert.equal((await app.tasks.get(task.id)).title, canonical.title); assert.equal((await app.tasks.get(task.id)).outcome, canonical.outcome);
    assert.deepEqual((await app.tasks.get(task.id)).state, canonical.state);
    const small = await app.tasks.create({ title: "Worker status owner", outcome: "Keep complete worker state", model: fixtureModel, sources: refs.slice(0, 2) });
    await app.tasks.enqueue(small.id, "Complete canonical queued content", "worker-input");
    const worker = await app.session.createConversation({ ownership: { kind: "ownerless" } }, BACKGROUND_CONTEXT);
    await app.tasks.inference.configure(worker, fixtureModel, { extensions: [tools.main] });
    const unbind = app.executions.bind(worker.id.toString(), { role: "worker", cwd: "/workspace/example", hosts: [], model: fixtureModel,
      async call() { throw new Error("A canonical read cannot execute host effects"); } });
    try {
      app.faux.setResponses([
        fauxAssistantMessage(fauxToolCall("read_task", { task_id: small.id, include_history: false }), { stopReason: "toolUse" }),
        input => { const value = parsed(toolText(input, "read_task")); assert.ok(object(value.task) && Array.isArray(value.task.pending));
          assert.equal(value.task.source_count, undefined); assert.deepEqual(value.task.model, fixtureModel);
          const pending = value.task.pending[0]; assert.ok(object(pending)); assert.equal(pending.content, "Complete canonical queued content");
          return fauxAssistantMessage(fauxToolCall("read_task", { conversation_id: task.conversation_id, include_history: true,
            entry_ids: [originalEntry] }), { stopReason: "toolUse" }); },
        input => { const value = parsed(toolText(input, "read_task")); assert.ok(Array.isArray(value.originals)); const exact = value.originals[0];
          assert.ok(object(exact) && typeof exact.data === "string"); assert.match(exact.data, /large-outcome-proof/u);
          assert.match(exact.data, /A bounded verified result is ready/u); assert.equal(typeof exact.next_offset, "number"); return fauxAssistantMessage(""); },
      ]);
      assert.equal((await (await worker.submit({ type: "input", content: "Read canonical status and exact evidence." }, BACKGROUND_CONTEXT)).wait(BACKGROUND_CONTEXT)).status, "done");
    } finally { unbind(); }
    t.diagnostic(JSON.stringify({ original_outcome_bytes: Buffer.byteLength(original.event.text), canonical_task_bytes: Buffer.byteLength(JSON.stringify(canonical)), maximum_review_view_bytes: Math.max(...observed) }));
  } finally { controller.abort(); await app.close(); await rm(directory, { recursive: true, force: true }); }
});

test("worker and main source pages bound large outcomes without losing cursors, mail or originals", { timeout: 30_000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-outcome-page-")), app = await openScheduled(directory);
  try {
    const mail = { id: "delivery-update", version: "original", source: { provider: "google", account: "mail-example", thread_id: "delivery" },
      title: "Delivery update", text: "Complete original delivery details. ".repeat(500) };
    await app.sources.admit({ source: { provider: "google", account: "mail-example" }, events: [mail],
      checkpoint: { value: "retained-mail" }, coverage: { status: "complete" } }, null);
    const retainedMail = (await app.sources.events({ provider: "google" }))[0]; assert.ok(retainedMail);
    const refs = Array.from({ length: 592 }, (_, index) => ({ provider: "google", account: "mail-example", thread_id: `${index}-` + "x".repeat(4000) }));
    const owner = await app.tasks.create({ title: "Published progress", outcome: "Retain the complete source snapshot", sources: refs, model: fixtureModel });
    const original = await publish(app, owner, "Verified result with exact provenance.", "outcome-page-proof");
    assert.ok(Buffer.byteLength(original.event.text) > 2_000_000);
    const keys = [original.key, retainedMail.key];
    for (const role of ["worker", "main"]) {
      const conversation = role === "main" ? await app.session.root(BACKGROUND_CONTEXT)
        : await app.session.createConversation({ ownership: { kind: "ownerless" } }, BACKGROUND_CONTEXT);
      await app.tasks.inference.configure(conversation, fixtureModel, { extensions: [role === "main" ? app.sourceTools.main : app.sourceTools.worker] });
      await conversation.configure({ compaction: { enabled: false } }, BACKGROUND_CONTEXT);
      const unbind = app.executions.bind(conversation.id.toString(), { ...(role === "worker" ? { role: "worker" } : {}),
        cwd: "/workspace/example", hosts: [], model: fixtureModel, async call() { throw new Error("A source read cannot execute host effects"); } });
      const read = async (cursor?: string) => {
        let body = "";
        app.faux.setResponses([
          fauxAssistantMessage(fauxToolCall("read_source_events", { keys, limit: 1, ...(cursor === undefined ? {} : { cursor }) }), { stopReason: "toolUse" }),
          input => { body = toolText(input, "read_source_events"); return fauxAssistantMessage(""); },
        ]);
        const result = await (await conversation.submit({ type: "input", content: "Read one exact source page." }, BACKGROUND_CONTEXT)).wait(BACKGROUND_CONTEXT);
        assert.equal(result.status, "done", JSON.stringify(result));
        assert.ok(Buffer.byteLength(body) < 40_000, `${role} source pages must bound outcome references before serialization`);
        return parsed(body);
      };
      try {
        const first = await read(); assert.equal(first.complete, false);
        assert.ok(Array.isArray(first.events)); const item = first.events[0];
        assert.ok(object(item) && object(item.outcome)); assert.equal(item.key, original.key);
        assert.equal(item.outcome.source_count, refs.length); assert.equal(item.outcome.sources_complete, false);
        assert.equal(item.outcome.entry_id, original.outcome?.entry_id); assert.equal(item.outcome.tool_call_id, original.outcome?.tool_call_id);
        assert.equal(item.outcome.original_bytes, Buffer.byteLength(original.event.text));
        assert.ok(typeof first.cursor === "string");
        const second = await read(first.cursor); assert.equal(second.complete, true); assert.equal(second.cursor, null);
        assert.ok(Array.isArray(second.events)); assert.deepEqual(second.events[0], retainedMail);
      } finally { unbind(); }
    }
    assert.deepEqual((await app.sources.originals(keys)).find(record => record.key === original.key), original);
    assert.deepEqual((await app.tasks.get(owner.id)).sources, refs);
  } finally { await app.close(); await rm(directory, { recursive: true, force: true }); }
});

test("a completed child result can continue its busy parent without being mistaken for root self-feedback", { timeout: 20_000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-child-outcome-review-")), app = await openScheduled(directory);
  const held = gate(), started = gate(), controller = new AbortController(); let running: Promise<void> | undefined;
  try {
    const { reader } = reviews(app), task = await app.tasks.create({ title: "Parent work", outcome: "Incorporate its child result", execution: "coordinator", model: fixtureModel });
    app.faux.setResponses([async () => { started.release(); await held.promise; return fauxAssistantMessage("The parent continues."); },
      input => fauxAssistantMessage(fauxToolCall("review_sources", { decisions: [{ type: "task", groups: groupIds(input), keys: [], task_id: task.id,
        job_id: null, work_url: null, title: task.title, outcome: task.outcome, input: "Incorporate the newly completed child result." }] }), { stopReason: "toolUse" }),
      input => { toolText(input, "review_sources"); return fauxAssistantMessage(""); }]);
    await app.tasks.enqueue(task.id, "Continue the parent work.", "parent-input"); running = app.tasks.runBackground(task.id); await started.promise;
    const child = await app.tasks.createAgent(task.id, { id: "child", title: "Bounded child work", outcome: "Return verified evidence", input: "Inspect invented evidence", inputId: "child-input", model: fixtureModel });
    const conversation = await app.tasks.conversation(child.conversation_id);
    await conversation.submit({ type: "write", entry: { kind: "fixture.child_result", model: [
      fauxAssistantMessage(fauxToolCall("report_result", { summary: "The child verified the requested evidence." }, { id: "child-report" }), { stopReason: "toolUse" }),
    ] } }, BACKGROUND_CONTEXT);
    await app.tasks.reportAgent(task.id, child.id, "The child verified the requested evidence.", undefined, { conversation_id: child.conversation_id, call_id: "child-report" });
    const published = (await app.sources.events({ provider: "task_outcomes", pending: true }))[0]; assert.ok(published?.outcome?.entry_id);
    assert.equal(published.outcome.conversation_id, child.conversation_id); assert.equal(app.tasks.isRunning(task.id), true);
    assert.equal(await reader.review(0, controller.signal), true);
    assert.deepEqual((await app.sources.originals([published.key]))[0]?.decision, { type: "task", task_id: task.id });
    const parent = await app.tasks.conversation(task.conversation_id), inbox = await app.session.snapshot(InboxDoc, parent.id, BACKGROUND_CONTEXT);
    assert.ok((await app.tasks.get(task.id)).pending.length > 0 || (inbox?.items.length ?? 0) > 0);
    assert.deepEqual(await notices(app), []);
    app.faux.appendResponses([fauxAssistantMessage("The parent incorporated the child evidence.")]); held.release(); await running; running = undefined;
  } finally { held.release(); controller.abort(); await running?.catch(() => {}); await app.close(); await rm(directory, { recursive: true, force: true }); }
});
