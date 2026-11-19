import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { createModels, fauxAssistantMessage, fauxProvider, fauxToolCall, type TranscriptContext } from "@earendil-works/pi-ai";
import { getCurrentSystemPrompt } from "@earendil-works/pi-ai/utils/transcript";
import { createRegistry, Harness } from "@earendil-works/pi-durable";
import { SqliteStorage } from "@earendil-works/pi-durable/storage/sqlite";
import { openConversationDatabase } from "../src/conversation-database.js";
import { CoordinatorEnvironment } from "../src/environment.js";
import type { ScheduledJobStatus } from "../src/generated/ScheduledJobStatus.js";
import type { SourceEvent } from "../src/generated/SourceEvent.js";
import type { SourceReviewDecision } from "../src/generated/SourceReviewDecision.js";
import { ConversationModels } from "../src/models.js";
import { createGuidance } from "../src/guidance.js";
import { SourceReviews, type SourceReviewOptions } from "../src/source-review.js";
import { SourceJournal } from "../src/sources.js";
import { TaskStore, type HostCall } from "../src/tasks.js";
import { UserNoticeEntry } from "../src/transcript.js";
import { Executions } from "../src/tools.js";
import { fixtureModel } from "./model.js";
import { FakeClock, pump } from "./fixtures/background-runtime.js";

function gate() {
  let release = () => {};
  const promise = new Promise<void>(resolve => { release = resolve; });
  return { promise, release };
}
function object(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}
type EvidenceGroup = { id: string; keys: string[]; threads: string[]; originals: string[] };
function userText(context: TranscriptContext): string {
  const input = context.messages.findLast(message => message.role === "user"); assert.ok(input?.role === "user");
  return typeof input.content === "string" ? input.content : input.content.flatMap(part => part.type === "text" ? [part.text] : []).join("\n");
}
function groups(context: TranscriptContext): EvidenceGroup[] {
  const parsed: unknown = JSON.parse(userText(context)); assert.ok(object(parsed) && Array.isArray(parsed.groups));
  return parsed.groups.map((value: unknown) => {
    assert.ok(object(value) && typeof value.id === "string" && Array.isArray(value.events));
    const keys: string[] = [], threads: string[] = [], originals: string[] = [];
    for (const item of value.events) {
      assert.ok(object(item) && typeof item.key === "string" && object(item.event) && typeof item.event.id === "string" &&
        object(item.event.source) && typeof item.event.source.thread_id === "string");
      keys.push(item.key); threads.push(item.event.source.thread_id); originals.push(item.event.id);
    }
    return { id: value.id, keys, threads, originals };
  });
}
function toolResult(context: TranscriptContext, name = "review_sources", error = false): string {
  const message = context.messages.findLast(message => message.role === "toolResult" && message.toolName === name);
  assert.ok(message?.role === "toolResult"); assert.equal(message.isError, error);
  return message.content.flatMap(part => part.type === "text" ? [part.text] : []).join("\n");
}
function dismissed(captured: EvidenceGroup[], reason = "The exact captured evidence needs no further action"): SourceReviewDecision[] {
  return [{ type: "dismiss", groups: captured.map(group => group.id), keys: [], reason }];
}
function routed(captured: EvidenceGroup[], overrides: { task_id?: string | null; job_id?: string | null; work_url?: string | null } = {}): SourceReviewDecision {
  return { type: "task", groups: captured.map(group => group.id), keys: [], task_id: overrides.task_id ?? null,
    job_id: overrides.job_id ?? null, work_url: overrides.work_url ?? null, title: "Investigate the retained evidence",
    outcome: "Resolve the authorized work once", input: "Inspect these exact originals and continue their authorized work." };
}
function answer(decisions: SourceReviewDecision[], id = "review-call") {
  return fauxAssistantMessage(fauxToolCall("review_sources", { decisions }, { id }), { stopReason: "toolUse" });
}
function event(id: string, thread = id, text = "Retained source evidence", provider = "slack"): SourceEvent {
  return { id, version: "v1", source: { provider, account: "example", thread_id: thread }, title: "Example " + id,
    text, occurred_at: Date.now() };
}
async function admit(journal: SourceJournal, events: SourceEvent[]): Promise<void> {
  const first = events[0]; assert.ok(first);
  const source = { provider: first.source.provider, account: first.source.account };
  const previous = (await journal.states()).find(state => state.source.provider === source.provider && state.source.account === source.account);
  await journal.admit({ source, events, checkpoint: null, coverage: { status: "complete" } }, previous?.checkpoint ?? null);
}
async function open(path: string, options: SourceReviewOptions = {}, workflows: ScheduledJobStatus[] = [], clock?: FakeClock, guidance?: HostCall) {
  const models = createModels(), registry = createRegistry();
  const faux = fauxProvider({ provider: "openai", models: [{ id: "fixture" }], tokensPerSecond: 1_000_000 }); models.setProvider(faux.provider);
  const inference = new ConversationModels(models, undefined, registry);
  const storage = await SqliteStorage.open(await openConversationDatabase(path));
  const session = await Harness.open(storage, { models, registry, identity: { attribution: null },
    ...(clock === undefined ? {} : { now: clock.now, setTimer: clock.setTimer }),
    settings: { retry: { enabled: false }, compaction: { enabled: false } } }, BACKGROUND_CONTEXT);
  const tasks = new TaskStore(session, inference), executions = new Executions(), sources = new SourceJournal(session, tasks, executions);
  const extensions = guidance === undefined ? [] : [createGuidance(executions)];
  for (const extension of extensions) registry.install(extension);
  const bound = new Set<string>();
  const reviews = new SourceReviews(session, registry, sources, tasks, executions, {
    extensions,
    enabled: () => true,
    settings: async () => ({ model: fixtureModel, cwd: "/workspace/example", hosts: [], instructions: "Complete authorized work and retain its evidence." }),
    workflows: async () => workflows,
    async legacy() { throw new Error("This fixture has no legacy source claim"); },
    bind(conversation, model) {
      const id = conversation.id.toString(); assert.equal(bound.has(id), false); bound.add(id);
      const unbind = executions.bind(id, { model, cwd: "/workspace/example", hosts: [], role: "review",
        call: guidance ?? (async () => { throw new Error("A source reviewer cannot perform provider effects"); }) });
      return () => { bound.delete(id); unbind(); };
    },
  }, options);
  registry.install(reviews.tools);
  await session.root(BACKGROUND_CONTEXT); await tasks.prepare(); await sources.prepare();
  return { session, storage, tasks, sources, reviews, faux, bound };
}

test("native reviewers overlap only disjoint entities and leave arrivals outside their captured versions pending", { timeout: 15_000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-parallel-source-review-")), app = await open(join(directory, "session.sqlite"), { workers: 3, maximumEntities: 1 });
  const hold = gate(), started = gate(), controller = new AbortController(), captured: EvidenceGroup[][] = [];
  let running: Promise<boolean>[] = [];
  try {
    await admit(app.sources, [event("first-a", "thread-a"), event("first-b", "thread-b")]);
    const response = async (input: TranscriptContext) => {
      captured.push(groups(input)); if (captured.length === 2) started.release();
      await hold.promise; return answer(dismissed(groups(input)), "disjoint-" + captured.length);
    };
    app.faux.setResponses([response, response, fauxAssistantMessage(""), fauxAssistantMessage("")]);
    running = [app.reviews.review(0, controller.signal), app.reviews.review(1, controller.signal)];
    await started.promise;
    assert.equal(app.bound.size, 2); assert.equal((await app.reviews.diagnostics()).active_reviews.length, 2);
    const selectedThreads = captured.flatMap(batch => batch.flatMap(group => group.threads));
    assert.deepEqual(new Set(selectedThreads), new Set(["thread-a", "thread-b"])); assert.equal(selectedThreads.length, 2);
    assert.equal(await app.reviews.review(0, controller.signal), false, "A slot cannot overlap its own native review");
    await admit(app.sources, [event("later-a", "thread-a")]);
    assert.equal(await app.reviews.review(2, controller.signal), false, "New messages cannot start a concurrent review of an already claimed entity");
    hold.release(); assert.deepEqual(await Promise.all(running), [true, true]); running = [];
    assert.equal(app.bound.size, 0);
    assert.deepEqual((await app.sources.events({ pending: true })).map(record => record.event.id), ["later-a"]);
    app.faux.setResponses([input => { const pending = groups(input); assert.deepEqual(pending.flatMap(group => group.originals), ["later-a"]);
      return answer(dismissed(pending)); }, fauxAssistantMessage("")]);
    assert.equal(await app.reviews.review(0, controller.signal), true);
    assert.deepEqual(await app.sources.events({ pending: true }), []);
  } finally { hold.release(); controller.abort(); await Promise.allSettled(running); await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("a manual judgment fences the stale entity claim before it can enqueue task work", { timeout: 10_000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-stale-source-review-")), app = await open(join(directory, "session.sqlite"));
  const hold = gate(), started = gate(), controller = new AbortController(); let running: Promise<boolean> | undefined;
  try {
    await admit(app.sources, [event("first", "shared-thread"), event("second", "shared-thread")]);
    let captured: EvidenceGroup[] = [];
    app.faux.setResponses([async input => { captured = groups(input); started.release(); await hold.promise;
      return answer([routed(captured)]); }, input => { assert.deepEqual(JSON.parse(toolResult(input)), { accounted: 0, superseded: 2 }); return fauxAssistantMessage(""); }]);
    running = app.reviews.review(0, controller.signal); await started.promise;
    const key = captured[0]?.keys[0]; assert.ok(key);
    await app.sources.decide([key], { type: "dismissed", reason: "Manually verified by the current owner" });
    hold.release(); assert.equal(await running, true); running = undefined;
    assert.deepEqual(await app.tasks.list(), []);
    assert.deepEqual((await app.sources.events({ keys: [key] }))[0]?.decision,
      { type: "dismissed", reason: "Manually verified by the current owner" });
    assert.equal((await app.sources.events({ pending: true })).length, 1);
    app.faux.setResponses([input => answer(dismissed(groups(input))), fauxAssistantMessage("")]);
    assert.equal(await app.reviews.review(0, controller.signal), true); assert.deepEqual(await app.sources.events({ pending: true }), []);
  } finally { hold.release(); controller.abort(); await Promise.allSettled([running]); await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("review task admission rolls back as one transaction, replays once and releases its slot before slow effects", { timeout: 15_000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-atomic-source-review-")), app = await open(join(directory, "session.sqlite"), { maximumEntities: 2 });
  const holdEffect = gate(), effectStarted = gate(), controller = new AbortController();
  const resource = "https://github.com/example/project/issues/42";
  let committed: SourceReviewDecision[] = [], owner = "";
  app.tasks.wakeBackground = async id => { effectStarted.release(); await holdEffect.promise; return app.tasks.get(id); };
  try {
    await admit(app.sources, [event("alpha", "thread-a", resource), event("beta", "thread-b", resource)]);
    app.faux.setResponses([
      input => { const captured = groups(input); assert.equal(captured.length, 2); const first = captured[0], second = captured[1]; assert.ok(first && second);
        return answer([routed([first], { work_url: resource }), routed([second], { task_id: "missing-owner" })], "reject-atomically"); },
      async input => {
        assert.match(toolResult(input, "review_sources", true), /Unknown task owner/u);
        assert.deepEqual(await app.tasks.list(), []); assert.equal((await app.sources.events({ pending: true })).length, 2);
        committed = [routed(groups(input), { work_url: resource })]; return answer(committed, "accept-once");
      },
      async input => {
        assert.deepEqual(JSON.parse(toolResult(input)), { accounted: 2, superseded: 0 });
        const active = (await app.reviews.diagnostics()).active_reviews[0]; assert.ok(active);
        assert.deepEqual(await app.reviews.apply(active.conversation_id, "accept-once", committed), { accounted: 2, superseded: 0 });
        await assert.rejects(app.reviews.apply(active.conversation_id, "changed-replay", dismissed(groups(input))), /cannot change/u);
        const task = (await app.tasks.list())[0]; assert.ok(task); owner = task.id;
        assert.equal(task.pending.length, 1); assert.equal(task.sources.length, 2); return fauxAssistantMessage("");
      },
    ]);
    assert.equal(await app.reviews.review(0, controller.signal), true);
    await effectStarted.promise;
    assert.equal((await app.reviews.diagnostics()).active_reviews.length, 0,
      "The source slot must be reusable while its durable task effect is still waiting");
    await admit(app.sources, [event("third", "thread-c", resource)]);
    app.faux.setResponses([input => answer([routed(groups(input), { work_url: resource })]), fauxAssistantMessage("")]);
    assert.equal(await app.reviews.review(0, controller.signal), true);
    assert.equal((await app.tasks.list()).length, 1); const retained = await app.tasks.get(owner);
    assert.equal(retained.pending.length, 2); assert.equal(new Set(retained.pending.map(input => input.id)).size, 2);
    assert.equal(retained.sources.length, 3); assert.deepEqual(await app.sources.events({ pending: true }), []);
    await app.tasks.create({ id: "corrected-owner", title: "Current manually corrected owner", outcome: "Continue under the current owner", execution: "coordinator" });
    await app.sources.decide((await app.sources.events()).map(record => record.key), { type: "task", task_id: "corrected-owner" });
    await admit(app.sources, [event("fourth", "thread-d", resource)]);
    app.faux.setResponses([input => answer([routed(groups(input), { work_url: resource })]), fauxAssistantMessage("")]);
    assert.equal(await app.reviews.review(0, controller.signal), true);
    assert.equal((await app.tasks.get("corrected-owner")).pending.length, 1,
      "A retained work-owner receipt cannot veto a later authoritative manual correction");
    assert.equal((await app.tasks.get(owner)).pending.length, 2);
  } finally { holdEffect.release(); controller.abort(); await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("retained GitHub decisions and configured workflows keep cross-thread signals with their existing owner", { timeout: 15_000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-source-workflow-owner-"));
  const job: ScheduledJobStatus = { job: { id: "configured-review", title: "Configured evidence review", prompt: "AUTHORIZED_WORKFLOW: inspect and resolve new evidence using the configured policy.",
    model: fixtureModel, timezone: "UTC", schedules: [], revision: "one", cwd: "/workspace/example" }, schedules: [], task_id: "workflow-owner" };
  const app = await open(join(directory, "session.sqlite"), { maximumEntities: 2 }, [job]), controller = new AbortController();
  app.tasks.wakeBackground = id => app.tasks.get(id);
  try {
    await app.tasks.create({ id: "retained-github-owner", title: "Existing issue work", outcome: "Finish the existing issue" });
    const workflow = await app.tasks.create({ id: "workflow-owner", title: job.job.title, outcome: job.job.prompt,
      execution: "coordinator", sources: [{ provider: "scheduled_jobs", account: "local", thread_id: job.job.id }] });
    const resource = "https://github.com/example/project/pull/77";
    await admit(app.sources, [event("old-provider-version", "notification-77", "Subject API: " + resource, "github")]);
    const original = (await app.sources.events({ provider: "github" }))[0]; assert.ok(original);
    await app.sources.decide([original.key], { type: "task", task_id: "retained-github-owner" });
    await app.session.close(BACKGROUND_CONTEXT);
    const reopened = await open(join(directory, "session.sqlite"), { maximumEntities: 2 }, [job]);
    try {
      reopened.tasks.wakeBackground = id => reopened.tasks.get(id);
      await admit(reopened.sources, [event("slack-mention", "unrelated-slack-thread", "Current evidence about " + resource)]);
      reopened.faux.setResponses([input => answer([routed(groups(input), { work_url: resource })]), fauxAssistantMessage("")]);
      assert.equal(await reopened.reviews.review(0, controller.signal), true);
      assert.equal((await reopened.tasks.list()).length, 2);
      assert.equal((await reopened.tasks.get("retained-github-owner")).pending.length, 1,
        "The rebuilt resource projection must retain the preexisting task owner");
      await admit(reopened.sources, [event("workflow-a", "workflow-thread-a", resource), event("workflow-b", "workflow-thread-b", resource)]);
      reopened.faux.setResponses([
        () => fauxAssistantMessage([fauxToolCall("list_source_workflows", {}), fauxToolCall("read_source_workflow", { job_id: job.job.id })], { stopReason: "toolUse" }),
        input => { assert.match(toolResult(input, "list_source_workflows"), /workflow-owner/u);
          assert.match(toolResult(input, "read_source_workflow"), /AUTHORIZED_WORKFLOW/u);
          return answer([routed(groups(input), { job_id: job.job.id, work_url: resource })]); },
        input => { toolResult(input); return fauxAssistantMessage(""); },
      ]);
      assert.equal(await reopened.reviews.review(0, controller.signal), true);
      assert.equal((await reopened.tasks.list()).length, 2);
      const saved = await reopened.tasks.get(workflow.id); assert.equal(saved.pending.length, 1);
      assert.match(saved.pending[0]?.content ?? "", /AUTHORIZED_WORKFLOW/u); assert.equal(saved.sources.length, 3);
      assert.equal((await reopened.tasks.get("retained-github-owner")).pending.length, 1,
        "A shared resource cannot replace the explicitly selected configured workflow's actor");
      assert.deepEqual(await reopened.sources.events({ pending: true }), []);
    } finally { await reopened.session.close(BACKGROUND_CONTEXT); }
  } finally { controller.abort(); await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("aborting and reopening a reviewer retains its exact claim, captured content and native input identity", { timeout: 15_000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-source-review-reopen-")), path = join(directory, "session.sqlite");
  let app = await open(path), running: Promise<boolean> | undefined;
  const controller = new AbortController(), hold = gate(), started = gate();
  try {
    await admit(app.sources, [event("retained", "retained-thread")]);
    let originalGroups: EvidenceGroup[] = [], originalContent = "";
    app.faux.setResponses([async (input, options) => { originalGroups = groups(input); originalContent = userText(input);
      options?.signal?.addEventListener("abort", hold.release, { once: true }); started.release(); await hold.promise; return fauxAssistantMessage(""); }]);
    running = app.reviews.review(0, controller.signal); const aborted = assert.rejects(running); await started.promise;
    const active = (await app.reviews.diagnostics()).active_reviews[0]; assert.ok(active);
    const conversation = await app.tasks.conversation(active.conversation_id), request = `source-review:${active.id}:0`;
    const original = await app.session.commit(tx => tx.submissionByRequest(conversation.id, request), BACKGROUND_CONTEXT); assert.ok(original);
    controller.abort(); await aborted; running = undefined; assert.equal(app.bound.size, 0);
    await admit(app.sources, [event("later", "later-thread")]);
    await app.session.close(BACKGROUND_CONTEXT); app = await open(path);
    const restored = (await app.reviews.diagnostics()).active_reviews[0]; assert.ok(restored);
    assert.equal(restored.id, active.id); assert.equal(restored.conversation_id, active.conversation_id); assert.equal(restored.events, 1);
    const retained = await app.session.commit(tx => tx.submissionByRequest(conversation.id, request), BACKGROUND_CONTEXT);
    assert.equal(retained?.id, original.id);
    app.faux.setResponses([input => { assert.deepEqual(groups(input), originalGroups); assert.equal(userText(input), originalContent);
      return answer(dismissed(groups(input))); }, fauxAssistantMessage("")]);
    assert.equal(await app.reviews.review(0, new AbortController().signal), true);
    assert.equal((await app.session.commit(tx => tx.submissionByRequest(conversation.id, request), BACKGROUND_CONTEXT))?.id, original.id);
    assert.deepEqual((await app.sources.events({ pending: true })).map(record => record.event.id), ["later"]);
  } finally { hold.release(); controller.abort(); await Promise.allSettled([running]); await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("a due follow-up retains one admitted input and retries a failed wake after reopen", { timeout: 15_000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-source-followup-retry-")), path = join(directory, "session.sqlite"), clock = new FakeClock(Date.now());
  let app = await open(path, { workers: 1 }, [], clock), wakeCalls = 0;
  const failed = gate();
  try {
    const task = await app.tasks.create({ id: "due-owner", title: "Existing due task", outcome: "Follow the existing authorization", execution: "coordinator" });
    await app.tasks.update(task.id, current => { current.state = { state: "waiting", next_check: new Date(Date.now() - 1).toISOString(), actor: "example-provider" }; });
    app.tasks.wakeBackground = async () => { wakeCalls++; failed.release(); throw new Error("The transport is temporarily unavailable"); };
    await app.reviews.start(); await failed.promise;
    const admitted = (await app.tasks.get(task.id)).pending; assert.equal(admitted.length, 1); const input = admitted[0]; assert.ok(input);
    await pump(async () => (await app.session.inspect(BACKGROUND_CONTEXT)).tasks.some(item =>
      item.record.kind === "agent.source.effects" && item.state.kind === "sleeping"));
    await app.session.close(BACKGROUND_CONTEXT); app = await open(path, { workers: 1 }, [], clock);
    app.tasks.wakeBackground = async id => { wakeCalls++; return app.tasks.get(id); };
    await app.reviews.start(); clock.advance(10_000); await pump(() => wakeCalls === 2);
    assert.deepEqual((await app.tasks.get(task.id)).pending, admitted,
      "The retained wake retries its existing outbox input rather than admitting a replacement follow-up");
    await pump(async () => !(await app.session.inspect(BACKGROUND_CONTEXT)).tasks.some(item => item.record.kind === "agent.source.effects"));
    clock.advance(30_000); await pump(async () => (await app.reviews.diagnostics()).active_reviews.length === 0);
    assert.equal(wakeCalls, 2); assert.equal((await app.tasks.get(task.id)).pending[0]?.id, input.id);
  } finally { await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("continuous fresh arrivals cannot starve an old original inside the same hot thread", { timeout: 10_000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-source-hot-thread-")), app = await open(join(directory, "session.sqlite"),
    { workers: 2, maximumEvents: 1, maximumEntities: 1 }), controller = new AbortController();
  try {
    await admit(app.sources, [{ ...event("day-old", "continuously-hot"), occurred_at: Date.now() - 86_400_000 }]);
    assert.equal(await app.reviews.review(0, controller.signal), false,
      "The dedicated fresh actor must stay available when only history is pending");
    assert.equal(app.faux.state.callCount, 0);
    const reviewed: string[] = [];
    for (let arrival = 0; arrival < 4 && !reviewed.includes("day-old"); arrival++) {
      await admit(app.sources, [event("fresh-" + arrival, "continuously-hot")]);
      app.faux.setResponses([input => { const captured = groups(input), originals = captured.flatMap(group => group.originals);
        assert.equal(originals.length, 1); reviewed.push(...originals); return answer(dismissed(captured)); }, fauxAssistantMessage("")]);
      assert.equal(await app.reviews.review(0, controller.signal), true);
    }
    assert.ok(reviewed.includes("day-old"), "Alternating indexed ends must advance history despite an arrival before every claim");
    assert.ok(reviewed.some(id => id.startsWith("fresh-"))); assert.equal(new Set(reviewed).size, reviewed.length);
  } finally { controller.abort(); await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("reviewers load current live guidance and workflow policy and reject judgments after a failed refresh", { timeout: 15_000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-source-review-guidance-")), controller = new AbortController();
  let version = "ONE", unavailable = false;
  const call: HostCall = async command => {
    if (command.type === "reload_guidance") return unavailable ? { type: "failed", message: "Synthetic current guidance is unavailable" }
      : { type: "shared_guidance", global: { text: "CURRENT_GLOBAL_" + version, sources: [] }, workflow_policy: "CURRENT_WORKFLOW_" + version,
        context_repositories: [], context_sources: [] };
    if (command.type === "project_guidance") return { type: "project_guidance", snapshot: { text: "CURRENT_PROJECT_" + version, sources: [] } };
    throw new Error("A reviewer cannot use guidance refresh to perform provider effects");
  };
  const app = await open(join(directory, "session.sqlite"), { workers: 1 }, [], undefined, call);
  try {
    const environment = new CoordinatorEnvironment(app.session);
    await environment.configure({ node: null, settings: { model: fixtureModel, instructions: "STALE_RETAINED_GUIDANCE",
      context_repositories: [], sources: [] } });
    environment.useLiveGuidance(); app.tasks.useEnvironment(environment);
    const current = (input: TranscriptContext, expected: string) => {
      const prompt = getCurrentSystemPrompt(input.messages);
      for (const scope of ["GLOBAL", "WORKFLOW", "PROJECT"]) assert.match(prompt, new RegExp("CURRENT_" + scope + "_" + expected));
      assert.doesNotMatch(prompt, /STALE_RETAINED_GUIDANCE/u);
      for (const previous of ["ONE", "TWO", "THREE"]) if (previous !== expected) assert.doesNotMatch(prompt, new RegExp("CURRENT_(?:GLOBAL|WORKFLOW|PROJECT)_" + previous));
    };
    await admit(app.sources, [event("guided-original")]);
    app.faux.setResponses([
      input => { current(input, "ONE"); version = "TWO";
        return fauxAssistantMessage(fauxToolCall("list_source_workflows", {}), { stopReason: "toolUse" }); },
      input => { current(input, "TWO"); toolResult(input, "list_source_workflows"); return answer(dismissed(groups(input))); },
      input => { current(input, "TWO"); toolResult(input); return fauxAssistantMessage(""); },
    ]);
    assert.equal(await app.reviews.review(0, controller.signal), true);
    await admit(app.sources, [event("blocked-original")]); unavailable = true;
    app.faux.setResponses([
      input => { const prompt = getCurrentSystemPrompt(input.messages); assert.match(prompt, /Workspace guidance is unavailable/u);
        assert.doesNotMatch(prompt, /CURRENT_GLOBAL_TWO/u); return answer(dismissed(groups(input))); },
      async input => { assert.match(toolResult(input, "review_sources", true), /guidance is unavailable/u);
        assert.deepEqual((await app.sources.events({ pending: true })).map(record => record.event.id), ["blocked-original"]);
        assert.deepEqual(await app.tasks.list(), []); return fauxAssistantMessage(""); },
    ]);
    await assert.rejects(app.reviews.review(0, controller.signal), /unaccounted/u);
    assert.equal((await app.reviews.diagnostics()).metrics.completed_reviews, 1);
    unavailable = false; version = "THREE";
    app.faux.setResponses([input => { current(input, "THREE"); return answer(dismissed(groups(input))); }, fauxAssistantMessage("")]);
    assert.equal(await app.reviews.review(0, controller.signal), true); assert.deepEqual(await app.sources.events({ pending: true }), []);
  } finally { controller.abort(); await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("a provider signal needing a user decision publishes once without manufacturing an investigation task", { timeout: 15_000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-source-direct-notice-")), path = join(directory, "session.sqlite");
  let app = await open(path, { workers: 1 });
  const controller = new AbortController(), text = "The proposed review needs a decision.";
  let proposals: SourceReviewDecision[] = [], conversation = "";
  try {
    await admit(app.sources, [event("needed-decision", "approval-thread", "Can the proposed review proceed?")]);
    app.faux.setResponses([
      input => { proposals = [{ type: "notify", groups: groups(input).map(group => group.id), keys: [], task_id: null, text }];
        return answer(proposals, "notify-provider"); },
      async input => { assert.deepEqual(JSON.parse(toolResult(input)), { accounted: 1, superseded: 0 });
        conversation = (await app.reviews.diagnostics()).active_reviews[0]?.conversation_id ?? ""; return fauxAssistantMessage(""); },
    ]);
    assert.equal(await app.reviews.review(0, controller.signal), true);
    assert.deepEqual(await app.reviews.apply(conversation, "notify-provider", proposals), { accounted: 1, superseded: 0 });
    await pump(async () => !(await app.session.inspect(BACKGROUND_CONTEXT)).tasks.some(task =>
      task.record.kind === "agent.source.effects"));
    assert.deepEqual(await app.tasks.list(), []); assert.deepEqual(await app.sources.events({ pending: true }), []);
    await app.session.close(BACKGROUND_CONTEXT); app = await open(path, { workers: 1 });
    const root = await app.session.root(BACKGROUND_CONTEXT);
    const notices = (await root.entries({ order: "ascending" }, 100, undefined, BACKGROUND_CONTEXT)).items.filter(UserNoticeEntry.is);
    assert.equal(notices.length, 1); assert.equal(notices[0]?.data.text, text);
  } finally { controller.abort(); await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});
