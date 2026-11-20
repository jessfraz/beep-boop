import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { createModels, fauxAssistantMessage, fauxProvider, fauxToolCall, type TranscriptContext } from "@earendil-works/pi-ai";
import { getCurrentSystemPrompt } from "@earendil-works/pi-ai/utils/transcript";
import { createRegistry, defineDoc, Harness, type TaskId } from "@earendil-works/pi-durable";
import { SqliteStorage } from "@earendil-works/pi-durable/storage/sqlite";
import { commitWrites } from "@earendil-works/pi-durable/testing";
import { openConversationDatabase } from "../src/conversation-database.js";
import { CoordinatorEnvironment } from "../src/environment.js";
import type { ScheduledJobStatus } from "../src/generated/ScheduledJobStatus.js";
import type { SourceEvent } from "../src/generated/SourceEvent.js";
import type { SourceReviewDecision } from "../src/generated/SourceReviewDecision.js";
import type { TaskOutcome } from "../src/generated/TaskOutcome.js";
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
async function open(path: string, options: SourceReviewOptions = {}, workflows: ScheduledJobStatus[] = [], clock?: FakeClock,
  guidance?: HostCall, ready?: (signal: AbortSignal) => Promise<void>) {
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
    ...(ready === undefined ? {} : { ready }),
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
  }, { workers: 2, ...options });
  registry.install(reviews.tools);
  await session.root(BACKGROUND_CONTEXT); await tasks.prepare(); await sources.prepare();
  return { session, storage, tasks, sources, reviews, faux, bound };
}

test("reserved email actor drains old and new mail across accounts while three Slack reviews are held", { timeout: 15_000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-priority-email-")), app = await open(join(directory, "session.sqlite"), { workers: 4, maximumEntities: 1 });
  const hold = gate(), started = gate(), controller = new AbortController();
  let running: Promise<boolean>[] = [], slack = 0;
  try {
    const now = Date.now();
    await admit(app.sources, [event("seed-mail", "seed-thread", "Invented warm-up mail", "google")]);
    app.faux.setResponses([input => answer(dismissed(groups(input)), "seed-mail"), fauxAssistantMessage("")]);
    assert.equal(await app.reviews.review(0, controller.signal), true);
    await admit(app.sources, [event("slack-fresh-a"), event("slack-fresh-b"), { ...event("slack-old"), occurred_at: now - 3_600_000 }]);
    const wait = async (input: TranscriptContext) => { if (++slack === 3) started.release(); await hold.promise; return answer(dismissed(groups(input)), "slack-" + slack); };
    let mail = 0; const reviewedMail: string[][] = [];
    const email = (input: TranscriptContext) => {
      reviewedMail.push(groups(input).flatMap(group => group.originals)); mail++;
      return answer(dismissed(groups(input)), "mail-" + mail);
    };
    app.faux.setResponses([wait, wait, wait, email, fauxAssistantMessage(""), email, fauxAssistantMessage(""),
      fauxAssistantMessage(""), fauxAssistantMessage(""), fauxAssistantMessage("")]);
    running = [1, 2, 3].map(slot => app.reviews.review(slot, controller.signal));
    await started.promise;
    for (const [id, account, age] of [["old-mail", "mail-a", 48 * 3_600_000], ["new-mail", "mail-b", 0]] satisfies [string, string, number][]) {
      const message = event(id, id, "Important invented email", "google");
      message.source.account = account; message.occurred_at = now - age; await admit(app.sources, [message]);
    }
    assert.equal(await app.reviews.review(0, controller.signal), true);
    assert.deepEqual(reviewedMail[0], ["new-mail"], "Reserved email review prioritizes fresh mail while Slack is busy");
    assert.equal(await app.reviews.review(0, controller.signal), true, "Reserved email review must also progress historical mail");
    assert.deepEqual(reviewedMail[1], ["old-mail"], "Reserved email review rotates its accounts");
    assert.equal(mail, 2); assert.equal(slack, 3);
    assert.deepEqual(await app.sources.events({ provider: "google", pending: true }), []);
    const diagnostics = await app.reviews.diagnostics();
    for (const account of ["mail-a", "mail-b"]) {
      const stats = diagnostics.streams.find(stream => stream.source.provider === "google" && stream.source.account === account)?.review;
      assert.ok(stats); assert.equal(stats.reviewed_events, 1); assert.equal(stats.completed_reviews, 1);
      assert.ok(stats.input_tokens + stats.output_tokens > 0); assert.ok(stats.sampled_model_requests >= 2);
      assert.equal(stats.expired_events, 0); assert.equal(stats.coalesced_events, 0);
    }
    assert.equal(await app.reviews.review(0, controller.signal), false, "Idle email capacity must not be taken by Slack");
    assert.equal((await app.reviews.diagnostics()).active_reviews.length, 3);
    hold.release(); assert.deepEqual(await Promise.all(running), [true, true, true]); running = [];
  } finally { hold.release(); controller.abort(); await Promise.allSettled(running); await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("email prioritizes fresh arrivals in an old hot thread while reserving historical progress", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-email-history-fairness-")), path = join(directory, "session.sqlite");
  let app = await open(path, { workers: 4, maximumEntities: 1, maximumEvents: 1 });
  const controller = new AbortController(), selected: string[] = [];
  const email = (id: string, occurred: number) => ({ ...event(id, "mail-thread", "Invented mail evidence", "google"), occurred_at: occurred });
  try {
    const now = Date.now();
    await admit(app.sources, [email("old-0", now - 3 * 86_400_000), email("old-1", now - 2 * 86_400_000)]);
    for (let index = 0; index < 8; index++) {
      await admit(app.sources, [email("fresh-" + index, Date.now())]);
      app.faux.setResponses([input => { const captured = groups(input); selected.push(...captured.flatMap(group => group.originals));
        return answer(dismissed(captured), "email-fairness-" + index); }, fauxAssistantMessage("")]);
      assert.equal(await app.reviews.review(0, controller.signal), true);
      if (index === 2) { await app.session.close(BACKGROUND_CONTEXT); app = await open(path, { workers: 4, maximumEntities: 1, maximumEvents: 1 }); }
    }
    assert.deepEqual(selected, ["fresh-0", "fresh-1", "fresh-2", "old-0", "fresh-4", "fresh-5", "fresh-6", "old-1"]);
    assert.deepEqual(new Set((await app.sources.events({ provider: "google", pending: true })).map(record => record.event.id)), new Set(["fresh-3", "fresh-7"]));
    assert.equal((await app.reviews.diagnostics()).streams.find(stream => stream.source.provider === "google")?.review.expired_events, 0);
  } finally { controller.abort(); await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

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

test("new local task outcomes reach the dedicated fresh reviewer and retain their first publication time across duplicate replay and reopen", async t => {
  const directory = await mkdtemp(join(tmpdir(), "agent-outcome-freshness-")), path = join(directory, "session.sqlite");
  let app = await open(path, { workers: 2 }); const controller = new AbortController();
  try {
    const task = await app.tasks.create({ title: "Completed local task", outcome: "Retain the local result" });
    const outcome: TaskOutcome = { task_id: task.id, conversation_id: task.conversation_id, entry_id: null, tool_call_id: null,
      summary: "A current local result", sources: [], state: { state: "progress" } };
    const before = Date.now(); await app.session.commit(tx => app.sources.recordIn(tx, outcome, false), BACKGROUND_CONTEXT); const after = Date.now();
    const first = (await app.sources.events({ provider: "task_outcomes" }))[0]; assert.ok(first);
    t.mock.timers.enable({ apis: ["Date"], now: after + 60_000 });
    try { await app.session.commit(tx => app.sources.recordIn(tx, outcome, false), BACKGROUND_CONTEXT); }
    finally { t.mock.timers.reset(); }
    await app.session.close(BACKGROUND_CONTEXT); app = await open(path, { workers: 2 });
    app.faux.setResponses([input => { assert.deepEqual(groups(input).flatMap(group => group.keys), [first.key]); return answer(dismissed(groups(input))); }, fauxAssistantMessage("")]);
    assert.equal(await app.reviews.review(0, controller.signal), true, "A new local outcome must be selectable by the reserved fresh actor");
    assert.ok(first.event.occurred_at !== undefined && first.event.occurred_at >= before && first.event.occurred_at <= after);
    const saved = await app.sources.originals([first.key]); assert.equal(saved.length, 1); assert.deepEqual(saved[0]?.event, first.event);
    assert.equal(saved[0]?.received_at, undefined); assert.deepEqual(saved[0]?.outcome, outcome);
    const metrics = (await app.reviews.diagnostics()).metrics; assert.equal(metrics.admitted_events, 1); assert.equal(metrics.decided_events, 1);
  } finally { controller.abort(); await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("new undated provider evidence reaches the fresh reviewer without inventing a provider timestamp", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-undated-freshness-")), app = await open(join(directory, "session.sqlite"), { workers: 2 });
  const controller = new AbortController();
  try {
    const original = event("new-undated", "undated-thread"); delete original.occurred_at;
    const before = Date.now(); await admit(app.sources, [original]); const after = Date.now();
    const pending = (await app.sources.events({ pending: true }))[0]; assert.ok(pending);
    assert.equal((await app.reviews.diagnostics()).streams.find(item => item.source.provider === "slack")?.oldest_pending_at, null);
    app.faux.setResponses([input => { assert.deepEqual(groups(input).flatMap(group => group.keys), [pending.key]); return answer(dismissed(groups(input))); }, fauxAssistantMessage("")]);
    assert.equal(await app.reviews.review(0, controller.signal), true, "First receipt supplies freshness when the provider has no usable clock");
    assert.ok(pending.received_at !== undefined && pending.received_at >= before && pending.received_at <= after);
    assert.deepEqual(pending.event, original); assert.equal(pending.event.occurred_at, undefined); assert.equal(pending.occurred_at, undefined);
  } finally { controller.abort(); await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("reviewing new evidence for an existing source owner prepares serializable context and completes its durable decision", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-existing-source-owner-")), path = join(directory, "session.sqlite");
  let app = await open(path, { workers: 2 }); const controller = new AbortController();
  try {
    const original = event("new-owner-evidence", "existing-owned-thread");
    const owner = await app.tasks.create({ title: "Existing source owner", outcome: "Continue the authorized work", model: fixtureModel, sources: [original.source] });
    await app.tasks.update(owner.id, task => { task.summary = "Existing verified task progress"; });
    await admit(app.sources, [original]);
    const pending = (await app.sources.events({ pending: true }))[0]; assert.ok(pending);
    app.faux.setResponses([
      input => {
        const prepared: unknown = JSON.parse(userText(input)); assert.ok(object(prepared) && Array.isArray(prepared.related_tasks));
        assert.deepEqual(prepared.related_tasks, [{ id: owner.id, title: owner.title, title_complete: true, state: { state: "open" }, state_complete: true,
          sources: [original.source], source_count: 1, sources_complete: true, summary: "Existing verified task progress", summary_complete: true }]);
        return answer(dismissed(groups(input), "The owning task already accounted for this exact evidence"));
      },
      input => { toolResult(input); return fauxAssistantMessage(""); },
    ]);
    assert.equal(await app.reviews.review(0, controller.signal), true);
    assert.equal(app.faux.state.callCount, 2);
    const metrics = (await app.reviews.diagnostics()).metrics;
    assert.ok(metrics.input_bytes > 0); assert.equal(metrics.completed_reviews, 1); assert.equal(metrics.failed_reviews, 0);
    await app.session.close(BACKGROUND_CONTEXT); app = await open(path, { workers: 2 });
    assert.deepEqual((await app.sources.originals([pending.key]))[0]?.decision,
      { type: "dismissed", reason: "The owning task already accounted for this exact evidence" });
    assert.deepEqual((await app.tasks.get(owner.id)).sources, [original.source]);
  } finally { controller.abort(); await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("review context and task reads retain captured ownership without expanding unrelated historical source links", { timeout: 30_000 }, async t => {
  const directory = await mkdtemp(join(tmpdir(), "agent-bounded-review-context-")), path = join(directory, "session.sqlite");
  let app = await open(path, { workers: 2 }); const controller = new AbortController();
  try {
    const first = event("captured-first", "first-thread"), shared = event("captured-shared", "shared-thread"), unowned = event("captured-unowned", "unowned-thread");
    const unrelated = Array.from({ length: 512 }, (_, index) => ({ provider: "mail", account: "retained-example",
      thread_id: "retained-source-link-" + index + "-original-evidence" }));
    const firstSources = [...unrelated, first.source, shared.source], secondSources = [...unrelated.slice(0, 256), shared.source];
    const firstOwner = await app.tasks.create({ id: "context-owner-a", title: "First existing owner", outcome: "Retain its actual work", model: fixtureModel, sources: firstSources });
    const secondOwner = await app.tasks.create({ id: "context-owner-b", title: "Second existing owner", outcome: "Keep its own scope", model: fixtureModel, sources: secondSources });
    await admit(app.sources, [first, shared, unowned]);
    let preparedText = "", readText = "";
    app.faux.setResponses([
      input => { preparedText = userText(input); return fauxAssistantMessage(fauxToolCall("read_source_task", { task_id: firstOwner.id }), { stopReason: "toolUse" }); },
      input => { readText = toolResult(input, "read_source_task"); return answer(dismissed(groups(input))); },
      input => { toolResult(input); return fauxAssistantMessage(""); },
    ]);
    assert.equal(await app.reviews.review(0, controller.signal), true);
    t.diagnostic(JSON.stringify({ captured_events: 3, retained_task_sources: [firstSources.length, secondSources.length],
      input_bytes: Buffer.byteLength(preparedText), task_read_bytes: Buffer.byteLength(readText) }));
    assert.ok(Buffer.byteLength(preparedText) < 12_000, "Unrelated historical task links must not inflate the captured model input");
    assert.ok(Buffer.byteLength(readText) < 4000, "Reading a related task must not reintroduce its retained source-link corpus");
    assert.doesNotMatch(preparedText + readText, /retained-source-link-/u);
    const prepared: unknown = JSON.parse(preparedText); assert.ok(object(prepared) && Array.isArray(prepared.related_tasks));
    const related = prepared.related_tasks; assert.equal(related.length, 2);
    const firstContext = related.find((item: unknown) => object(item) && item.id === firstOwner.id);
    const secondContext = related.find((item: unknown) => object(item) && item.id === secondOwner.id);
    assert.ok(object(firstContext) && Array.isArray(firstContext.sources)); assert.ok(object(secondContext) && Array.isArray(secondContext.sources));
    assert.deepEqual(firstContext.sources, [first.source, shared.source]);
    assert.deepEqual(secondContext.sources, [shared.source]);
    assert.equal(firstContext.source_count, firstSources.length); assert.equal(secondContext.source_count, secondSources.length);
    assert.equal(firstContext.sources_complete, false); assert.equal(secondContext.sources_complete, false);
    const read: unknown = JSON.parse(readText); assert.ok(object(read) && Array.isArray(read.sources));
    assert.deepEqual(read.sources, [first.source, shared.source]);
    assert.equal(read.source_count, firstSources.length); assert.equal(read.sources_complete, false);
    assert.deepEqual((await app.sources.events()).map(record => record.event), [unowned, shared, first]);
    await app.session.close(BACKGROUND_CONTEXT); app = await open(path, { workers: 2 });
    assert.deepEqual((await app.tasks.get(firstOwner.id)).sources, firstSources);
    assert.deepEqual((await app.tasks.get(secondOwner.id)).sources, secondSources);
    assert.deepEqual(await app.sources.events({ pending: true }), []);
  } finally { controller.abort(); await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
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

test("retained GitHub decisions preserve their owner while configured workflow evidence gets a separate work item", { timeout: 15_000 }, async () => {
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
      const records = await reopened.tasks.list(); assert.equal(records.length, 3);
      const saved = await reopened.tasks.get(workflow.id); assert.equal(saved.pending.length, 0);
      assert.deepEqual(saved.sources, workflow.sources);
      const work = records.filter(record => record.workflow?.owner_task_id === workflow.id); assert.equal(work.length, 1);
      const item = work[0]; assert.ok(item); assert.notEqual(item.conversation_id, workflow.conversation_id);
      assert.deepEqual(item.workflow?.job, job.job); assert.equal(item.workflow?.work_key, "github:example/project:77");
      assert.equal(item.pending.length, 1); assert.equal(item.sources.length, 2);
      assert.match(item.pending[0]?.content ?? "", /workflow-a/u); assert.match(item.pending[0]?.content ?? "", /workflow-b/u);
      assert.equal((await reopened.tasks.get("retained-github-owner")).pending.length, 1,
        "A shared resource cannot replace the explicitly selected configured workflow's actor");
      assert.deepEqual(await reopened.sources.events({ pending: true }), []);
    } finally { await reopened.session.close(BACKGROUND_CONTEXT); }
  } finally { controller.abort(); await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("critical mail packets isolate old holds, preserve model priority and serialize only the verified parcel", { timeout: 15_000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-critical-mail-packets-"));
  const job: ScheduledJobStatus = { job: { id: "delivery-review", title: "Delivery review", prompt: "Save current verified shipment evidence.",
    priority: "critical", model: fixtureModel, timezone: "UTC", schedules: [], revision: "one", cwd: "/workspace/example" }, schedules: [], task_id: "delivery-owner" };
  const app = await open(join(directory, "session.sqlite"), { maximumEntities: 1 }, [job]), controller = new AbortController();
  app.tasks.wakeBackground = id => app.tasks.get(id);
  try {
    const owner = await app.tasks.create({ id: "delivery-owner", title: job.job.title, outcome: job.job.prompt,
      sources: [{ provider: "scheduled_jobs", account: "local", thread_id: job.job.id }] });
    await app.tasks.update(owner.id, task => { task.summary = "OLD_BROWSER_HOLD"; task.state = { state: "waiting", next_check: null, actor: "Old parcel inspection" }; });
    for (const [id, label] of [["fresh-a", "123456789012"], ["fresh-b", "223456789012"], ["fresh-a-update", "123456789012"]]) {
      await admit(app.sources, [event(id!, id!, "Verified label https://www.fedex.com/fedextrack/?trknbr=" + label, "google")]);
      app.faux.setResponses([input => answer([routed(groups(input), { job_id: job.job.id })]), fauxAssistantMessage("")]);
      assert.equal(await app.reviews.review(0, controller.signal), true);
    }
    const packets = (await app.tasks.list()).filter(task => task.workflow?.owner_task_id === owner.id);
    assert.equal(packets.length, 3); assert.equal(new Set(packets.map(task => task.conversation_id)).size, 3);
    assert.deepEqual(packets.map(task => task.workflow?.work_key).sort(), ["shipment:fedex:123456789012", "shipment:fedex:123456789012", "shipment:fedex:223456789012"]);
    for (const packet of packets) {
      assert.equal(packet.pending.length, 1); assert.equal(packet.sources.length, 1);
      assert.doesNotMatch(packet.pending[0]?.content ?? "", /OLD_BROWSER_HOLD/u);
      const conversation = await app.tasks.conversation(packet.conversation_id);
      await app.session.commit(async tx => {
        assert.equal(await app.tasks.inference.workClassIn(tx, conversation), "critical");
        assert.equal(await app.tasks.inference.workGroupIn(tx, conversation), job.job.id);
      }, BACKGROUND_CONTEXT);
    }
    assert.equal((await app.tasks.get(owner.id)).pending.length, 0);
    assert.deepEqual(await app.sources.events({ pending: true }), []);
  } finally { controller.abort(); await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("startup replaces a terminal reviewer while retaining its exact claim, captured content and native input identity", { timeout: 15_000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-source-review-reopen-")), path = join(directory, "session.sqlite");
  const clock = new FakeClock(Date.now()), ready = gate();
  let app = await open(path, { workers: 1 }, [], clock, undefined, async signal => {
    signal.addEventListener("abort", ready.release, { once: true }); await ready.promise; signal.throwIfAborted();
  }), running: Promise<boolean> | undefined;
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
    await app.reviews.start();
    await pump(async () => (await app.session.inspect(BACKGROUND_CONTEXT)).tasks.some(task => task.record.kind === "agent.source.reviewer" && task.state.kind === "running"));
    const worker = (await app.session.inspect(BACKGROUND_CONTEXT)).tasks.find(task => task.record.kind === "agent.source.reviewer")?.record;
    assert.ok(worker && worker.state.status !== "terminal" && worker.state.checkpoint !== undefined);
    await app.session.close(BACKGROUND_CONTEXT);
    const storage = await SqliteStorage.open(await openConversationDatabase(path));
    try {
      await commitWrites(storage, [{ type: "task", value: { ...worker, restarts: 5,
        state: { status: "pending", checkpoint: worker.state.checkpoint } } }], BACKGROUND_CONTEXT);
    } finally { await storage.close(BACKGROUND_CONTEXT); }
    app = await open(path, { workers: 1 }, [], clock);
    const failedWorker = await app.session.waitForTask(worker.id, BACKGROUND_CONTEXT);
    assert.equal(failedWorker.state.outcome.status, "faulted");
    const restored = (await app.reviews.diagnostics()).active_reviews[0]; assert.ok(restored);
    assert.equal(restored.id, active.id); assert.equal(restored.conversation_id, active.conversation_id); assert.equal(restored.events, 1);
    const retained = await app.session.commit(tx => tx.submissionByRequest(conversation.id, request), BACKGROUND_CONTEXT);
    assert.equal(retained?.id, original.id);
    app.faux.setResponses([input => { assert.deepEqual(groups(input), originalGroups); assert.equal(userText(input), originalContent);
      return answer(dismissed(groups(input))); }, fauxAssistantMessage("")]);
    await app.reviews.start();
    await pump(async () => (await app.reviews.diagnostics()).active_reviews.length === 0);
    assert.equal((await app.session.commit(tx => tx.submissionByRequest(conversation.id, request), BACKGROUND_CONTEXT))?.id, original.id);
    assert.deepEqual((await app.sources.events({ pending: true })).map(record => record.event.id), ["later"]);
    assert.deepEqual(await app.session.getTask(worker.id, BACKGROUND_CONTEXT), failedWorker);
    const replacement = (await app.session.inspect(BACKGROUND_CONTEXT)).tasks.find(task => task.record.kind === "agent.source.reviewer")?.record;
    assert.ok(replacement); assert.notEqual(replacement.id, worker.id);
  } finally { ready.release(); hold.release(); controller.abort(); await Promise.allSettled([running]); await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("startup replaces a terminal source effect and retains its already admitted follow-up", { timeout: 15_000 }, async () => {
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
    const effect = (await app.session.inspect(BACKGROUND_CONTEXT)).tasks.find(item => item.record.kind === "agent.source.effects")?.record;
    assert.ok(effect?.state.status === "sleeping");
    await app.session.close(BACKGROUND_CONTEXT);
    const storage = await SqliteStorage.open(await openConversationDatabase(path));
    try {
      await commitWrites(storage, [{ type: "task", value: { ...effect, restarts: 5,
        state: { status: "pending", checkpoint: effect.state.checkpoint } } }], BACKGROUND_CONTEXT);
    } finally { await storage.close(BACKGROUND_CONTEXT); }
    app = await open(path, { workers: 1 }, [], clock);
    const failedEffect = await app.session.waitForTask(effect.id, BACKGROUND_CONTEXT);
    assert.equal(failedEffect.state.outcome.status, "faulted");
    app.tasks.wakeBackground = async id => { wakeCalls++; return app.tasks.get(id); };
    await app.reviews.start(); clock.advance(10_000); await pump(() => wakeCalls === 2);
    assert.deepEqual((await app.tasks.get(task.id)).pending, admitted,
      "The retained wake retries its existing outbox input rather than admitting a replacement follow-up");
    await pump(async () => !(await app.session.inspect(BACKGROUND_CONTEXT)).tasks.some(item => item.record.kind === "agent.source.effects"));
    clock.advance(30_000); await pump(async () => (await app.reviews.diagnostics()).active_reviews.length === 0);
    assert.equal(wakeCalls, 2); assert.equal((await app.tasks.get(task.id)).pending[0]?.id, input.id);
    assert.deepEqual(await app.session.getTask(effect.id, BACKGROUND_CONTEXT), failedEffect,
      "Recovery must create new native work, never rewrite the terminal task");
    const effects = await app.session.commit(tx => tx.scanTasks({ kind: "agent.source.effects" }, 10), BACKGROUND_CONTEXT);
    assert.equal(effects.items.length, 2);
    assert.ok(effects.items.some(item => item.id !== effect.id && item.state.status === "terminal" && item.state.outcome.status === "completed"));
  } finally { await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("startup continues a terminal claim effect after its published notice without repeating task admission", { timeout: 15_000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-source-effect-cursor-")), path = join(directory, "session.sqlite"), clock = new FakeClock(Date.now());
  let app = await open(path, { workers: 1 }, [], clock), wakeCalls = 0;
  const failed = gate();
  try {
    await admit(app.sources, [event("decision", "decision-thread"), event("work", "work-thread")]);
    app.faux.setResponses([input => {
      const captured = groups(input), notice = captured.find(group => group.threads.includes("decision-thread"));
      const work = captured.find(group => group.threads.includes("work-thread")); assert.ok(notice && work);
      return answer([{ type: "notify", groups: [notice.id], keys: [], task_id: null, text: "A retained decision needs attention." }, routed([work])]);
    }, fauxAssistantMessage("")]);
    app.tasks.wakeBackground = async () => { wakeCalls++; failed.release(); throw new Error("The worker transport is temporarily unavailable"); };
    await app.reviews.start(); await failed.promise;
    await pump(async () => (await app.session.inspect(BACKGROUND_CONTEXT)).tasks.some(item => item.record.kind === "agent.source.effects" && item.state.kind === "sleeping"));
    const effect = (await app.session.inspect(BACKGROUND_CONTEXT)).tasks.find(item => item.record.kind === "agent.source.effects")?.record;
    assert.ok(effect?.state.status === "sleeping");
    const task = (await app.tasks.list())[0]; assert.ok(task); assert.equal(task.pending.length, 1);
    const root = await app.session.root(BACKGROUND_CONTEXT);
    const notices = (await root.entries({}, 100, undefined, BACKGROUND_CONTEXT)).items.filter(entry => UserNoticeEntry.is(entry));
    assert.equal(notices.length, 1, "The first effect must be durably published before the failed wake");
    await app.session.close(BACKGROUND_CONTEXT);
    const storage = await SqliteStorage.open(await openConversationDatabase(path));
    try {
      await commitWrites(storage, [{ type: "task", value: { ...effect, restarts: 5,
        state: { status: "pending", checkpoint: effect.state.checkpoint } } }], BACKGROUND_CONTEXT);
    } finally { await storage.close(BACKGROUND_CONTEXT); }
    app = await open(path, { workers: 1 }, [], clock);
    const failedEffect = await app.session.waitForTask(effect.id, BACKGROUND_CONTEXT);
    assert.equal(failedEffect.state.outcome.status, "faulted");
    app.tasks.wakeBackground = async id => { wakeCalls++; return app.tasks.get(id); };
    await app.reviews.start(); await pump(() => wakeCalls === 2);
    await pump(async () => (await app.reviews.diagnostics()).active_reviews.length === 0);
    assert.deepEqual((await app.tasks.get(task.id)).pending, task.pending, "Recovery must reuse the admitted task input");
    assert.equal((await app.tasks.list()).length, 1);
    const restoredRoot = await app.session.root(BACKGROUND_CONTEXT);
    assert.deepEqual((await restoredRoot.entries({}, 100, undefined, BACKGROUND_CONTEXT)).items.filter(entry => UserNoticeEntry.is(entry)), notices,
      "The retained effect cursor must skip the already published notice");
    assert.deepEqual(await app.session.getTask(effect.id, BACKGROUND_CONTEXT), failedEffect);
    const effects = await app.session.commit(tx => tx.scanTasks({ kind: "agent.source.effects" }, 10), BACKGROUND_CONTEXT);
    assert.equal(effects.items.length, 2);
    assert.ok(effects.items.some(item => item.id !== effect.id && item.state.status === "terminal" && item.state.outcome.status === "completed"));
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

test("fresh stream rotation cannot reset backfill progress across providers or reopen", { timeout: 15_000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-source-class-fairness-")), path = join(directory, "session.sqlite");
  const options = { workers: 2, maximumEvents: 1, maximumEntities: 1 }, controller = new AbortController();
  const legacyState = defineDoc<{ generation: number; enabled: boolean; workers: TaskId[]; due: TaskId | null;
    slots: (string | null)[]; turn: number; after: string | null }>({ kind: "agent.source.reviews", scope: "session", version: 1,
    initial: () => ({ generation: 0, enabled: false, workers: [], due: null, slots: [null, null], turn: 0, after: null }), checkpointWhen: () => true });
  let app = await open(path, options);
  try {
    await app.session.commit(tx => tx.doc(legacyState), BACKGROUND_CONTEXT);
    await app.session.close(BACKGROUND_CONTEXT); app = await open(path, options);
    const persisted = await app.session.snapshot(legacyState, BACKGROUND_CONTEXT); assert.ok(persisted);
    assert.equal(Object.hasOwn(persisted, "fresh_after"), false, "The existing version-one document has no fresh cursor");
    const old = Date.now() - 86_400_000;
    for (const provider of ["github", "slack"]) await admit(app.sources, Array.from({ length: 6 }, (_, index) =>
      ({ ...event(provider + "-" + index, provider + "-backlog", "Older pending evidence", provider), occurred_at: old + index })));
    const task = await app.tasks.create({ title: "Fresh local results", outcome: "Retain each newly completed result" });
    const backfill: string[] = [], fresh: string[] = [];
    for (let turn = 0; turn < 4; turn++) {
      app.faux.setResponses([input => { const captured = groups(input); backfill.push(...captured.flatMap(group => group.threads));
        return answer(dismissed(captured)); }, fauxAssistantMessage("")]);
      assert.equal(await app.reviews.review(1, controller.signal), true);
      await app.session.commit(tx => app.sources.recordIn(tx, { task_id: task.id, conversation_id: task.conversation_id,
        entry_id: null, tool_call_id: null, summary: "Fresh completed result " + turn, sources: [], state: { state: "progress" } }, false), BACKGROUND_CONTEXT);
      app.faux.setResponses([input => { const captured = groups(input); fresh.push(...captured.flatMap(group => group.threads));
        return answer(dismissed(captured)); }, fauxAssistantMessage("")]);
      assert.equal(await app.reviews.review(0, controller.signal), true);
      if (turn === 0) { await app.session.close(BACKGROUND_CONTEXT); app = await open(path, options); }
    }
    assert.deepEqual(backfill, ["github-backlog", "slack-backlog", "github-backlog", "slack-backlog"],
      "A fresh task_outcomes claim must not steer the next historical turn back to GitHub and starve Slack");
    assert.deepEqual(fresh, Array.from({ length: 4 }, () => task.id));
    assert.equal((await app.sources.events({ provider: "github", pending: true })).length, 4);
    assert.equal((await app.sources.events({ provider: "slack", pending: true })).length, 4);
    assert.deepEqual(await app.sources.events({ provider: "task_outcomes", pending: true }), []);
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
