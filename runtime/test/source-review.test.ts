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
import type { SourceBatch } from "../src/generated/SourceBatch.js";
import type { SourceReviewDecision } from "../src/generated/SourceReviewDecision.js";
import type { TaskOutcome } from "../src/generated/TaskOutcome.js";
import type { TaskRecord } from "../src/generated/TaskRecord.js";
import { ConversationModels } from "../src/models.js";
import { createGuidance } from "../src/guidance.js";
import { SourceReviews, type SourceReviewOptions } from "../src/source-review.js";
import { createSourceTools, SourceJournal } from "../src/sources.js";
import { TaskStore, type HostCall, type SearchQuery } from "../src/tasks.js";
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
function packetEvidence(task: TaskRecord): Record<string, unknown>[] {
  assert.equal(task.pending.length, 1);
  const content = task.pending[0]?.content; assert.ok(content);
  const evidence: unknown = JSON.parse(content.slice(content.lastIndexOf("\n\n") + 2));
  assert.ok(Array.isArray(evidence));
  return evidence.map((item: unknown) => { assert.ok(object(item)); return item; });
}
async function admit(journal: SourceJournal, events: SourceEvent[]): Promise<void> {
  const first = events[0]; assert.ok(first);
  const source = { provider: first.source.provider, account: first.source.account };
  const previous = (await journal.states()).find(state => state.source.provider === source.provider && state.source.account === source.account);
  await journal.admit({ source, events, checkpoint: null, coverage: { status: "complete" } }, previous?.checkpoint ?? null);
}
async function open(path: string, options: SourceReviewOptions = {}, workflows: ScheduledJobStatus[] = [], clock?: FakeClock,
  guidance?: HostCall, ready?: (signal: AbortSignal) => Promise<void>, search?: SearchQuery) {
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
        ...(search === undefined ? {} : { search }),
        call: guidance ?? (async () => { throw new Error("A source reviewer cannot perform provider effects"); }) });
      return () => { bound.delete(id); unbind(); };
    },
  }, { workers: 2, ...options });
  registry.install(reviews.tools);
  await session.root(BACKGROUND_CONTEXT); await tasks.prepare(); await sources.prepare();
  return { session, storage, tasks, sources, reviews, faux, bound, registry, executions };
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

test("source ownership lookup uses current indexes when semantic history is unavailable", { timeout: 15_000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-indexed-source-lookup-")), path = join(directory, "session.sqlite");
  let searches = 0;
  const search: SearchQuery = async () => { searches++; return { candidates: [], gaps: ["Semantic history is unavailable"] }; };
  const app = await open(path, {}, [], undefined, undefined, undefined, search), controller = new AbortController();
  app.tasks.wakeBackground = id => app.tasks.get(id);
  try {
    const original = event("current-direct-mention", "owned-thread", "A direct message mentions <@EXAMPLE> and needs its existing owner.");
    const owner = await app.tasks.create({ id: "current-owner", title: "Current authorized owner", outcome: "Continue the authorized work", sources: [original.source] });
    const unrelated = await app.tasks.create({ id: "text-only-match", title: "Current authorized owner in unrelated history", outcome: original.text });
    await admit(app.sources, [original]);
    let lookup: unknown;
    app.faux.setResponses([
      () => fauxAssistantMessage(fauxToolCall("find_source_tasks", { query: "Current authorized owner", limit: 8 }), { stopReason: "toolUse" }),
      input => { lookup = JSON.parse(toolResult(input, "find_source_tasks")); return answer([routed(groups(input), { task_id: owner.id })]); },
      input => { toolResult(input); return fauxAssistantMessage(""); },
    ]);
    assert.equal(await app.reviews.review(0, controller.signal), true);
    assert.ok(object(lookup) && Array.isArray(lookup.tasks));
    assert.deepEqual(lookup.tasks.map((item: unknown) => object(item) ? item.id : null), [owner.id],
      "A linked current owner must remain discoverable without semantic history");
    assert.equal(searches, 0, "Source triage must not queue semantic history or embedding work");
    assert.equal(lookup.owners_complete, true); assert.equal(lookup.history_searched, false);
    assert.ok(Array.isArray(lookup.gaps)); assert.match(lookup.gaps.join(" "), /absence is not established/u);
    assert.equal((await app.tasks.get(owner.id)).pending.length, 1); assert.equal((await app.tasks.get(unrelated.id)).pending.length, 0);
    await app.session.close(BACKGROUND_CONTEXT);
    const reopened = await open(path);
    try {
      assert.deepEqual((await reopened.sources.events())[0]?.decision, { type: "task", task_id: owner.id });
      assert.deepEqual((await reopened.sources.events())[0]?.event, original);
      assert.equal((await reopened.tasks.get(owner.id)).pending.length, 1);
    } finally { await reopened.session.close(BACKGROUND_CONTEXT); }
  } finally { controller.abort(); await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("indexed ownership lookup reports truncation and narrows only to captured groups", { timeout: 15_000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-scoped-source-lookup-")), app = await open(join(directory, "session.sqlite"), { maximumEntities: 3 });
  const controller = new AbortController();
  try {
    const crowded = event("crowded", "crowded-thread"), independent = event("independent", "independent-thread"), unknown = event("unknown", "unowned-thread");
    for (const id of ["owner-a", "owner-b", "owner-c"]) await app.tasks.create({ id, title: id, outcome: "Retain this source ownership", sources: [crowded.source] });
    await app.tasks.create({ id: "independent-owner", title: "Independent owner", outcome: "Retain its separate work", sources: [independent.source] });
    await admit(app.sources, [crowded, independent, unknown]);
    let bounded: unknown, narrowed: unknown, absent: unknown, denied = "", deniedError = false;
    app.faux.setResponses([
      input => { const group = groups(input).find(item => item.threads.includes(crowded.source.thread_id)); assert.ok(group);
        return fauxAssistantMessage(fauxToolCall("find_source_tasks", { query: "legacy hint", groups: [group.id], limit: 2 }), { stopReason: "toolUse" }); },
      input => { bounded = JSON.parse(toolResult(input, "find_source_tasks"));
        const group = groups(input).find(item => item.threads.includes(independent.source.thread_id)); assert.ok(group);
        return fauxAssistantMessage(fauxToolCall("find_source_tasks", { query: "legacy hint", groups: [group.id], limit: 2 }), { stopReason: "toolUse" }); },
      input => { narrowed = JSON.parse(toolResult(input, "find_source_tasks"));
        const group = groups(input).find(item => item.threads.includes(unknown.source.thread_id)); assert.ok(group);
        return fauxAssistantMessage(fauxToolCall("find_source_tasks", { query: "legacy hint", groups: [group.id] }), { stopReason: "toolUse" }); },
      input => { absent = JSON.parse(toolResult(input, "find_source_tasks"));
        return fauxAssistantMessage(fauxToolCall("find_source_tasks", { query: "legacy hint", groups: ["outside-this-claim"] }), { stopReason: "toolUse" }); },
      input => { const reply = input.messages.findLast(message => message.role === "toolResult" && message.toolName === "find_source_tasks"); assert.ok(reply?.role === "toolResult");
        deniedError = reply.isError === true; denied = reply.content.flatMap(part => part.type === "text" ? [part.text] : []).join("\n");
        return answer(dismissed(groups(input))); },
      input => { toolResult(input); return fauxAssistantMessage(""); },
    ]);
    assert.equal(await app.reviews.review(0, controller.signal), true);
    assert.ok(object(bounded) && Array.isArray(bounded.tasks));
    assert.equal(bounded.tasks.length, 2); assert.equal(bounded.owners_complete, false); assert.equal(bounded.history_searched, false);
    assert.deepEqual(bounded.tasks.map((item: unknown) => object(item) ? item.id : null), ["owner-a", "owner-b"]);
    assert.ok(Array.isArray(bounded.gaps)); assert.match(bounded.gaps.join(" "), /More current owners/u);
    assert.ok(object(narrowed) && Array.isArray(narrowed.tasks));
    assert.deepEqual(narrowed.tasks.map((item: unknown) => object(item) ? item.id : null), ["independent-owner"]);
    assert.equal(narrowed.owners_complete, true);
    assert.ok(object(absent) && Array.isArray(absent.tasks) && Array.isArray(absent.gaps));
    assert.deepEqual(absent.tasks, []); assert.equal(absent.history_searched, false); assert.match(absent.gaps.join(" "), /absence is not established/u);
    assert.equal(deniedError, true); assert.match(denied, /outside this claim/u);
  } finally { controller.abort(); await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("indexed ownership lookup recovers tracked cross-source resources after reopen without history research", { timeout: 15_000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-resource-source-lookup-")), path = join(directory, "session.sqlite");
  let app = await open(path); const controller = new AbortController();
  const resource = "https://github.com/example/project/issues/42", ownerId = "retained-resource-owner";
  try {
    await app.tasks.create({ id: ownerId, title: "Tracked issue", outcome: "Continue its existing work" });
    await app.tasks.create({ id: "previous-thread-owner", title: "Previous thread work", outcome: "Keep its own authorized scope" });
    await admit(app.sources, [event("original-notification", "notification-42", "Subject API: https://api.github.com/repos/example/project/issues/42", "github")]);
    const retained = (await app.sources.events())[0]; assert.ok(retained);
    await app.sources.decide([retained.key], { type: "task", task_id: ownerId });
    await admit(app.sources, [event("previous-thread-evidence", "new-thread", "Earlier unrelated work in the same thread")]);
    const previous = (await app.sources.events({ provider: "slack" }))[0]; assert.ok(previous);
    await app.sources.decide([previous.key], { type: "task", task_id: "previous-thread-owner" });
    await app.session.close(BACKGROUND_CONTEXT);
    let searches = 0;
    const search: SearchQuery = async () => { searches++; return { candidates: [], gaps: ["Semantic history is unavailable"] }; };
    app = await open(path, {}, [], undefined, undefined, undefined, search);
    app.tasks.wakeBackground = id => app.tasks.get(id);
    await admit(app.sources, [event("new-slack-mention", "new-thread", "New evidence about " + resource)]);
    let lookup: unknown, denied = "", deniedError = false;
    app.faux.setResponses([
      () => fauxAssistantMessage(fauxToolCall("find_source_tasks", { query: resource, work_url: resource, limit: 1 }), { stopReason: "toolUse" }),
      input => { lookup = JSON.parse(toolResult(input, "find_source_tasks")); return fauxAssistantMessage(fauxToolCall("find_source_tasks", {
        query: "unrelated resource", work_url: "https://github.com/example/project/issues/99" }), { stopReason: "toolUse" }); },
      input => { const reply = input.messages.findLast(message => message.role === "toolResult" && message.toolName === "find_source_tasks"); assert.ok(reply?.role === "toolResult");
        deniedError = reply.isError === true; denied = reply.content.flatMap(part => part.type === "text" ? [part.text] : []).join("\n");
        return answer([routed(groups(input), { work_url: resource })]); },
      input => { toolResult(input); return fauxAssistantMessage(""); },
    ]);
    assert.equal(await app.reviews.review(0, controller.signal), true);
    assert.ok(object(lookup) && Array.isArray(lookup.tasks));
    assert.deepEqual(lookup.tasks.map((item: unknown) => object(item) ? item.id : null), [ownerId]);
    assert.equal(searches, 0); assert.equal(lookup.history_searched, false); assert.equal(lookup.owners_complete, false);
    assert.equal(deniedError, true); assert.match(denied, /outside the selected captured evidence/u);
    assert.equal((await app.tasks.list()).length, 2); assert.equal((await app.tasks.get(ownerId)).pending.length, 1);
    assert.equal((await app.tasks.get("previous-thread-owner")).pending.length, 0);
    assert.equal((await app.sources.events()).filter(record => record.decision?.type === "task" && record.decision.task_id === ownerId).length, 2);
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

test("workflow results continue their existing owner without spawning the same workflow again", { timeout: 15_000 }, async () => {
  for (const legacy of [false, true]) {
    const directory = await mkdtemp(join(tmpdir(), "agent-workflow-result-"));
    const job: ScheduledJobStatus = { job: { id: "parcel-review", title: "Parcel review", prompt: "Handle current parcel evidence.",
      priority: "critical", model: fixtureModel, timezone: "UTC", schedules: [], revision: "one", cwd: "/workspace/example" },
      schedules: [], task_id: "parcel-owner" };
    const app = await open(join(directory, "session.sqlite"), {}, [job]); const controller = new AbortController();
    app.tasks.wakeBackground = id => app.tasks.get(id);
    try {
      const owner = await app.tasks.create({ id: "parcel-owner", title: job.job.title, outcome: job.job.prompt,
        sources: [{ provider: "scheduled_jobs", account: "local", thread_id: job.job.id }] });
      const worker = legacy ? await app.tasks.create({ id: "legacy-worker", title: "Legacy parcel review", outcome: "Retain the verified result",
        sources: [{ provider: "scheduled_jobs", account: "local", thread_id: job.job.id }] }) : await app.tasks.create({ id: "parcel-worker", title: "One parcel", outcome: "Verify this parcel once",
        execution: "coordinator", workflow: { job: job.job, owner_task_id: owner.id, work_key: "source:one-parcel" } });
      const outcome: TaskOutcome = { task_id: worker.id, conversation_id: worker.conversation_id, entry_id: null, tool_call_id: null,
        summary: "The parcel is saved. Retire its old missing-tracking check.", sources: worker.sources, state: { state: "progress" } };
      await app.session.commit(tx => app.sources.recordIn(tx, outcome, false), BACKGROUND_CONTEXT);
      app.faux.setResponses([
        input => answer([routed(groups(input), { job_id: job.job.id })], "recursive-workflow"),
        input => { assert.match(toolResult(input, "review_sources", true), /existing.*task|existing.*owner/iu);
          return answer([routed(groups(input), { task_id: owner.id })], "existing-workflow-owner"); },
        input => { toolResult(input); return fauxAssistantMessage(""); },
      ]);
      assert.equal(await app.reviews.review(0, controller.signal), true);
      assert.equal((await app.tasks.list()).length, 2, "A result must not create another workflow actor");
      assert.equal(app.faux.getPendingResponseCount(), 0);
      const saved = await app.tasks.get(owner.id); assert.equal(saved.pending.length, 1);
      assert.match(saved.pending[0]?.content ?? "", /Inspect these exact originals/u);
      const records = await app.sources.events({ provider: "task_outcomes" }); assert.equal(records.length, 1);
      assert.deepEqual(records[0]?.decision, { type: "task", task_id: owner.id });
      assert.deepEqual(await app.sources.events({ pending: true }), []);
    } finally { controller.abort(); await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
  }
});

test("idle actors do not wake for their own progress but retain explicit failure recovery", { timeout: 15_000 }, async () => {
  for (const failed of [false, true]) {
    const directory = await mkdtemp(join(tmpdir(), "agent-owned-progress-"));
    const app = await open(join(directory, "session.sqlite")); const controller = new AbortController();
    app.tasks.wakeBackground = id => app.tasks.get(id);
    try {
      const owner = await app.tasks.create({ id: "existing-owner", title: "Existing work", outcome: "Preserve its evidence" });
      await app.session.commit(tx => app.sources.recordIn(tx, { task_id: owner.id, conversation_id: owner.conversation_id,
        entry_id: null, tool_call_id: null, summary: "Retained native result", sources: [],
        state: failed ? { state: "failed", reason: "The owned execution failed before completion" } : { state: "progress" } }, false), BACKGROUND_CONTEXT);
      app.faux.setResponses([input => answer([routed(groups(input), { task_id: owner.id })]),
        input => { toolResult(input, "review_sources", !failed);
          return failed ? fauxAssistantMessage("") : answer(dismissed(groups(input), "This actor already owns its recorded progress"), "dismiss-own-progress"); },
        ...(failed ? [] : [((input: TranscriptContext) => { toolResult(input); return fauxAssistantMessage(""); })]),
      ]);
      assert.equal(await app.reviews.review(0, controller.signal), true);
      assert.equal((await app.tasks.list()).length, 1);
      assert.equal((await app.tasks.get(owner.id)).pending.length, failed ? 1 : 0);
      assert.equal(app.faux.getPendingResponseCount(), 0);
      assert.deepEqual(await app.sources.events({ pending: true }), []);
    } finally { controller.abort(); await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
  }
});

test("a different workflow can act on a task result in a fresh packet", { timeout: 15_000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-cross-workflow-result-"));
  const job: ScheduledJobStatus = { job: { id: "calendar-review", title: "Calendar review", prompt: "Reconcile the new travel dates.",
    model: fixtureModel, timezone: "UTC", schedules: [], revision: "one", cwd: "/workspace/example" }, schedules: [], task_id: "calendar-owner" };
  const app = await open(join(directory, "session.sqlite"), {}, [job]); const controller = new AbortController();
  app.tasks.wakeBackground = id => app.tasks.get(id);
  try {
    const producer = await app.tasks.create({ id: "travel-owner", title: "Travel review", outcome: "Read the original booking",
      sources: [{ provider: "scheduled_jobs", account: "local", thread_id: "travel-review" }] });
    await app.session.commit(tx => app.sources.recordIn(tx, { task_id: producer.id, conversation_id: producer.conversation_id,
      entry_id: null, tool_call_id: null, summary: "The requested booking has verified travel dates.", sources: producer.sources,
      state: { state: "resolved", evidence: "The original booking supplies the verified dates." } }, false), BACKGROUND_CONTEXT);
    app.faux.setResponses([input => answer([routed(groups(input), { job_id: job.job.id })]),
      input => { toolResult(input); return fauxAssistantMessage(""); }]);
    assert.equal(await app.reviews.review(0, controller.signal), true);
    const records = await app.tasks.list(); assert.equal(records.length, 3);
    const packet = records.find(task => task.workflow?.job.id === job.job.id); assert.ok(packet);
    assert.equal(packet.workflow?.owner_task_id, job.task_id);
    assert.equal(packet.pending.length, 1);
    assert.equal((await app.tasks.get(producer.id)).pending.length, 0);
    assert.deepEqual(await app.sources.events({ pending: true }), []);
  } finally { controller.abort(); await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("ordinary Slack proposals share one bounded workflow packet and replay keeps that exact admission after reopen", { timeout: 15_000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-slack-workflow-batch-")), path = join(directory, "session.sqlite");
  const job: ScheduledJobStatus = { job: { id: "brief-review", title: "Evidence brief", prompt: "Review current evidence under the configured policy.",
    model: fixtureModel, timezone: "UTC", schedules: [], revision: "one", cwd: "/workspace/example" }, schedules: [], task_id: "brief-owner" };
  let app = await open(path, { maximumEntities: 4 }, [job]); const controller = new AbortController();
  let proposals: SourceReviewDecision[] = [], reviewer = "";
  const originals = Array.from({ length: 6 }, (_, index) => ({ ...event("ordinary-" + index, `CEXAMPLE:1700000000.${index}`,
    "A complete ordinary service observation " + index), attention: "ordinary" } satisfies SourceEvent));
  try {
    app.tasks.wakeBackground = id => app.tasks.get(id);
    assert.ok(job.task_id);
    const owner = await app.tasks.create({ id: job.task_id, title: job.job.title, outcome: job.job.prompt,
      sources: [{ provider: "scheduled_jobs", account: "local", thread_id: job.job.id }] });
    const ownerConversation = await app.tasks.conversation(owner.conversation_id);
    await (await ownerConversation.submit({ type: "write", entry: { kind: "fixture.previous_work", model: [{ role: "user", content: "OLDER_WORKFLOW_TRANSCRIPT", timestamp: 1 }] } }, BACKGROUND_CONTEXT)).wait(BACKGROUND_CONTEXT);
    await admit(app.sources, originals);
    app.faux.setResponses([input => { proposals = groups(input).map(group => routed([group], { job_id: job.job.id }));
      assert.equal(proposals.length, 4); return answer(proposals, "batch-all-proposals"); },
    async input => { toolResult(input); reviewer = (await app.reviews.diagnostics()).active_reviews[0]?.conversation_id ?? ""; return fauxAssistantMessage(""); }]);
    assert.equal(await app.reviews.review(0, controller.signal), true);
    const packets = (await app.tasks.list()).filter(task => task.workflow?.job.id === job.job.id);
    assert.equal(packets.length, 1, "One claim must batch ordinary threads across separate routing proposals");
    const batch = packets[0]; assert.ok(batch); assert.match(batch.workflow?.work_key ?? "", /^source-batch:/u);
    const evidence = packetEvidence(batch), keys = evidence.map(item => item.key);
    assert.equal(evidence.length, 4); assert.equal(batch.sources.length, 4); assert.notEqual(batch.conversation_id, owner.conversation_id);
    assert.doesNotMatch(batch.pending[0]?.content ?? "", /OLDER_WORKFLOW_TRANSCRIPT/u);
    const originalsByKey = new Map((await app.sources.events({ limit: 20 })).map(record => [record.key, record.event]));
    for (const item of evidence) { assert.equal(typeof item.key, "string"); assert.deepEqual(item.event, originalsByKey.get(String(item.key))); }
    assert.deepEqual(new Set((await app.sources.events()).filter(record => record.decision?.type === "task").map(record => record.key)), new Set(keys));
    assert.deepEqual(await app.reviews.apply(reviewer, "batch-all-proposals", proposals), { accounted: 4, superseded: 0 });
    await app.session.close(BACKGROUND_CONTEXT);
    app = await open(path, { maximumEntities: 4 }, [job]); app.tasks.wakeBackground = id => app.tasks.get(id);
    assert.deepEqual(await app.tasks.get(batch.id), batch);
    assert.deepEqual(await app.reviews.apply(reviewer, "batch-all-proposals", proposals), { accounted: 4, superseded: 0 });
    await admit(app.sources, originals);
    assert.equal((await app.tasks.list()).filter(task => task.workflow !== undefined).length, 1);
    app.faux.setResponses([input => answer(groups(input).map(group => routed([group], { job_id: job.job.id })), "next-batch"), fauxAssistantMessage("")]);
    assert.equal(await app.reviews.review(0, controller.signal), true);
    const later = (await app.tasks.list()).filter(task => task.workflow?.job.id === job.job.id && task.id !== batch.id);
    assert.equal(later.length, 1); assert.equal(packetEvidence(later[0]!).length, 2);
    assert.notEqual(later[0]?.conversation_id, batch.conversation_id); assert.notEqual(later[0]?.workflow?.work_key, batch.workflow?.work_key);
    assert.deepEqual(await app.tasks.get(batch.id), batch, "A later claim cannot append to or mutate the first batch");
    assert.equal((await app.tasks.get(owner.id)).pending.length, 0);
    assert.deepEqual(await app.sources.events({ pending: true }), []);
  } finally { controller.abort(); await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("Slack batching preserves protected, unknown, tracked, user-owned, critical and email work", { timeout: 15_000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-slack-batch-protection-"));
  const normal: ScheduledJobStatus = { job: { id: "brief-review", title: "Evidence brief", prompt: "Review current evidence.", model: fixtureModel,
    timezone: "UTC", schedules: [], revision: "one", cwd: "/workspace/example" }, schedules: [], task_id: "brief-owner" };
  const critical: ScheduledJobStatus = { ...normal, job: { ...normal.job, id: "urgent-review", priority: "critical" }, task_id: "urgent-owner" };
  const app = await open(join(directory, "session.sqlite"), {}, [normal, critical]), controller = new AbortController();
  const ordinary = (id: string, channel: string, text = "Complete ordinary source evidence") => ({ ...event(id, `${channel}:1700000000.1`, text), attention: "ordinary" } satisfies SourceEvent);
  try {
    app.tasks.wakeBackground = id => app.tasks.get(id);
    const userSignal = ordinary("user-owned", "CUSER"), tracked = ordinary("tracked", "CTRACKED");
    const user = await app.tasks.create({ id: "user-owner", title: "User-chosen work", outcome: "Preserve the current user request", sources: [userSignal.source] });
    await app.tasks.enqueue(user.id, "Keep this current user decision.", "trusted-user-input", undefined, "steer", undefined, "cli");
    const current = (await app.tasks.get(user.id)).pending[0]; assert.ok(current);
    const existing = await app.tasks.create({ id: "tracked-owner", title: "Existing tracked work", outcome: "Preserve its scope", sources: [tracked.source] });
    const sameThread = ordinary("mixed-ordinary", "CMIXED");
    const signals: SourceEvent[] = [ordinary("ordinary-a", "CNOISEA"), ordinary("ordinary-b", "CNOISEB"),
      { ...ordinary("direct", "DEXAMPLE"), attention: "direct" },
      { ...ordinary("mention", "CMENTION", "Please check <@UEXAMPLE>"), attention: "mention" },
      { ...ordinary("protected", "GPRIVATE"), attention: "protected" }, event("unknown", "CUNKNOWN:1700000000.1"),
      ordinary("empty", "CEMPTY", ""), ordinary("resource", "CRESOURCE", "Current https://github.com/example/project/issues/42"),
      sameThread, { ...ordinary("mixed-mention", "CMIXED", "Please check <@UEXAMPLE>"), source: sameThread.source, attention: "mention" },
      userSignal, tracked, ordinary("critical-a", "CURGENTA"), ordinary("critical-b", "CURGENTB")];
    await admit(app.sources, signals);
    app.faux.setResponses([input => answer(groups(input).map(group => routed([group], group.originals.includes("user-owned") ? { task_id: user.id }
      : { job_id: group.originals.some(id => id.startsWith("critical-")) ? critical.job.id : normal.job.id }))), fauxAssistantMessage("")]);
    assert.equal(await app.reviews.review(0, controller.signal), true);
    const tasks = await app.tasks.list(), batches = tasks.filter(task => task.workflow?.work_key.startsWith("source-batch:"));
    assert.equal(batches.length, 1); const batch = batches[0]; assert.ok(batch);
    const evidence = packetEvidence(batch);
    assert.deepEqual(new Set(evidence.map(item => object(item.event) ? item.event.id : null)), new Set(["ordinary-a", "ordinary-b"]));
    const saved = await app.sources.events({ provider: "slack", limit: 30 });
    for (const signal of signals.filter(signal => !signal.id.startsWith("ordinary-"))) {
      const record = saved.find(record => record.event.id === signal.id); assert.ok(record?.decision?.type === "task");
      assert.notEqual(record.decision.task_id, batch.id);
    }
    assert.deepEqual((await app.tasks.get(user.id)).pending[0], current); assert.equal((await app.tasks.get(user.id)).pending.length, 2);
    assert.deepEqual(await app.tasks.get(existing.id), existing);
    const urgent = tasks.filter(task => task.workflow?.job.id === critical.job.id);
    assert.equal(urgent.length, 2); assert.ok(urgent.every(task => !task.workflow?.work_key.startsWith("source-batch:")));
    const mail = [event("mail-a", "mail-a", "Important email evidence", "google"), event("mail-b", "mail-b", "Separate email evidence", "google")];
    await admit(app.sources, mail);
    app.faux.setResponses([input => answer(groups(input).map(group => routed([group], { job_id: normal.job.id }))), fauxAssistantMessage("")]);
    assert.equal(await app.reviews.review(0, controller.signal), true);
    const email = (await app.tasks.list()).filter(task => task.sources.some(source => source.provider === "google"));
    assert.equal(email.length, 2); assert.ok(email.every(task => !task.workflow?.work_key.startsWith("source-batch:")));
  } finally { controller.abort(); await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("Slack batch chunks obey the native UTF-8 input bound without changing their exact originals", { timeout: 15_000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-slack-batch-bytes-")), path = join(directory, "session.sqlite");
  const job: ScheduledJobStatus = { job: { id: "brief-review", title: "Evidence brief", prompt: "Review exact current source evidence.", model: fixtureModel,
    timezone: "UTC", schedules: [], revision: "one", cwd: "/workspace/example" }, schedules: [], task_id: "brief-owner" };
  let app = await open(path, {}, [job]); const controller = new AbortController();
  try {
    app.tasks.wakeBackground = id => app.tasks.get(id);
    const model = app.tasks.inference.models.getModel("openai", "fixture"); assert.ok(model); model.contextWindow = 256 * 1024;
    const originals = Array.from({ length: 3 }, (_, index) => ({ ...event("unicode-" + index, `CUNICODE:1700000000.${index}`, "🚀".repeat(8200)), attention: "ordinary" } satisfies SourceEvent));
    await admit(app.sources, originals);
    app.faux.setResponses([input => { assert.equal(groups(input).length, 3); return answer(groups(input).map(group => routed([group], { job_id: job.job.id }))); },
      input => { toolResult(input); return fauxAssistantMessage(""); }]);
    assert.equal(await app.reviews.review(0, controller.signal), true);
    const chunks = (await app.tasks.list()).filter(task => task.workflow?.job.id === job.job.id);
    assert.equal(chunks.length, 3, "Two full Unicode bodies already exceed the native byte limit before their envelope");
    for (const task of chunks) {
      assert.match(task.workflow?.work_key ?? "", /^source-batch:/u);
      assert.ok(Buffer.byteLength(task.pending[0]?.content ?? "") <= 65536);
      assert.equal(packetEvidence(task).length, 1);
    }
    assert.deepEqual(new Set(chunks.flatMap(task => packetEvidence(task).map(item => object(item.event) ? item.event.id : null))), new Set(originals.map(event => event.id)));
    await app.session.close(BACKGROUND_CONTEXT); app = await open(path, {}, [job]);
    for (const chunk of chunks) assert.deepEqual(await app.tasks.get(chunk.id), chunk);
    assert.deepEqual(new Set((await app.sources.events({ limit: 10 })).map(record => record.event.text)), new Set(originals.map(event => event.text)));
  } finally { controller.abort(); await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

for (const { encoding, body, provider, critical } of [
  { encoding: "Unicode", body: "🧭".repeat(16384), provider: "slack", critical: false },
  { encoding: "JSON escaping", body: "\u0000".repeat(65536), provider: "slack", critical: false },
  { encoding: "legacy plain text", body: "x".repeat(1_500_000), provider: "slack", critical: false },
  { encoding: "transport-sized JSON escaping", body: "\u0000".repeat(Math.floor((2 * 1024 * 1024 - 4096) / 6)), provider: "slack", critical: false },
  { encoding: "protected critical email", body: "x".repeat(1_500_000), provider: "google", critical: true },
]) test(`an oversized source original (${encoding}) gets a bounded preview and remains readable by the real worker after reopen`, { timeout: 30_000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-slack-batch-large-")), path = join(directory, "session.sqlite");
  const job: ScheduledJobStatus = { job: { id: "brief-review", title: "Evidence brief", prompt: "Review exact current source evidence.", model: fixtureModel,
    priority: critical ? "critical" : "normal", timezone: "UTC", schedules: [], revision: "one", cwd: "/workspace/example" }, schedules: [], task_id: "brief-owner" };
  let app = await open(path, {}, [job]); const controller = new AbortController();
  try {
    app.tasks.wakeBackground = id => app.tasks.get(id);
    const large = { ...event("oversized-body", "CLARGE:1700000000.1", body, provider), attention: critical ? "protected" : "ordinary" } satisfies SourceEvent;
    const batch: SourceBatch = { source: { provider, account: large.source.account }, events: [large], checkpoint: null, coverage: { status: "complete" } };
    assert.ok(Buffer.byteLength(JSON.stringify(batch)) <= 2 * 1024 * 1024, "The original fits the existing serialized source transport contract");
    await app.sources.admit(batch, null);
    let routingError = "";
    app.faux.setResponses([input => answer([routed(groups(input), { job_id: job.job.id })]),
      input => { const reply = input.messages.findLast(message => message.role === "toolResult" && message.toolName === "review_sources"); assert.ok(reply?.role === "toolResult");
        if (reply.isError) routingError = toolResult(input, "review_sources", true);
        else toolResult(input); return fauxAssistantMessage(""); }]);
    assert.equal(await app.reviews.review(0, controller.signal).catch(error => {
      throw new Error("The original must be admitted without a retry loop: " + routingError, { cause: error });
    }), true);
    const record = (await app.sources.events({ limit: 10 })).find(record => record.event.id === large.id); assert.ok(record?.decision?.type === "task");
    const packet = await app.tasks.get(record.decision.task_id), retained = packet.pending[0]; assert.ok(retained);
    if (critical) {
      assert.doesNotMatch(packet.workflow?.work_key ?? "", /^source-batch:/u);
      assert.deepEqual(packet.sources, [large.source]);
      const native = await app.tasks.conversation(packet.conversation_id);
      await app.session.commit(async tx => {
        assert.equal(await app.tasks.inference.workClassIn(tx, native), "critical");
        assert.equal(await app.tasks.inference.workGroupIn(tx, native), job.job.id);
      }, BACKGROUND_CONTEXT);
    }
    assert.ok(Buffer.byteLength(retained.content) <= 65536); assert.match(retained.content, /read_source_events/u);
    const view = packetEvidence(packet)[0]; assert.ok(view && object(view.event));
    assert.equal(view.key, record.key); assert.equal(view.text_complete, false); assert.equal(view.original_text_bytes, Buffer.byteLength(large.text));
    assert.ok(typeof view.event.text === "string" && view.event.text.length < large.text.length);
    await app.session.close(BACKGROUND_CONTEXT); app = await open(path, {}, [job]);
    assert.deepEqual((await app.tasks.get(packet.id)).pending, packet.pending);
    assert.deepEqual((await app.sources.originals([record.key]))[0]?.event, large);
    const worker = createSourceTools(app.sources, app.executions).worker; app.registry.install(worker);
    const conversation = await app.tasks.conversation(packet.conversation_id);
    await app.tasks.inference.configure(conversation, fixtureModel, { extensions: [worker], instructions: "Read the exact retained original before acting." });
    app.faux.setResponses([fauxAssistantMessage(fauxToolCall("read_source_events", { keys: [record.key], limit: 1 }), { stopReason: "toolUse" }),
      input => { const output = toolResult(input, "read_source_events");
        assert.ok(Buffer.byteLength(output) > 50 * 1024 && Buffer.byteLength(output) <= 4 * 1024 * 1024);
        if (encoding.includes("escaping")) assert.ok(Buffer.byteLength(output) > 128 * 1024);
        const reply: unknown = JSON.parse(output); assert.ok(object(reply) && Array.isArray(reply.events));
        assert.equal(reply.complete, true); assert.equal(reply.cursor, null);
        const original: unknown = reply.events[0]; assert.ok(object(original)); assert.deepEqual(original.event, large);
        return fauxAssistantMessage("The exact original was read without losing its Unicode body."); }]);
    const completed = await (await conversation.submit({ type: "input", requestId: retained.id, content: retained.content }, BACKGROUND_CONTEXT)).wait(BACKGROUND_CONTEXT);
    assert.equal(completed.status, "done", JSON.stringify(completed).slice(0, 1800)); assert.equal(completed.requestId, retained.id);
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
