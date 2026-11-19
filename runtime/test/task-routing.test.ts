import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setImmediate } from "node:timers/promises";
import { test } from "node:test";
import { BACKGROUND_CONTEXT, type Context } from "@earendil-works/chord/context";
import { createModels, fauxAssistantMessage, fauxProvider } from "@earendil-works/pi-ai";
import { createRegistry, defineDoc, defineDocFamily, Harness, ProviderDoc, type Tx } from "@earendil-works/pi-durable";
import { SqliteStorage } from "@earendil-works/pi-durable/storage/sqlite";
import { openConversationDatabase } from "../src/conversation-database.js";
import { CoordinatorEnvironment } from "../src/environment.js";
import { InferenceCapacity } from "../src/inference-capacity.js";
import { ConversationModels } from "../src/models.js";
import { PartitionedRecords } from "../src/partitioned-records.js";
import { TaskStore } from "../src/tasks.js";
import type { SourceRef } from "../src/generated/SourceRef.js";
import type { InferenceClass } from "../src/generated/InferenceClass.js";
import type { TaskRecord } from "../src/generated/TaskRecord.js";
import type { WorkerHost } from "../src/generated/WorkerHost.js";
import type { WorkerRef } from "../src/generated/WorkerRef.js";
import type { RuntimeCommand } from "../src/generated/RuntimeCommand.js";
import { fixtureModel } from "./model.js";

const NOW = Date.UTC(2035, 0, 1);
const source: SourceRef = { provider: "slack", account: "workspace-one", thread_id: "thread-one" };
const host: WorkerHost = { host_id: "11111111-1111-4111-8111-111111111111", name: "example-worker", rex_server: null,
  coordinator: null, cwd: "/workspace/example", agent_program: "/example/agent" };
const binding: WorkerRef = { host_id: "11111111-1111-4111-8111-111111111111", session_id: "session:example",
  window_id: "window:example", block_id: "block:example" };
const RoutingMigration = defineDoc<{ migrated: number; complete: boolean }>({ kind: "agent.task.routing.migration",
  scope: "session", version: 1, initial: () => ({ migrated: 0, complete: false }), checkpointWhen: () => true });
const MembershipMigration = defineDoc<{ migrated: number; complete: boolean }>({ kind: "agent.task.members.migration",
  scope: "session", version: 1, initial: () => ({ migrated: 0, complete: false }), checkpointWhen: () => true });
const ModelPolicy = defineDocFamily<{ policy: { work_class?: InferenceClass } | null }, null>({ kind: "agent.model_policy", scope: "session",
  family: true, version: 1, initial: () => ({ policy: null }), checkpointWhen: () => true });

async function pump(predicate: () => boolean | Promise<boolean>): Promise<void> {
  for (let attempt = 0; attempt < 1000; attempt += 1) {
    if (await predicate()) return;
    await setImmediate();
  }
  throw new Error("Task did not reach its expected native state");
}

async function open(path: string) {
  const storage = await SqliteStorage.open(await openConversationDatabase(path));
  const models = createModels(), faux = fauxProvider({ provider: "openai", models: [{ id: "fixture" }], tokensPerSecond: 1_000_000 });
  models.setProvider(faux.provider);
  const registry = createRegistry(), capacity = new InferenceCapacity({ maxConcurrent: 4, maxBackfillConcurrent: 1 });
  const inference = new ConversationModels(models, undefined, registry, capacity);
  const session = await Harness.open(storage, { models, registry, identity: { attribution: null } }, BACKGROUND_CONTEXT);
  return { storage, session, models, faux, inference, tasks: new TaskStore(session, inference) };
}

async function workClass(app: Awaited<ReturnType<typeof open>>, id: string): Promise<InferenceClass | undefined> {
  const conversation = await app.tasks.conversation(id), native = await app.session.snapshot(ProviderDoc, conversation.id, BACKGROUND_CONTEXT);
  assert.ok(native);
  return (await app.session.snapshot(ModelPolicy, native.sessionId, BACKGROUND_CONTEXT))?.policy?.work_class;
}

test("trusted user task input retains foreground capacity across later source admissions and reopen", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-task-user-capacity-")), path = join(directory, "session.sqlite");
  let app = await open(path);
  try {
    const sender = await app.session.root(BACKGROUND_CONTEXT), classes: InferenceClass[] = ["backfill", "cron"];
    const conversations: string[] = [];
    for (const background of classes) {
      const task = await app.tasks.create({ id: "user-" + background, title: "Existing background task", outcome: "Keep its original work", execution: "coordinator",
        sources: background === "cron" ? [{ provider: "scheduled_jobs", account: "local", thread_id: "example-job" }] : [source] });
      conversations.push(task.conversation_id);
      const review = { task_id: task.id, input_id: "source-review:original-" + background, content: "Retained background evidence",
        sender_conversation_id: sender.id.toString(), work_class: background };
      await app.session.commit(tx => app.tasks.enqueueReviewIn(tx, review), BACKGROUND_CONTEXT);
      assert.equal(await workClass(app, task.conversation_id), background);
      await app.tasks.enqueue(task.id, "Automatic context without direct-input provenance", "unknown-origin-" + background);
      assert.equal(await workClass(app, task.conversation_id), background, "Omitted direct-input origin is not proof of user ownership");
      await app.tasks.enqueue(task.id, "Explicit human steering", "user-steer-" + background, undefined, "steer", undefined, "ios");
      assert.equal(await workClass(app, task.conversation_id), "foreground");
      await app.session.commit(tx => app.tasks.enqueueReviewIn(tx, { ...review, input_id: "source-review:later-" + background }), BACKGROUND_CONTEXT);
      assert.equal(await workClass(app, task.conversation_id), "foreground", "Background admission cannot reclaim a conversation taken over by the user");
      await app.session.commit(tx => app.tasks.enqueueReviewIn(tx, review), BACKGROUND_CONTEXT);
      await assert.rejects(app.session.commit(tx => app.tasks.enqueueReviewIn(tx, { ...review, work_class: "foreground" }), BACKGROUND_CONTEXT), /cannot be reused/u);
    }
    await app.session.close(BACKGROUND_CONTEXT); app = await open(path);
    for (const id of conversations) assert.equal(await workClass(app, id), "foreground");
  } finally { await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("a later interactive worker run promotes the exact retained task before its real provider request", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-task-interactive-capacity-")), app = await open(join(directory, "session.sqlite"));
  try {
    const task = await app.tasks.create({ title: "Existing source work", outcome: "Retain its input", model: fixtureModel });
    const sender = await app.session.root(BACKGROUND_CONTEXT);
    await app.session.commit(tx => app.tasks.enqueueReviewIn(tx, { task_id: task.id, input_id: "source-review:before-interaction",
      content: "The exact pending source input", sender_conversation_id: sender.id.toString(), work_class: "backfill" }), BACKGROUND_CONTEXT);
    await app.tasks.dispatch(task.id, host, async () => ({ type: "spawned", binding }));
    const saved = await app.tasks.get(task.id); assert.ok(saved.worker.state === "bound"); let observed: InferenceClass | undefined;
    await assert.rejects(app.tasks.run({ type: "run_task", task_id: task.id, allocation_id: "unconfirmed-allocation", binding,
      instructions: "Unconfirmed interaction", model: fixtureModel, cwd: host.cwd, interactive: true }, () => () => {}), /binding is not confirmed/u);
    assert.equal(await workClass(app, task.conversation_id), "backfill");
    app.faux.setResponses([async () => { observed = await workClass(app, saved.conversation_id); return fauxAssistantMessage("The retained input completed"); }]);
    await app.tasks.run({ type: "run_task", task_id: task.id, allocation_id: saved.worker.allocation_id, binding,
      instructions: "An explicit interactive session", model: fixtureModel, cwd: host.cwd, interactive: true }, () => () => {});
    assert.equal(observed, "foreground"); assert.equal(await workClass(app, task.conversation_id), "foreground");
  } finally { await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("child agents inherit durable background policy and trusted child input can override it", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-child-inference-class-")), path = join(directory, "session.sqlite");
  let app = await open(path);
  try {
    const sender = await app.session.root(BACKGROUND_CONTEXT), classes: InferenceClass[] = ["backfill", "cron", "fresh"];
    const children: string[] = [];
    for (const background of classes) {
      const task = await app.tasks.create({ id: "parent-" + background, title: "Parent background work", outcome: "Keep the same budget", execution: "coordinator", model: fixtureModel });
      await app.session.commit(tx => app.tasks.enqueueReviewIn(tx, { task_id: task.id, input_id: "source-review:parent-" + background,
        content: "Bounded source work", sender_conversation_id: sender.id.toString(), work_class: background }), BACKGROUND_CONTEXT);
      const child = await app.tasks.createAgent(task.id, { id: "child", title: "Bounded child", outcome: "Review only its owned evidence",
        input: "Inspect the retained source evidence", inputId: "child-original" }); children.push(child.conversation_id);
      assert.equal(await workClass(app, child.conversation_id), background);
      await app.inference.configure(await app.tasks.conversation(child.conversation_id), fixtureModel, {});
      assert.equal(await workClass(app, child.conversation_id), background);
      const inherited = background === "cron" ? "backfill" : "cron";
      await app.session.commit(tx => app.tasks.enqueueReviewIn(tx, { task_id: task.id, input_id: "source-review:reclassified-" + background,
        content: "Later bounded work", sender_conversation_id: sender.id.toString(), work_class: inherited }), BACKGROUND_CONTEXT);
      await assert.rejects(app.tasks.continueAgent(task.id, child.id, "Continue under the parent's current budget", "parent-reclassification", async () => ({ type: "written" })), /background execution is unavailable/u);
      assert.equal(await workClass(app, child.conversation_id), inherited);
      await assert.rejects(app.tasks.continueAgent(task.id, child.id, "Trusted human correction", "human-child", async () => ({ type: "written" }),
        "steer", undefined, undefined, "cli"), /background execution is unavailable/u);
      assert.equal(await workClass(app, child.conversation_id), "foreground");
      await assert.rejects(app.tasks.continueAgent(task.id, child.id, "Later parent context", "parent-context", async () => ({ type: "written" })), /background execution is unavailable/u);
      assert.equal(await workClass(app, child.conversation_id), "foreground");
    }
    await app.session.close(BACKGROUND_CONTEXT); app = await open(path);
    for (const id of children) assert.equal(await workClass(app, id), "foreground");
  } finally { await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("an inherited default foreground class does not claim user ownership when the parent becomes background work", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-inherited-foreground-")), app = await open(join(directory, "session.sqlite"));
  try {
    const task = await app.tasks.create({ title: "Initially unclassified parent", outcome: "Follow its current work budget", execution: "coordinator", model: fixtureModel });
    const child = await app.tasks.createAgent(task.id, { id: "existing-child", title: "Existing child", outcome: "Continue the parent's work",
      input: "The original child assignment", inputId: "initial-child" });
    await app.inference.configure(await app.tasks.conversation(child.conversation_id), fixtureModel, {});
    const sender = await app.session.root(BACKGROUND_CONTEXT);
    await app.session.commit(tx => app.tasks.enqueueReviewIn(tx, { task_id: task.id, input_id: "source-review:now-background",
      content: "Later retained background evidence", sender_conversation_id: sender.id.toString(), work_class: "backfill" }), BACKGROUND_CONTEXT);
    await assert.rejects(app.tasks.continueAgent(task.id, child.id, "Continue under the actual parent budget", "inherited-background", async () => ({ type: "written" })), /background execution is unavailable/u);
    assert.equal(await workClass(app, task.conversation_id), "backfill"); assert.equal(await workClass(app, child.conversation_id), "backfill");
  } finally { await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("due task projection orders inclusive deadlines and atomically follows state and pending inputs across reopen", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-task-due-")), path = join(directory, "session.sqlite");
  let app = await open(path);
  try {
    const deadlines = new Map<string, number>([["ancient", -1], ["first", NOW - 1], ["boundary-a", NOW], ["boundary-b", NOW], ["future", NOW + 1]]);
    for (const [id, deadline] of deadlines) {
      await app.tasks.create({ id, title: id, outcome: "Retain exact due work" });
      await app.tasks.update(id, task => { task.state = { state: "waiting", next_check: new Date(deadline).toISOString(), actor: "example-provider" }; });
    }
    await app.tasks.create({ id: "invalid", title: "Invalid date", outcome: "Keep metadata" });
    await app.tasks.update("invalid", task => { task.state = { state: "waiting", next_check: "not-a-date", actor: "example-provider" }; });
    assert.deepEqual((await app.tasks.dueChecks(NOW, 3)).map(task => task.id), ["ancient", "first", "boundary-a"]);
    assert.deepEqual((await app.tasks.dueChecks(NOW)).map(task => task.id), ["ancient", "first", "boundary-a", "boundary-b"]);
    await app.tasks.enqueue("first", "A new input owns the next turn", "new-input");
    await app.tasks.update("ancient", task => { task.state = { state: "resolved", evidence: "Finished" }; });
    await app.tasks.update("boundary-a", task => { task.state = { state: "waiting", next_check: new Date(NOW + 100).toISOString(), actor: "example-provider" }; });
    await assert.rejects(app.session.commit(async tx => {
      await app.tasks.updateIn(tx, "boundary-b", task => { task.state = { state: "resolved", evidence: "Finished" }; });
      throw new Error("Reject the deadline change");
    }, BACKGROUND_CONTEXT), /Reject the deadline change/u);
    await app.session.close(BACKGROUND_CONTEXT); app = await open(path);
    assert.deepEqual(await app.tasks.dueChecks(NOW), [{ id: "boundary-b", next_check: new Date(NOW).toISOString() }]);
    await app.tasks.update("first", task => { task.pending = []; task.state = { state: "waiting", next_check: new Date(NOW - 1).toISOString(), actor: "example-provider" }; });
    assert.deepEqual((await app.tasks.dueChecks(NOW)).map(task => task.id), ["first", "boundary-b"]);
    assert.deepEqual(await app.tasks.dueChecks(NOW, 0), []);
    await assert.rejects(app.tasks.dueChecks(Number.NaN), /finite time/u);
    await assert.rejects(app.tasks.dueChecks(NOW, 101), /bounded limit/u);
  } finally { await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("cold source and due lookups hydrate bounded candidates beside unrelated retained tasks", { timeout: 30_000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-task-bounded-")), path = join(directory, "session.sqlite");
  let app = await open(path);
  try {
    await app.tasks.prepare();
    for (let start = 0; start < 520; start += 32) {
      await app.session.commit(async tx => {
        for (let index = start; index < Math.min(520, start + 32); index += 1) {
          const id = "task-" + String(index).padStart(4, "0");
          await app.tasks.createIn(tx, { id, title: id, outcome: "Retained evidence. ".repeat(128),
            sources: [index < 140 ? source : { ...source, thread_id: "unrelated-" + index }] });
          await app.tasks.updateIn(tx, id, task => { task.state = { state: "waiting", next_check: new Date(index < 140 ? NOW : NOW + 1).toISOString(), actor: "example-provider" }; });
        }
      }, BACKGROUND_CONTEXT);
    }
    await app.session.close(BACKGROUND_CONTEXT); app = await open(path);
    const reads: string[] = [], legacyMembershipReads: string[] = [], find = app.storage.findDocument.bind(app.storage);
    app.storage.findDocument = async (address, at, context) => {
      if (address.kind === "agent.task.records" && typeof address.key === "string") reads.push(address.key);
      if (address.kind === "agent.task.membership" && typeof address.key === "string") legacyMembershipReads.push(address.key);
      return find(address, at, context);
    };
    assert.deepEqual((await app.tasks.findBySources([source, source], 8)).map(task => task.id),
      Array.from({ length: 8 }, (_, index) => "task-" + String(index).padStart(4, "0")));
    assert.equal(new Set(reads).size, 8, "Source ownership lookup must hydrate only its bounded result candidates");
    assert.deepEqual(legacyMembershipReads, [], "Completed migration must stop hydrating unbounded hash membership buckets");
    reads.length = 0;
    assert.deepEqual(await app.tasks.findBySources([{ ...source, account: "other-account" }]), []);
    assert.deepEqual(reads, [], "Unknown account ownership must not scan retained task originals");
    const due = await app.tasks.dueChecks(NOW, 4);
    assert.equal(due.length, 4); assert.ok(reads.length <= 4);
    const all = await app.session.commit(tx => app.tasks.sourceLinksIn(tx, [source]), BACKGROUND_CONTEXT);
    assert.equal(all.length, 140, "Source correction still finds every owner across projection pages");
    const other = { ...source, account: "other-account" };
    await app.session.commit(tx => app.tasks.relinkSourcesIn(tx, "task-0000", [other], [source]), BACKGROUND_CONTEXT);
    assert.equal((await app.tasks.findBySources([source], 1))[0]?.id, "task-0001");
    assert.equal((await app.tasks.findBySources([other], 1))[0]?.id, "task-0000");
    assert.equal(new Set((await app.tasks.findBySources([source, other], 8)).map(task => task.id)).size, 8);
  } finally { await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("ordered membership and task routing migration resume after interruption while retaining legacy originals", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-task-migration-")), path = join(directory, "session.sqlite");
  let app = await open(path);
  try {
    const legacy = new PartitionedRecords<TaskRecord>("agent.task");
    await legacy.prepare(app.session, async () => [], task => task.id);
    const originals: TaskRecord[] = [];
    for (let start = 0; start < 70; start += 32) {
      await app.session.commit(async tx => {
        for (let index = start; index < Math.min(70, start + 32); index += 1) {
          const conversation = await tx.createConversation({ ownership: { kind: "ownerless" } });
          const task: TaskRecord = { id: "legacy-" + index, conversation_id: conversation.id.toString(), title: "Legacy " + index,
            outcome: "Preserve the original", summary: "Immutable old metadata", model: null, worker: { state: "unassigned" },
            state: { state: "waiting", next_check: new Date(NOW + index).toISOString(), actor: "example-provider" },
            sources: [source], pending: [], agents: [], updated_at: index };
          await legacy.insertIn(tx, task.id, task); originals.push(task);
        }
      }, BACKGROUND_CONTEXT);
    }
    const commit = app.session.commit.bind(app.session); let writes = 0;
    app.session.commit = async <T>(change: (tx: Tx) => T | Promise<T>, context: Context): Promise<T> => {
      if (++writes === 5) throw new Error("Simulate process exit between migration batches");
      return commit(change, context);
    };
    await assert.rejects(app.tasks.prepare(), /Simulate process exit/u);
    app.session.commit = commit;
    assert.deepEqual(await app.session.snapshot(MembershipMigration, BACKGROUND_CONTEXT), { migrated: 70, complete: true });
    assert.deepEqual(await app.session.snapshot(RoutingMigration, BACKGROUND_CONTEXT), { migrated: 32, complete: false });
    await app.session.close(BACKGROUND_CONTEXT); app = await open(path);
    assert.equal((await app.tasks.findBySources([source], 8)).length, 8);
    assert.deepEqual(await app.tasks.dueChecks(NOW), [{ id: "legacy-0", next_check: new Date(NOW).toISOString() }]);
    assert.deepEqual(await app.session.snapshot(RoutingMigration, BACKGROUND_CONTEXT), { migrated: 70, complete: true });
    for (const original of originals) assert.deepEqual(await legacy.get(app.session, original.id), original);
    const upgraded = new PartitionedRecords<TaskRecord>("agent.task", true, true);
    await upgraded.prepare(app.session, async () => [], task => task.id);
    await app.session.commit(async tx => {
      assert.equal((await upgraded.valueIn(tx, "legacy-69"))?.summary, "Immutable old metadata");
      assert.equal(await upgraded.valueIn(tx, "missing"), undefined);
    }, BACKGROUND_CONTEXT);
  } finally { await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("source review creates its native actor, exact durable input and inference class in one transaction", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-task-review-")), path = join(directory, "session.sqlite");
  let app = await open(path); let release = () => {}; let calls: Promise<unknown>[] = [];
  try {
    const sender = await app.session.root(BACKGROUND_CONTEXT); await app.tasks.prepare();
    const review: { task_id: string; input_id: string; content: string; sender_conversation_id: string; work_class: InferenceClass } = {
      task_id: "reviewed", input_id: "source-review:one", content: "Review the exact retained source evidence.",
      sender_conversation_id: sender.id.toString(), work_class: "backfill" };
    await app.session.commit(async tx => {
      await app.tasks.createIn(tx, { id: review.task_id, title: "Reviewed source", outcome: "Resolve the source", execution: "coordinator", sources: [source] });
      await app.tasks.enqueueReviewIn(tx, review);
    }, BACKGROUND_CONTEXT);
    const task = await app.tasks.get(review.task_id);
    assert.equal(task.execution, "coordinator"); assert.equal(app.tasks.isBackgroundTask(task), true);
    assert.deepEqual(task.pending, [{ id: review.input_id, content: review.content, mode: "follow_up",
      sender: { type: "coordinator", conversation_id: sender.id.toString() } }]);
    await assert.rejects(app.session.commit(async tx => {
      await app.tasks.createIn(tx, { id: "rolled-back", title: "Atomic failure", outcome: "Do not survive failure", execution: "coordinator" });
      await app.tasks.enqueueReviewIn(tx, { ...review, task_id: "rolled-back", sender_conversation_id: "999999999" });
    }, BACKGROUND_CONTEXT), /does not exist/u);
    await assert.rejects(app.tasks.get("rolled-back"), /Unknown task/u);
    const conversation = await app.tasks.conversation(task.conversation_id);
    await app.inference.configure(conversation, fixtureModel, {});
    const connection = await app.session.snapshot(ProviderDoc, conversation.id, BACKGROUND_CONTEXT); assert.ok(connection);
    await app.session.close(BACKGROUND_CONTEXT); app = await open(path);
    await app.session.commit(tx => app.tasks.enqueueReviewIn(tx, review), BACKGROUND_CONTEXT);
    assert.equal((await app.tasks.get(review.task_id)).pending.length, 1);
    const changes: typeof review[] = [{ ...review, content: "Different evidence" }, { ...review, work_class: "fresh" },
      { ...review, sender_conversation_id: task.conversation_id }];
    for (const changed of changes) {
      await assert.rejects(app.session.commit(tx => app.tasks.enqueueReviewIn(tx, changed), BACKGROUND_CONTEXT), /cannot be reused/u);
    }
    const starts: string[] = [], releases: (() => void)[] = [];
    app.faux.setResponses(Array.from({ length: 2 }, () => async (_context, options) => {
      starts.push(options?.sessionId ?? ""); await new Promise<void>(resolve => { releases.push(resolve); });
      return fauxAssistantMessage("Review complete");
    }));
    release = () => { for (const done of releases.splice(0)) done(); };
    const model = app.models.getModel("openai", "fixture"); assert.ok(model);
    calls = Array.from({ length: 2 }, () => app.models.streamSimple(model,
      { messages: [{ role: "user", content: "Check the durable inference class", timestamp: 1 }] },
      { sessionId: connection.sessionId, maxTokens: 16 }).result());
    await pump(() => starts.length === 1);
    for (let step = 0; step < 8; step += 1) await setImmediate();
    assert.equal(starts.length, 1, "Restored source actor must retain backfill's single inference slot");
    release(); await pump(() => starts.length === 2); release(); await Promise.all(calls); calls = [];
    // Consume the durable actor outbox and settle a later wait. The exact
    // receipt outlives app pending, so a retry cannot reopen completed work.
    await app.tasks.update(task.id, current => { current.pending = []; current.state = { state: "waiting", next_check: new Date(NOW).toISOString(), actor: "example-provider" }; });
    await app.session.close(BACKGROUND_CONTEXT); app = await open(path);
    await app.session.commit(tx => app.tasks.enqueueReviewIn(tx, review), BACKGROUND_CONTEXT);
    const replayed = await app.tasks.get(task.id);
    assert.deepEqual(replayed.pending, []); assert.equal(replayed.state.state, "waiting");
    assert.deepEqual(await app.tasks.dueChecks(NOW), [{ id: task.id, next_check: new Date(NOW).toISOString() }]);
  } finally { release(); await Promise.allSettled(calls); await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("source effect wake preserves an existing Rex binding and immutable task execution ownership", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-task-wake-")), path = join(directory, "session.sqlite");
  const app = await open(path);
  try {
    const task = await app.tasks.create({ id: "legacy", title: "Existing worker", outcome: "Continue existing work" });
    await app.tasks.enqueue(task.id, "Original input", "original");
    await app.tasks.dispatch(task.id, host, async () => ({ type: "spawned", binding }));
    const saved = await app.tasks.get(task.id); assert.equal(saved.worker.state, "bound");
    const sender = await app.session.root(BACKGROUND_CONTEXT);
    await app.session.commit(tx => app.tasks.enqueueReviewIn(tx, { task_id: task.id, input_id: "source-review:legacy",
      content: "Fresh source evidence", sender_conversation_id: sender.id.toString(), work_class: "fresh" }), BACKGROUND_CONTEXT);
    let resumes = 0;
    app.tasks.useBackgroundExecution({ available: () => true, bind: () => () => {}, extensions: () => [],
      async call(command, owningHost) {
        assert.deepEqual(owningHost, host);
        if (command.type === "inspect_worker") return { type: "worker_presence", presence: { state: "present", running: false } };
        if (command.type === "resume_worker") { resumes += 1; assert.deepEqual(command.binding, binding); return { type: "spawned", binding }; }
        throw new Error("Unexpected worker allocation: " + command.type);
      } });
    const woken = await app.tasks.wakeBackground(task.id);
    assert.equal(resumes, 1); assert.equal(woken.execution, undefined); assert.equal(app.tasks.isBackgroundTask(woken), false);
    assert.ok(woken.worker.state === "bound"); assert.deepEqual(woken.worker.binding, binding);
    const existing = await app.tasks.create({ id: task.id, title: "Do not replace", outcome: "Do not convert", execution: "coordinator" });
    assert.equal(existing.execution, undefined);
    await assert.rejects(app.tasks.update(task.id, current => { current.execution = "coordinator"; }), /execution owner cannot be changed/u);
    assert.equal((await app.tasks.get(task.id)).execution, undefined);
  } finally { await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("new source coordinator wake returns before completion and keeps due work hidden while its real actor is running", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-task-native-wake-")), path = join(directory, "session.sqlite");
  const app = await open(path); let release = () => {}, taskId = "";
  try {
    const environment = new CoordinatorEnvironment(app.session);
    await environment.configure({ settings: { model: fixtureModel, instructions: "", sources: [], context_repositories: [] },
      node: { host_id: binding.host_id, name: "example-worker", home_directory: "/workspace/example", agent_program: "/example/agent",
        rex_server: null, coordinator: null, launch: { model: fixtureModel, runtime: null, state_dir: null, config: null, instructions: null } } });
    environment.setConnected([binding.host_id]); app.tasks.useEnvironment(environment);
    app.tasks.useBackgroundExecution({ available: () => true, bind: () => () => {}, extensions: () => [],
      async call() { throw new Error("A coordinator actor must not allocate a Rex worker"); } });
    const task = await app.tasks.create({ title: "Source coordinator", outcome: "Read the source", execution: "coordinator", model: fixtureModel });
    taskId = task.id;
    const sender = await app.session.root(BACKGROUND_CONTEXT);
    await app.session.commit(tx => app.tasks.enqueueReviewIn(tx, { task_id: task.id, input_id: "source-review:native",
      content: "Inspect this source", sender_conversation_id: sender.id.toString(), work_class: "fresh" }), BACKGROUND_CONTEXT);
    let started = false;
    app.faux.setResponses([async () => {
      started = true; await new Promise<void>(resolve => { release = resolve; }); return fauxAssistantMessage("Source checked");
    }]);
    const admitted = await app.tasks.wakeBackground(task.id);
    assert.equal(admitted.conversation_id, task.conversation_id);
    await pump(() => started);
    await app.tasks.update(task.id, current => { current.state = { state: "waiting", next_check: new Date(NOW).toISOString(), actor: "example-provider" }; });
    assert.deepEqual(await app.tasks.dueChecks(NOW), [], "A live actor retains the due lease until its turn finishes");
    release(); await pump(() => !app.tasks.isRunning(task.id));
    assert.deepEqual(await app.tasks.dueChecks(NOW), [{ id: task.id, next_check: new Date(NOW).toISOString() }]);
    assert.deepEqual((await app.tasks.get(task.id)).pending, []);
    const conversation = await app.tasks.conversation(task.conversation_id);
    const native = await conversation.commit(tx => tx.submissionByRequest(conversation.id, "source-review:native"), BACKGROUND_CONTEXT);
    assert.equal(native?.status, "done");
    await app.session.commit(tx => app.tasks.enqueueReviewIn(tx, { task_id: task.id, input_id: "source-review:native",
      content: "Inspect this source", sender_conversation_id: sender.id.toString(), work_class: "fresh" }), BACKGROUND_CONTEXT);
    assert.deepEqual((await app.tasks.get(task.id)).pending, []);
  } finally { release(); await pump(() => !app.tasks.isRunning(taskId)).catch(() => {}); await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("source effect wake drains an attached interactive actor without replacing its worker or conversation", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-task-attached-review-")), path = join(directory, "session.sqlite");
  const app = await open(path), controller = new AbortController(); let running: Promise<unknown> | undefined;
  try {
    const task = await app.tasks.create({ title: "Attached worker", outcome: "Retain this actor", model: fixtureModel });
    await app.tasks.enqueue(task.id, "Initial task input", "initial");
    await app.tasks.dispatch(task.id, host, async () => ({ type: "spawned", binding }));
    const bound = await app.tasks.get(task.id); assert.ok(bound.worker.state === "bound");
    const input: Extract<RuntimeCommand, { type: "run_task" }> = { type: "run_task", task_id: task.id,
      allocation_id: bound.worker.allocation_id, binding, instructions: "", model: fixtureModel, cwd: host.cwd, interactive: true };
    app.faux.setResponses([fauxAssistantMessage("Initial work complete"), fauxAssistantMessage("Fresh source reviewed")]);
    let idles = 0;
    running = app.tasks.run(input, () => () => {}, [], undefined, undefined, {
      signal: controller.signal, async ready() {}, answer() {}, idle() { idles += 1; },
    });
    await pump(() => idles === 1);
    const sender = await app.session.root(BACKGROUND_CONTEXT);
    await app.session.commit(tx => app.tasks.enqueueReviewIn(tx, { task_id: task.id, input_id: "source-review:attached",
      content: "Read the new source evidence", sender_conversation_id: sender.id.toString(), work_class: "backfill" }), BACKGROUND_CONTEXT);
    await app.tasks.wakeBackground(task.id);
    await pump(() => idles === 2);
    const restored = await app.tasks.get(task.id);
    assert.equal(restored.conversation_id, task.conversation_id); assert.equal(restored.execution, undefined);
    assert.ok(restored.worker.state === "bound"); assert.deepEqual(restored.worker.binding, binding);
    assert.deepEqual(restored.pending, []); assert.equal(app.faux.state.callCount, 2);
    const conversation = await app.tasks.conversation(task.conversation_id);
    const receipt = await conversation.commit(tx => tx.submissionByRequest(conversation.id, "source-review:attached"), BACKGROUND_CONTEXT);
    assert.equal(receipt?.status, "done");
    controller.abort(); await running; running = undefined;
  } finally { controller.abort(); await running?.catch(() => {}); await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});
