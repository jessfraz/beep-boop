import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { awaitWithContext, BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import type { Message, ToolCall } from "@earendil-works/pi-ai";
import { builtinModels } from "@earendil-works/pi-ai/providers/all";
import { AgentDoc, AssistantEntry, createRegistry, defineExtension, GenerationTask, Harness, hook, InboxDoc, LiveDoc,
  ToolResultEntry, ToolTask, UserEntry, type Conversation, type GenerationCheckpoint } from "@earendil-works/pi-durable";
import { SqliteStorage } from "@earendil-works/pi-durable/storage/sqlite";
import { openConversationDatabase } from "../src/conversation-database.js";
import { CoordinatorEnvironment } from "../src/environment.js";
import type { RuntimeCommand } from "../src/generated/RuntimeCommand.js";
import type { WorkerHost } from "../src/generated/WorkerHost.js";
import type { WorkerRef } from "../src/generated/WorkerRef.js";
import { createInputOrigins } from "../src/input_origin.js";
import { ConversationModels, DEFAULT_MODEL } from "../src/models.js";
import { TaskStore, type HostCall } from "../src/tasks.js";
import { createAppTools, Executions } from "../src/tools.js";
import { history, transcript } from "../src/transcript.js";
import { FakeClock, pump } from "./fixtures/background-runtime.js";

const binding: WorkerRef = { host_id: "11111111-1111-4111-8111-111111111111",
  session_id: "session:22222222-2222-4222-8222-222222222222", window_id: "window:33333333-3333-4333-8333-333333333333",
  block_id: "block:44444444-4444-4444-8444-444444444444" };
const allocation = "55555555-5555-4555-8555-555555555555";
const peerText = "Return a source receipt. The original delivery work is still unfinished.";
const humanText = "Finish the original delivery work now.";

async function open(directory: string, clock: FakeClock) {
  const models = builtinModels(), registry = createRegistry();
  const storage = await SqliteStorage.open(await openConversationDatabase(join(directory, "session.sqlite")));
  const session = await Harness.open(storage, { models, registry, now: clock.now, setTimer: clock.setTimer,
    identity: { attribution: null }, settings: { steeringMode: "all", compaction: { enabled: false } } }, BACKGROUND_CONTEXT);
  const inference = new ConversationModels(models), tasks = new TaskStore(session, inference), executions = new Executions();
  const environment = new CoordinatorEnvironment(session); tasks.useEnvironment(environment);
  await environment.configure({ node: { host_id: binding.host_id, name: "fixture", rex_server: null, coordinator: null,
    agent_program: process.execPath, home_directory: directory,
    launch: { state_dir: directory, runtime: null, config: null, instructions: null, model: DEFAULT_MODEL } },
    settings: { model: DEFAULT_MODEL, instructions: "Current workflow guidance", context_repositories: [], sources: [] } });
  environment.setConnected([binding.host_id]);
  const tools = createAppTools(tasks, executions), origins = createInputOrigins(session);
  registry.install(tools.worker); registry.install(origins);
  return { session, storage, inference, tasks, executions, registry, tools, origins };
}

/** Exercise the registered production tool through Pi, without a model double. */
async function invoke(session: Harness, conversation: Conversation, call: ToolCall): Promise<unknown> {
  const id = await conversation.commit(async tx => {
    const assistant = await tx.appendEntry(AssistantEntry, conversation.id, { model: [{ role: "assistant", content: [call],
      api: "openai-responses", provider: DEFAULT_MODEL.provider, model: DEFAULT_MODEL.model_id, stopReason: "toolUse", timestamp: 1,
      usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0,
        cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } } }] });
    return tx.createTask(ToolTask, { kind: "model", assistant: assistant.id, callId: call.id }, { ownership: { kind: "conversation" } });
  }, BACKGROUND_CONTEXT);
  const outcome = (await session.waitForTask(id, BACKGROUND_CONTEXT)).state.outcome;
  const tail = (await conversation.entries({ order: "descending" }, 3, undefined, BACKGROUND_CONTEXT)).items;
  assert.equal(outcome.status, "completed", JSON.stringify({ outcome, tail }));
  if (outcome.status !== "completed" || outcome.result.kind !== "model") throw new Error("The native tool task did not settle");
  const entryId = outcome.result.entryId;
  const entry = await conversation.commit(tx => tx.entry(ToolResultEntry, entryId), BACKGROUND_CONTEXT);
  const message = entry?.model?.[0]; assert.ok(message?.role === "toolResult");
  assert.equal(message.isError, false, "Peer coordination must use the registered native message_task tool");
  return JSON.parse(message.content.flatMap(part => part.type === "text" ? [part.text] : []).join(""));
}
function object(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

test("peer messages retain their sender through native steering and reopening without replacing the owning job or human input", { timeout: 15_000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-task-messages-")), clock = new FakeClock();
  let app = await open(directory, clock), worker: Promise<unknown> | undefined, releaseSender = () => {};
  const controller = new AbortController();
  let conversation: Conversation | undefined;
  let captured: readonly Message[] | undefined;
  const boundary = defineExtension({ name: "request-boundary-observer", hooks: [hook(GenerationTask, {
    async beforeRequest(request, _api, context) {
      captured = request.messages;
      // Hold at the actual provider boundary; no substitute model or network call.
      await awaitWithContext(new Promise<never>(() => {}), context);
    },
  })] });
  const noHost: HostCall = async command => assert.fail(`Coordination must not launch host effects: ${command.type}`);
  try {
    const owner = await app.tasks.create({ title: "Delivery job", outcome: "Save and verify the existing delivery records.",
      model: DEFAULT_MODEL, sources: [{ provider: "scheduled_jobs", account: "local", thread_id: "delivery-review" }] });
    const sender = await app.tasks.create({ title: "Morning review", outcome: "Review useful changes.", model: DEFAULT_MODEL });
    const host: WorkerHost = { host_id: binding.host_id, name: "fixture", rex_server: null, coordinator: null,
      agent_program: process.execPath, cwd: directory };
    await app.tasks.update(owner.id, task => { task.worker = { state: "bound", allocation_id: allocation, host, binding }; });
    await app.tasks.enqueue(owner.id, "Continue the delivery job", "original-job");
    conversation = await app.tasks.conversation(owner.conversation_id);
    const native = await conversation.commit(async tx => {
      const user = await tx.appendEntry(UserEntry, conversation!.id, { model: [{ role: "user", content: "Continue the delivery job", timestamp: 1 }] });
      const submission = await tx.createSubmission({ type: "input", conversationId: conversation!.id,
        requestId: "original-job", status: "placed", entry: user.id });
      const assistant = await tx.appendEntry(AssistantEntry, conversation!.id, { model: [] });
      const generation = await tx.createTask(GenerationTask, {}, { ownership: { kind: "conversation" } });
      const live = await tx.doc(LiveDoc, conversation!.id);
      live.run = { taskId: generation, inputs: [submission.id] }; live.tools = [];
      return { generation, assistant: assistant.id };
    }, BACKGROUND_CONTEXT);
    const record = await app.session.getTask(native.generation, BACKGROUND_CONTEXT); assert.ok(record);
    const deadline = clock.value + 3_600_000;
    const checkpoint: GenerationCheckpoint = { phase: "tools", assistant: native.assistant, tools: [], pending: [] };
    await app.storage.commit([{ type: "task", value: { ...record, state: { status: "sleeping", until: deadline, checkpoint } } }], BACKGROUND_CONTEXT);
    await app.session.close(BACKGROUND_CONTEXT); app = await open(directory, clock);
    app.registry.install(boundary);
    conversation = await app.tasks.conversation(owner.conversation_id);
    const command: Extract<RuntimeCommand, { type: "run_task" }> = { type: "run_task", task_id: owner.id,
      allocation_id: allocation, binding, instructions: "Current shared guidance", model: DEFAULT_MODEL, cwd: directory, interactive: true };
    worker = app.tasks.run(command, (current, executionHost, model) => app.executions.bind(current.id.toString(), {
      cwd: directory, hosts: [executionHost], model, role: "worker", call: noHost,
    }), [app.origins, boundary], noHost, undefined, { signal: controller.signal, async ready() {}, answer() {}, idle() {} });
    void worker.catch(() => {});
    await pump(async () => (await app.tasks.get(owner.id)).pending.length === 0);
    const configuration = await app.session.snapshot(AgentDoc, conversation.id, BACKGROUND_CONTEXT); assert.ok(configuration);
    const from = await app.tasks.conversation(sender.conversation_id);
    releaseSender = app.executions.bind(from.id.toString(), { cwd: directory, hosts: [host], role: "worker", call: noHost });
    await from.configure({ extensions: [app.tools.worker] }, BACKGROUND_CONTEXT);
    const result = await invoke(app.session, from, { type: "toolCall", id: "peer-coordination", name: "message_task",
      arguments: { task_id: owner.id, input: peerText, mode: "steer" } });
    assert.ok(object(result) && typeof result.input_id === "string");
    const peerId = result.input_id;
    const queued = (await app.tasks.get(owner.id)).pending.find(input => input.id === peerId); assert.ok(queued);
    assert.ok("sender" in queued);
    const expectedSender = { type: "task", task_id: sender.id, conversation_id: sender.conversation_id, title: sender.title };
    assert.deepEqual(queued.sender, expectedSender);
    await app.tasks.update(sender.id, task => { task.title = "Renamed morning review"; });
    // The retry reaches the public enqueue boundary as a recovered native tool
    // or CLI request does. Titles describe the sender; they are not identity.
    await Reflect.apply(app.tasks.enqueue, app.tasks, [owner.id, peerText, peerId, undefined, "steer", undefined, undefined,
      { ...expectedSender, title: "Renamed morning review" }]);
    assert.deepEqual((await app.tasks.get(owner.id)).pending.find(input => input.id === peerId), queued);
    await assert.rejects(Reflect.apply(app.tasks.enqueue, app.tasks, [owner.id, peerText, peerId, undefined, "steer", undefined, undefined,
      { ...expectedSender, conversation_id: owner.conversation_id }]), /cannot be reused/);
    await assert.rejects(Reflect.apply(app.tasks.enqueue, app.tasks, [owner.id, peerText, peerId, undefined, "steer", undefined, undefined,
      { ...expectedSender, task_id: owner.id }]), /cannot be reused/);
    await app.tasks.enqueue(owner.id, humanText, "human-correction", undefined, "steer", undefined, "ios");
    const inbox = await app.session.snapshot(InboxDoc, conversation.id, BACKGROUND_CONTEXT); assert.ok(inbox);
    assert.equal(inbox.items.length, 2);
    const peer = inbox.items[0], human = inbox.items[1]; assert.ok(peer?.mode === "steer" && human?.mode === "steer");
    assert.equal(human.content, humanText);
    assert.ok(object(peer.entry?.data)); assert.deepEqual(peer.entry.data.sender, expectedSender);
    assert.equal(peer.entry.data.text, peerText);
    assert.equal((await app.tasks.get(owner.id)).outcome, owner.outcome);
    assert.deepEqual(await app.session.snapshot(AgentDoc, conversation.id, BACKGROUND_CONTEXT), configuration);
    clock.advance(3_600_000);
    await pump(() => captured !== undefined);
    const entries = (await conversation.entries({ order: "ascending" }, 100, undefined, BACKGROUND_CONTEXT)).items;
    const peerEntry = entries.find(entry => entry.kind === "agent.input" && object(entry.data) && entry.data.text === peerText);
    const humanEntry = entries.find(entry => entry.kind === "agent.input" && object(entry.data) && entry.data.text === humanText);
    assert.ok(peerEntry && humanEntry, "Pi's real tool boundary must place both steers");
    const peerMessages = transcript(peerEntry), humanMessages = transcript(humanEntry);
    assert.equal(peerMessages.length, 1); assert.equal(peerMessages[0]?.role, "notice"); assert.equal(peerMessages[0]?.text, peerText);
    assert.deepEqual(peerMessages[0]?.presentation, { type: "agent_coordination", action: "send_message",
      sender_thread_id: sender.conversation_id, receiver_thread_ids: [owner.conversation_id], agents: [] });
    assert.equal(humanMessages[0]?.role, "user"); assert.equal(humanMessages[0]?.text, humanText);
    const modelPeer = captured?.find(message => message.role === "user" && typeof message.content === "string" && message.content.includes(peerText));
    assert.ok(modelPeer?.role === "user" && typeof modelPeer.content === "string");
    const quoted: unknown = JSON.parse(modelPeer.content.slice(modelPeer.content.indexOf("\n") + 1));
    assert.deepEqual(quoted, { sender: expectedSender, text: peerText });
    assert.ok(captured?.some(message => message.role === "user" && message.content === humanText));
    controller.abort(); await conversation.abort(BACKGROUND_CONTEXT); await worker; worker = undefined;
    releaseSender(); releaseSender = () => {};
    await app.session.close(BACKGROUND_CONTEXT); app = await open(directory, clock);
    conversation = await app.tasks.conversation(owner.conversation_id);
    const restored = (await history(conversation, null, app.session)).flatMap(frame => frame.messages);
    assert.deepEqual(restored.filter(message => message.entry_id === peerEntry.id.toString()), peerMessages);
    assert.deepEqual(restored.filter(message => message.entry_id === humanEntry.id.toString()), humanMessages);
    assert.equal((await app.tasks.get(owner.id)).outcome, owner.outcome);
  } finally {
    controller.abort(); releaseSender(); await conversation?.abort(BACKGROUND_CONTEXT).catch(() => {}); await worker?.catch(() => {});
    await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true });
  }
});
