import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { fauxAssistantMessage, fauxToolCall } from "@earendil-works/pi-ai";
import { runScheduled } from "../src/scheduled.js";
import { FakeClock, pump, START } from "./fixtures/background-runtime.js";
import { openScheduled, scheduledOccurrence } from "./fixtures/scheduled-runtime.js";
import { fixtureModel } from "./model.js";

const exhausted = () => fauxAssistantMessage("", {
  stopReason: "error", errorMessage: "subscription_sharing_usage_limit_exceeded",
});

test("quota recovery retains the same scheduled input and completed tool across restart", { timeout: 15_000 }, async (t) => {
  t.mock.timers.enable({ apis: ["Date"], now: START });
  const directory = await mkdtemp(join(tmpdir(), "agent-quota-recovery-")), clock = new FakeClock();
  let app = await openScheduled(directory, "original", clock);
  let controller = new AbortController();
  let running: Promise<void> | undefined;
  try {
    app.faux.setResponses([
      fauxAssistantMessage(fauxToolCall("exec", { program: "perform-once", args: [], timeout_seconds: 1 }), { stopReason: "toolUse" }),
      exhausted(),
    ]);
    running = runScheduled(scheduledOccurrence, controller.signal, app);
    void running.catch(() => {});
    await pump(() => app.faux.state.callCount === 2);
    await pump(async () => (await app.session.inspect(BACKGROUND_CONTEXT)).tasks.every(task => task.state.kind !== "running"));
    const task = (await app.tasks.list())[0]; assert.ok(task);
    const conversation = await app.tasks.conversation(task.conversation_id);
    const original = await app.session.commit(tx => tx.submissionByRequest(conversation.id, scheduledOccurrence.id), BACKGROUND_CONTEXT);
    assert.equal(original?.status, "placed", "Quota exhaustion must keep the original input recoverable");
    assert.equal(app.commands.filter(command => command.type === "exec" && command.program === "perform-once").length, 1);
    const deferred = (await app.session.inspect(BACKGROUND_CONTEXT)).tasks.find(task => task.record.kind === "pi.generation");
    assert.equal(deferred?.state.kind, "sleeping");
    clock.advance(60_000); t.mock.timers.tick(60_000);
    await pump(async () => (await app.session.inspect(BACKGROUND_CONTEXT)).tasks.some(task =>
      task.record.kind === "pi.generation" && task.state.kind === "sleeping" && task.state.until !== undefined && task.state.until > clock.value));
    assert.equal(app.faux.state.callCount, 2, "The account must stay quiet during cooldown");
    controller.abort(); await running.catch(() => {}); running = undefined;
    await app.close();
    app = await openScheduled(directory, "updated", clock);
    app.faux.setResponses([context => {
      assert.ok(context.messages.some(message => message.role === "toolResult" && message.toolName === "exec"));
      return fauxAssistantMessage("The original review finished.");
    }]);
    controller = new AbortController();
    running = runScheduled(scheduledOccurrence, controller.signal, app, task.id);
    void running.catch(() => {});
    await pump(() => app.tasks.hasExecutionOwner(task.id, conversation.id));
    clock.advance(3_600_000); t.mock.timers.tick(3_600_000);
    await pump(async () => (await app.session.commit(tx => tx.submissionByRequest(conversation.id, scheduledOccurrence.id), BACKGROUND_CONTEXT))?.status === "done");
    await running; running = undefined;
    const restored = await app.session.commit(tx => tx.submissionByRequest(conversation.id, scheduledOccurrence.id), BACKGROUND_CONTEXT);
    assert.equal(restored?.id, original?.id);
    assert.equal(app.faux.state.callCount, 1);
    assert.equal(app.commands.filter(command => command.type === "exec" && command.program === "perform-once").length, 0);
    const retainedTasks = await app.tasks.list();
    assert.deepEqual(retainedTasks.filter(record => record.workflow?.work_key === "occurrence:" + scheduledOccurrence.id).map(record => record.id), [task.id],
      "Recovery must keep exactly one execution for the admitted occurrence");
    assert.equal(retainedTasks.length, 2, "The canonical workflow owner is separate from its occurrence execution");
  } finally {
    controller.abort();
    await app.close(); await running?.catch(() => {});
    await rm(directory, { recursive: true, force: true });
  }
});

test("one account check releases concurrent conversations when quota returns", { timeout: 15_000 }, async (t) => {
  t.mock.timers.enable({ apis: ["Date"], now: START });
  const directory = await mkdtemp(join(tmpdir(), "agent-quota-shared-")), clock = new FakeClock();
  const app = await openScheduled(directory, "original", clock);
  let release = () => {};
  try {
    const first = await app.session.createConversation({ ownership: { kind: "ownerless" } }, BACKGROUND_CONTEXT);
    const second = await app.session.createConversation({ ownership: { kind: "ownerless" } }, BACKGROUND_CONTEXT);
    await app.tasks.inference.configure(first, fixtureModel, {});
    await app.tasks.inference.configure(second, fixtureModel, {});
    const probe = new Promise<void>(resolve => { release = resolve; });
    app.faux.setResponses([exhausted(), async () => { await probe; return fauxAssistantMessage("Capacity returned."); }, fauxAssistantMessage("The other turn finished.")]);
    const firstInput = await first.submit({ type: "input", content: "First request.", requestId: "quota-first" }, BACKGROUND_CONTEXT);
    await pump(async () => app.faux.state.callCount === 1 && (await app.session.inspect(BACKGROUND_CONTEXT)).tasks.some(task => task.state.kind === "sleeping"));
    const secondInput = await second.submit({ type: "input", content: "Second request.", requestId: "quota-second" }, BACKGROUND_CONTEXT);
    await pump(async () => (await app.session.inspect(BACKGROUND_CONTEXT)).tasks.filter(task => task.state.kind === "sleeping").length === 2);
    assert.equal(app.faux.state.callCount, 1);
    clock.advance(3_600_000); t.mock.timers.tick(3_600_000);
    await pump(() => app.faux.state.callCount === 2);
    for (let index = 0; index < 10; index += 1) await new Promise<void>(resolve => setImmediate(resolve));
    assert.equal(app.faux.state.callCount, 2, "Only one retained request may check an exhausted account");
    release();
    await pump(async () => (await firstInput.status(BACKGROUND_CONTEXT)).status === "done");
    clock.advance(3_600_000); t.mock.timers.tick(3_600_000);
    await pump(async () => (await secondInput.status(BACKGROUND_CONTEXT)).status === "done");
    assert.equal(app.faux.state.callCount, 3);
  } finally { release(); await app.close(); await rm(directory, { recursive: true, force: true }); }
});
