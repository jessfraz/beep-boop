import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createInterface } from "node:readline";
import { test } from "node:test";
import { fileURLToPath } from "node:url";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { fauxAssistantMessage } from "@earendil-works/pi-ai";
import { AgentDoc, LiveDoc } from "@earendil-works/pi-durable";
import { pump } from "./fixtures/background-runtime.js";
import { openScheduled } from "./fixtures/scheduled-runtime.js";

function gate() {
  let release = () => {};
  const promise = new Promise<void>(resolve => { release = resolve; });
  return { promise, release };
}

test("a resumed actor admits steering and follow-ups while joining its placed native turn with an empty app outbox", { timeout: 15_000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-resumed-input-admission-"));
  const child = spawn(process.execPath, ["--no-warnings", fileURLToPath(new URL("./fixtures/scheduled-admission-child.js", import.meta.url)),
    directory, "continuation", "graceful"], { stdio: ["pipe", "pipe", "pipe"] });
  const exited = new Promise<number | null>(resolve => child.once("close", resolve));
  const lines = createInterface({ input: child.stdout });
  const joined = gate(), providerStarted = gate(), providerHeld = gate();
  let app: Awaited<ReturnType<typeof openScheduled>> | undefined, stderr = "";
  child.stderr.on("data", (data: Buffer) => { stderr += data.toString(); });
  try {
    assert.equal((await lines[Symbol.asyncIterator]().next()).value, "PLACED", stderr);
    child.stdin.end(); assert.equal(await exited, 0, stderr); lines.close();
    app = await openScheduled(directory, "resumed");
    const task = (await app.tasks.list())[0]; assert.ok(task); assert.deepEqual(task.pending, []);
    const conversation = await app.tasks.conversation(task.conversation_id);
    const receipt = (requestId: string) => conversation.commit(tx => tx.submissionByRequest(conversation.id, requestId), BACKGROUND_CONTEXT);
    const original = await receipt("continuation-input"); assert.equal(original?.status, "placed");
    const previousRun = (await app.session.snapshot(LiveDoc, conversation.id, BACKGROUND_CONTEXT))?.run; assert.ok(previousRun);
    // Observe the actual idle join, then delegate to Pi unchanged. The actor
    // must own this resumed run even though its application outbox was drained.
    const findConversation = app.tasks.conversation.bind(app.tasks);
    app.tasks.conversation = async id => {
      const native = await findConversation(id);
      if (id === task.conversation_id) {
        const waitForIdle = native.waitForIdle.bind(native);
        native.waitForIdle = context => { joined.release(); return waitForIdle(context); };
      }
      return native;
    };
    app.faux.setResponses([async () => { providerStarted.release(); await providerHeld.promise; return fauxAssistantMessage("The original retained turn completed."); },
      ...Array.from({ length: 4 }, () => fauxAssistantMessage("The retained input and its correction were verified."))]);
    await app.tasks.resumeBackground(); await Promise.all([joined.promise, providerStarted.promise]);
    assert.deepEqual((await app.tasks.get(task.id)).pending, []);
    const configuration = await app.session.snapshot(AgentDoc, conversation.id, BACKGROUND_CONTEXT);
    await app.views.input({ type: "conversation_input", conversation_id: task.conversation_id, input_id: "resumed-steer",
      input: "Apply this correction to the ongoing retained turn.", mode: "steer", origin: "cli" }, app.call, () => {});
    await app.views.input({ type: "conversation_input", conversation_id: task.conversation_id, input_id: "resumed-follow-up",
      input: "Continue with this separate follow-up after the retained work.", mode: "follow_up", origin: "cli" }, app.call, () => {});
    const steer = await receipt("resumed-steer"), followUp = await receipt("resumed-follow-up");
    assert.ok(steer, "A steer must reach Pi before the original provider finishes, even when reattachment has no application outbox item");
    assert.equal(steer.status, "queued"); assert.equal(followUp?.status, "queued");
    assert.deepEqual(await receipt("continuation-input"), original);
    assert.deepEqual((await app.session.snapshot(LiveDoc, conversation.id, BACKGROUND_CONTEXT))?.run, previousRun);
    assert.deepEqual(await app.session.snapshot(AgentDoc, conversation.id, BACKGROUND_CONTEXT), configuration);
    providerHeld.release();
    const restored = app;
    await pump(async () => !restored.tasks.isRunning(task.id) &&
      (await receipt("resumed-steer"))?.status === "done" && (await receipt("resumed-follow-up"))?.status === "done");
    assert.equal((await receipt("continuation-input"))?.status, "done");
    assert.deepEqual((await app.tasks.get(task.id)).pending, []);
    assert.equal(app.commands.filter(command => command.type === "exec").length, 0, "Reattachment must not replay the retained host tool");
    await app.close(); app = await openScheduled(directory, "reopened");
    const reopened = await app.tasks.conversation(task.conversation_id);
    for (const previous of [original, steer, followUp]) {
      assert.ok(previous?.requestId);
      const requestId = previous.requestId;
      const saved = await reopened.commit(tx => tx.submissionByRequest(reopened.id, requestId), BACKGROUND_CONTEXT);
      assert.equal(saved?.id, previous.id); assert.equal(saved?.status, "done");
    }
    await app.tasks.resumeBackground(); assert.equal(app.faux.state.callCount, 0);
    assert.equal(app.tasks.isRunning(task.id), false);
  } finally {
    providerHeld.release(); child.stdin.end(); child.kill(); lines.close(); await exited;
    await app?.close(); await rm(directory, { recursive: true, force: true });
  }
});
