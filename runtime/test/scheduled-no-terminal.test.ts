import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { fauxAssistantMessage, fauxToolCall } from "@earendil-works/pi-ai";
import { runScheduled } from "../src/scheduled.js";
import { pump } from "./fixtures/background-runtime.js";
import { openScheduled, scheduledOccurrence } from "./fixtures/scheduled-runtime.js";

test("an admitted job executes its host tool and saves its native answer without a terminal attachment", { timeout: 10_000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-job-without-terminal-")), app = await openScheduled(directory);
  const controller = new AbortController(); let running: Promise<void> | undefined;
  try {
    app.faux.setResponses([
      fauxAssistantMessage(fauxToolCall("exec", { program: "example", args: [], timeout_seconds: 1 }), { stopReason: "toolUse" }),
      (context) => {
        assert.ok(context.messages.some((message) => message.role === "toolResult" && message.content.some((part) =>
          part.type === "text" && part.text.includes("owned host tool output"))));
        return fauxAssistantMessage("The scheduled host result is verified.");
      },
    ]);
    running = runScheduled(scheduledOccurrence, controller.signal, app);
    await pump(() => app.faux.state.callCount > 0 || app.commands.some((command) => command.type === "spawn_worker"));
    assert.equal(app.commands.some((command) => command.type === "spawn_worker"), false,
      "A scheduled job must execute without creating a Rex session or waiting for run_task to attach");
    await running;
    const task = (await app.tasks.list())[0]; assert.ok(task);
    const conversation = await app.tasks.conversation(task.conversation_id);
    const native = await app.session.commit((tx) => tx.submissionByRequest(conversation.id, scheduledOccurrence.id), BACKGROUND_CONTEXT);
    assert.equal(native?.status, "done"); assert.deepEqual(task.pending, []);
    assert.ok(native?.type === "input" && native.status === "done");
    const answer = await app.session.commit((tx) => tx.entry(native.answer), BACKGROUND_CONTEXT);
    assert.ok(answer?.model?.some((message) => message.role === "assistant" && message.content.some((part) =>
      part.type === "text" && part.text === "The scheduled host result is verified.")));
    const effect = app.commands.find((command) => command.type === "exec"); assert.ok(effect?.type === "exec");
    assert.equal(effect.cwd, scheduledOccurrence.job.cwd);
  } finally {
    controller.abort(); await running?.catch(() => {}); await app.session.close(BACKGROUND_CONTEXT);
    await rm(directory, { recursive: true, force: true });
  }
});
