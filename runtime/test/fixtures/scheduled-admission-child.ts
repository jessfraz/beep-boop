import { getCurrentSystemPrompt } from "@earendil-works/pi-ai/utils/transcript";
import { fauxAssistantMessage, fauxToolCall } from "@earendil-works/pi-ai";
import { runScheduled } from "../../src/scheduled.js";
import { pump } from "./background-runtime.js";
import { openScheduled, scheduledOccurrence } from "./scheduled-runtime.js";

const directory = process.argv[2];
if (directory === undefined) throw new Error("Missing scheduled fixture directory");
process.stdin.resume();
const app = await openScheduled(directory);
if (process.argv[4] === "graceful") process.stdin.once("end", () => {
  void app.close().then(() => process.exit(0), error => { process.stderr.write(String(error)); process.exit(1); });
});
const phase = process.argv[3];
if (phase !== undefined) {
  if (phase !== "child" && phase !== "continuation") throw new Error("Unknown placed-work fixture phase");
  app.faux.setResponses([fauxAssistantMessage("The original occurrence completed.")]);
  await runScheduled(scheduledOccurrence, new AbortController().signal, app);
  const task = (await app.tasks.list())[0];
  if (task === undefined) throw new Error("The fixture task was not created");
  await pump(() => !app.tasks.isRunning(task.id));
  app.faux.setResponses([fauxAssistantMessage(fauxToolCall("exec", { program: "example", args: [] }), { stopReason: "toolUse" }),
    async (_context, options) => {
      const signal = options?.signal;
      if (signal === undefined) throw new Error("The held provider request needs its cancellation signal");
      process.stdout.write("PLACED\n");
      await new Promise<void>((resolve) => { if (signal.aborted) resolve(); else signal.addEventListener("abort", () => resolve(), { once: true }); });
      return fauxAssistantMessage("The interrupted provider response must not complete the saved input.");
    }]);
  if (phase === "child") {
    await app.tasks.createAgent(task.id, { id: "child", title: "Child assignment", outcome: "Finish the bounded child", input: "Continue child work", inputId: "child-input" });
    await app.tasks.dispatchAgent(task.id, "child", app.call);
  } else {
    await app.views.input({ type: "conversation_input", conversation_id: task.conversation_id, input_id: "continuation-input",
      input: "Continue the same saved workflow.", mode: "follow_up" }, app.call, () => {});
  }
  await new Promise(() => {});
}
app.faux.setResponses([async (context) => {
  if (!getCurrentSystemPrompt(context.messages).includes("FRESH_PROJECT_POLICY")) throw new Error("The worker ran without its current project guidance");
  await pump(async () => (await app.tasks.list())[0]?.pending.length === 0);
  process.stdout.write("PLACED\n");
  return new Promise(() => {});
}]);
const scheduled = runScheduled(scheduledOccurrence, new AbortController().signal, app);
await scheduled;
