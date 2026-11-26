import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { openNodeSqliteDatabase } from "@earendil-works/chord/sqlite/node";
import { createModels, fauxAssistantMessage, fauxProvider, fauxToolCall, type TranscriptContext } from "@earendil-works/pi-ai";
import { AgentDoc, CompactionEntry, createRegistry, defineDocFamily, defineExtension, Harness, UsageDoc } from "@earendil-works/pi-durable";
import { SqliteStorage } from "@earendil-works/pi-durable/storage/sqlite";
import { ConversationModels } from "../src/models.js";
import { fixtureModel } from "./model.js";
import type { CompactionFailure } from "../src/compaction.js";
import { SqlTrace } from "./fixtures/state-storage.js";

const WINDOW = 8192;
async function open(directory: string, window = WINDOW, trace = new SqlTrace()) {
  const models = createModels(), registry = createRegistry();
  const faux = fauxProvider({ provider: "openai", models: [{ id: "fixture", contextWindow: window, maxTokens: 512 }], tokensPerSecond: 1_000_000 });
  models.setProvider(faux.provider);
  const inference = new ConversationModels(models, undefined, registry);
  const owned = defineExtension({ name: "owned_tools" }), excluded = defineExtension({ name: "excluded_tools" });
  registry.install(owned); registry.install(excluded);
  const database = await openNodeSqliteDatabase(join(directory, "session.sqlite"));
  const session = await Harness.open(await SqliteStorage.open(trace.database(database)), {
    models, registry, identity: { attribution: null }, settings: { compaction: { enabled: false, keepRecentTokens: 0 }, retry: { enabled: false } },
  }, BACKGROUND_CONTEXT);
  inference.bind(session);
  const root = await session.root(BACKGROUND_CONTEXT);
  await inference.configure(root, fixtureModel, { extensions: [owned] });
  return { session, root, inference, faux, database, trace };
}
const Failures = defineDocFamily<{ failure: CompactionFailure | null }, null>({
  kind: "agent.compaction.failure", scope: "conversation", family: true, version: 1, history: "latest", fork: "initial",
  initial: () => ({ failure: null }), checkpointWhen: () => true,
});
function text(context: TranscriptContext): string {
  return context.messages.map((message) => message.role === "system" || message.role === "user"
    ? typeof message.content === "string" ? message.content : message.content.flatMap((part) => part.type === "text" ? [part.text] : []).join("\n") : "").join("\n");
}
async function seed(app: Awaited<ReturnType<typeof open>>) {
  const body = "FIRST_MARKER " + "e\u0301🦊漢字 ".repeat(1800) + " LAST_MARKER";
  await app.root.submit({ type: "write", entry: { kind: "fixture.history", model: [
    { role: "user", content: body, timestamp: 1 },
    fauxAssistantMessage(fauxToolCall("completed_effect", { receipt: "EFFECT_ONCE", payload: "argument ".repeat(1200) }), { stopReason: "toolUse" }),
    { role: "toolResult", toolCallId: "old-effect", toolName: "completed_effect", content: [{ type: "text", text: "RESULT_MARKER " + "result ".repeat(1200) }], isError: false, timestamp: 2 },
  ] } }, BACKGROUND_CONTEXT);
  await app.root.submit({ type: "write", entry: { kind: "fixture.tail", model: [{ role: "user", content: "KEEP_THIS_TAIL", timestamp: 3 }] } }, BACKGROUND_CONTEXT);
  return body;
}

test("native compaction reduces oversized inputs, arguments and results without widening tools or changing originals", { timeout: 15_000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-bounded-compaction-")), app = await open(directory);
  const requests: string[] = [];
  try {
    const body = await seed(app);
    const before = (await app.root.entries({ order: "ascending" }, 100, undefined, BACKGROUND_CONTEXT)).items;
    const respond: Parameters<typeof app.faux.setResponses>[0][number] = (context, options) => {
      const source = text(context); requests.push(source);
      if (Buffer.byteLength(source) + (options?.maxTokens ?? 0) > WINDOW) return fauxAssistantMessage("", { stopReason: "error", errorMessage: "Your input exceeds the context window of this model." });
      assert.equal(context.messages.length, 2); assert.notEqual(options?.reasoning, "xhigh");
      assert.equal(options?.toolChoice, "none", "Checkpoint requests cannot select provider-hosted tools either");
      assert.ok(context.messages.every((message) => message.role !== "system" || message.toolsAdded === undefined));
      assert.equal(source.includes("�"), false);
      return fauxAssistantMessage("Preserve FIRST_MARKER LAST_MARKER EFFECT_ONCE RESULT_MARKER; completed effect stays complete.");
    };
    app.faux.setResponses(Array.from({ length: 80 }, () => respond));
    const compacted = await app.session.waitForTask(await app.root.compact(undefined, BACKGROUND_CONTEXT), BACKGROUND_CONTEXT);
    assert.equal(compacted.state.outcome.status, "completed"); assert.ok(requests.length > 2);
    const received = requests.join("\n");
    for (const marker of ["FIRST_MARKER", "LAST_MARKER", "EFFECT_ONCE", "RESULT_MARKER"]) assert.ok(received.includes(marker));
    assert.ok(body.length > WINDOW);
    const after = (await app.root.entries({ order: "ascending" }, 100, undefined, BACKGROUND_CONTEXT)).items;
    assert.deepEqual(after.filter((entry) => !CompactionEntry.is(entry)), before);
    const summaries = after.filter(CompactionEntry.is); assert.equal(summaries.length, 1);
    assert.equal(summaries[0]?.head, before.at(-1)?.id);
    const agent = await app.session.snapshot(AgentDoc, app.root.id, BACKGROUND_CONTEXT);
    assert.deepEqual(agent?.extensions, ["owned_tools", "bounded_compaction"]);
    const ledger = await app.session.snapshot(UsageDoc, app.root.id, BACKGROUND_CONTEXT);
    assert.ok((ledger?.models["openai/fixture"]?.totalTokens ?? 0) > 0);
    app.faux.setResponses([fauxAssistantMessage("Continued with the retained checkpoint.")]);
    const continued = await app.root.submit({ type: "input", content: "Continue saved work." }, BACKGROUND_CONTEXT);
    assert.equal((await continued.wait(BACKGROUND_CONTEXT)).status, "done");
  } finally { await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

const failures: ("length" | "empty" | "tool")[] = ["length", "empty", "tool"];
for (const failure of failures) {
  test(`an incomplete ${failure} fragment cannot send the original oversized fallback or discard saved work`, { timeout: 15_000 }, async () => {
    const directory = await mkdtemp(join(tmpdir(), "agent-bounded-failure-")), app = await open(directory);
    const requests: string[] = [];
    try {
      await seed(app);
      const before = (await app.root.entries({}, 100, undefined, BACKGROUND_CONTEXT)).items;
      const respond: Parameters<typeof app.faux.setResponses>[0][number] = (context, options) => {
        const source = text(context); requests.push(source);
        assert.ok(Buffer.byteLength(source) + (options?.maxTokens ?? 0) < WINDOW, "No oversized fallback may reach the provider");
        if (requests.length === 1) return fauxAssistantMessage("Retained completed fragment EFFECT_ONCE.");
        return failure === "tool" ? fauxAssistantMessage(fauxToolCall("never_execute", {}), { stopReason: "toolUse" })
          : fauxAssistantMessage("", { stopReason: failure === "length" ? "length" : "stop" });
      };
      app.faux.setResponses(Array.from({ length: 80 }, () => respond));
      const failed = await app.session.waitForTask(await app.root.compact(undefined, BACKGROUND_CONTEXT), BACKGROUND_CONTEXT);
      assert.equal(failed.state.outcome.status, "failed"); assert.equal(requests.length, 2,
        JSON.stringify(requests.map((source) => ({ bytes: Buffer.byteLength(source), start: source.slice(0, 100) }))));
      assert.deepEqual((await app.root.entries({}, 100, undefined, BACKGROUND_CONTEXT)).items, before);
      const completed = requests[0], repeated: string[] = [];
      const finish: Parameters<typeof app.faux.setResponses>[0][number] = (context) => {
        repeated.push(text(context)); return fauxAssistantMessage("Merged retained checkpoint EFFECT_ONCE.");
      };
      app.faux.setResponses(Array.from({ length: 80 }, () => finish));
      const recovered = await app.session.waitForTask(await app.root.compact(undefined, BACKGROUND_CONTEXT), BACKGROUND_CONTEXT);
      assert.equal(recovered.state.outcome.status, "completed"); assert.ok(completed);
      assert.equal(repeated.includes(completed), false);
    } finally { await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
  });
}

test("an incomplete tool-result checkpoint cannot fall back to Pi's truncated transcript", { timeout: 15_000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-bounded-tool-result-")), app = await open(directory, 16_384);
  let calls = 0;
  try {
    await app.root.submit({ type: "write", entry: { kind: "fixture.result", model: [
      fauxAssistantMessage(fauxToolCall("completed_effect", {}, { id: "original-effect" }), { stopReason: "toolUse" }),
      { role: "toolResult", toolName: "completed_effect", toolCallId: "original-effect",
      content: [{ type: "text", text: "result ".repeat(600) + " IMPORTANT_TAIL_EFFECT_RECEIPT" }], isError: false, timestamp: 1 }] } }, BACKGROUND_CONTEXT);
    await app.root.submit({ type: "write", entry: { kind: "fixture.tail", model: [{ role: "user", content: "Keep this tail.", timestamp: 2 }] } }, BACKGROUND_CONTEXT);
    const incomplete: Parameters<typeof app.faux.setResponses>[0][number] = (context) => {
      calls += 1; assert.ok(text(context).includes("IMPORTANT_TAIL_EFFECT_RECEIPT"));
      return fauxAssistantMessage("", { stopReason: "length" });
    };
    app.faux.setResponses([incomplete, incomplete]);
    const failed = await app.session.waitForTask(await app.root.compact(undefined, BACKGROUND_CONTEXT), BACKGROUND_CONTEXT);
    assert.equal(failed.state.outcome.status, "failed"); assert.equal(calls, 1);
    app.faux.setResponses([(context) => {
      assert.ok(text(context).includes("IMPORTANT_TAIL_EFFECT_RECEIPT")); return fauxAssistantMessage("Completed effect receipt IMPORTANT_TAIL_EFFECT_RECEIPT.");
    }]);
    const recovered = await app.session.waitForTask(await app.root.compact(undefined, BACKGROUND_CONTEXT), BACKGROUND_CONTEXT);
    assert.equal(recovered.state.outcome.status, "completed", JSON.stringify(recovered.state.outcome));
  } finally { await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("quoted truncation markers in a small user input remain ordinary checkpoint data", { timeout: 15_000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-bounded-quoted-marker-")), app = await open(directory);
  try {
    await app.root.submit({ type: "write", entry: { kind: "fixture.quoted", model: [{ role: "user", timestamp: 1,
      content: "Explain this log format: [Tool result]: example [... 10 more characters truncated]" }] } }, BACKGROUND_CONTEXT);
    await app.root.submit({ type: "write", entry: { kind: "fixture.tail", model: [{ role: "user", content: "Keep the tail.", timestamp: 2 }] } }, BACKGROUND_CONTEXT);
    app.faux.setResponses([(context) => { assert.ok(text(context).includes("Explain this log format")); return fauxAssistantMessage("The user is asking about a quoted log marker."); }]);
    const result = await app.session.waitForTask(await app.root.compact(undefined, BACKGROUND_CONTEXT), BACKGROUND_CONTEXT);
    assert.equal(result.state.outcome.status, "completed"); assert.equal(app.faux.state.callCount, 1);
  } finally { await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("reopening interrupted native compaction reuses completed fragments and their usage", { timeout: 15_000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-bounded-reopen-")); let app = await open(directory);
  let secondStarted = () => {}; const second = new Promise<void>((resolve) => { secondStarted = resolve; });
  let firstSource = "", calls = 0;
  try {
    await seed(app);
    const respond: Parameters<typeof app.faux.setResponses>[0][number] = async (context, options) => {
      calls += 1;
      if (calls === 1) { firstSource = text(context); return fauxAssistantMessage("Completed first fragment EFFECT_ONCE."); }
      secondStarted(); const signal = options?.signal; assert.ok(signal);
      await new Promise<void>((resolve) => { if (signal.aborted) resolve(); else signal.addEventListener("abort", () => resolve(), { once: true }); });
      return fauxAssistantMessage("", { stopReason: "aborted" });
    };
    app.faux.setResponses(Array.from({ length: 80 }, () => respond));
    const id = await app.root.compact(undefined, BACKGROUND_CONTEXT);
    await second; const before = await app.session.snapshot(UsageDoc, app.root.id, BACKGROUND_CONTEXT);
    assert.ok((before?.models["openai/fixture"]?.totalTokens ?? 0) > 0, JSON.stringify({ before, firstSourceBytes: Buffer.byteLength(firstSource), calls }));
    await app.session.close(BACKGROUND_CONTEXT); app = await open(directory);
    const sources: string[] = [];
    const finish: Parameters<typeof app.faux.setResponses>[0][number] = (context) => { sources.push(text(context)); return fauxAssistantMessage("Merged retained fragment EFFECT_ONCE."); };
    app.faux.setResponses(Array.from({ length: 80 }, () => finish));
    const result = await app.session.waitForTask(id, BACKGROUND_CONTEXT);
    assert.equal(result.state.outcome.status, "completed"); assert.ok(sources.length > 0);
    assert.equal(sources.includes(firstSource), false, "Committed first-fragment inference must not repeat");
    const after = await app.session.snapshot(UsageDoc, app.root.id, BACKGROUND_CONTEXT);
    assert.ok((after?.models["openai/fixture"]?.totalTokens ?? 0) > (before?.models["openai/fixture"]?.totalTokens ?? 0));
    assert.equal((await app.root.entries({}, 100, undefined, BACKGROUND_CONTEXT)).items.filter(CompactionEntry.is).length, 1);
  } finally { await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

for (const sensitive of [false, true]) test(`failed compaction retains a ${sensitive ? "redacted bounded" : "complete"} private error receipt and reuses paid fragments after reopen`, { timeout: 15_000 }, async t => {
  const directory = await mkdtemp(join(tmpdir(), "agent-compaction-failure-")); let app = await open(directory);
  const secrets = ["fixture bearer secret", "fixture password with spaces", "fixture-api-key", "fixture-refresh-token",
    "fixture-client-secret", "fixture-basic-secret", "fixture-url-password", "ghp_" + "x".repeat(36), "sk-" + "x".repeat(64),
    "fixture-cookie-first", "fixture-cookie-second", "fixture-set-cookie-first", "fixture-set-cookie-second"];
  const actualError = "Unsupported parameter max_output_tokens; provider rejected the bounded checkpoint.";
  const prefix = actualError + " " + JSON.stringify({ Authorization: "Bearer " + secrets[0], password: secrets[1], api_key: secrets[2],
    refreshToken: secrets[3], client_secret: secrets[4], header: "Basic " + secrets[5],
    url: `https://operator:${secrets[6]}@example.invalid/`, github: secrets[7] }) + "\n" +
    `Cookie: sid=${secrets[9]}; cf_clearance=${secrets[10]}\nSet-Cookie: sid=${secrets[11]}; refresh=${secrets[12]}; Path=/\n`;
  const error = sensitive ? prefix + "x".repeat(4089 - Buffer.byteLength(prefix)) + " " + secrets[8] + "🦊漢字".repeat(1000) : actualError;
  const requests: string[] = []; let charged = 0;
  const observeUsage = () => {
    const complete = app.inference.models.completeSimple.bind(app.inference.models);
    app.inference.models.completeSimple = async (model, context, options) => {
      const message = await complete(model, context, options); charged += message.usage.totalTokens; return message;
    };
  };
  try {
    await seed(app); observeUsage(); app.trace.enabled = true;
    const respond: Parameters<typeof app.faux.setResponses>[0][number] = context => {
      requests.push(text(context));
      return requests.length === 1 ? fauxAssistantMessage("Paid completed fragment EFFECT_ONCE.")
        : fauxAssistantMessage("", { stopReason: "error", errorMessage: error });
    };
    app.faux.setResponses(Array.from({ length: 80 }, () => respond));
    const task = await app.root.compact(undefined, BACKGROUND_CONTEXT);
    const failed = await app.session.waitForTask(task, BACKGROUND_CONTEXT);
    assert.equal(failed.state.outcome.status, "failed"); assert.equal(requests.length, 2);
    const receipt = (await app.session.snapshot(Failures, app.root.id, task.toString(), BACKGROUND_CONTEXT))?.failure;
    assert.ok(receipt, "The failed bounded fragment must retain its original error before Pi's fallback replaces it");
    assert.equal(receipt.task_id, task.toString()); assert.match(receipt.fragment_key, /^[a-f0-9]{64}$/u);
    assert.equal(receipt.provider, "openai"); assert.equal(receipt.model, "fixture"); assert.equal(receipt.stop_reason, "error");
    const detail = receipt.error.detail; assert.ok(detail); assert.ok(detail.startsWith(actualError));
    assert.equal(receipt.error.source_bytes, Buffer.byteLength(error));
    assert.equal(receipt.error.retained_bytes, Buffer.byteLength(detail));
    assert.ok(receipt.error.retained_bytes <= 4096); assert.equal(receipt.error.truncated, sensitive);
    assert.equal(receipt.error.credentials_redacted, sensitive);
    assert.equal(receipt.error.redacted_bytes > receipt.error.retained_bytes, sensitive);
    assert.ok(!detail.includes("�"));
    for (const [index, secret] of secrets.entries()) assert.ok(!detail.includes(secret), `Synthetic credential form ${index} must be redacted`);
    assert.ok(!detail.includes("sk-"), "A credential crossing the truncation boundary cannot leave its prefix behind");
    if (!sensitive) assert.equal(detail, actualError);
    assert.ok(!JSON.stringify(receipt).includes("FIRST_MARKER"), "Failure metadata must not copy its source fragment");
    const usage = (await app.session.snapshot(UsageDoc, app.root.id, BACKGROUND_CONTEXT))?.models["openai/fixture"];
    assert.ok(usage); assert.equal(usage.totalTokens, charged); assert.ok(charged > 0);
    const plans = await app.trace.report(app.database);
    assert.ok(plans.every(sample => !sample.plan.some(detail => detail.startsWith("SCAN documents") || detail.startsWith("SCAN document_revisions"))));
    t.diagnostic(JSON.stringify({ failure_bytes: receipt.error.retained_bytes,
      plans: plans.filter(sample => sample.sql.trim().startsWith("SELECT") && sample.sql.includes("document")).map(sample => sample.plan) }));
    await app.session.close(BACKGROUND_CONTEXT); app = await open(directory); observeUsage();
    assert.deepEqual((await app.session.snapshot(Failures, app.root.id, task.toString(), BACKGROUND_CONTEXT))?.failure, receipt);
    assert.deepEqual((await app.session.snapshot(UsageDoc, app.root.id, BACKGROUND_CONTEXT))?.models["openai/fixture"], usage);
    const repeated: string[] = [];
    const finish: Parameters<typeof app.faux.setResponses>[0][number] = context => {
      repeated.push(text(context)); return fauxAssistantMessage("Merged retained checkpoint EFFECT_ONCE.");
    };
    app.faux.setResponses(Array.from({ length: 80 }, () => finish));
    const retry = await app.root.compact(undefined, BACKGROUND_CONTEXT);
    assert.equal((await app.session.waitForTask(retry, BACKGROUND_CONTEXT)).state.outcome.status, "completed");
    assert.ok(requests[0]); assert.ok(requests[1]);
    assert.equal(repeated.includes(requests[0]), false, "A committed paid fragment must not be requested again");
    assert.equal(repeated.includes(requests[1]), true, "Retry must resume the failed uncached fragment");
    assert.equal((await app.session.snapshot(UsageDoc, app.root.id, BACKGROUND_CONTEXT))?.models["openai/fixture"]?.totalTokens, charged);
    assert.equal(await app.session.snapshot(Failures, app.root.id, retry.toString(), BACKGROUND_CONTEXT), undefined);
    assert.deepEqual((await app.session.snapshot(Failures, app.root.id, task.toString(), BACKGROUND_CONTEXT))?.failure, receipt);
  } finally { await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});
