import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout } from "node:timers/promises";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { createModels, fauxAssistantMessage, fauxProvider, fauxToolCall, type TranscriptContext } from "@earendil-works/pi-ai";
import { createRegistry, Harness } from "@earendil-works/pi-durable";
import { SqliteStorage } from "@earendil-works/pi-durable/storage/sqlite";
import { openConversationDatabase } from "../src/conversation-database.js";
import type { SourceEvent } from "../src/generated/SourceEvent.js";
import type { SourceReviewDecision } from "../src/generated/SourceReviewDecision.js";
import { ConversationModels } from "../src/models.js";
import { SourceReviews } from "../src/source-review.js";
import { SourceJournal } from "../src/sources.js";
import { TaskStore } from "../src/tasks.js";
import { Executions } from "../src/tools.js";
import { fixtureModel } from "./model.js";

function gate() {
  let release = () => {};
  const promise = new Promise<void>(resolve => { release = resolve; });
  return { promise, release };
}
function object(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}
function groupIds(context: TranscriptContext): string[] {
  const input = context.messages.findLast(message => message.role === "user"); assert.ok(input?.role === "user");
  const text = typeof input.content === "string" ? input.content : input.content.flatMap(part => part.type === "text" ? [part.text] : []).join("\n");
  const value: unknown = JSON.parse(text); assert.ok(object(value) && Array.isArray(value.groups));
  return value.groups.map((item: unknown) => { assert.ok(object(item) && typeof item.id === "string"); return item.id; });
}
function argument(name: string, fallback: number, minimum: number, maximum: number): number {
  const position = process.argv.indexOf(name), text = position < 0 ? undefined : process.argv[position + 1];
  const value = text === undefined ? fallback : Number(text);
  if (!Number.isSafeInteger(value) || value < minimum || value > maximum) throw new Error(`${name} must be an integer between ${minimum} and ${maximum}`);
  return value;
}
function corpus(count: number, now: number): SourceEvent[] {
  return Array.from({ length: count }, (_, index) => {
    const bucket = index % 20, provider = bucket < 13 ? "slack" : bucket < 17 ? "mail" : "github";
    const fresh = index % 4 === 0;
    // Slack carries 65% of the evidence; half of it shares four hot threads.
    // The rest spans enough independent entities to admit concurrent actors.
    const thread = provider === "slack" && index % 2 === 0 ? `hot-${Math.floor(index / 4) % 4}` : `thread-${index % 80}`;
    return { id: `${provider}-original-${index}`, version: "v1", source: { provider, account: index % 3 === 0 ? "example-two" : "example-one", thread_id: thread },
      title: `Example evidence ${index}`, text: "Invented original evidence. ".repeat(24), occurred_at: now - (fresh ? 1000 : 86_400_000) + index };
  });
}
type Measurement = {
  reviewers: number; synthetic_latency_ms_per_model_request: number; admitted_events: number; decided_events: number;
  elapsed_ms: number; decisions_per_second: number; first_fresh_completion_ms: number; pending_when_fresh_completed: number;
  model_requests: number; simulated_wait_ms_sum: number; completed_reviews: number; slow_effect_still_blocked_at_completion: boolean;
};
async function run(events: SourceEvent[], reviewers: number, latency: number): Promise<Measurement> {
  const directory = await mkdtemp(join(tmpdir(), `agent-review-benchmark-${reviewers}-`));
  const models = createModels(), registry = createRegistry();
  const faux = fauxProvider({ provider: "openai", models: [{ id: "fixture" }], tokensPerSecond: 100_000_000 }); models.setProvider(faux.provider);
  const inference = new ConversationModels(models, undefined, registry);
  const session = await Harness.open(await SqliteStorage.open(await openConversationDatabase(join(directory, "session.sqlite"))),
    { models, registry, identity: { attribution: null }, settings: { retry: { enabled: false }, compaction: { enabled: false } } }, BACKGROUND_CONTEXT);
  const tasks = new TaskStore(session, inference), executions = new Executions(), sources = new SourceJournal(session, tasks, executions);
  const firstModel = gate(), heldEffect = gate(), effectEntered = gate(), controller = new AbortController();
  let effectBlocked = false, modelRequests = 0, simulatedWait = 0, routed = false, decided = 0;
  let freshAdmitted = 0, firstFresh = 0, freshPending = 0;
  const reviews = new SourceReviews(session, registry, sources, tasks, executions, {
    enabled: () => true, workflows: async () => [],
    settings: async () => ({ model: fixtureModel, cwd: "/workspace/example", hosts: [], instructions: "Review invented evidence only." }),
    async legacy() { throw new Error("The benchmark has no legacy claim"); },
    bind: conversation => executions.bind(conversation.id.toString(), { role: "review", cwd: "/workspace/example", hosts: [],
      async call() { throw new Error("Synthetic source review cannot perform external provider effects"); } }),
  }, { workers: reviewers, maximumEvents: 24, maximumEntities: 4 });
  registry.install(reviews.tools);
  tasks.wakeBackground = async id => { effectBlocked = true; effectEntered.release(); await heldEffect.promise; effectBlocked = false; return tasks.get(id); };
  const stop = session.subscribeCommits(publication => {
    for (const change of publication.changes) {
      if (change.type !== "document" || change.record.kind !== "agent.source.event.records" || !object(change.value) || !object(change.value.value)) continue;
      const record = change.value.value;
      if (record.decision === null || !object(record.event)) continue;
      decided++;
      if (record.event.id === "fresh-probe" && firstFresh === 0) {
        firstFresh = performance.now(); freshPending = events.length + 1 - decided;
      }
    }
  });
  let workers: Promise<void>[] = [];
  try {
    await session.root(BACKGROUND_CONTEXT); await tasks.prepare(); await sources.prepare();
    const streams = new Map<string, SourceEvent[]>();
    for (const event of events) {
      const key = JSON.stringify([event.source.provider, event.source.account]), stream = streams.get(key) ?? [];
      stream.push(event); streams.set(key, stream);
    }
    for (const stream of streams.values()) {
      const first = stream[0]; assert.ok(first);
      await sources.admit({ source: { provider: first.source.provider, account: first.source.account }, events: stream,
        checkpoint: null, coverage: { status: "complete" } }, null);
    }
    const response = async (context: TranscriptContext) => {
      modelRequests++; firstModel.release(); const began = performance.now();
      if (latency > 0) await setTimeout(latency, undefined, { signal: controller.signal });
      simulatedWait += performance.now() - began;
      const previous = context.messages.findLast(message => message.role === "toolResult" || message.role === "user");
      if (previous?.role === "toolResult") {
        assert.equal(previous.isError, false, JSON.stringify(previous.content)); return fauxAssistantMessage("");
      }
      const groups = groupIds(context), decisions: SourceReviewDecision[] = [];
      if (!routed) {
        const first = groups.shift(); assert.ok(first); routed = true;
        decisions.push({ type: "task", groups: [first], keys: [], task_id: null, job_id: null, work_url: null,
          title: "Synthetic slow worker", outcome: "Hold one admitted effect without holding a reviewer", input: "Inspect invented evidence without external effects." });
      }
      if (groups.length > 0) decisions.push({ type: "dismiss", groups, keys: [], reason: "Synthetic evidence was reviewed" });
      return fauxAssistantMessage(fauxToolCall("review_sources", { decisions }), { stopReason: "toolUse" });
    };
    faux.setResponses(Array.from({ length: (events.length + 1) * 2 }, () => response));
    const began = performance.now();
    workers = Array.from({ length: reviewers }, (_, slot) => (async () => {
      for (;;) {
        const worked = await reviews.review(slot, controller.signal);
        if (decided === events.length + 1) return;
        if (!worked) await setTimeout(20, undefined, { signal: controller.signal });
        if (performance.now() - began > 120_000) throw new Error("Synthetic source backlog did not drain within two minutes");
      }
    })());
    await firstModel.promise;
    freshAdmitted = performance.now();
    await sources.admit({ source: { provider: "mail", account: "fresh-probe-account" }, events: [{ id: "fresh-probe", version: "v1",
      source: { provider: "mail", account: "fresh-probe-account", thread_id: "fresh-probe-thread" },
      title: "Fresh evidence arriving during catchup", text: "Invented current evidence", occurred_at: Date.now() }], checkpoint: null, coverage: { status: "complete" } }, null);
    await Promise.all(workers); await effectEntered.promise;
    const elapsed = performance.now() - began, diagnostics = await reviews.diagnostics();
    assert.equal(diagnostics.pending_events, 0); assert.equal(decided, events.length + 1); assert.ok(firstFresh > freshAdmitted); assert.ok(freshPending > 0);
    assert.equal(diagnostics.active_reviews.length, 0); assert.equal(effectBlocked, true);
    return { reviewers, synthetic_latency_ms_per_model_request: latency, admitted_events: events.length + 1, decided_events: decided,
      elapsed_ms: Math.round(elapsed), decisions_per_second: Math.round(decided * 1000 / elapsed),
      first_fresh_completion_ms: Math.round(firstFresh - freshAdmitted), pending_when_fresh_completed: freshPending,
      model_requests: modelRequests, simulated_wait_ms_sum: Math.round(simulatedWait), completed_reviews: diagnostics.metrics.completed_reviews,
      slow_effect_still_blocked_at_completion: effectBlocked };
  } finally {
    heldEffect.release(); controller.abort(); await Promise.allSettled(workers); stop();
    await session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true });
  }
}

const count = argument("--events", 600, 200, 4000), latency = argument("--latency-ms", 40, 1, 1000), corpusNow = Date.now(), events = corpus(count, corpusNow);
const measurements: Measurement[] = [];
for (const reviewers of [1, 2, 4]) {
  for (const delay of [0, latency]) {
    process.stderr.write(JSON.stringify({ starting_reviewers: reviewers, synthetic_latency_ms: delay }) + "\n");
    const measured = await run(events, reviewers, delay); measurements.push(measured);
    process.stderr.write(JSON.stringify(measured) + "\n");
  }
}
const comparisons = [1, 2, 4].map(reviewers => {
  const native = measurements.find(item => item.reviewers === reviewers && item.synthetic_latency_ms_per_model_request === 0);
  const fixed = measurements.find(item => item.reviewers === reviewers && item.synthetic_latency_ms_per_model_request === latency);
  const serial = measurements.find(item => item.reviewers === 1 && item.synthetic_latency_ms_per_model_request === latency);
  assert.ok(native && fixed && serial);
  return { reviewers, native_zero_latency_ms: native.elapsed_ms, fixed_latency_ms: fixed.elapsed_ms,
    added_wall_ms_with_synthetic_latency: fixed.elapsed_ms - native.elapsed_ms,
    fixed_latency_speedup_over_one_reviewer: Number((fixed.decisions_per_second / serial.decisions_per_second).toFixed(2)) };
});
process.stdout.write(JSON.stringify({ benchmark: "source_reviews_native_sqlite", corpus_events: events.length,
  timing_scope: "Review drain plus one fresh admission; initial corpus seeding is excluded.",
  provider_events: Object.fromEntries(["slack", "mail", "github"].map(provider => [provider, events.filter(event => event.source.provider === provider).length])),
  initial_fresh_events: events.filter(event => (event.occurred_at ?? 0) >= corpusNow - 15 * 60_000).length,
  idle_actor_poll_ms: 20, measurements, comparisons,
  interpretation: "Synthetic fixed latency model requests with real native SQLite admissions, decisions, task outbox and effect tasks. Zero latency runs measure application/native/faux overhead. Cases are separate executions and include system-load variation. Concurrent simulated wait totals overlap and cannot be subtracted from elapsed wall time. These are not real LLM throughput or provider quota estimates." }, null, 2) + "\n");
