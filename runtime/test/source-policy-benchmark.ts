/** Explicit, disposable qualification of the native Slack noise consumer.
 * Setup and conservation checks are outside the sweep timings. No model call
 * or provider delay is simulated in the measured path. From the repository root:
 * nix develop --command node runtime/dist/test/source-policy-benchmark.js --events 2500
 */
import assert from "node:assert/strict";
import { mkdtemp, rm, stat } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import type { SqliteDatabase } from "@earendil-works/chord/sqlite";
import { createModels, fauxProvider } from "@earendil-works/pi-ai";
import { createRegistry, Harness } from "@earendil-works/pi-durable";
import { SqliteStorage } from "@earendil-works/pi-durable/storage/sqlite";
import { openConversationDatabase } from "../src/conversation-database.js";
import type { SourceEvent } from "../src/generated/SourceEvent.js";
import type { SourceKey } from "../src/generated/SourceKey.js";
import { ConversationModels } from "../src/models.js";
import type { SourceRecord } from "../src/source-index.js";
import { SourceMetrics, SourceStatistics } from "../src/source-metrics.js";
import { SourceNoisePolicy, sourceStatisticsKey } from "../src/source-policy.js";
import { SourceJournal } from "../src/sources.js";
import { TaskStore } from "../src/tasks.js";

const MINUTE = 60_000, DAY = 24 * 60 * MINUTE, BATCH = 64;
const now = Date.UTC(2027, 0, 2, 12), slackSource = { provider: "slack", account: "workspace-example" };
function original(id: string, thread: number, age: number, changes: Partial<SourceEvent> = {}): SourceEvent {
  return { id, version: "v1", source: { ...slackSource, thread_id: `CEXAMPLE:1700000000.${thread}` },
    title: "Slack: engineering", text: "Invented ordinary channel context. ".repeat(12),
    occurred_at: now - age, attention: "ordinary", ...changes };
}
function identity(event: SourceEvent): string {
  return JSON.stringify([event.source.provider, event.source.account, event.id, event.version]);
}
function corpus(count: number) {
  const ordinary = Array.from({ length: count }, (_, index) => original(`old-${index}`, index + 100, 2 * DAY));
  const large = original("large-protected", 1, 4 * DAY, { attention: "mention", text: "Invented retained mention context. ".repeat(20_000) });
  const direct = Array.from({ length: 12 }, (_, index) => original(`direct-${index}`, index, 3 * DAY,
    { attention: "direct", source: { ...slackSource, thread_id: `DEXAMPLE:1700000000.${index}` } }));
  const mentions = Array.from({ length: 12 }, (_, index) => original(`mention-${index}`, index + 20, 3 * DAY,
    { attention: "mention", text: `Invented <@UEXAMPLE> request ${index}` }));
  const tracked = Array.from({ length: 12 }, (_, index) => original(`tracked-${index}`, index + count + 200, 3 * DAY));
  const workUrl = original("recognized-work", count + 300, 3 * DAY, { text: "Follow https://github.com/example/project/pull/17" });
  const hot = Array.from({ length: 12 }, (_, index) => {
    const older = original(`hot-old-${index}`, index + count + 400, 5 * MINUTE);
    return { older, latest: original(`hot-new-${index}`, index + count + 400, 2 * MINUTE) };
  });
  const mail = Array.from({ length: 16 }, (_, index) => original(`mail-${index}`, index, 4 * DAY,
    { source: { provider: "google", account: index % 2 ? "work-example" : "personal-example", thread_id: `mail-${index}` } }));
  const slack = [large, ...direct, ...mentions, ...tracked, workUrl, ...ordinary, ...hot.flatMap(pair => [pair.older, pair.latest])];
  return { ordinary, large, tracked, hot, mail, slack, all: [...slack, ...mail],
    protectedPending: 1 + direct.length + mentions.length + tracked.length + 1 + hot.length };
}
async function admit(journal: SourceJournal, events: SourceEvent[]): Promise<void> {
  const first = events[0]; assert.ok(first);
  const source = { provider: first.source.provider, account: first.source.account };
  for (let offset = 0; offset < events.length; offset += BATCH) {
    const prior = (await journal.states()).find(state => state.source.provider === source.provider && state.source.account === source.account);
    const result = await journal.admit({ source, events: events.slice(offset, offset + BATCH), checkpoint: null, coverage: { status: "complete" } }, prior?.checkpoint ?? null);
    assert.equal(result.new_events, Math.min(BATCH, events.length - offset));
  }
}
async function originals(journal: SourceJournal, source: SourceKey): Promise<SourceRecord[]> {
  const result: SourceRecord[] = []; let cursor: string | undefined;
  do {
    const page = await journal.eventPage({ ...source, limit: 100, ...(cursor === undefined ? {} : { cursor }) });
    assert.ok(page.scanned <= 100); result.push(...page.events); cursor = page.cursor ?? undefined;
    if (page.complete) assert.equal(cursor, undefined);
  } while (cursor !== undefined);
  return result;
}
function percentile(samples: number[], fraction: number): number {
  const ordered = [...samples].sort((a, b) => a - b), value = ordered[Math.max(0, Math.min(ordered.length - 1, Math.ceil(ordered.length * fraction) - 1))];
  assert.ok(value !== undefined); return Number(value.toFixed(3));
}

async function benchmark(count: number) {
  const directory = await mkdtemp(join(tmpdir(), "agent-native-source-policy-benchmark-"));
  let session: Harness | undefined, database: SqliteDatabase | undefined;
  try {
    database = await openConversationDatabase(join(directory, "session.sqlite"));
    const synchronous = await database.get<{ synchronous: number }>("PRAGMA synchronous");
    const fullfsync = await database.get<{ fullfsync: number }>("PRAGMA fullfsync");
    assert.equal(synchronous?.synchronous, 2); assert.equal(fullfsync?.fullfsync, 1);
    const models = createModels(), registry = createRegistry();
    const faux = fauxProvider({ provider: "openai", models: [{ id: "fixture" }], tokensPerSecond: 1_000_000 }); models.setProvider(faux.provider);
    const inference = new ConversationModels(models, undefined, registry);
    session = await Harness.open(await SqliteStorage.open(database), { models, registry, identity: { attribution: null },
      settings: { steeringMode: "all", followUpMode: "one-at-a-time", documentRetentionMs: 1000 } }, BACKGROUND_CONTEXT);
    const harness = session, tasks = new TaskStore(harness, inference), journal = new SourceJournal(harness, tasks);
    await harness.root(BACKGROUND_CONTEXT); await tasks.prepare(); await journal.prepare();
    const events = corpus(count);
    const task = await tasks.create({ title: "Invented tracked work", outcome: "Retain its existing discussion", sources: events.tracked.map(event => event.source) });
    const seedMetrics = await harness.snapshot(SourceMetrics, BACKGROUND_CONTEXT);
    await admit(journal, events.slack);
    const accounts = ["personal-example", "work-example"];
    for (const account of accounts) await admit(journal, events.mail.filter(event => event.source.account === account));
    const before = await harness.snapshot(SourceMetrics, BACKGROUND_CONTEXT); assert.ok(before);
    assert.equal(before.admitted_events - (seedMetrics?.admitted_events ?? 0), events.all.length);
    const countsBefore = await harness.commit(tx => journal.index.countsIn(tx, slackSource), BACKGROUND_CONTEXT);
    assert.equal(countsBefore.retained, events.slack.length); assert.equal(countsBefore.pending, events.slack.length);

    const policy = new SourceNoisePolicy(journal), durations: number[] = [];
    let scanned = 0, expired = 0, coalesced = 0, maximumScanned = 0;
    let first: Awaited<ReturnType<SourceNoisePolicy["sweep"]>> | undefined;
    const began = performance.now();
    for (;;) {
      const started = performance.now(), result = await policy.sweep(now);
      durations.push(performance.now() - started); first ??= result;
      assert.ok(result.scanned <= BATCH);
      scanned += result.scanned; expired += result.expired; coalesced += result.coalesced;
      maximumScanned = Math.max(maximumScanned, result.scanned);
      assert.ok(scanned <= events.slack.length, "A bounded sweep cannot revisit its protected prefix in the same pass");
      if (!result.more) break;
    }
    const elapsed = performance.now() - began;
    assert.deepEqual(first, { scanned: 1, expired: 0, coalesced: 0, more: true }, "An oversized protected original owns its batch");
    assert.equal(scanned, events.slack.length); assert.equal(expired, events.ordinary.length); assert.equal(coalesced, events.hot.length);
    const countsAfter = await harness.commit(tx => journal.index.countsIn(tx, slackSource), BACKGROUND_CONTEXT);
    assert.equal(countsAfter.retained, countsBefore.retained); assert.equal(countsAfter.pending, events.protectedPending);
    assert.equal(countsAfter.dirty_entities, events.protectedPending);
    const records = await originals(journal, slackSource);
    for (const account of accounts) {
      const key = { provider: "google", account }, mailCounts = await harness.commit(tx => journal.index.countsIn(tx, key), BACKGROUND_CONTEXT);
      assert.equal(mailCounts.pending, events.mail.filter(event => event.source.account === account).length);
      records.push(...await originals(journal, key));
    }
    const expected = new Map(events.all.map(event => [identity(event), event])), byKey = new Map(records.map(record => [record.key, record]));
    assert.equal(records.length, events.all.length); assert.equal(byKey.size, records.length);
    let verifiedExpired = 0, verifiedCoalesced = 0, verifiedPending = 0;
    for (const record of records) {
      const original = expected.get(identity(record.event)); assert.ok(original); assert.deepEqual(record.event, original);
      assert.equal(expected.delete(identity(record.event)), true);
      if (record.decision === null) { verifiedPending++; continue; }
      assert.equal(record.decision.type, "skipped"); assert.ok(record.decision.type === "skipped");
      assert.equal(record.decision.policy.at, now);
      if (record.decision.policy.reason === "expired") { assert.ok(record.event.id.startsWith("old-")); verifiedExpired++; }
      else {
        const replacement = byKey.get(record.decision.policy.replacement); assert.ok(replacement);
        assert.equal(replacement.event.id, record.event.id.replace("hot-old-", "hot-new-"));
        assert.equal(replacement.decision, null); verifiedCoalesced++;
      }
    }
    assert.equal(expected.size, 0); assert.equal(verifiedExpired, expired); assert.equal(verifiedCoalesced, coalesced);
    assert.equal(verifiedPending, events.protectedPending + events.mail.length);
    const after = await harness.snapshot(SourceMetrics, BACKGROUND_CONTEXT); assert.ok(after);
    assert.equal(after.admitted_events, before.admitted_events);
    assert.equal(after.decided_events - before.decided_events, expired + coalesced);
    assert.equal(after.expired_events - before.expired_events, expired); assert.equal(after.coalesced_events - before.coalesced_events, coalesced);
    assert.equal(after.completed_reviews, before.completed_reviews); assert.equal(after.review_milliseconds, before.review_milliseconds);
    const statistics = await harness.snapshot(SourceStatistics, sourceStatisticsKey(slackSource), BACKGROUND_CONTEXT); assert.ok(statistics);
    assert.equal(statistics.expired_events, expired); assert.equal(statistics.coalesced_events, coalesced);
    assert.equal(statistics.reviewed_events, 0); assert.equal(statistics.completed_reviews, 0); assert.equal(faux.state.callCount, 0);
    assert.deepEqual((await tasks.get(task.id)).sources, events.tracked.map(event => event.source));
    assert.equal((await database.get<{ quick_check: string }>("PRAGMA quick_check"))?.quick_check, "ok");
    return { engine: "native Pi SQLite", synchronous: "FULL", fullfsync: true, setup_in_timing: false,
      ordinary_old_events: count, retained_originals: records.length, pending_slack: countsAfter.pending, pending_email: events.mail.length,
      expired, coalesced, model_calls: faux.state.callCount, sweeps: durations.length, scanned, maximum_records_per_sweep: maximumScanned,
      elapsed_ms: Number(elapsed.toFixed(3)), settled_per_second: Number(((expired + coalesced) * 1000 / elapsed).toFixed(2)),
      sweep_latency_ms: { minimum: percentile(durations, 0), median: percentile(durations, 0.5), p95: percentile(durations, 0.95), maximum: percentile(durations, 1) },
      byte_budget_probe: { protected_original_bytes: Buffer.byteLength(JSON.stringify(events.large)), first_sweep_scanned: first.scanned, later_progress: expired > 0 },
      conservation: { originals: true, pending: true, policy_decisions: true, counters: true, task_links: true }, integrity: "ok" };
  } finally {
    try { if (session !== undefined) await session.close(BACKGROUND_CONTEXT); else await database?.close(); }
    finally { await rm(directory, { recursive: true, force: true }); }
    await assert.rejects(stat(directory), error => typeof error === "object" && error !== null && "code" in error && error.code === "ENOENT");
  }
}

const position = process.argv.indexOf("--events"), count = position < 0 ? 2500 : Number(process.argv[position + 1]);
if (!Number.isSafeInteger(count) || count < 2000 || count > 100_000) throw new Error("--events must be an integer between 2000 and 100000");
const result = await benchmark(count);
console.log(JSON.stringify({ ...result, fixture_removed: true }));
