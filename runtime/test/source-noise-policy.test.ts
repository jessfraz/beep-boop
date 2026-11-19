import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { createModels, fauxProvider } from "@earendil-works/pi-ai";
import { createRegistry, Harness } from "@earendil-works/pi-durable";
import { SqliteStorage } from "@earendil-works/pi-durable/storage/sqlite";
import { openConversationDatabase } from "../src/conversation-database.js";
import type { SourceEvent } from "../src/generated/SourceEvent.js";
import { ConversationModels } from "../src/models.js";
import { sourceEntityId } from "../src/source-index.js";
import { SourceMetrics, SourceStatistics } from "../src/source-metrics.js";
import { SourceNoisePolicy, sourceStatisticsKey, type SourceNoisePolicyOptions } from "../src/source-policy.js";
import { SourceJournal } from "../src/sources.js";
import { TaskStore } from "../src/tasks.js";

const NOW = Date.UTC(2027, 0, 2, 12), MINUTE = 60_000, DAY = 24 * 60 * MINUTE;
const source = { provider: "slack", account: "example" };
function slack(index: number, age = 2 * DAY, overrides: Partial<SourceEvent> = {}): SourceEvent {
  return { id: `event-${index}`, version: "v1", source: { ...source, thread_id: `CEXAMPLE:1700000000.${index}` },
    title: "Slack: engineering", text: `Retained context ${index}`, occurred_at: NOW - age, attention: "ordinary", ...overrides };
}
async function open(path: string, options: SourceNoisePolicyOptions = {}) {
  const models = createModels(), registry = createRegistry();
  const faux = fauxProvider({ provider: "openai", models: [{ id: "fixture" }], tokensPerSecond: 1_000_000 });
  models.setProvider(faux.provider);
  const inference = new ConversationModels(models, undefined, registry);
  const storage = await SqliteStorage.open(await openConversationDatabase(path));
  const session = await Harness.open(storage, { models, registry, identity: { attribution: null } }, BACKGROUND_CONTEXT);
  const tasks = new TaskStore(session, inference), journal = new SourceJournal(session, tasks);
  await session.root(BACKGROUND_CONTEXT); await tasks.prepare(); await journal.prepare();
  return { session, tasks, journal, policy: new SourceNoisePolicy(journal, options), faux };
}
type App = Awaited<ReturnType<typeof open>>;
async function admit(app: App, events: SourceEvent[]) {
  const first = events[0]; assert.ok(first);
  const key = { provider: first.source.provider, account: first.source.account };
  const previous = (await app.journal.states()).find(state => state.source.provider === key.provider && state.source.account === key.account);
  return app.journal.admit({ source: key, events, checkpoint: null, coverage: { status: "complete" } }, previous?.checkpoint ?? null);
}
async function record(app: App, id: string) {
  const records = await app.journal.events({ limit: 100 });
  const found = records.find(item => item.event.id === id); assert.ok(found, `Retained original ${id}`); return found;
}
async function metrics(app: App) {
  const value = await app.session.snapshot(SourceMetrics, BACKGROUND_CONTEXT); assert.ok(value); return value;
}

test("noise expiry preserves mail, GitHub, direct messages, mentions and unknown metadata without a model call", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-source-noise-protection-")), app = await open(join(directory, "session.sqlite"));
  try {
    const direct = slack(2, 2 * DAY, { attention: "direct", source: { ...source, thread_id: "DEXAMPLE:1700000000.2" } });
    const unknown = slack(6, 2 * DAY, { source: { ...source, thread_id: "GUNKNOWN:1700000000.6" }, title: "Slack: private" });
    delete unknown.attention;
    const undated = slack(7); delete undated.occurred_at;
    const originals = [slack(1), direct, slack(3, 2 * DAY, { attention: "mention" }),
      slack(4, 2 * DAY, { text: "A direct <@UEXAMPLE> mention" }), slack(5, 2 * DAY, { text: "A broad <!channel> mention" }),
      unknown, undated, slack(8, -MINUTE), slack(9, NOW),
      slack(12, 2 * DAY, { attention: "protected", source: { ...source, thread_id: "GPRIVATE:1700000000.12" } })];
    await admit(app, originals);
    const mail = slack(10, 4 * DAY, { source: { provider: "google", account: "mail-example", thread_id: "mail-thread" } });
    const github = slack(11, 4 * DAY, { source: { provider: "github", account: "code-example", thread_id: "code-thread" } });
    await admit(app, [mail]); await admit(app, [github]);
    assert.deepEqual(await app.policy.sweep(NOW), { scanned: 10, expired: 1, coalesced: 0, more: false });
    assert.deepEqual((await record(app, "event-1")).decision, { type: "skipped", policy: { reason: "expired", at: NOW } });
    for (const original of [...originals.slice(1), mail, github]) {
      const saved = await record(app, original.id); assert.equal(saved.decision, null); assert.deepEqual(saved.event, original);
    }
    const counters = await metrics(app);
    assert.equal(counters.expired_events, 1); assert.equal(counters.coalesced_events, 0);
    assert.equal(counters.completed_reviews, 0); assert.equal(counters.review_milliseconds, 0);
    assert.equal(app.faux.state.callCount, 0);
  } finally { await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("duplicate attention observations classify legacy history without rewriting or readmitting its original", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-source-noise-enrichment-")), path = join(directory, "session.sqlite");
  let app = await open(path);
  try {
    const original = slack(1); delete original.attention;
    const renamed = slack(2, 2 * DAY, { title: "Slack: old-name" }); delete renamed.attention;
    assert.equal((await admit(app, [original, renamed])).new_events, 2);
    assert.equal((await app.policy.sweep(NOW)).expired, 0);
    const before = await metrics(app), key = (await record(app, original.id)).key;
    assert.equal((await admit(app, [{ ...original, attention: "ordinary" }])).new_events, 0);
    assert.deepEqual((await record(app, original.id)).event, original);
    await app.session.close(BACKGROUND_CONTEXT); app = await open(path);
    const result = await app.policy.sweep(NOW); assert.equal(result.expired, 1);
    const saved = await record(app, original.id); assert.equal(saved.key, key); assert.deepEqual(saved.event, original);
    assert.equal(saved.decision?.type, "skipped"); assert.equal((await record(app, renamed.id)).decision, null);
    const after = await metrics(app); assert.equal(after.admitted_events, before.admitted_events);
    assert.equal(after.expired_events, 1); assert.equal(after.completed_reviews, 0); assert.equal(app.faux.state.callCount, 0);
  } finally { await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("new protected observations cannot inherit a channel's earlier public hint", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-source-noise-protected-hint-")), path = join(directory, "session.sqlite");
  let app = await open(path);
  try {
    const publicObservation = slack(1, 0), legacy = slack(2), protectedObservation = slack(3, 2 * DAY, { attention: "protected" });
    delete legacy.attention;
    await admit(app, [publicObservation, legacy]); await admit(app, [protectedObservation]);
    await app.session.close(BACKGROUND_CONTEXT); app = await open(path);
    assert.equal((await app.policy.sweep(NOW)).expired, 0);
    for (const original of [publicObservation, legacy, protectedObservation]) {
      const saved = await record(app, original.id); assert.equal(saved.decision, null); assert.deepEqual(saved.event, original);
    }
    assert.equal(app.faux.state.callCount, 0);
  } finally { await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("a protected duplicate keeps its immutable ordinary original protected after later ordinary channel observations", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-source-noise-original-protection-")), path = join(directory, "session.sqlite");
  let app = await open(path);
  try {
    const original = slack(1), laterOrdinary = slack(2);
    assert.equal((await admit(app, [original])).new_events, 1);
    assert.equal((await admit(app, [{ ...original, attention: "protected" }])).new_events, 0);
    assert.equal((await admit(app, [laterOrdinary])).new_events, 1);
    const before = await record(app, original.id); assert.deepEqual(before.event, original);
    await app.session.close(BACKGROUND_CONTEXT); app = await open(path);
    assert.equal((await app.policy.sweep(NOW)).expired, 1);
    const saved = await record(app, original.id);
    assert.equal(saved.key, before.key); assert.equal(saved.decision, null); assert.deepEqual(saved.event, original);
    assert.equal((await record(app, laterOrdinary.id)).decision?.type, "skipped");
    assert.equal((await metrics(app)).admitted_events, 2); assert.equal(app.faux.state.callCount, 0);
  } finally { await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("a learned direct-channel observation permanently protects earlier ordinary history across reopen", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-source-noise-direct-history-")), path = join(directory, "session.sqlite");
  let app = await open(path);
  try {
    const ordinary = slack(1), direct = slack(2, 2 * DAY, { attention: "direct" }), laterOrdinary = slack(3);
    await admit(app, [ordinary]); await admit(app, [direct]); await admit(app, [laterOrdinary]);
    await app.session.close(BACKGROUND_CONTEXT); app = await open(path);
    assert.equal((await app.policy.sweep(NOW)).expired, 0, "A positive direct observation protects the channel's retained history");
    for (const item of [ordinary, direct, laterOrdinary]) assert.equal((await record(app, item.id)).decision, null);
    assert.equal(app.faux.state.callCount, 0);
  } finally { await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("bounded policy sweeps advance past protected prefixes and resume the exact native cursor after reopen", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-source-noise-cursor-")), path = join(directory, "session.sqlite");
  const options = { batchSize: 3 }; let app = await open(path, options);
  try {
    const protectedItems = Array.from({ length: 5 }, (_, index) => slack(index, 4 * DAY + index, { attention: "mention" }));
    const ordinary = Array.from({ length: 7 }, (_, index) => slack(index + 10, 2 * DAY + index));
    await admit(app, [...protectedItems, ...ordinary]);
    assert.deepEqual(await app.policy.sweep(NOW), { scanned: 3, expired: 0, coalesced: 0, more: true });
    await app.session.close(BACKGROUND_CONTEXT); app = await open(path, options);
    let expired = 0;
    for (let pass = 0; pass < 8 && expired < ordinary.length; pass++) {
      const result = await app.policy.sweep(NOW); assert.ok(result.scanned <= options.batchSize); expired += result.expired;
    }
    assert.equal(expired, ordinary.length, "Protected oldest records cannot pin the sweep to the same prefix");
    for (const item of protectedItems) assert.equal((await record(app, item.id)).decision, null);
    for (const item of ordinary) assert.equal((await record(app, item.id)).decision?.type, "skipped");
    assert.equal((await metrics(app)).expired_events, ordinary.length); assert.equal(app.faux.state.callCount, 0);
  } finally { await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("policy cursor rotation gives each case-sensitive source account a turn", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-source-noise-account-fairness-")), app = await open(join(directory, "session.sqlite"), { batchSize: 1 });
  try {
    const lower = slack(1, 2 * DAY, { source: { provider: "slack", account: "a-account", thread_id: "CEXAMPLE:1700000000.1" } });
    const upper = slack(2, 2 * DAY, { source: { provider: "slack", account: "A-account", thread_id: "CEXAMPLE:1700000000.2" } });
    await admit(app, [lower]); await admit(app, [upper]);
    const first = await app.policy.sweep(NOW), second = await app.policy.sweep(NOW);
    assert.equal(first.expired + second.expired, 2, "A drained account cannot prevent the next source account from being visited");
    assert.equal((await record(app, lower.id)).decision?.type, "skipped");
    assert.equal((await record(app, upper.id)).decision?.type, "skipped"); assert.equal(app.faux.state.callCount, 0);
  } finally { await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("a large protected original owns its batch without skipping later eligible records", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-source-noise-byte-bound-")), app = await open(join(directory, "session.sqlite"), { batchSize: 3 });
  try {
    const large = slack(1, 3 * DAY, { attention: "mention", text: "Large retained context. ".repeat(30_000) });
    const later = [slack(2), slack(3)]; await admit(app, [large, ...later]);
    assert.deepEqual(await app.policy.sweep(NOW), { scanned: 1, expired: 0, coalesced: 0, more: true });
    assert.deepEqual(await app.policy.sweep(NOW), { scanned: 2, expired: 2, coalesced: 0, more: false });
    const saved = await record(app, large.id); assert.equal(saved.decision, null); assert.deepEqual(saved.event, large);
    assert.equal(app.faux.state.callCount, 0);
  } finally { await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("a rejected native owner guard preserves decisions and both sweep cursors for the next owner", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-source-noise-owner-guard-")), path = join(directory, "session.sqlite");
  const options = { batchSize: 1 }; let app = await open(path, options);
  try {
    const first = slack(1, 3 * DAY, { attention: "mention", source: { ...source, account: "a-account", thread_id: "CEXAMPLE:1700000000.1" } });
    const next = slack(2, 2 * DAY + 1, { source: { ...source, account: "a-account", thread_id: "CEXAMPLE:1700000000.2" } });
    const following = slack(3, 2 * DAY, { source: { ...source, account: "a-account", thread_id: "CEXAMPLE:1700000000.3" } });
    const other = slack(4, 2 * DAY, { source: { ...source, account: "b-account", thread_id: "CEXAMPLE:1700000000.4" } });
    await admit(app, [first, next, following]); await admit(app, [other]);
    assert.deepEqual(await app.policy.sweep(NOW, async () => true), { scanned: 1, expired: 0, coalesced: 0, more: true });
    const before = await metrics(app);
    assert.deepEqual(await app.policy.sweep(NOW, async () => false), { scanned: 0, expired: 0, coalesced: 0, more: false });
    assert.deepEqual(await metrics(app), before);
    for (const original of [first, next, following, other]) assert.equal((await record(app, original.id)).decision, null);
    await app.session.close(BACKGROUND_CONTEXT); app = await open(path, options);
    assert.equal((await app.policy.sweep(NOW, async () => true)).expired, 1);
    assert.equal((await record(app, other.id)).decision?.type, "skipped", "A rejected guard cannot advance the account cursor");
    assert.equal((await record(app, next.id)).decision, null);
    assert.equal((await app.policy.sweep(NOW, async () => true)).expired, 1);
    assert.equal((await record(app, next.id)).decision?.type, "skipped", "A rejected guard cannot advance the pending-position cursor");
    assert.equal((await record(app, following.id)).decision, null); assert.equal(app.faux.state.callCount, 0);
  } finally { await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("coalescing retains the newest thread original and exact replacement accounting across duplicate replay and reopen", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-source-noise-coalesce-")), path = join(directory, "session.sqlite");
  let app = await open(path);
  try {
    const first = slack(1, 3 * MINUTE), latest = slack(2, MINUTE, { source: first.source });
    await admit(app, [first, latest]); const newest = await record(app, latest.id);
    assert.deepEqual(await app.policy.sweep(NOW), { scanned: 2, expired: 0, coalesced: 1, more: false });
    assert.deepEqual((await record(app, first.id)).decision,
      { type: "skipped", policy: { reason: "coalesced", at: NOW, replacement: newest.key } });
    assert.equal((await record(app, latest.id)).decision, null);
    assert.equal((await admit(app, [first, latest])).new_events, 0);
    await app.session.close(BACKGROUND_CONTEXT); app = await open(path);
    const result = await app.policy.sweep(NOW); assert.equal(result.expired, 0); assert.equal(result.coalesced, 0);
    assert.deepEqual((await record(app, first.id)).event, first); assert.deepEqual((await record(app, latest.id)).event, latest);
    const counters = await metrics(app); assert.equal(counters.coalesced_events, 1); assert.equal(counters.completed_reviews, 0);
    const stats = await app.session.snapshot(SourceStatistics, sourceStatisticsKey(source), BACKGROUND_CONTEXT); assert.ok(stats);
    assert.equal(stats.coalesced_events, 1); assert.equal(stats.reviewed_events, 0); assert.equal(app.faux.state.callCount, 0);
  } finally { await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("backlog pressure expires older ordinary chatter while protecting recent arrivals and tracked references", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-source-noise-pressure-")), app = await open(join(directory, "session.sqlite"), { maximumPending: 2 });
  try {
    const oldNoise = slack(1, 20 * MINUTE), recent = slack(2, MINUTE), tracked = slack(3, 2 * DAY);
    await admit(app, [oldNoise, recent, tracked]);
    await app.tasks.create({ title: "Tracked discussion", outcome: "Resolve the authorized work", sources: [tracked.source] });
    const result = await app.policy.sweep(NOW); assert.equal(result.expired, 1);
    assert.deepEqual((await record(app, oldNoise.id)).decision, { type: "skipped", policy: { reason: "expired", at: NOW } });
    assert.equal((await record(app, recent.id)).decision, null); assert.equal((await record(app, tracked.id)).decision, null);
    const trackedRecord = await record(app, tracked.id);
    const entity = await app.session.commit(tx => app.journal.index.entityIn(tx, sourceEntityId(trackedRecord)), BACKGROUND_CONTEXT);
    assert.equal(entity?.owner_task_id, null, "Manual task associations are protected without requiring a journal routing decision");
    assert.equal(app.faux.state.callCount, 0);
  } finally { await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("native leases, concurrent arrivals, manual corrections and recognized cross-source work fence noise settlement", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-source-noise-ownership-")), app = await open(join(directory, "session.sqlite"));
  try {
    const held = slack(1), arrival = slack(2, 2 * DAY, { source: held.source }); await admit(app, [held]);
    const retained = await record(app, held.id), entityId = sourceEntityId(retained);
    await app.session.commit(async tx => { assert.ok(await app.journal.index.claimEntityIn(tx, entityId, "active-review")); }, BACKGROUND_CONTEXT);
    const [, swept] = await Promise.all([admit(app, [arrival]), app.policy.sweep(NOW)]); assert.equal(swept.expired, 0);
    assert.equal((await record(app, held.id)).decision, null); assert.equal((await record(app, arrival.id)).decision, null);
    const task = await app.tasks.create({ title: "Corrected owner", outcome: "Retain the tracked action" });
    await app.session.commit(async tx => {
      await app.journal.decideBatchIn(tx, [retained.key], { type: "task", task_id: task.id });
      const entity = await app.journal.index.entityIn(tx, entityId); assert.equal(entity?.decision_revision, 1);
      assert.equal(await app.journal.index.releaseEntityIn(tx, entityId, "active-review"), true);
    }, BACKGROUND_CONTEXT);
    const github = slack(10, 2 * DAY, { source: { provider: "github", account: "code-example", thread_id: "https://github.com/example/project/pull/17" },
      text: "Subject API: https://api.github.com/repos/example/project/pulls/17" });
    await admit(app, [github]); const work = await record(app, github.id);
    await app.session.commit(tx => app.journal.decideBatchIn(tx, [work.key], { type: "task", task_id: task.id }), BACKGROUND_CONTEXT);
    const manuallyAssociated = slack(20, 2 * DAY, { source: { provider: "github", account: "code-example", thread_id: "https://github.com/example/project/issues/99" },
      text: "Subject API: https://api.github.com/repos/example/project/issues/99" });
    await admit(app, [manuallyAssociated]);
    await app.tasks.create({ title: "Manually tracked issue", outcome: "Retain its existing owner", sources: [manuallyAssociated.source] });
    const journalOwners = await app.session.commit(tx => app.journal.index.resourceOwnersIn(tx, "github:example/project:99", 1), BACKGROUND_CONTEXT);
    assert.deepEqual(journalOwners, [], "Manual task associations need not have journal routing receipts");
    const linked = slack(11, 2 * DAY, { text: "Continue https://github.com/example/project/pull/17" });
    const unowned = slack(12, 2 * DAY, { text: "Context https://github.com/example/project/pull/18" });
    const manualLink = slack(21, 2 * DAY, { text: "Continue https://github.com/example/project/issues/99" });
    const ordinary = slack(22); await admit(app, [linked, unowned, manualLink, ordinary]);
    const result = await app.policy.sweep(NOW); assert.equal(result.expired, 1);
    assert.equal((await record(app, arrival.id)).decision, null); assert.equal((await record(app, linked.id)).decision, null);
    assert.equal((await record(app, unowned.id)).decision, null, "Recognized work URLs remain conservatively protected");
    assert.equal((await record(app, manualLink.id)).decision, null); assert.equal((await record(app, ordinary.id)).decision?.type, "skipped");
    assert.deepEqual((await record(app, held.id)).decision, { type: "task", task_id: task.id }); assert.equal(app.faux.state.callCount, 0);
  } finally { await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});
