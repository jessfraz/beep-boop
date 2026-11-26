import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { fauxAssistantMessage, fauxToolCall } from "@earendil-works/pi-ai";
import { AssistantEntry, defineDoc, defineDocFamily, UserEntry, type Cursor } from "@earendil-works/pi-durable";
import type { TaskOutcome } from "../src/generated/TaskOutcome.js";
import type { SourceState } from "../src/generated/SourceState.js";
import type { StoredEvent } from "../src/sources.js";
import { openScheduled } from "./fixtures/scheduled-runtime.js";
import { fixtureModel } from "./model.js";
import { openStateStorage, SqlTrace } from "./fixtures/state-storage.js";

const Migration = defineDocFamily<{ cursor: string | null; target?: string | null; complete: boolean }, null>({
  kind: "agent.source.reconciliation.migration", scope: "session", family: true, version: 1,
  initial: () => ({ cursor: null, complete: false }), checkpointWhen: () => true,
});
const LegacyOutcomes = defineDoc<{ sources: SourceState[]; events: StoredEvent[]; conversation_id: string | null; active: null }>({
  kind: "agent.sources", scope: "session", version: 1,
  initial: () => ({ sources: [], events: [], conversation_id: null, active: null }), checkpointWhen: () => false,
});

test("successful cold recovery checks compact receipts without materializing historical source originals", { timeout: 20_000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-reconciliation-reads-"));
  let app = await openScheduled(directory);
  try {
    const sources = Array.from({ length: 128 }, (_, index) => ({ provider: "mail", account: "fixture",
      thread_id: `retained-${index}-` + "source-reference".repeat(16) }));
    const task = await app.tasks.create({ title: "Owned retained review", outcome: "Reconcile retained work", execution: "coordinator",
      model: fixtureModel, sources });
    const conversation = await app.tasks.conversation(task.conversation_id), requestId = "original-retained-input";
    const original = await conversation.commit(async tx => {
      const entry = await tx.appendEntry(UserEntry, conversation.id,
        { model: [{ role: "user", content: "Retained authorized input", timestamp: 1 }] });
      return tx.createSubmission({ type: "input", conversationId: conversation.id, requestId,
        status: "unanswered", entry: entry.id, reason: "model_error" });
    }, BACKGROUND_CONTEXT);
    for (let index = 0; index < 12; index++) {
      const outcome: TaskOutcome = { task_id: task.id, conversation_id: task.conversation_id, entry_id: null, tool_call_id: null,
        sources, summary: `Earlier retained progress ${index}`, state: { state: "progress" } };
      await app.session.commit(tx => app.sources.recordIn(tx, outcome, true), BACKGROUND_CONTEXT);
    }
    app.faux.setResponses([
      fauxAssistantMessage(fauxToolCall("record_task", { summary: "The original input is reconciled.", reconciles_input: requestId,
        state: { state: "waiting", actor: "Independent upstream work", next_check: null } }), { stopReason: "toolUse" }),
      fauxAssistantMessage("Verified the unfinished work."),
    ]);
    await app.tasks.enqueue(task.id, "Recover the exact retained input.", "recovery-input");
    await app.tasks.runBackground(task.id);
    const recovery = await app.session.commit(tx => tx.submissionByRequest(conversation.id, "recovery-input"), BACKGROUND_CONTEXT);
    assert.equal(recovery?.status, "done");
    await app.close(); app = await openScheduled(directory); await app.sources.prepare();
    const originals = new Set<number>();
    let cursor: Cursor | undefined;
    do {
      const page = await app.storage.scanDocuments({ scope: { kind: "session" }, kind: "agent.source.event.records", at: "current" }, 128, cursor, BACKGROUND_CONTEXT);
      for (const record of page.items) originals.add(record.id);
      cursor = page.next;
    } while (cursor !== undefined);
    assert.ok(originals.size >= 13);
    let reads = 0;
    const document = app.storage.document.bind(app.storage);
    app.storage.document = async (id, at, context) => {
      if (originals.has(id)) reads++;
      return document(id, at, context);
    };
    assert.equal(await app.session.commit(tx => app.sources.reconcilesIn(tx, task.id, conversation.id, requestId), BACKGROUND_CONTEXT), true);
    assert.equal(reads, 0, "Atomic settlement must not load the historical source payloads into its transaction");
    assert.deepEqual(await app.session.commit(tx => tx.submissionByRequest(conversation.id, requestId), BACKGROUND_CONTEXT), original);
    assert.deepEqual(await app.session.commit(tx => tx.submissionByRequest(conversation.id, "recovery-input"), BACKGROUND_CONTEXT), recovery);
  } finally { await app.close(); await rm(directory, { recursive: true, force: true }); }
});

test("legacy recovery receipt preparation is scoped, interruptible and durable across concurrent reopen", { timeout: 20_000 }, async t => {
  const directory = await mkdtemp(join(tmpdir(), "agent-legacy-reconciliation-")), path = join(directory, "session.sqlite");
  let app = await openStateStorage(path);
  const taskId = "retained-work", requestId = "original-input", recoveryId = "recovery-input";
  try {
    const retained = await app.session.commit(async tx => {
      const conversation = await tx.createConversation({ ownership: { kind: "ownerless" } });
      const entry = await tx.appendEntry(UserEntry, conversation.id, { model: [{ role: "user", content: "Retained input", timestamp: 1 }] });
      const original = await tx.createSubmission({ type: "input", conversationId: conversation.id, requestId,
        status: "unanswered", entry: entry.id, reason: "model_error" });
      const recoveryEntry = await tx.appendEntry(UserEntry, conversation.id,
        { model: [{ role: "user", content: "Inspect the retained work", timestamp: 2 }] });
      const recovery = await tx.createSubmission({ type: "input", conversationId: conversation.id, requestId: recoveryId,
        status: "placed", entry: recoveryEntry.id });
      const origin = await tx.appendEntry(AssistantEntry, conversation.id,
        { model: [fauxAssistantMessage(fauxToolCall("record_task", { reconciles_input: requestId }, { id: "legacy-record" }), { stopReason: "toolUse" })] });
      const answered = await tx.createSubmission({ type: "input", conversationId: conversation.id, requestId: "independent-done-input",
        status: "done", entry: recoveryEntry.id, answer: origin.id });
      const events = (await tx.doc(LegacyOutcomes)).events;
      for (let index = 0; index < 7; index++) {
        const outcome: TaskOutcome = { task_id: index === 6 ? "independent-work" : taskId, conversation_id: conversation.id.toString(),
          entry_id: origin.id.toString(), tool_call_id: "legacy-record", summary: `Retained result ${index}`,
          sources: [{ provider: "mail", account: "fixture", thread_id: "original-reference-" + "retained".repeat(4096) }],
          state: { state: "progress" }, ...([1, 2, 3, 5].includes(index) ? { reconciliation: {
            original: { request_id: requestId, submission_id: index === 1 ? original.id.toString() + "0" : original.id.toString() },
            recovery_inputs: index === 3 ? [] : index === 5 ? [{ request_id: recoveryId, submission_id: recovery.id.toString() }]
              : [{ request_id: "independent-done-input", submission_id: index === 2 ? answered.id.toString() + "0" : answered.id.toString() }],
          } } : {}) };
        const event = { id: `retained-outcome-${index}`, version: "v1",
          source: { provider: "task_outcomes", account: "local", thread_id: outcome.task_id },
          title: "Retained result", text: JSON.stringify(outcome) };
        const key = createHash("sha256").update(JSON.stringify([event.source.provider, event.source.account, event.id, event.version])).digest("hex");
        events.push({ key, event, outcome, decision: { type: "dismissed", reason: "Previously reviewed" } });
      }
      return { conversation: conversation.id, original, recovery, origin: origin.id };
    }, BACKGROUND_CONTEXT);
    await app.sources.prepare();
    await app.session.close(BACKGROUND_CONTEXT); app = await openStateStorage(path); await app.sources.prepare();
    const identity = JSON.stringify([taskId, retained.conversation.toString()]), controller = new AbortController();
    const observe = app.session.observeCommits({ advance(publication) {
      if (publication.changes.some(change => change.type === "document" && change.record.kind === Migration.definition.kind)) controller.abort();
    }, end() {} });
    await assert.rejects(app.sources.prepareReconciliations(taskId, retained.conversation, requestId, controller.signal), { name: "AbortError" });
    observe.detach();
    const paused = await app.session.snapshot(Migration, identity, BACKGROUND_CONTEXT);
    assert.ok(paused?.cursor); assert.equal(paused.complete, false);
    await app.session.close(BACKGROUND_CONTEXT);
    const trace = new SqlTrace(); app = await openStateStorage(path, trace); await app.sources.prepare();
    const progress = await app.session.snapshot(Migration, identity, BACKGROUND_CONTEXT); assert.deepEqual(progress, paused);
    const originals = new Set<number>();
    let cursor: Cursor | undefined;
    do {
      const page = await app.storage.scanDocuments({ scope: { kind: "session" }, kind: "agent.source.event.records", at: "current" }, 128, cursor, BACKGROUND_CONTEXT);
      page.items.forEach(record => originals.add(record.id)); cursor = page.next;
    } while (cursor !== undefined);
    let perCommit = 0, maximum = 0, reads = 0;
    const document = app.storage.document.bind(app.storage);
    app.storage.document = async (id, at, context) => {
      if (originals.has(id)) { perCommit++; reads++; }
      return document(id, at, context);
    };
    const commits = app.session.observeCommits({ advance() { maximum = Math.max(maximum, perCommit); perCommit = 0; }, end() {} });
    trace.enabled = true;
    await Promise.all([app.sources.prepareReconciliations(taskId, retained.conversation, requestId),
      app.sources.prepareReconciliations(taskId, retained.conversation, requestId)]);
    commits.detach();
    assert.equal(reads, 5, "Resume must skip the durable cursor and unrelated actors");
    assert.equal(maximum, 1, "Each atomic backfill commit may materialize only one source original");
    assert.equal((await app.session.snapshot(Migration, identity, BACKGROUND_CONTEXT))?.complete, true);
    const check = () => app.session.commit(tx => app.sources.reconcilesIn(tx, taskId, retained.conversation, requestId), BACKGROUND_CONTEXT);
    assert.equal(await check(), false, "An attestation made by a placed recovery input is not successful settlement");
    await app.session.commit(async tx => {
      const answer = await tx.appendEntry(AssistantEntry, retained.conversation, { model: [fauxAssistantMessage("The recovery finished.")] });
      tx.settleSubmission(retained.recovery.id, { status: "done", answer: answer.id });
    }, BACKGROUND_CONTEXT);
    assert.equal(await check(), true);
    assert.deepEqual(await app.session.commit(tx => tx.submissionByRequest(retained.conversation, requestId), BACKGROUND_CONTEXT), retained.original);
    const plans = await trace.report(app.database);
    assert.ok(plans.every(sample => !sample.plan.some(detail => detail.startsWith("SCAN document_revisions") || detail.startsWith("SCAN documents"))));
    t.diagnostic(JSON.stringify({ legacy_originals_read: reads, maximum_originals_per_commit: maximum,
      query_plans: plans.map(sample => ({ sql: sample.sql, plan: sample.plan })) }));
    await app.session.close(BACKGROUND_CONTEXT); app = await openStateStorage(path); await app.sources.prepare();
    assert.equal(await check(), true, "Completed legacy attestation survives reopen without modifying the original input");
    await app.sources.prepareReconciliations(taskId, retained.conversation, requestId, new AbortController().signal);
  } finally { await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("recording reconciliation does not settle a recovery that subsequently fails", { timeout: 20_000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-reconciliation-failed-turn-")), app = await openScheduled(directory);
  try {
    const task = await app.tasks.create({ title: "Owned interrupted work", outcome: "Continue unfinished work", execution: "coordinator", model: fixtureModel });
    const conversation = await app.tasks.conversation(task.conversation_id), requestId = "original-failed-input";
    const read = (id: string) => app.session.commit(tx => tx.submissionByRequest(conversation.id, id), BACKGROUND_CONTEXT);
    app.faux.setResponses([fauxAssistantMessage("", { stopReason: "error", errorMessage: "The fixture model stopped" })]);
    await app.tasks.enqueue(task.id, "Original authorized work", requestId);
    await assert.rejects(app.tasks.runBackground(task.id));
    const original = await read(requestId); assert.equal(original?.status, "unanswered");
    for (const successful of [false, true]) {
      const recoveryId = successful ? "completed-recovery" : "failed-recovery";
      app.faux.setResponses([
        fauxAssistantMessage(fauxToolCall("record_task", { summary: "Verified the interrupted original.", reconciles_input: requestId,
          state: { state: "waiting", actor: "Upstream work", next_check: null } }), { stopReason: "toolUse" }),
        successful ? fauxAssistantMessage("The verified recovery finished.")
          : fauxAssistantMessage("", { stopReason: "error", errorMessage: "The fixture recovery stopped after recording" }),
      ]);
      await app.tasks.enqueue(task.id, "Inspect completed effects and reconcile only this exact original input.", recoveryId);
      const running = app.tasks.runBackground(task.id);
      if (successful) await running; else await assert.rejects(running);
      assert.equal((await read(recoveryId))?.status, successful ? "done" : "unanswered");
      await app.sources.prepareReconciliations(task.id, conversation.id, requestId);
      assert.equal(await app.session.commit(tx => app.sources.reconcilesIn(tx, task.id, conversation.id, requestId), BACKGROUND_CONTEXT), successful);
      assert.deepEqual(await read(requestId), original, "Recovery never rewrites the failed original input");
    }
  } finally { await app.close(); await rm(directory, { recursive: true, force: true }); }
});
