import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { createHash } from "node:crypto";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { LegacySources, LegacyTasks, openStateStorage, seedLegacy } from "./fixtures/state-storage.js";
import type { TaskOutcome } from "../src/generated/TaskOutcome.js";

for (const kind of ["agent.task.directory", "agent.source.event.directory"]) {
  test(`partition migration resumes after native close at ${kind} without changing legacy ownership`, async () => {
    const directory = await mkdtemp(join(tmpdir(), "agent-state-migration-")), path = join(directory, "session.sqlite");
    let app = await openStateStorage(path), closing: Promise<void> | undefined;
    try {
      const original = await seedLegacy(app, 70, 1024), first = original.tasks[0]; assert.ok(first);
      const active = { id: "retained-review", events: original.events.slice(0, 2).map(record => record.key),
        due: [{ id: first.id, next_check: "2026-01-01T00:00:00Z" }], attempt: 3, content: "Exact admitted review input" };
      await app.session.commit(async tx => {
        const journal = await tx.doc(LegacySources);
        journal.conversation_id = String((await tx.createConversation({ ownership: { kind: "ownerless" } })).id);
        journal.active = active;
      }, BACKGROUND_CONTEXT);
      const legacyTasks = await app.session.snapshot(LegacyTasks, BACKGROUND_CONTEXT), legacySources = await app.session.snapshot(LegacySources, BACKGROUND_CONTEXT);
      const stop = app.session.subscribeCommits(publication => {
        for (const change of publication.changes) if (change.type === "document" && change.record.kind === kind &&
          change.value !== null && change.value.migrated === 32 && !change.value.complete && closing === undefined) {
          closing = app.session.close(BACKGROUND_CONTEXT);
        }
      });
      await assert.rejects(app.tasks.get(first.id), /clos|end|stop/);
      assert.ok(closing, "The first bounded migration batch must be independently durable");
      await closing; stop(); app = await openStateStorage(path);
      assert.equal((await app.tasks.list()).length, original.tasks.length);
      assert.deepEqual(await app.tasks.get(first.id), first);
      const records = await app.sources.events({ limit: 100 });
      assert.deepEqual(records, [...original.events].reverse());
      assert.deepEqual((await app.sources.states()).find(state => state.source.provider === "mail"), original.state);
      const control = await app.storage.findDocument({ kind: "agent.source.control", scope: { kind: "session" } }, "current", BACKGROUND_CONTEXT);
      assert.ok(control);
      const resumed = await app.storage.document(control.id, "current", BACKGROUND_CONTEXT);
      assert.deepEqual(resumed?.value.active, active, "The interrupted native claim must retain its exact input, due work, identity and attempt");
      assert.equal(resumed?.value.conversation_id, legacySources?.conversation_id);
      assert.deepEqual(resumed?.value.last_claimed_source, legacySources?.last_claimed_source);
      assert.deepEqual(resumed?.value.next_event, legacySources?.next_event);
      const destination = original.tasks[1]; assert.ok(destination);
      await app.sources.decide([original.events[0]?.key ?? ""], { type: "task", task_id: destination.id });
      assert.equal((await app.tasks.get(first.id)).sources.length, 0);
      assert.deepEqual((await app.tasks.get(destination.id)).sources, [original.events[0]?.event.source]);
      assert.deepEqual(await app.session.snapshot(LegacyTasks, BACKGROUND_CONTEXT), legacyTasks, "Migration must retain the exact old task store");
      assert.deepEqual(await app.session.snapshot(LegacySources, BACKGROUND_CONTEXT), legacySources, "Migration must retain every old event/version and decision");
    } finally { await closing?.catch(() => {}); await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
  });
}

test("migration retains pending legacy outcomes without a stored outcome coverage row", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-source-legacy-outcome-")), app = await openStateStorage(join(directory, "session.sqlite"));
  try {
    const original = await seedLegacy(app, 2), task = original.tasks[0]; assert.ok(task);
    const outcome: TaskOutcome = { task_id: task.id, conversation_id: task.conversation_id, entry_id: null, tool_call_id: null,
      summary: "Retained native outcome", sources: [], state: { state: "progress" } };
    const event = { id: task.id + ":" + task.conversation_id, version: "legacy", source: { provider: "task_outcomes", account: "local", thread_id: task.id },
      title: "Task progress", text: JSON.stringify(outcome) };
    const key = createHash("sha256").update(JSON.stringify([event.source.provider, event.source.account, event.id, event.version])).digest("hex");
    await app.session.commit(async tx => {
      const journal = await tx.doc(LegacySources);
      for (const record of journal.events) record.decision = { type: "dismissed", reason: "Already handled" };
      journal.events.push({ key, event, outcome, decision: null });
    }, BACKGROUND_CONTEXT);
    assert.equal(await app.sources.hasWork(), true, "Pending outcomes must remain claimable independently of old coverage metadata");
    assert.deepEqual((await app.sources.events({ pending: true })).map(record => record.key), [key]);
  } finally { await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("cold point lookups and hot task admission materialize only their owned records", async t => {
  const directory = await mkdtemp(join(tmpdir(), "agent-state-point-read-")), path = join(directory, "session.sqlite");
  let app = await openStateStorage(path);
  try {
    const original = await seedLegacy(app, 128, 2048, 2048), first = original.tasks[0]; assert.ok(first);
    await app.tasks.get(first.id); await app.sources.states(); await app.session.close(BACKGROUND_CONTEXT);
    app = await openStateStorage(path); app.trace.enabled = true;
    assert.deepEqual(await app.tasks.get(first.id), first); await app.sources.states();
    const readBytes = [...app.trace.samples.values()].reduce((sum, sample) => sum + sample.content_bytes, 0);
    assert.ok(readBytes < 16 * 1024, `A cold task/coverage lookup read ${readBytes} bytes from unrelated retained state`);
    app.trace.samples.clear();
    await app.tasks.enqueue(first.id, "One exact hot input", "hot-input");
    const written = [...app.trace.samples.values()].reduce((sum, sample) => sum + sample.bound_bytes, 0);
    assert.ok(written < 16 * 1024, `Hot admission bound ${written} bytes instead of one owned task record`);
    const plans = await app.trace.report(app.database);
    t.diagnostic(JSON.stringify({ cold_content_bytes: readBytes, hot_bound_bytes: written, plans }));
    await app.session.close(BACKGROUND_CONTEXT); app = await openStateStorage(path);
    assert.equal((await app.tasks.get(first.id)).pending[0]?.id, "hot-input");
  } finally { await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});
