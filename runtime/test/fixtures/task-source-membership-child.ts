import assert from "node:assert/strict";
import { join } from "node:path";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { createModels } from "@earendil-works/pi-ai";
import { createRegistry, Harness } from "@earendil-works/pi-durable";
import { SqliteStorage } from "@earendil-works/pi-durable/storage/sqlite";
import { openConversationDatabase } from "../../src/conversation-database.js";
import type { SourceRef } from "../../src/generated/SourceRef.js";
import { ConversationModels } from "../../src/models.js";
import { TaskStore } from "../../src/tasks.js";

const directory = process.argv[2];
if (directory === undefined) throw new Error("A disposable native session directory is required");
const models = createModels(), session = await Harness.open(await SqliteStorage.open(await openConversationDatabase(join(directory, "session.sqlite"))),
  { models, registry: createRegistry(), identity: { attribution: null } }, BACKGROUND_CONTEXT);
const tasks = new TaskStore(session, new ConversationModels(models));
try {
  await tasks.prepare();
  const sender = await session.root(BACKGROUND_CONTEXT);
  const sources: SourceRef[] = Array.from({ length: 7124 }, (_, index) => ({ provider: "slack", account: "invented-workspace",
    thread_id: "retained-thread-" + String(index).padStart(5, "0") }));
  const task = await tasks.create({ id: "retained-owner", title: "Retained source work", outcome: "Preserve every source and queued input" });
  await tasks.update(task.id, current => {
    current.sources = sources;
    current.pending = Array.from({ length: 345 }, (_, index) => ({ id: "retained-input-" + index,
      content: "Invented retained evidence. ".repeat(270) }));
  });
  const measure = async (operation: string, run: () => Promise<unknown>) => {
    const began = performance.now(); await run();
    process.stdout.write("MEASURE " + JSON.stringify({ operation, elapsed_ms: performance.now() - began }) + "\n");
  };
  process.stdout.write("READY\n");
  await measure("metadata-update", () => tasks.update(task.id, current => { current.summary = "Metadata changed while its source references stayed identical"; }));
  await measure("review-admission", () => session.commit(tx => tasks.enqueueReviewIn(tx, { task_id: task.id,
    content: "New bounded evidence", input_id: "source-review:bounded-admission", sender_conversation_id: sender.id.toString(), work_class: "backfill" }), BACKGROUND_CONTEXT));
  const removed = sources.at(-1), added: SourceRef = { provider: "github", account: "invented-account", thread_id: "new-thread" };
  assert.ok(removed);
  await measure("source-relink", () => session.commit(tx => tasks.relinkSourcesIn(tx, task.id, [added, added], [removed]), BACKGROUND_CONTEXT));
  const saved = await tasks.get(task.id);
  assert.deepEqual(saved.sources, [...sources.slice(0, -1), added]); assert.equal(saved.pending.length, 346);
  assert.deepEqual(await tasks.findBySources([removed]), []);
  assert.equal((await tasks.findBySources([added]))[0]?.id, task.id);
  process.stdout.write("DONE\n");
} finally { await session.close(BACKGROUND_CONTEXT); }
