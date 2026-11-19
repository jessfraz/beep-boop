import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createInterface } from "node:readline";
import { test } from "node:test";
import { fileURLToPath } from "node:url";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { createModels } from "@earendil-works/pi-ai";
import { createRegistry, Harness } from "@earendil-works/pi-durable";
import { SqliteStorage } from "@earendil-works/pi-durable/storage/sqlite";
import { openConversationDatabase } from "../src/conversation-database.js";
import type { SourceRef } from "../src/generated/SourceRef.js";
import { ConversationModels } from "../src/models.js";
import { TaskStore } from "../src/tasks.js";

async function open(path: string) {
  const models = createModels(), session = await Harness.open(await SqliteStorage.open(await openConversationDatabase(path)),
    { models, registry: createRegistry(), identity: { attribution: null } }, BACKGROUND_CONTEXT);
  return { session, tasks: new TaskStore(session, new ConversationModels(models)) };
}

test("source membership changes preserve exact duplicate order, transactional owners and deadlines across migration and reopen", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-task-source-membership-")), path = join(directory, "session.sqlite");
  let app = await open(path);
  const a: SourceRef = { provider: "slack", account: "workspace-one", thread_id: "thread-one" }, b = { ...a, thread_id: "thread-two" };
  const account = { ...a, account: "workspace-two" }, provider = { ...a, provider: "github" }, added = { ...a, thread_id: "thread-three" };
  const original = [a, a, b, account, provider], retained = [b, account, provider, added];
  try {
    // Atomic admission can precede prepare. Both routing projections must agree
    // while the native store is still finishing its legacy migration.
    await app.session.commit(async tx => {
      await app.tasks.createIn(tx, { id: "owner", title: "Exact source owner", outcome: "Keep its retained references", sources: original });
      await app.tasks.relinkSourcesIn(tx, "owner", [a, added, added], [a, added]);
      assert.deepEqual(await app.tasks.sourceLinksIn(tx, [a]), []);
      assert.deepEqual(await app.tasks.sourceLinksIn(tx, [b, account, provider, added]), retained.map(source => ({ task_id: "owner", source })));
    }, BACKGROUND_CONTEXT);
    const owner = await app.tasks.get("owner"); assert.deepEqual(owner.sources, retained);
    const conversation = await app.tasks.conversation(owner.conversation_id);
    const entries = await conversation.entries({}, 10, undefined, BACKGROUND_CONTEXT);
    assert.deepEqual(entries.items.find(entry => entry.kind === "agent.source_links")?.data,
      { task_id: "owner", added: [added], removed: [a, a] });
    await app.tasks.update("owner", task => {
      task.summary = "Only metadata changed";
      task.state = { state: "waiting", actor: "invented-provider", next_check: new Date(0).toISOString() };
    });
    assert.deepEqual(await app.tasks.dueChecks(0), [{ id: "owner", next_check: new Date(0).toISOString() }]);
    const replacement = [added, added, account, a];
    await app.tasks.update("owner", task => { task.sources = replacement; });
    assert.deepEqual((await app.tasks.get("owner")).sources, replacement);
    for (const removed of [b, provider]) assert.deepEqual(await app.tasks.findBySources([removed]), []);
    for (const source of [a, account, added]) assert.equal((await app.tasks.findBySources([source]))[0]?.id, "owner");
    await assert.rejects(app.session.commit(async tx => {
      await app.tasks.relinkSourcesIn(tx, "owner", [b], [added]);
      throw new Error("Rollback source correction");
    }, BACKGROUND_CONTEXT), /Rollback source correction/u);
    assert.deepEqual((await app.tasks.get("owner")).sources, replacement);
    await app.session.close(BACKGROUND_CONTEXT); app = await open(path);
    assert.deepEqual((await app.tasks.get("owner")).sources, replacement);
    assert.deepEqual(await app.tasks.findBySources([b, provider]), []);
    assert.equal((await app.tasks.findBySources([added]))[0]?.id, "owner");
    assert.deepEqual(await app.tasks.dueChecks(0), [{ id: "owner", next_check: new Date(0).toISOString() }]);
  } finally { await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

test("a retained source owner admits metadata, review input and correction within the collector deadline", { timeout: 90_000 }, async t => {
  const directory = await mkdtemp(join(tmpdir(), "agent-task-source-scale-"));
  const child = spawn(process.execPath, ["--no-warnings", fileURLToPath(new URL("./fixtures/task-source-membership-child.js", import.meta.url)), directory],
    { stdio: ["ignore", "pipe", "pipe"] });
  const closed = new Promise<number | null>((resolve, reject) => { child.once("close", resolve); child.once("error", reject); });
  const lines = createInterface({ input: child.stdout });
  let stderr = "", timer: ReturnType<typeof setTimeout> | undefined;
  child.stderr.setEncoding("utf8").on("data", (chunk: string) => { stderr += chunk; });
  t.signal.addEventListener("abort", () => child.kill("SIGKILL"), { once: true });
  try {
    await new Promise<void>((resolve, reject) => {
      child.once("error", reject);
      child.once("close", () => reject(new Error("Native source owner stopped before completing its admission: " + stderr)));
      lines.on("line", line => {
        if (line === "READY") timer = setTimeout(() => reject(new Error("Retained source ownership blocked admission beyond the 30-second collector deadline")), 30_000);
        else if (line === "DONE") { clearTimeout(timer); resolve(); }
        else t.diagnostic(line);
      });
    });
    assert.equal(await closed, 0, stderr);
  } finally {
    clearTimeout(timer); child.kill("SIGKILL"); await closed; lines.close();
    await rm(directory, { recursive: true, force: true });
  }
});
