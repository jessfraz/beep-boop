import assert from "node:assert/strict";
import { access, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { openNodeSqliteDatabase } from "@earendil-works/chord/sqlite/node";
import { createModels } from "@earendil-works/pi-ai";
import { createRegistry, Harness, UserEntry } from "@earendil-works/pi-durable";
import { SqliteStorage } from "@earendil-works/pi-durable/storage/sqlite";
import type { EmbeddingSpace } from "../src/generated/EmbeddingSpace.js";
import { HistoryStore } from "../src/history.js";
import { ConversationModels } from "../src/models.js";
import { Retrieval } from "../src/retrieval.js";
import { TaskStore, type HostCall } from "../src/tasks.js";

const space: EmbeddingSpace = { id: "background-retrieval", model_revision: "fixture", model_digest: "fixture",
  dimensions: 3, pooling: "last", normalization: "l2", instruction_version: 1 };

test("closing retrieval fences queued and future work without opening SQLite", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-retrieval-close-"));
  const models = createModels();
  const session = await Harness.open(await SqliteStorage.open(await openNodeSqliteDatabase(join(directory, "session.sqlite"))),
    { models, registry: createRegistry(), identity: { attribution: null } }, BACKGROUND_CONTEXT);
  const histories = new HistoryStore(session), tasks = new TaskStore(session, new ConversationModels(models));
  tasks.useHistory(histories);
  const path = join(directory, "search.sqlite"), retrieval = new Retrieval(session, histories, tasks, path);
  const call: HostCall = async () => { assert.fail("Closed retrieval must not start a host effect"); };
  try {
    const indexing = retrieval.sync(call);
    const closing = retrieval.close();
    const outcome = await Promise.allSettled([indexing, closing]);
    assert.equal(outcome[0]?.status, "rejected", "Queued catchup must retire before opening the index");
    assert.equal(outcome[1]?.status, "fulfilled");
    assert.equal(retrieval.close(), closing, "Closing retains one terminal owner");
    await assert.rejects(retrieval.sync(call), /retrieval is closed/);
    for (const result of [await retrieval.snapshot("connector", [], 8), await retrieval.search("connector", [], 8, call)]) {
      assert.deepEqual(result.candidates, []);
      assert.ok(result.gaps.some((gap) => gap.includes("retrieval is closed")));
    }
    await assert.rejects(access(path), { code: "ENOENT" });
  } finally {
    await retrieval.close(); await session.close(BACKGROUND_CONTEXT);
    await rm(directory, { recursive: true, force: true });
  }
});

test("retired indexing cannot publish a late embedding and remains resumable", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-retrieval-cancel-"));
  const models = createModels();
  const session = await Harness.open(await SqliteStorage.open(await openNodeSqliteDatabase(join(directory, "session.sqlite"))),
    { models, registry: createRegistry(), identity: { attribution: null } }, BACKGROUND_CONTEXT);
  const histories = new HistoryStore(session), tasks = new TaskStore(session, new ConversationModels(models));
  tasks.useHistory(histories);
  const retrieval = new Retrieval(session, histories, tasks, join(directory, "search.sqlite"));
  const controller = new AbortController();
  const sync: (call: HostCall, all: boolean, signal: AbortSignal) => Promise<void> = retrieval.sync.bind(retrieval);
  const embedding: HostCall = async (command) => {
    if (command.type !== "embed") throw new Error("Unexpected host effect");
    return { type: "embeddings", batch: { space, vectors: command.texts.map(() => [1, 0, 0]) } };
  };
  try {
    const conversation = await session.root(BACKGROUND_CONTEXT);
    await conversation.commit((tx) => tx.appendEntry(UserEntry, conversation.id, {
      model: [{ role: "user", content: "Original connector specification", timestamp: 1 }],
    }), BACKGROUND_CONTEXT);
    await retrieval.sync(embedding);
    const pending = await conversation.commit((tx) => tx.appendEntry(UserEntry, conversation.id, {
      model: [{ role: "user", content: "Revised connector torque", timestamp: 2 }],
    }), BACKGROUND_CONTEXT);
    await assert.rejects(sync(async (command) => {
      controller.abort(new Error("Index invocation retired"));
      return embedding(command);
    }, false, controller.signal), /Index invocation retired/);
    const retained = await retrieval.search("connector", [], 8, embedding);
    assert.ok(retained.candidates[0]?.matches.every((match) => match.entry_id !== pending.id.toString()),
      "A late host reply cannot publish a retired invocation's watermark");
    await retrieval.sync(embedding);
    const resumed = await retrieval.search("connector", [], 8, embedding);
    assert.ok(resumed.candidates.some((candidate) => candidate.matches.some((match) => match.entry_id === pending.id.toString())));
    assert.deepEqual(resumed.gaps, []);
  } finally {
    await retrieval.close(); await session.close(BACKGROUND_CONTEXT);
    await rm(directory, { recursive: true, force: true });
  }
});

test("semantic reads retain the published generation while background indexing owns new native text", { timeout: 10_000 }, async (t) => {
  const directory = await mkdtemp(join(tmpdir(), "agent-retrieval-background-"));
  const models = createModels();
  const session = await Harness.open(await SqliteStorage.open(await openNodeSqliteDatabase(join(directory, "session.sqlite"))),
    { models, registry: createRegistry(), identity: { attribution: null } }, BACKGROUND_CONTEXT);
  const histories = new HistoryStore(session), tasks = new TaskStore(session, new ConversationModels(models));
  tasks.useHistory(histories);
  const retrieval = new Retrieval(session, histories, tasks, join(directory, "search.sqlite"));
  let release = () => {}, entered = () => {}, holding = false, released = false;
  const held = new Promise<void>((resolve) => { release = () => { released = true; resolve(); }; });
  const started = new Promise<void>((resolve) => { entered = resolve; });
  t.signal.addEventListener("abort", release, { once: true });
  const documentOwners: string[] = [];
  const embedding = (owner: "foreground" | "background"): HostCall => async (command) => {
    assert.equal(command.type, "embed", "Retrieval must use its actual embedding host boundary only");
    if (command.type !== "embed") throw new Error("Unexpected host effect");
    if (holding && command.purpose === "document") {
      documentOwners.push(owner); entered(); await held;
    }
    return { type: "embeddings", batch: { space, vectors: command.texts.map(() => [1, 0, 0]) } };
  };
  let indexing: Promise<void> | undefined, query: Promise<Awaited<ReturnType<Retrieval["search"]>>> | undefined;
  try {
    const conversation = await session.root(BACKGROUND_CONTEXT);
    const original = await conversation.commit((tx) => tx.appendEntry(UserEntry, conversation.id, {
      model: [{ role: "user", content: "Automobile published connector specification", timestamp: 1 }],
    }), BACKGROUND_CONTEXT);
    await retrieval.sync(embedding("background"));
    const pending = await conversation.commit((tx) => tx.appendEntry(UserEntry, conversation.id, {
      model: [{ role: "user", content: "Automobile revised connector torque", timestamp: 2 }],
    }), BACKGROUND_CONTEXT);
    holding = true;
    // A lookup arriving just before the background lane must never steal its
    // dirty document work or wait for that work before reading the old view.
    query = retrieval.search("car wheel repair", [], 8, embedding("foreground"));
    indexing = retrieval.sync(embedding("background"));
    await started;
    assert.deepEqual(documentOwners, ["background"], "Foreground search must never take ownership of document indexing");
    const published = await query;
    assert.equal(released, false, "The published read must finish while the real document host effect remains held");
    assert.equal(published.candidates[0]?.conversation_id, conversation.id.toString());
    assert.ok(published.candidates[0]?.matches.some((match) => match.entry_id === original.id.toString()));
    assert.ok(published.candidates.every((candidate) => candidate.matches.every((match) => match.entry_id !== pending.id.toString())));
    assert.ok(published.gaps.length > 0, "Pending native text must remain explicit partial coverage");
    release(); await indexing;
    const updated = await retrieval.search("car wheel repair", [], 8, embedding("foreground"));
    assert.ok(updated.candidates.some((candidate) => candidate.matches.some((match) => match.entry_id === pending.id.toString()
      && match.text === "Automobile revised connector torque")), "The background owner must publish the new native entry");
    assert.deepEqual(updated.gaps, []);
    assert.deepEqual(documentOwners, ["background"], "The foreground read must not repeat background embedding work");
  } finally {
    release(); await Promise.allSettled([indexing, query]);
    await retrieval.close(); await session.close(BACKGROUND_CONTEXT);
    await rm(directory, { recursive: true, force: true });
  }
});
