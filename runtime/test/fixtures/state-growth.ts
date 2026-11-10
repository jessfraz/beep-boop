import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { openStateStorage, seedLegacy } from "./state-storage.js";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";

const counts = (process.env.AGENT_STATE_COUNTS ?? "128,1024").split(",").map(Number);
const sourceBytes = Number(process.env.AGENT_STATE_SOURCE_BYTES ?? 4096), taskBytes = Number(process.env.AGENT_STATE_TASK_BYTES ?? 2048);
const runs = Number(process.env.AGENT_STATE_RUNS ?? 12), results = [];
for (const count of counts) {
  assert.ok(Number.isSafeInteger(count) && count > 0); assert.ok(Number.isSafeInteger(runs) && runs > 0);
  const directory = await mkdtemp(join(tmpdir(), "agent-state-growth-")), path = join(directory, "session.sqlite");
  let began = performance.now(), app = await openStateStorage(path);
  const initialOpenMs = performance.now() - began;
  try {
    const original = await seedLegacy(app, count, sourceBytes, taskBytes), first = original.tasks[0]; assert.ok(first);
    await app.session.close(BACKGROUND_CONTEXT);
    began = performance.now(); app = await openStateStorage(path);
    const firstOpenMs = performance.now() - began;
    began = performance.now(); await app.tasks.get(first.id); await app.sources.states();
    const firstReadyMs = performance.now() - began;
    await app.session.close(BACKGROUND_CONTEXT);
    began = performance.now(); app = await openStateStorage(path);
    const warmOpenMs = performance.now() - began;
    app.trace.enabled = true; began = performance.now(); await app.tasks.get(first.id); await app.sources.states();
    const coldPointMs = performance.now() - began;
    const coldPlans = await app.trace.report(app.database);
    app.trace.samples.clear(); app.trace.enabled = true;
    const taskAdmission: number[] = [], sourceAdmission: number[] = [];
    let checkpoint = original.state.checkpoint;
    for (let index = 0; index < runs; index += 1) {
      began = performance.now(); await app.tasks.enqueue(first.id, `Hot input ${index}`, `hot-${index}`); taskAdmission.push(performance.now() - began);
      began = performance.now();
      const admitted = await app.sources.admit({ source: original.state.source, checkpoint: { value: `hot-${index}` }, coverage: { status: "complete" },
        events: [{ id: `new-${index}`, version: "v1", source: { ...original.state.source, thread_id: `new-${index}` }, title: "New live source", text: "Small current source evidence" }] }, checkpoint);
      sourceAdmission.push(performance.now() - began); checkpoint = admitted.state.checkpoint;
    }
    const hotPlans = await app.trace.report(app.database);
    const summary = (values: number[]) => {
      const sorted = [...values].sort((left, right) => left - right);
      return { median_ms: sorted[Math.floor(sorted.length / 2)], p95_ms: sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * 0.95))] };
    };
    const result = { count, source_bytes: count * sourceBytes, task_bytes: count * taskBytes, initial_open_ms: initialOpenMs,
      first_open_ms: firstOpenMs, first_ready_ms: firstReadyMs, warm_open_ms: warmOpenMs, cold_point_ms: coldPointMs,
      hot_task_admission: summary(taskAdmission), hot_source_admission: summary(sourceAdmission), cold_plans: coldPlans, hot_plans: hotPlans };
    results.push(result); process.stdout.write(JSON.stringify(result) + "\n");
  } finally { await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
}
const output = process.env.AGENT_STATE_REPORT;
if (output !== undefined) await writeFile(output, JSON.stringify(results, null, 2) + "\n");
