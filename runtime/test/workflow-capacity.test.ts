import assert from "node:assert/strict";
import { test } from "node:test";
import type { TaskWorkflow } from "../src/generated/TaskWorkflow.js";
import { DEFAULT_MODEL } from "../src/models.js";
import { WorkflowCapacity } from "../src/workflow-capacity.js";

function gate() {
  let release = () => {};
  const promise = new Promise<void>(resolve => { release = resolve; });
  return { promise, release };
}
function work(job: string, resource: string): TaskWorkflow {
  return { owner_task_id: job, work_key: resource, job: { id: job, title: job, prompt: "Resolve the current evidence",
    model: DEFAULT_MODEL, timezone: "UTC", schedules: [], revision: "one", cwd: "/workspace/example" } };
}

test("blocked workflow resources leave independent work and other workflows runnable", async () => {
  const capacity = new WorkflowCapacity(2), first = gate(), second = gate(), started = gate(), other = gate();
  const order: string[] = [];
  const active = capacity.run("old", work("deliveries", "parcel-a"), async () => { order.push("old"); started.release(); await first.promise; });
  await started.promise;
  const duplicate = capacity.run("old", work("deliveries", "parcel-a"), async () => assert.fail("One task must have one live execution"));
  const same = capacity.run("same", work("deliveries", "parcel-a"), async () => { order.push("same"); });
  const fresh = capacity.run("fresh", work("deliveries", "parcel-b"), async () => { order.push("fresh"); other.release(); await second.promise; });
  await other.promise;
  await capacity.run("spam", work("spam", "mail-a"), async () => { order.push("spam"); });
  assert.deepEqual(order, ["old", "fresh", "spam"]);
  first.release(); await Promise.all([active, duplicate, same]);
  assert.deepEqual(order, ["old", "fresh", "spam", "same"]);
  second.release(); await fresh;
});

test("cancelled waiters do not start effects and failures release their resource", async () => {
  const capacity = new WorkflowCapacity(2), hold = gate(), started = gate(), controller = new AbortController();
  const first = capacity.run("first", work("deliveries", "parcel"), async () => { started.release(); await hold.promise; });
  await started.promise;
  const cancelled = capacity.run("cancelled", work("deliveries", "parcel"), async () => assert.fail("Cancelled effects cannot run"), controller.signal);
  const rejected = assert.rejects(cancelled, /cancelled/u); controller.abort(new Error("cancelled")); await rejected;
  hold.release(); await first;
  await assert.rejects(capacity.run("failed", work("deliveries", "parcel"), async () => { throw new Error("effect failed"); }), /effect failed/u);
  await capacity.run("later", work("deliveries", "parcel"), async () => {});
});
