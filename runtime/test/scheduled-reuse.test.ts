import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { fauxAssistantMessage, fauxToolCall } from "@earendil-works/pi-ai";
import { getCurrentSystemPrompt } from "@earendil-works/pi-ai/utils/transcript";
import { BackgroundScheduler } from "../src/background.js";
import { openJobActor, runScheduled } from "../src/scheduled.js";
import { UserNoticeEntry } from "../src/transcript.js";
import { WorkflowProgress } from "../src/workflow-progress.js";
import type { TaskState } from "../src/generated/TaskState.js";
import { DAY, START, FakeClock, exampleJob, openBackground, planned, pump } from "./fixtures/background-runtime.js";
import { openScheduled } from "./fixtures/scheduled-runtime.js";

test("new manual and timer occurrences use fresh conversations while exact replays retain their own result", { timeout: 20_000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-scheduled-fresh-")), clock = new FakeClock();
  const app = await openScheduled(directory, "original", clock);
  let release = () => {};
  const scheduler = new BackgroundScheduler(app.session, app.registry, app.tasks, {
    plan: async (cadence, after, now) => planned(cadence, after, now), refreshSources: async () => {},
    triageSources: async () => {}, collectResources: async () => {}, indexHistory: async () => {},
    runJob: (occurrence, signal, actor) => runScheduled(occurrence, signal, app, actor),
  });
  try {
    const original = exampleJob("original", "ORIGINAL_JOB_POLICY");
    const owner = await app.tasks.create({ title: original.title, outcome: original.prompt, model: original.model,
      sources: [{ provider: "scheduled_jobs", account: "local", thread_id: original.id }] });
    const retained = await app.tasks.conversation(owner.conversation_id);
    await (await retained.submit({ type: "write", entry: { kind: "fixture.previous_run",
      model: [{ role: "user", content: "OLD_RUN_CONTEXT", timestamp: 1 }] } }, BACKGROUND_CONTEXT)).wait(BACKGROUND_CONTEXT);
    await app.tasks.update(owner.id, task => { task.summary = "OLD_RUN_SUMMARY"; });
    await scheduler.start({ poll_interval_seconds: 60, sources: false, jobs: [original] });
    await pump(async () => (await scheduler.getJob(original.id)).schedules[0]?.next_at !== null);
    const held = new Promise<void>(resolve => { release = resolve; });
    let started = () => {}; const firstStarted = new Promise<void>(resolve => { started = resolve; });
    app.faux.setResponses([
      async context => {
        started();
        assert.match(getCurrentSystemPrompt(context.messages), /ORIGINAL_JOB_POLICY/);
        assert.match(JSON.stringify(context.messages), /CURRENT_REQUEST_ONE/);
        assert.doesNotMatch(JSON.stringify(context.messages), /OLD_RUN_CONTEXT|OLD_RUN_SUMMARY/);
        await held; return fauxAssistantMessage("First requested review complete.");
      },
      context => {
        assert.match(JSON.stringify(context.messages), /CURRENT_REQUEST_TWO/);
        assert.doesNotMatch(JSON.stringify(context.messages), /CURRENT_REQUEST_ONE|OLD_RUN_CONTEXT|OLD_RUN_SUMMARY/);
        return fauxAssistantMessage("Second requested review complete.");
      },
      (context, _options, _state, model) => {
        assert.match(getCurrentSystemPrompt(context.messages), /UPDATED_JOB_POLICY/);
        assert.equal(model.id, "fixture-updated");
        assert.doesNotMatch(JSON.stringify(context.messages), /CURRENT_REQUEST_ONE|CURRENT_REQUEST_TWO|OLD_RUN_CONTEXT|OLD_RUN_SUMMARY/);
        return fauxAssistantMessage("Updated timer review complete.");
      },
    ]);
    const first = await scheduler.runJob({ type: "run_job", request_id: "first", job_id: original.id,
      schedule_id: null, input: "CURRENT_REQUEST_ONE" });
    await firstStarted;
    const second = await scheduler.runJob({ type: "run_job", request_id: "second", job_id: original.id,
      schedule_id: null, input: "CURRENT_REQUEST_TWO" });
    assert.notEqual(second.task_id, first.task_id);
    assert.notEqual(first.task_id, owner.id);
    assert.equal(app.tasks.isRunning(first.task_id), true, "Admitting another occurrence must preserve the running actor");
    assert.deepEqual(second.manual, { input: "CURRENT_REQUEST_TWO" });
    const replay = await scheduler.runJob({ type: "run_job", request_id: "second", job_id: original.id,
      schedule_id: null, input: "CURRENT_REQUEST_TWO" });
    assert.deepEqual(replay, second);
    await assert.rejects(scheduler.runJob({ type: "run_job", request_id: "second", job_id: original.id,
      schedule_id: null, input: "A changed request" }), /identity already belongs/);
    const updated = { ...original, revision: "updated", prompt: "UPDATED_JOB_POLICY",
      model: { ...original.model, model_id: "fixture-updated" } };
    await scheduler.syncJobs([updated]);
    release(); await pump(async () => (await scheduler.status()).pending === 0);
    clock.advance(DAY);
    await pump(() => app.faux.state.callCount === 3);
    await pump(async () => (await scheduler.status()).pending === 0);
    const latest = (await scheduler.getJob(original.id)).last_run;
    assert.ok(latest?.task_id);
    assert.notEqual(latest.task_id, first.task_id); assert.notEqual(latest.task_id, second.task_id);
    const runIds = [first.task_id, second.task_id, latest.task_id];
    const conversations = new Set<string>();
    for (const id of runIds) {
      const task = await app.tasks.get(id); conversations.add(task.conversation_id);
      assert.equal(task.workflow?.owner_task_id, owner.id);
      assert.equal(task.sources.some(ref => ref.provider === "scheduled_jobs" && ref.account === "local"), false);
      const conversation = await app.tasks.conversation(task.conversation_id);
      const page = await conversation.entries({}, 100, undefined, BACKGROUND_CONTEXT);
      assert.equal(page.items.filter(entry => entry.model?.some(message => message.role === "user")).length, 1);
    }
    assert.equal(conversations.size, 3);
    assert.equal(await openJobActor(updated, app.tasks), owner.id);
    assert.equal((await app.tasks.get(owner.id)).conversation_id, retained.id.toString());
    assert.equal((await retained.entries({}, 10, undefined, BACKGROUND_CONTEXT)).items.length, 1);
    const root = await app.session.root(BACKGROUND_CONTEXT);
    const notices = (await root.entries({}, 100, undefined, BACKGROUND_CONTEXT)).items.flatMap(entry =>
      UserNoticeEntry.is(entry) && typeof entry.data?.text === "string" ? [entry.data.text] : []);
    assert.equal(notices.filter(text => text === "First requested review complete.").length, 1);
    assert.equal(notices.filter(text => text === "Second requested review complete.").length, 1);
    assert.equal(notices.includes("Updated timer review complete."), false);
    assert.deepEqual((await scheduler.status()).errors, []);
  } finally { release(); await app.close(); await rm(directory, { recursive: true, force: true }); }
});

test("manual job admission chooses a neutral schedule while focused alternatives remain explicit", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-manual-schedule-")), clock = new FakeClock();
  const app = await openBackground(directory, clock);
  try {
    const original = exampleJob(), schedule = original.schedules[0]!;
    const job = { ...original, schedules: [{ ...schedule, id: "early" }, { ...schedule, id: "main" }] };
    await app.scheduler.start({ poll_interval_seconds: 60, sources: false, jobs: [job] });
    const main = await app.scheduler.runJob({ type: "run_job", request_id: "neutral-main", job_id: job.id, schedule_id: null });
    assert.equal(main.schedule_id, "main");
    assert.deepEqual(main.manual, {}, "Every direct run retains its result obligation without requiring extra input");
    await app.scheduler.syncJobs([{ ...job, revision: "neutral-first", schedules: [{ ...schedule, id: "early" }, { ...schedule, id: "late" }] }]);
    const first = await app.scheduler.runJob({ type: "run_job", request_id: "neutral-first", job_id: job.id, schedule_id: null });
    assert.equal(first.schedule_id, "early");
    await app.scheduler.syncJobs([{ ...job, revision: "focused", schedules: [
      { ...schedule, id: "first", focus: "first", prompt: "Review the first subset." },
      { ...schedule, id: "second", focus: "second", prompt: "Review the second subset." },
    ] }]);
    await assert.rejects(app.scheduler.runJob({ type: "run_job", request_id: "focused", job_id: job.id, schedule_id: null }), /Choose a schedule/);
    const explicit = await app.scheduler.runJob({ type: "run_job", request_id: "explicit", job_id: job.id, schedule_id: "second" });
    assert.equal(explicit.schedule_id, "second");
    await pump(async () => (await app.scheduler.status()).pending === 0);
    assert.deepEqual((await app.receipts())?.jobs.map(occurrence => [occurrence.schedule_id, occurrence.manual]),
      [["main", {}], ["early", {}], ["second", {}]]);
  } finally { await app.session.close(BACKGROUND_CONTEXT); await rm(directory, { recursive: true, force: true }); }
});

for (const notified of ["A concrete blocker is being investigated.", "The requested review is complete; no new items."]) {
  test(`a direct run delivers its final result once after notification ${JSON.stringify(notified)}`, async () => {
    const directory = await mkdtemp(join(tmpdir(), "agent-manual-result-")), app = await openScheduled(directory);
    try {
      const result = "The requested review is complete; no new items.";
      const occurrence = { id: "manual:notification-result", job: exampleJob(), schedule_id: "daily", scheduled_at: START, manual: {} };
      app.faux.setResponses([
        fauxAssistantMessage(fauxToolCall("notify_user", { text: notified }), { stopReason: "toolUse" }),
        fauxAssistantMessage(result),
      ]);
      await runScheduled(occurrence, new AbortController().signal, app);
      await runScheduled(occurrence, new AbortController().signal, app);
      const root = await app.session.root(BACKGROUND_CONTEXT);
      const texts = (await root.entries({ order: "ascending" }, 100, undefined, BACKGROUND_CONTEXT)).items.flatMap(entry =>
        UserNoticeEntry.is(entry) && typeof entry.data?.text === "string" ? [entry.data.text] : []);
      assert.equal(texts.filter(text => text === result).length, 1);
      assert.deepEqual(texts, notified === result ? [result] : [notified, result]);
      assert.equal(app.faux.state.callCount, 2, "Exact result replay must not run another model turn");
    } finally { await app.close(); await rm(directory, { recursive: true, force: true }); }
  });
}

for (const scenario of ["resolved", "waiting", "preliminary notice", "already delivered"]) {
  test(`a manual scheduler run publishes its recorded ${scenario} outcome when the final is empty`, async () => {
    const directory = await mkdtemp(join(tmpdir(), "agent-manual-recorded-result-")), clock = new FakeClock();
    const app = await openScheduled(directory, "original", clock), job = exampleJob();
    const monitor = new WorkflowProgress(app.session, app.registry, app.tasks, clock.now);
    monitor.useReceipts((tx, id, conversation, request) => app.sources.reconcilesIn(tx, id, conversation, request));
    const scheduler = new BackgroundScheduler(app.session, app.registry, app.tasks, {
      plan: async (cadence, after, now) => planned(cadence, after, now), refreshSources: async () => {},
      triageSources: async () => {}, collectResources: async () => {}, indexHistory: async () => {},
      runJob: (occurrence, signal, actor) => runScheduled(occurrence, signal, app, actor),
    });
    try {
      const summary = "Current review: the exact candidates are Example One and Example Two. Their provider records were verified.";
      const state: TaskState = scenario === "waiting" ? { state: "waiting", actor: "user", next_check: new Date(clock.value + DAY).toISOString() }
        : { state: "resolved", evidence: "Verified the current provider records." };
      const notified = scenario === "preliminary notice" ? "A preliminary check is still running." : scenario === "already delivered" ? summary : undefined;
      app.faux.setResponses([
        ...(notified === undefined ? [] : [fauxAssistantMessage(fauxToolCall("notify_user", { text: notified }), { stopReason: "toolUse" })]),
        fauxAssistantMessage(fauxToolCall("record_task", { summary, state }), { stopReason: "toolUse" }),
        fauxAssistantMessage(""),
      ]);
      await monitor.start(); await scheduler.start({ poll_interval_seconds: 60, sources: false, jobs: [job] });
      const request = { type: "run_job", request_id: "empty-final", job_id: job.id, schedule_id: null } satisfies Parameters<BackgroundScheduler["runJob"]>[0];
      const receipt = await scheduler.runJob(request);
      await pump(async () => {
        const status = (await scheduler.getJob(job.id)).last_run?.state.status;
        return !app.tasks.isRunning(receipt.task_id) && (status === "completed" || status === "retrying");
      });
      const task = await app.tasks.get(receipt.task_id), conversation = await app.tasks.conversation(task.conversation_id);
      const original = await app.session.commit(tx => tx.submissionByRequest(conversation.id, "manual:empty-final"), BACKGROUND_CONTEXT);
      assert.equal(original?.status, "done"); assert.deepEqual(task.state, state);
      clock.advance(30_001);
      if (state.state === "resolved") await pump(async () => (await monitor.status(job.id)).pending_work === 0);
      const result = await scheduler.getJob(job.id);
      assert.equal(result.last_run?.state.status, "completed", JSON.stringify(result.last_run?.state));
      const root = await app.session.root(BACKGROUND_CONTEXT);
      const notices = (await root.entries({ order: "ascending" }, 100, undefined, BACKGROUND_CONTEXT)).items.filter(UserNoticeEntry.is);
      const texts = notices.map(entry => entry.data.text);
      assert.deepEqual(texts, notified !== undefined && notified !== summary ? [notified, summary] : [summary]);
      const notice = notices.find(entry => entry.data.text === summary); assert.ok(notice);
      const source = (await conversation.entries({}, 100, undefined, BACKGROUND_CONTEXT)).items.find(entry => entry.id.toString() === notice.data.source_entry_id);
      assert.ok(source?.model?.some(message => message.role === "assistant" && message.content.some(part =>
        part.type === "toolCall" && part.name === (notified === summary ? "notify_user" : "record_task"))));
      assert.deepEqual(await scheduler.runJob(request), receipt);
      assert.equal((await scheduler.status()).pending, 0);
      assert.equal(app.faux.state.callCount, notified === undefined ? 2 : 3);
    } finally { await app.close(); await rm(directory, { recursive: true, force: true }); }
  });
}

for (const scenario of ["open progress", "resolved then reopened", "unrecorded metadata", "failed record"]) {
  test(`an empty manual final cannot use ${scenario} as a recorded result`, async () => {
    const directory = await mkdtemp(join(tmpdir(), "agent-manual-unverified-result-")), app = await openScheduled(directory);
    try {
      const summary = "A summary that is not a completed current-run result.", state: TaskState = { state: "resolved", evidence: "Earlier metadata evidence." };
      if (scenario === "unrecorded metadata" || scenario === "failed record") {
        app.faux.setResponses([async () => {
          const task = (await app.tasks.list()).find(task => task.workflow !== undefined); assert.ok(task);
          await app.tasks.update(task.id, task => { task.summary = summary; task.state = state; });
          return scenario === "unrecorded metadata" ? fauxAssistantMessage("")
            : fauxAssistantMessage(fauxToolCall("record_task", { summary, state: { state: "waiting", actor: "provider", next_check: "invalid-date" } }), { stopReason: "toolUse" });
        }, fauxAssistantMessage("")]);
      } else {
        app.faux.setResponses([
          ...(scenario !== "resolved then reopened" ? [] : [fauxAssistantMessage(fauxToolCall("record_task", { summary, state }), { stopReason: "toolUse" })]),
          fauxAssistantMessage(fauxToolCall("record_task", { summary, state: { state: "open" } }), { stopReason: "toolUse" }),
          fauxAssistantMessage(""),
        ]);
      }
      const occurrence = { id: "manual:unverified-result", job: exampleJob(), schedule_id: "daily", scheduled_at: START, manual: {} };
      await assert.rejects(runScheduled(occurrence, new AbortController().signal, app), /empty result/);
      const root = await app.session.root(BACKGROUND_CONTEXT);
      assert.deepEqual((await root.entries({}, 100, undefined, BACKGROUND_CONTEXT)).items.filter(UserNoticeEntry.is), []);
      assert.equal(app.faux.state.callCount, scenario === "unrecorded metadata" ? 1 : scenario === "resolved then reopened" ? 3 : 2);
    } finally { await app.close(); await rm(directory, { recursive: true, force: true }); }
  });
}

test("a fulfilled manual result stays bound to its original outcome across later owned inputs and reopen", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-manual-fulfilled-result-")); let app = await openScheduled(directory);
  try {
    const summary = "The original recorded review has no new candidates.", state: TaskState = { state: "resolved", evidence: "Verified the exact current records." };
    const occurrence = { id: "manual:fulfilled-result", job: exampleJob(), schedule_id: "daily", scheduled_at: START, manual: {} };
    const responses = (summary: string) => [fauxAssistantMessage(fauxToolCall("record_task", { summary, state }), { stopReason: "toolUse" }), fauxAssistantMessage("")];
    const notices = async () => (await (await app.session.root(BACKGROUND_CONTEXT)).entries({ order: "ascending" }, 100, undefined, BACKGROUND_CONTEXT)).items.filter(UserNoticeEntry.is);
    app.faux.setResponses(responses(summary));
    await runScheduled(occurrence, new AbortController().signal, app);
    const task = (await app.tasks.list()).find(task => task.workflow !== undefined); assert.ok(task);
    await pump(() => !app.tasks.isRunning(task.id));
    const original = await notices(); assert.equal(original.length, 1);
    for (const later of [summary, "The later owned check found a different current result."]) {
      await app.tasks.enqueue(task.id, "Recheck this retained work without republishing the original manual result.", "later:" + later, undefined, "follow_up");
      app.faux.setResponses(responses(later)); await app.tasks.runBackground(task.id);
      const beforeCalls = app.faux.state.callCount;
      await runScheduled(occurrence, new AbortController().signal, app, task.id);
      assert.deepEqual(await notices(), original, "Replaying the original obligation must retain its first actual notice and provenance");
      assert.equal(app.faux.state.callCount, beforeCalls);
    }
    await app.close(); app = await openScheduled(directory);
    await runScheduled(occurrence, new AbortController().signal, app, task.id);
    assert.deepEqual(await notices(), original);
    assert.equal(app.faux.state.callCount, 0);
  } finally { await app.close(); await rm(directory, { recursive: true, force: true }); }
});

test("an interrupted manual publication retains its selected result before later work changes the actor", async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-manual-selected-result-")); let app = await openScheduled(directory);
  try {
    const summary = "The original verified result is ready.", state: TaskState = { state: "resolved", evidence: "Verified the original records." };
    const occurrence = { id: "manual:selected-result", job: exampleJob(), schedule_id: "daily", scheduled_at: START, manual: {} };
    const responses = (summary: string) => [fauxAssistantMessage(fauxToolCall("record_task", { summary, state }), { stopReason: "toolUse" }), fauxAssistantMessage("")];
    const original = app.session, getRoot = original.root.bind(original), root = await getRoot(BACKGROUND_CONTEXT), submit = root.submit.bind(root);
    original.root = async () => root;
    root.submit = new Proxy(submit, { async apply(target, thisArg: unknown, args: unknown[]) {
      const receipt: unknown = await Reflect.apply(target, thisArg, args), draft = args[0];
      if (typeof draft === "object" && draft !== null && "type" in draft && draft.type === "write" &&
          "entry" in draft && typeof draft.entry === "object" && draft.entry !== null && "kind" in draft.entry &&
          draft.entry.kind === UserNoticeEntry.kind) await original.close(BACKGROUND_CONTEXT);
      return receipt;
    } });
    app.faux.setResponses(responses(summary));
    try { await assert.rejects(runScheduled(occurrence, new AbortController().signal, app), /closed|closing/i); }
    finally { original.root = getRoot; root.submit = submit; }
    await app.close(); app = await openScheduled(directory);
    const task = (await app.tasks.list()).find(task => task.workflow !== undefined); assert.ok(task);
    await app.tasks.enqueue(task.id, "Continue later owned work.", "later-owned-input", undefined, "follow_up");
    app.faux.setResponses(responses("The later work has a different verified result.")); await app.tasks.runBackground(task.id);
    await runScheduled(occurrence, new AbortController().signal, app, task.id);
    const restored = await app.session.root(BACKGROUND_CONTEXT);
    const notices = (await restored.entries({ order: "ascending" }, 100, undefined, BACKGROUND_CONTEXT)).items.filter(UserNoticeEntry.is);
    assert.deepEqual(notices.map(entry => entry.data.text), [summary]);
    assert.equal(app.faux.state.callCount, 2, "Retrying the retained publication cannot run another model turn");
  } finally { await app.close(); await rm(directory, { recursive: true, force: true }); }
});


test("interrupting a workflow waiting for capacity withdraws its input before effects can start", { timeout: 20_000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), "agent-workflow-admission-stop-"));
  const app = await openScheduled(directory), job = exampleJob();
  let release = () => {}; const hold = new Promise<void>(resolve => { release = resolve; });
  let starts = 0;
  const attached = new Set<string>();
  app.faux.setResponses(Array.from({ length: 9 }, () => async () => { starts++; return fauxAssistantMessage("Finished current work"); }));
  const running: Promise<void>[] = [];
  try {
    const actors = [];
    for (let index = 0; index < 9; index++) {
      const task = await app.tasks.create({ title: "Current work " + index, outcome: "Complete current evidence", model: job.model,
        execution: "coordinator", workflow: { job, owner_task_id: "workflow-owner", work_key: "parcel:" + index } });
      await app.tasks.enqueue(task.id, "Process this parcel", "parcel-input:" + index); actors.push(task);
    }
    app.tasks.guardResources({ attach: async id => { attached.add(id); await hold; } });
    for (const actor of actors.slice(0, 8)) running.push(app.tasks.runBackground(actor.id));
    await pump(() => attached.size === 8);
    const queued = actors[8]; assert.ok(queued);
    const ninth = app.tasks.runBackground(queued.id); const stopped = assert.rejects(ninth, /abort/u);
    await app.tasks.get(queued.id);
    assert.equal(await app.tasks.interruptBackground(queued.conversation_id), true); await stopped;
    const saved = await app.tasks.get(queued.id); assert.equal(saved.pending.length, 0);
    const conversation = await app.tasks.conversation(saved.conversation_id);
    const original = await app.session.commit(tx => tx.submissionByRequest(conversation.id, "parcel-input:8"), BACKGROUND_CONTEXT);
    assert.ok(original?.type === "input" && original.status === "unanswered" && original.reason === "aborted");
    release(); await Promise.all(running); assert.equal(starts, 8, "Stopped capacity waiters must never start a provider request");
  } finally { release(); await app.close(); await Promise.allSettled(running); await rm(directory, { recursive: true, force: true }); }
});
