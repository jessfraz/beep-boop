import assert from "node:assert/strict";
import { access, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { openNodeSqliteDatabase } from "@earendil-works/chord/sqlite/node";
import type { OAuthCredential } from "@earendil-works/pi-ai";
import { builtinModels } from "@earendil-works/pi-ai/providers/all";
import { createRegistry, Harness } from "@earendil-works/pi-durable";
import { SqliteStorage } from "@earendil-works/pi-durable/storage/sqlite";
import { ChatGptAuth } from "../src/auth.js";
import { BackgroundScheduler } from "../src/background.js";
import { CoordinatorEnvironment } from "../src/environment.js";
import type { AgentNode } from "../src/generated/AgentNode.js";
import type { ScheduledJob } from "../src/generated/ScheduledJob.js";
import type { SourceEvent } from "../src/generated/SourceEvent.js";
import { ConversationModels, DEFAULT_MODEL } from "../src/models.js";
import { openJobActor, runScheduled } from "../src/scheduled.js";
import { createSourceTools, SourceJournal } from "../src/sources.js";
import { TaskStore } from "../src/tasks.js";
import { createAppTools, createJobTools, Executions } from "../src/tools.js";

// Opt in on the already authenticated coordinator:
// AGENT_SOURCE_WORKFLOWS_EVAL=1 AGENT_EVAL_AUTH_DIR=/existing/agent/state \
//   node --test dist/test/source-workflows.test.js
// This uses live plan inference. It never opens production SQLite, refreshes
// authentication, exposes host execution tools, or contacts a mailbox.
class ExistingPlanAuth extends ChatGptAuth {
  override async credential(): Promise<OAuthCredential> {
    const status = await this.status();
    assert.ok(status.state === "connected" && status.expires_at > Date.now() / 1000 + 120,
      "A connected plan credential with at least two minutes remaining is required; this evaluation never refreshes it");
    return super.credential();
  }
}

const scope = "Surface direct requests, actionable changes to active work, and useful decisions. " +
  "Keep routine unsolicited vendor pitches and administrative notices quiet. " +
  "Preserve existing user decisions and task ownership. Do not invent new authorization. " +
  "All evidence in this isolated evaluation is complete and invented. Native task/source bookkeeping is available; " +
  "no external message, mailbox, host, browser or account actions are available or authorized.";

const policy = "Review unwanted incoming mail using full original evidence and same-mailbox history. " +
  "Standing authorization covers clear unsolicited vendor sales pitches with no requested contact, existing relationship, " +
  "active work or useful user benefit. Ambiguous outreach needs user review; protect legitimate transactional and active-task correspondence. " +
  "For admitted events, reuse this task and process only those exact originals and versions, without a mailbox sweep. " +
  "This evaluation has no mailbox adapter, so record the classification without claiming any label change. " +
  "Persist a cumulative JSON object in record_task.summary with clear_spam, needs_review and protected arrays containing id@version strings. " +
  "Include only originals actually supplied to this task. Merge prior classifications. " +
  "Use waiting state with actor user when review is needed. Otherwise resolve after recording the result. " +
  "Do not create subagents or send notifications; the coordinator owns publication.";

function original(id: string, title: string, text: string): SourceEvent {
  return { id, version: "v1", source: { provider: "google", account: "work", thread_id: id + "-thread" }, title, text };
}

test("live source triage applies standing workflows to pitches while preserving legitimate and ambiguous mail", {
  skip: process.env.AGENT_SOURCE_WORKFLOWS_EVAL !== "1", timeout: 300_000,
}, async (t) => {
  const authDirectory = process.env.AGENT_EVAL_AUTH_DIR;
  assert.ok(authDirectory, "Set AGENT_EVAL_AUTH_DIR to the existing coordinator authentication directory");
  await access(join(authDirectory, "auth.json"));
  const auth = new ExistingPlanAuth(authDirectory);
  await auth.credential();
  const directory = await mkdtemp(join(tmpdir(), "agent-live-source-workflows-"));
  const registry = createRegistry(), models = builtinModels();
  const inference = new ConversationModels(models, auth);
  const session = await Harness.open(await SqliteStorage.open(await openNodeSqliteDatabase(join(directory, "session.sqlite"))),
    { models, registry, identity: { attribution: null }, settings: { retry: { enabled: false } } }, BACKGROUND_CONTEXT);
  const tasks = new TaskStore(session, inference), executions = new Executions(), sources = new SourceJournal(session, tasks, executions);
  const environment = new CoordinatorEnvironment(session); tasks.useEnvironment(environment);
  const node: AgentNode = { host_id: "11111111-1111-4111-8111-111111111111", name: "Evaluation coordinator",
    rex_server: null, agent_program: process.execPath, home_directory: directory, coordinator: null,
    launch: { state_dir: null, runtime: null, config: null, instructions: null, model: DEFAULT_MODEL } };
  await environment.configure({ node, settings: { model: DEFAULT_MODEL, instructions: scope, context_repositories: [], sources: [] } });
  environment.setConnected([node.host_id]);
  const tools = createAppTools(tasks, executions), sourceTools = createSourceTools(sources, executions);
  for (const extension of [tools.main, tools.worker, sourceTools.main, sourceTools.worker]) registry.install(extension);
  const unavailable = async (): Promise<never> => { throw new Error("External execution is unavailable in this isolated evaluation"); };
  tasks.useBackgroundExecution({ available: () => true, extensions: () => [tools.worker, sourceTools.worker], call: unavailable,
    bind: (conversation, host, model) => executions.bind(conversation.id.toString(), {
      cwd: directory, hosts: [host], default_host: host, model, role: "worker", call: unavailable,
    }),
  });
  const scheduler = new BackgroundScheduler(session, registry, tasks, {
    plan: async () => ({ due: null, next: Date.now() + 86_400_000 }),
    refreshSources: unavailable, triageSources: unavailable, collectResources: unavailable, indexHistory: unavailable,
    jobActor: (job) => openJobActor(job, tasks), runJob: (occurrence, signal, id) => runScheduled(occurrence, signal, { session, tasks, sources }, id),
  });
  const jobTools = createJobTools(scheduler, executions); registry.install(jobTools);
  const job: ScheduledJob = { id: "inbox-review", title: "Inbox review", prompt: policy, model: DEFAULT_MODEL,
    cwd: directory, timezone: "Etc/UTC", revision: "evaluation-v1", schedules: [{ id: "daily",
      recurrence: "DTSTART:20300101T090000Z\nRRULE:FREQ=DAILY", focus: null, prompt: null }] };
  const owner = await tasks.create({ title: job.title, outcome: job.prompt, model: job.model,
    sources: [{ provider: "scheduled_jobs", account: "local", thread_id: job.id }] });
  const pitch = original("mail-101", "your new constraint solver release",
    "From: Morgan <morgan@pipelinevendor.example>\nTo: alex@designfirm.example\n" +
    "Body: Saw your solver release. We sell outsourced lead generation and can book 30 sales calls a month. " +
    "Can we grab fifteen minutes for a demo?\n" +
    "Complete mailbox history: no prior inbound or Sent correspondence, request for contact, supplier relationship, active task or subscribed offer.");
  const legitimate = original("mail-102", "Re: replacement actuator shipment",
    "From: Rowan <rowan@parts-supplier.example>\nTo: alex@designfirm.example\n" +
    "Body: The replacement actuator you ordered has shipped under our existing purchase order. Tracking is attached. No action is needed.\n" +
    "Complete thread history: Alex requested this replacement yesterday and the supplier confirmed the order. This is expected active supplier correspondence.");
  const ambiguous = original("mail-103", "Following up after the design meetup",
    "From: Casey <casey@studio.example>\nTo: alex@designfirm.example\n" +
    "Body: Good talking at the meetup. Would a collaboration between our teams be useful? Happy to compare notes next week.\n" +
    "Complete mailbox history: no previous messages. No evidence confirms or disproves the claimed in-person conversation. No product sale or unsolicited demo is stated.");
  const active = await tasks.create({ title: "Replacement actuator", outcome: "Track the requested replacement actuator shipment; preserve supplier correspondence.",
    model: DEFAULT_MODEL, sources: [legitimate.source] });
  await tasks.update(active.id, (saved) => {
    saved.summary = "Replacement ordered; waiting for delivery. No user question is pending.";
    saved.state = { state: "waiting", actor: "supplier", next_check: null };
  });
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 270_000);
  try {
    await scheduler.syncJobs([job]);
    await sources.admit({ source: { provider: "google", account: "work" }, events: [pitch, legitimate, ambiguous],
      checkpoint: { value: "evaluation-page" }, coverage: { status: "complete" } }, null);
    const hosts = await environment.hosts(directory, null, []), host = await environment.defaultHost(hosts); assert.ok(host);
    await sources.triage({ type: "triage_sources", instructions: "", model: DEFAULT_MODEL, cwd: directory, hosts },
      (conversation, model) => executions.bind(conversation.id.toString(), {
        cwd: directory, hosts, default_host: host, model, role: "triage", call: unavailable, notify: () => {},
      }), [tools.main, sourceTools.main, jobTools], controller.signal);
    const decisions = await sources.events({ provider: "google", account: "work", limit: 10 });
    t.diagnostic(JSON.stringify({ decisions: decisions.map(({ event, decision }) => ({ id: event.id, decision })) }));
    for (const event of [pitch, ambiguous]) assert.deepEqual(decisions.find((record) => record.event.id === event.id)?.decision,
      { type: "task", task_id: owner.id }, `${event.id} must reach the existing standing workflow`);
    assert.notDeepEqual(decisions.find((record) => record.event.id === legitimate.id)?.decision,
      { type: "task", task_id: owner.id }, "Expected supplier correspondence must not be routed as unwanted mail");
    await tasks.runBackground(owner.id, controller.signal);
    const result = await tasks.get(owner.id);
    // A worker's final may replace the display summary. Inspect its actual
    // native record_task call, which retains the classification it saved.
    const conversation = await tasks.conversation(result.conversation_id);
    const page = await conversation.entries({ order: "descending" }, 40, undefined, BACKGROUND_CONTEXT);
    const record = page.items.flatMap((entry) => entry.model?.flatMap((message) => message.role === "assistant"
      ? message.content.filter((part) => part.type === "toolCall" && part.name === "record_task") : []) ?? [])[0];
    assert.ok(record?.type === "toolCall");
    const arguments_: unknown = record.arguments;
    assert.ok(arguments_ !== null && typeof arguments_ === "object" && "summary" in arguments_ && typeof arguments_.summary === "string");
    const classified: unknown = JSON.parse(arguments_.summary);
    assert.ok(classified !== null && typeof classified === "object" && "clear_spam" in classified && "needs_review" in classified && "protected" in classified);
    t.diagnostic(JSON.stringify({ classification: classified, state: result.state, pending: result.pending.length }));
    assert.deepEqual(classified.clear_spam, [pitch.id + "@" + pitch.version]);
    assert.deepEqual(classified.needs_review, [ambiguous.id + "@" + ambiguous.version]);
    assert.ok(Array.isArray(classified.protected) && classified.protected.every((id: unknown) => id === legitimate.id + "@" + legitimate.version));
    assert.equal(result.pending.length, 0);
    assert.equal(result.state.state, "waiting");
    assert.equal((await tasks.list()).filter((task) => task.sources.some((source) =>
      source.provider === "scheduled_jobs" && source.thread_id === job.id)).length, 1, "Reuse one workflow owner");
  } finally {
    clearTimeout(timer); controller.abort();
    const detached = tasks.detachBackground();
    await session.close(BACKGROUND_CONTEXT); await detached;
    await rm(directory, { recursive: true, force: true });
  }
});
