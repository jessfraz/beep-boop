import { randomUUID } from "node:crypto";
import { copyJson, type JsonRepresentation, type JsonValue } from "@earendil-works/chord";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { createAssistantMessageEventStream, type Api, type AssistantMessage, type AssistantMessageEvent, type DeferredFetchOptions,
  type DeferredHandle, type Message, type Model, type MutableModels, type Provider, type ProviderRequestOptions,
  type ProviderResponse, type SimpleStreamOptions, type TranscriptContext, type Usage } from "@earendil-works/pi-ai";
import { normalizeContext } from "@earendil-works/pi-ai/utils/transcript";
import { isRetryableAssistantError } from "@earendil-works/pi-ai/utils/retry";
import { defineDoc, defineDocFamily, defineEntry, ROOT_CONVERSATION_ID, type Harness, type SetTimer, type Tx } from "@earendil-works/pi-durable";
import { UserNoticeEntry } from "./transcript.js";

type SavedOptions = Pick<SimpleStreamOptions, "reasoning" | "maxTokens" | "temperature" | "sessionId" | "transport" | "cacheRetention" | "toolChoice" | "thinkingBudgets" | "timeoutMs" | "deferred">;
type PendingRequest = { account: string; provider: string; model: string } & (
  { phase: "pending"; messages: JsonRepresentation<Message>[]; options: JsonRepresentation<SavedOptions>; usage: JsonRepresentation<Usage> }
  | { phase: "complete"; result: JsonRepresentation<AssistantMessage> });
type Probe = { owner: string; token: string; until: number };
type Cooldown = { attempt: number; next: number; probe: Probe | null; epoch: number };
type QuotaState = { accounts: Record<string, Cooldown> };
const State = defineDoc<QuotaState>({ kind: "agent.quota", scope: "session", version: 1,
  initial: () => ({ accounts: {} }), checkpointWhen: () => true });
const Pending = defineDocFamily<{ request: PendingRequest | null }, null>({ kind: "agent.quota_request", scope: "session", family: true,
  version: 1, initial: () => ({ request: null }), checkpointWhen: () => true });
const Notice = defineEntry<{ provider: string; paused: boolean }>("agent.quota_state");
const PREFIX = "beep-boop:quota:", BASE = 300_000, LIMIT = 3_600_000, PROBE_LEASE = 90_000;
export type QuotaClock = { now?: () => number; random?: () => number; setTimer?: SetTimer };
const zeroUsage = (): Usage => ({ input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0,
  cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } });
export function isQuotaExhaustion(message: string | undefined): boolean {
  return /\b(?:subscription_sharing_usage_limit_exceeded|subscription_sharing_workplace_individual_usage_limit_exceeded|insufficient_quota)\b/u.test(message ?? "");
}
function sum(left: Usage, right: Usage): Usage {
  return { input: left.input + right.input, output: left.output + right.output, cacheRead: left.cacheRead + right.cacheRead,
    cacheWrite: left.cacheWrite + right.cacheWrite, totalTokens: left.totalTokens + right.totalTokens,
    ...(left.reasoning === undefined && right.reasoning === undefined ? {} : { reasoning: (left.reasoning ?? 0) + (right.reasoning ?? 0) }),
    ...(left.cacheWrite1h === undefined && right.cacheWrite1h === undefined ? {} : { cacheWrite1h: (left.cacheWrite1h ?? 0) + (right.cacheWrite1h ?? 0) }),
    cost: { input: left.cost.input + right.cost.input, output: left.cost.output + right.cost.output,
      cacheRead: left.cost.cacheRead + right.cost.cacheRead, cacheWrite: left.cost.cacheWrite + right.cost.cacheWrite,
      total: left.cost.total + right.cost.total } };
}
/** The input is already Pi's typed transcript; only strict-JSON ownership changes here. */
function storedMessages(messages: AssistantMessage[]): JsonRepresentation<AssistantMessage>[];
function storedMessages(messages: Message[]): JsonRepresentation<Message>[];
function storedMessages(messages: Message[]): JsonRepresentation<Message>[] {
  const value = copyJson(messages, { omitUndefinedProperties: true });
  const array = (copy: JsonValue): copy is JsonRepresentation<Message>[] => Array.isArray(copy);
  if (!array(value)) throw new Error("Pi transcript must be a message array");
  return value;
}
function safeOptions(options: SimpleStreamOptions): SavedOptions {
  const { reasoning, maxTokens, temperature, sessionId, transport, cacheRetention, toolChoice, thinkingBudgets, timeoutMs, deferred } = options;
  return { ...(reasoning === undefined ? {} : { reasoning }), ...(maxTokens === undefined ? {} : { maxTokens }),
    ...(temperature === undefined ? {} : { temperature }), ...(sessionId === undefined ? {} : { sessionId }),
    ...(transport === undefined ? {} : { transport }), ...(cacheRetention === undefined ? {} : { cacheRetention }),
    ...(toolChoice === undefined ? {} : { toolChoice }), ...(thinkingBudgets === undefined ? {} : { thinkingBudgets }),
    ...(timeoutMs === undefined ? {} : { timeoutMs }), ...(deferred === undefined ? {} : { deferred }) };
}
function deadline(value: string, now: number): number {
  const number = Number(value);
  if (Number.isFinite(number) && number > 0) return number >= 1e12 ? number : number >= 1e9 ? number * 1000 : now + number * 1000;
  const duration = [...value.matchAll(/(\d+(?:\.\d+)?)(ms|s|m|h|d)/gu)];
  if (duration.length) return now + duration.reduce((total, match) => total + Number(match[1]) *
    (match[2] === "ms" ? 1 : match[2] === "s" ? 1000 : match[2] === "m" ? 60_000 : match[2] === "h" ? 3_600_000 : 86_400_000), 0);
  const date = Date.parse(value); return Number.isFinite(date) ? date : 0;
}
function resetAt(response: ProviderResponse | undefined, error: string | undefined, now: number): number {
  let next = 0;
  for (const [name, value] of Object.entries(response?.headers ?? {})) {
    if (/^(?:retry-after|x-ratelimit-reset(?:-requests|-tokens)?)$/iu.test(name)) next = Math.max(next, deadline(value, now));
    if (name.toLowerCase() === "retry-after-ms") next = Math.max(next, now + (Number(value) || 0));
  }
  for (const match of (error ?? "").matchAll(/\b(?:reset_at|resets_at|retry_at)["']?\s*[:=]\s*["']?([\d.T:+-]+Z?)/gu)) {
    if (match[1]) next = Math.max(next, deadline(match[1], now));
  }
  return next;
}

/** Quota waits stay inside Pi's deferred generation, preserving its input and completed tool rounds. */
export class QuotaRecovery {
  private readonly now: () => number;
  private readonly random: () => number;
  private readonly probes = new Map<string, string>();
  constructor(private readonly models: MutableModels, private readonly session: Promise<Harness>,
    private readonly account: (model: Model<Api>, options: ProviderRequestOptions) => Promise<string>, private readonly clock: QuotaClock = {}) {
    this.now = clock.now ?? Date.now; this.random = clock.random ?? Math.random;
  }
  owns(handle: DeferredHandle | undefined): handle is DeferredHandle { return handle?.id.startsWith(PREFIX) === true; }
  private async notice(tx: Tx, provider: string, paused: boolean): Promise<void> {
    const entry = await tx.appendEntry(Notice, ROOT_CONVERSATION_ID, { data: { provider, paused } });
    await tx.appendEntry(UserNoticeEntry, ROOT_CONVERSATION_ID, { data: { task_id: null, source_conversation_id: ROOT_CONVERSATION_ID.toString(),
      source_entry_id: entry.id.toString(), text: paused
        ? "Model usage is exhausted. Pending work is saved; availability will be checked quietly and work will resume automatically."
        : "Model capacity is available again. Retained work is resuming automatically." } });
  }
  private deferred(model: Model<Api>, id: string, next: number): AssistantMessage {
    return { role: "assistant", content: [], provider: model.provider, model: model.id, api: model.api,
      usage: zeroUsage(), stopReason: "deferred", timestamp: this.now(),
      deferred: { provider: model.provider, modelId: model.id, api: model.api, id, pollAfterMs: Math.max(1000, Math.min(60_000, next - this.now())) } };
  }
  private stream(model: Model<Api>, run: (emit: (event: AssistantMessageEvent) => void) => Promise<AssistantMessage>, options: ProviderRequestOptions) {
    const output = createAssistantMessageEventStream(); let started = false;
    const emit = (event: AssistantMessageEvent) => { if (event.type === "start") started = true; output.push(event); };
    void run(emit).then(message => {
      if (message.stopReason === "pending") throw new Error("Model response did not finish");
      if (!started && message.stopReason !== "error" && message.stopReason !== "aborted") emit({ type: "start", partial: { ...message, content: [], stopReason: "pending" } });
      output.push(message.stopReason === "error" || message.stopReason === "aborted"
        ? { type: "error", reason: message.stopReason, error: message } : { type: "done", reason: message.stopReason, message }); output.end();
    }).catch(() => {
      const message: AssistantMessage = { role: "assistant", content: [], provider: model.provider, model: model.id, api: model.api,
        usage: zeroUsage(), timestamp: this.now(), stopReason: options.signal?.aborted ? "aborted" : "error",
        errorMessage: options.signal?.aborted ? "Model request aborted" : "Could not read retained model availability state; work is preserved." };
      output.push({ type: "error", reason: message.stopReason === "aborted" ? "aborted" : "error", error: message }); output.end();
    });
    return output;
  }
  wrapProvider(provider: Provider): Provider {
    return { ...provider,
      streamSimple: (model, context, options = {}) => {
        const id = PREFIX + randomUUID();
        return this.stream(model, emit => this.request(provider, model, id, context, options, emit), options);
      },
      fetchDeferred: (model, handle, options = {}) => this.owns(handle)
        ? this.stream(model, emit => this.poll(provider, model, handle, options, emit), options)
        : provider.fetchDeferred?.(model, handle, options) ?? this.stream(model, async () => { throw new Error("Unsupported provider deferred response"); }, options),
      cancelDeferred: async (model, handle, options) => {
        if (this.owns(handle)) await this.cancel(handle.id);
        else await provider.cancelDeferred?.(model, handle, options);
      },
    };
  }
  private async cancel(id: string, remove = true): Promise<void> {
    const session = await this.session;
    if ((await session.snapshot(Pending, id, BACKGROUND_CONTEXT))?.request == null) return;
    await session.commit(async tx => {
      const state = await tx.doc(State), slot = await tx.doc(Pending, id, null), pending = slot.request;
      if (pending !== null) {
        const cooldown = state.accounts[pending.account];
        if (cooldown?.probe?.owner === id) {
          if (this.probes.get(pending.account) === cooldown.probe.token) this.probes.delete(pending.account);
          cooldown.probe = null;
        }
        if (remove) slot.request = null;
      }
    }, BACKGROUND_CONTEXT);
  }
  private async poll(provider: Provider, model: Model<Api>, handle: DeferredHandle, options: DeferredFetchOptions, emit: (event: AssistantMessageEvent) => void): Promise<AssistantMessage> {
    const pending = (await (await this.session).snapshot(Pending, handle.id, BACKGROUND_CONTEXT))?.request;
    if (pending == null || pending.provider !== model.provider || pending.model !== model.id) throw new Error("Retained quota request is unavailable");
    if (pending.phase === "complete") return pending.result;
    return this.request(provider, model, handle.id, normalizeContext({ messages: pending.messages }), { ...pending.options, ...options }, emit, true);
  }
  private async request(provider: Provider, model: Model<Api>, id: string, context: TranscriptContext, options: SimpleStreamOptions, emit: (event: AssistantMessageEvent) => void, retained = false): Promise<AssistantMessage> {
    options.signal?.throwIfAborted();
    const session = await this.session, account = await this.account(model, options);
    await session.root(BACKGROUND_CONTEXT);
    const permit = await session.commit(async tx => {
      const state = await tx.doc(State), cooldown = state.accounts[account];
      if (retained) {
        const previous = (await tx.doc(Pending, id, null)).request;
        if (previous?.phase === "complete") return { type: "cached", message: previous.result };
        if (previous !== null) previous.account = account;
      }
      if (cooldown === undefined || cooldown.next === 0) return { type: "normal", epoch: cooldown?.epoch ?? 0 };
      (await tx.doc(Pending, id, null)).request ??= { phase: "pending", account, provider: model.provider, model: model.id, messages: storedMessages(context.messages), options: safeOptions(options), usage: zeroUsage() };
      const activeProbe = cooldown.probe !== null && this.probes.get(account) === cooldown.probe.token;
      if (cooldown.next > this.now() || cooldown.probe !== null && (cooldown.probe.until > this.now() || activeProbe)) {
        return { type: "wait", next: Math.max(cooldown.next, cooldown.probe?.until ?? 0, activeProbe ? this.now() + 60_000 : 0) };
      }
      const token = randomUUID(); cooldown.probe = { owner: id, token, until: this.now() + PROBE_LEASE };
      return { type: "probe", token };
    }, BACKGROUND_CONTEXT);
    if (permit.type === "cached" && permit.message !== undefined) return permit.message;
    if (permit.type === "wait") return this.deferred(model, id, permit.next ?? this.now() + BASE);
    if (permit.type === "probe" && permit.token !== undefined) this.probes.set(account, permit.token);
    let response: ProviderResponse | undefined;
    let message: AssistantMessage;
    try {
      const stream = provider.streamSimple(model, context, { ...options,
        onResponse: async (value, actual) => { response = value; await options.onResponse?.(value, actual); } });
      for await (const event of stream) if (event.type !== "done" && event.type !== "error") emit(event);
      message = await stream.result();
    } catch (error) {
      if (this.probes.get(account) === permit.token) this.probes.delete(account);
      await this.cancel(id, false); throw error;
    }
    if (options.signal?.aborted) { await this.cancel(id, false); options.signal.throwIfAborted(); }
    return session.commit(async (tx): Promise<AssistantMessage> => {
      const state = await tx.doc(State), cooldown = state.accounts[account];
      const slot = retained || permit.type === "probe" ? await tx.doc(Pending, id, null) : undefined, previous = slot?.request ?? undefined;
      const accumulated = sum(previous?.phase === "pending" ? previous.usage : zeroUsage(), message.usage);
      if (permit.type === "probe" && previous === undefined) return { ...message, content: [], stopReason: "aborted", errorMessage: "Quota request cancelled" };
      if (permit.type === "normal" && permit.epoch !== (cooldown?.epoch ?? 0) && isQuotaExhaustion(message.errorMessage)) {
        (await tx.doc(Pending, id, null)).request = { phase: "pending", account, provider: model.provider, model: model.id, messages: storedMessages(context.messages), options: safeOptions(options), usage: accumulated };
        return this.deferred(model, id, cooldown?.next ?? this.now());
      }
      if (permit.type === "probe" && cooldown?.probe?.token !== permit.token) {
        if (previous?.phase === "pending") previous.usage = accumulated;
        return this.deferred(model, id, cooldown?.next ?? this.now());
      }
      if (isQuotaExhaustion(message.errorMessage) && message.stopReason === "error" || permit.type === "probe" &&
          (isRetryableAssistantError(message) || message.stopReason === "aborted")) {
        if (cooldown === undefined || cooldown.next === 0 || permit.type === "probe") {
          const attempt = (cooldown?.attempt ?? 0) + 1;
          const wait = Math.min(LIMIT, BASE * 2 ** Math.min(attempt - 1, 4));
          const next = Math.max(this.now() + Math.min(LIMIT, wait * (1 + this.random() * 0.1)), resetAt(response, message.errorMessage, this.now()));
          state.accounts[account] = { attempt, next, probe: null, epoch: (cooldown?.epoch ?? 0) + 1 };
          if (cooldown === undefined || cooldown.next === 0) await this.notice(tx, model.provider, true);
        }
        (await tx.doc(Pending, id, null)).request = { phase: "pending", account, provider: model.provider, model: model.id, messages: storedMessages(context.messages), options: safeOptions(options), usage: accumulated };
        return this.deferred(model, id, state.accounts[account]?.next ?? this.now() + BASE);
      }
      if (permit.type === "probe" && cooldown !== undefined) {
        cooldown.probe = null;
        if (message.stopReason !== "error" && message.stopReason !== "aborted") {
          state.accounts[account] = { attempt: 0, next: 0, probe: null, epoch: cooldown.epoch + 1 }; await this.notice(tx, model.provider, false);
        } else cooldown.next = this.now() + BASE;
      }
      const result = { ...message, usage: accumulated };
      if (slot !== undefined) {
        const saved = storedMessages([result])[0];
        if (saved === undefined) throw new Error("The completed model response was unavailable");
        // Pi classifies this result in its next commit. Keep a replayable receipt
        // across that boundary while discarding the retained input transcript.
        slot.request = { phase: "complete", account, provider: model.provider, model: model.id, result: saved };
      }
      return result;
    }, BACKGROUND_CONTEXT).finally(() => {
      if (this.probes.get(account) === permit.token) this.probes.delete(account);
    });
  }
  /** Native compaction uses completeSimple, whose caller cannot consume deferred handles. */
  wrapComplete(): void {
    const complete = this.models.completeSimple.bind(this.models);
    this.models.completeSimple = async (model, context, options) => {
      let message = await complete(model, context, options);
      try {
        while (this.owns(message.deferred) && message.stopReason === "deferred") {
          const handle = message.deferred;
          await this.wait(handle.pollAfterMs ?? 60_000, options?.signal);
          message = await this.models.fetchDeferred(model, handle, { ...(options?.signal === undefined ? {} : { signal: options.signal }),
            ...(options?.telemetry === undefined ? {} : { telemetry: options.telemetry }) });
        }
        return message;
      } catch (error) { if (this.owns(message.deferred) && options?.signal?.aborted) await this.cancel(message.deferred.id); throw error; }
    };
  }
  private wait(delay: number, signal?: AbortSignal): Promise<void> {
    return new Promise((resolve, reject) => {
      signal?.throwIfAborted();
      const finished = () => { signal?.removeEventListener("abort", abort); resolve(); };
      const timer = this.clock.setTimer?.(finished, delay, { background: false });
      const timeout = timer === undefined ? setTimeout(finished, delay) : undefined;
      const abort = () => { timer?.(); if (timeout !== undefined) clearTimeout(timeout); signal?.removeEventListener("abort", abort); reject(new Error("Model request aborted")); };
      signal?.addEventListener("abort", abort, { once: true });
    });
  }
}
