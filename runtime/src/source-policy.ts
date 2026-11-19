import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { defineDoc, defineDocFamily, type Tx } from "@earendil-works/pi-durable";
import type { SourceAttention } from "./generated/SourceAttention.js";
import type { SourceEvent } from "./generated/SourceEvent.js";
import type { SourceKey } from "./generated/SourceKey.js";
import type { SourcePolicyDecision } from "./generated/SourcePolicyDecision.js";
import type { SourceRecord } from "./source-index.js";
import { sourceEntityId } from "./source-index.js";
import { SourceMetrics, SourceStatistics } from "./source-metrics.js";
import { sourceWorkResources } from "./source-work.js";
import type { SourceJournal } from "./sources.js";

type Hint = { attention: SourceAttention; title: string };
const Channels = defineDocFamily<{ hint: Hint | null }, null>({ kind: "agent.source.slack.attention", scope: "session", family: true,
  version: 1, initial: () => ({ hint: null }), checkpointWhen: () => true });
const Cursor = defineDoc<{ after_source: string | null; positions: Record<string, string | null> }>({
  kind: "agent.source.slack.policy", scope: "session", version: 1,
  initial: () => ({ after_source: null, positions: {} }), checkpointWhen: () => true });

export function sourceStatisticsKey(source: SourceKey): string { return JSON.stringify([source.provider, source.account]); }
function channelKey(event: SourceEvent): string | undefined {
  if (event.source.provider !== "slack") return undefined;
  // This is the native adapter's channel:thread identity, not a channel-type heuristic.
  const channel = /^([A-Z][A-Z0-9]+):[0-9]+\.[0-9]+$/u.exec(event.source.thread_id)?.[1];
  return channel === undefined ? undefined : JSON.stringify([event.source.account, channel]);
}

/** Conversation metadata is scheduling evidence, never part of immutable content.
 * A positive direct-message observation permanently protects its history. */
export async function rememberSlackAttentionIn(tx: Tx, event: SourceEvent): Promise<void> {
  if (event.attention === undefined || event.attention === "mention") return;
  const key = channelKey(event); if (key === undefined) return;
  const state = await tx.doc(Channels, key, null);
  if (state.hint?.attention === "direct") return;
  if (state.hint?.attention === event.attention && state.hint.title === event.title) return;
  state.hint = { attention: event.attention, title: event.title };
}

export type SourceNoisePolicyOptions = { maximumAgeMs?: number; maximumPending?: number; batchSize?: number };

/** Only pending review work expires. Originals, task ownership and native model
 * receipts are retained, and every skipped version gets an explicit decision. */
export class SourceNoisePolicy {
  private readonly age: number;
  private readonly maximum: number;
  private readonly batch: number;
  constructor(private readonly journal: SourceJournal, options: SourceNoisePolicyOptions = {}) {
    this.age = options.maximumAgeMs ?? 24 * 60 * 60_000;
    this.maximum = options.maximumPending ?? 5000;
    this.batch = options.batchSize ?? 64;
    if (![this.age, this.maximum, this.batch].every(value => Number.isSafeInteger(value) && value > 0) || this.batch > 256) {
      throw new Error("Slack policy limits must be positive safe integers, with at most 256 records per batch");
    }
  }
  private async ordinaryIn(tx: Tx, record: SourceRecord): Promise<boolean> {
    const event = record.event;
    if (event.source.provider !== "slack" || /<[@!]/u.test(event.text)) return false;
    const key = channelKey(event), hint = key === undefined ? null : (await tx.doc(Channels, key, null)).hint;
    if (hint?.attention === "direct" || hint?.attention === "protected") return false;
    const attention = record.attention ?? event.attention;
    if (attention !== undefined) return attention === "ordinary";
    if (key === undefined || /^Slack: mpdm-/u.test(event.title)) return false;
    // Public-channel evidence may classify legacy records with the same native
    // identity and name. Renamed/converted, private and unknown history stays protected.
    return hint?.attention === "ordinary" && hint.title === event.title;
  }
  async sweep(now = Date.now(), owns?: (tx: Tx) => Promise<boolean>): Promise<{ scanned: number; expired: number; coalesced: number; more: boolean }> {
    await this.journal.prepare();
    await this.journal.tasks.prepare();
    if (await this.journal.legacyActive()) return { scanned: 0, expired: 0, coalesced: 0, more: false };
    const sources = (await this.journal.states()).filter(state => state.source.provider === "slack")
      .map(state => state.source).sort((a, b) => {
        const left = sourceStatisticsKey(a), right = sourceStatisticsKey(b);
        return left < right ? -1 : left > right ? 1 : 0;
      });
    if (sources.length === 0) return { scanned: 0, expired: 0, coalesced: 0, more: false };
    return this.journal.session.commit(async tx => {
      if (owns !== undefined && !await owns(tx)) return { scanned: 0, expired: 0, coalesced: 0, more: false };
      const cursor = await tx.doc(Cursor);
      const source = sources.find(source => sourceStatisticsKey(source) > (cursor.after_source ?? "")) ?? sources[0];
      if (source === undefined) throw new Error("Slack policy source disappeared");
      const key = sourceStatisticsKey(source); cursor.after_source = key;
      const counts = await this.journal.index.countsIn(tx, source), after = cursor.positions[key];
      const rows = await this.journal.index.streamPendingIn(tx, source, { limit: this.batch, ...(after == null ? {} : { after }) });
      let scanned = 0, expired = 0, coalesced = 0, bytes = 0;
      for (const item of rows) {
        const record = await this.journal.sourceRecordIn(tx, item.key);
        if (record === undefined) throw new Error("Slack policy original is unavailable");
        const size = Buffer.byteLength(JSON.stringify(record.event));
        if (scanned > 0 && bytes + size > 512 * 1024) break;
        scanned++; bytes += size; cursor.positions[key] = item.order;
        const occurred = record.occurred_at ?? record.event.occurred_at;
        if (record.decision !== null || occurred === undefined || !Number.isSafeInteger(occurred) || occurred <= 0 || occurred > now ||
            !await this.ordinaryIn(tx, record)) continue;
        const entity = await this.journal.index.entityIn(tx, sourceEntityId(record));
        if (entity === undefined || entity.claim_id !== null || entity.owner_task_id !== null ||
            (await this.journal.tasks.sourceLinksIn(tx, [record.event.source], 1)).length > 0) continue;
        // Resource ownership may also come from a manual task association.
        // The journal's routing index cannot prove those URLs untracked.
        if (sourceWorkResources(record.event).length > 0) continue;
        const age = now - occurred;
        let policy: SourcePolicyDecision | undefined;
        if (age >= this.age || counts.pending - expired - coalesced > this.maximum && age >= 15 * 60_000) policy = { reason: "expired", at: now };
        else if (age >= 60_000) {
          const newest = (await this.journal.index.pendingIn(tx, sourceEntityId(record), { reverse: true, limit: 1 }))[0];
          if (newest !== undefined && newest.key !== record.key) policy = { reason: "coalesced", at: now, replacement: newest.key };
        }
        if (policy === undefined) continue;
        await this.journal.decideBatchIn(tx, [record.key], { type: "skipped", policy });
        const metrics = await tx.doc(SourceMetrics), stats = await tx.doc(SourceStatistics, key, null);
        if (policy.reason === "expired") { expired++; metrics.expired_events = (metrics.expired_events ?? 0) + 1; stats.expired_events++; }
        else { coalesced++; metrics.coalesced_events = (metrics.coalesced_events ?? 0) + 1; stats.coalesced_events++; }
      }
      const more = rows.length === this.batch || scanned < rows.length;
      if (!more) cursor.positions[key] = null;
      return { scanned, expired, coalesced, more };
    }, BACKGROUND_CONTEXT);
  }
}
