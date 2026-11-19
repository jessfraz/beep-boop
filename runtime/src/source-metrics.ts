import { defineDoc, defineDocFamily } from "@earendil-works/pi-durable";
import type { SourcePipelineMetrics } from "./generated/SourcePipelineMetrics.js";
import type { SourceReviewStatistics } from "./generated/SourceReviewStatistics.js";

export function emptySourceMetrics(): SourcePipelineMetrics {
  return { admitted_events: 0, decided_events: 0, claimed_events: 0, completed_reviews: 0, failed_reviews: 0,
    input_bytes: 0, claim_milliseconds: 0, review_milliseconds: 0, expired_events: 0, coalesced_events: 0 };
}

export function emptyReviewStatistics(): SourceReviewStatistics {
  return { reviewed_events: 0, routed_events: 0, notified_events: 0, completed_reviews: 0, expired_events: 0, coalesced_events: 0,
    input_tokens: 0, output_tokens: 0, cache_read_tokens: 0, review_milliseconds: 0,
    sampled_model_milliseconds: 0, sampled_model_requests: 0, complete_entry_timing_samples: 0 };
}

export const SourceStatistics = defineDocFamily<SourceReviewStatistics, null>({ kind: "agent.source.review.statistics",
  scope: "session", family: true, version: 1, initial: emptyReviewStatistics, checkpointWhen: () => true });

/** Only aggregate counters and elapsed times belong in diagnostics. */
export const SourceMetrics = defineDoc<SourcePipelineMetrics>({ kind: "agent.source.metrics", scope: "session", version: 1,
  initial: emptySourceMetrics, checkpointWhen: () => true });
