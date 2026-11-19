import { defineDoc } from "@earendil-works/pi-durable";
import type { SourcePipelineMetrics } from "./generated/SourcePipelineMetrics.js";

export function emptySourceMetrics(): SourcePipelineMetrics {
  return { admitted_events: 0, decided_events: 0, claimed_events: 0, completed_reviews: 0, failed_reviews: 0,
    input_bytes: 0, claim_milliseconds: 0, review_milliseconds: 0 };
}

/** Only aggregate counters and elapsed times belong in diagnostics. */
export const SourceMetrics = defineDoc<SourcePipelineMetrics>({ kind: "agent.source.metrics", scope: "session", version: 1,
  initial: emptySourceMetrics, checkpointWhen: () => true });
