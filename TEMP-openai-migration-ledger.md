# OpenAI migration ledger

Application: Beep Boop. Implementation authorized: upgrade upstream Pi, move
all agent workloads from GPT-6.1 Sol to GPT-6.2 Sol, deploy and observe actual
spam and shipment completion. Preserve ChatGPT sign-in and existing API-key
ownership rules, local durable history, tool permissions and reasoning settings.
No direct provider rewrite or new credential is required.

The user subsequently made spam and shipments the two critical workflows:
isolate independent work, preserve email evidence and prior decisions, recover
interrupted work and report concrete blockers. Upgrade Pi first; qualify durable
compatibility before rollout, then validate actual external outcomes.

## Approved stages

1. Pi 1.6 compatibility and GPT-6.2 Sol defaults/catalog. Implemented.
2. Fresh workflow execution, isolated critical work, CLI spam inspection and
   durable progress monitoring. Implemented with ownership and receipt fences.
3. Package qualification, managed rollout, saved-model migration and live
   provider verification are authorized release gates. Receipts are retained
   privately on the coordinator, outside repository and public package caches.

## Capability assessment

Every heading in the migration guidance was reviewed. Existing integration is
upstream Pi's OpenAI Responses provider. Optional API redesigns are excluded by
the user's instruction to upgrade Pi first and observe; none is silently added.

| Capability | Applicable/support | Decision and evidence |
| --- | --- | --- |
| Model selection | Yes/yes | GPT-6.2 Sol explicitly requested; Pi 1.6 catalog contains it. Validate real requests and saved selections. |
| Instruction file naming | Yes/yes | Preserve runtime-loaded AGENTS.md; no copies or renames. |
| Responses migration | Already supported | Retain upstream Responses route and other configured providers. |
| Streaming | Yes/yes | Preserve Pi streaming, tool events and client rendering; run existing tests. |
| WebSocket mode | Potential/unknown | No transport redesign in this upgrade; current streaming remains. |
| Zero Data Retention | Existing account policy | Do not infer ZDR or change storage behavior; preserve current provider options and local durable records. |
| Data residency | No change | No endpoint, project, account or jurisdiction changes. |
| Processing tiers | Yes/yes | Preserve selected standard/fast policy; do not enable additional tiers. |
| Prompt cache keys | Yes/upstream | Preserve upstream cache ownership and existing keys; add no key fragmentation. |
| Extended cache retention | No new requirement | Preserve upstream/provider behavior; no retention extension. |
| Explicit cache breakpoints | Potential/unknown | Pi owns serialization; no independent request rewriting for this scope. |
| Cache diagnostics | Potential/unknown | Do not add new response-ID tracking in this upgrade. Existing usage accounting remains. |
| State management | Yes/yes | Pi owns complete durable conversations and tool receipts. Qualify old-state migration and restart recovery. |
| Instruction persistence | Yes/yes | Existing runtime refresh remains; verify provider serialization after upgrade. |
| Encrypted reasoning items | Yes/upstream | Preserve native typed history and tool-call ordering; no summary substitution. |
| Reasoning effort | Yes/yes | Preserve existing efforts; 6.2 supports configured levels. Measure latency before selecting a different effort. |
| Reasoning preservation | Yes/upstream | Review 6.2 prefix/model-change behavior; preserve complete upstream items. |
| Reasoning summaries/preambles | Yes/upstream | Preserve current rendering and native event semantics. |
| Verbosity | No change | Preserve existing provider and user choices. |
| Output tokens | No change | Preserve limits; no invented cap. |
| Assistant message phases | Yes/upstream | Existing typed events and tool loop must keep working. |
| Context compaction | Yes/upstream | Preserve local/native compaction; no new compaction strategy. |
| Structured outputs | Yes/yes | Keep generated Rust schemas and upstream tool validation. |
| Tool choice | Yes/yes | Preserve permissions; spam explicitly uses configured Gmail CLI. |
| Tool search/additional tools | No new requirement | Existing tool discovery remains. |
| Parallel tools | Yes/upstream | Preserve shared-state boundaries; critical per-item work is separately reviewed stage 2. |
| Programmatic tool calling | Existing execution only | No new provider program route. |
| Model-aligned interfaces | Existing interfaces | Retain CLI/tools; no tool redesign merely for model selection. |

## Validation and completion

Pi compatibility, fresh-run isolation, critical provider admission, interrupted
input recovery, notification ordering and progress monitoring have targeted
behavioral coverage and independent lifecycle review. Exact regressions cover
queue interruption, stale recovery fencing and retained legacy monitoring.
The full Pi 1.4 merge-base suite cannot run unchanged against Pi 1.6 APIs;
compatible fresh-run and monitor regressions have explicit fail-first evidence.
Native monitoring SQL uses bounded indexed lookups on representative data.

The release also requires the Makefile checks, exact-commit platform CI/package
publication, a consistent private database backup with its original application,
Pi 1.6 qualification on a disposable copy, managed activation, saved-model
readback and real Gmail/Home Assistant outcomes. Successful local handling is
verified through provider readback; deployment is recorded only after live
activation and recovery checks.

Official model reference: https://developers.openai.com/api/docs/models/gpt-6.2-sol
Official migration reference: https://developers.openai.com/api/docs/guides/latest-model
