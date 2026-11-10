# CLI performance

The separate **CLI performance** CI job compares a change with its pull request
base or previous main commit on the same runner. It builds both in release mode,
alternates baseline and candidate launches, and saves every timing in JSON.

The clock starts before launching the real CLI through the existing PTY fixture.
It stops when a sentence from persisted conversation history appears in the
terminal's visible cells. A `vt100` screen parser applies cursor movement,
clearing and wrapping. A menu, composer, ANSI-stripped byte match, or text already
scrolled offscreen does not satisfy the measurement.

Current scenarios open the chief's 40-message history with cold and warm
coordinators, and resume a saved 5,000-message conversation. The corpus also
contains 3,000 unrelated saved discussions. All use real Pi/SQLite storage,
the coordinator, native history pagination and the regular TUI. Each version
runs its own Rust
coordinator and TypeScript runtime. Seeding and coordinator startup
are outside the timed interval. Every CLI launch is fresh. The cold scenario
also starts a fresh service for each sample; other scenarios retain theirs.
The first pair warms the services and is recorded but excluded from the gate.
The isolated environment exposes only Node on PATH, and provider requests are
rejected. No model, credentials, network provider or Rex is involved. `/resume`
opens saved history and `/exit` closes each sample; no model prompt is submitted.

Seven measured pairs run per scenario. Each candidate gets the greater of a
25% or 20-millisecond allowance over its paired baseline. CI fails when the
median fraction of that budget exceeds 1. Pairing prevents uneven runner
stalls from turning separate medians into a false regression.
There are no automatic retries or discarded slow samples. A missing conversation
or failed CLI also fails the job. `performance/results.json` contains the raw
samples and thresholds. A JSONL file retains completed samples if a later run
fails; a failure JSON records the error.

Run locally after building both revisions' runtime and CLI:

```sh
nix develop --command cargo build --release --bin beep-boop \
  --example terminal-fixture --example cli-performance
nix develop --command target/release/examples/cli-performance \
  --baseline /path/to/base/target/release/beep-boop \
  --baseline-runtime /path/to/base/runtime/dist/src/bridge.js \
  --candidate target/release/beep-boop \
  --candidate-runtime runtime/dist/src/bridge.js \
  --terminal target/release/examples/terminal-fixture \
  --seed runtime/dist/test/fixtures/performance.js \
  --output performance/results.json
```

Add a scenario to the typed `Scenario` enum in `examples/cli_performance.rs` and
seed its synthetic retained data in `runtime/test/fixtures/performance.ts`.
The paired sampling and regression gate apply to every enum variant. Keep
conversation text synthetic and test the real path the scenario is measuring.

The same job measures native search and cached indexing against 65,536
file-backed chunks with 1,024-dimensional synthetic vectors. Dense retrieval,
full-text retrieval with hydration, and vector-cache reuse have separate paired
measurements. Seeding, candidate preparation and inference stay outside the
clock; each sample checks the returned rankings, hydrated text and provenance
against the other revision. Query samples contain eight lookups; cached-index
samples contain four updates. The same
paired-budget gate applies. Both reports use schema 2 and retain the raw
medians alongside the paired budget score. Native results are in
`performance/search.json` and JSONL.

Run that comparison after building both runtimes:

```sh
nix develop .#ci --command node runtime/dist/test/fixtures/search-performance.js \
  --baseline /path/to/base/runtime/dist/src/search.js \
  --candidate runtime/dist/src/search.js \
  --output performance/search.json
```

Add native scenarios to `runtime/test/fixtures/search-performance.ts`. Keep
setup outside timing and verify the actual result as well as its latency.
These synthetic measurements complement live corpus latency and retrieval
recall checks.

Live qualification against 1.35 million published chunks measured 112 ms for
keyword-only retrieval and 12.8 seconds for an exact dense scan. Two concurrent
CLI searches took 11–18 seconds end to end; their query embeddings took
617–653 ms. A retained-history page loaded in 0.8 seconds. These are observations
under ongoing background work, not CI thresholds.

Explicit keyword mode still runs dense retrieval for tie-breaking and fallback.
Automatic chat context uses keyword-only retrieval on its separate reader, so
it does not wait for that scan.

Large projections now scan compact binary vectors for 4,096 candidates and
rerank their original float vectors. The compact table is derived in bounded
publisher transactions from retained embeddings; absent or incomplete tables
retain exact retrieval. Smaller projections keep the contiguous exact scan,
which outperformed scattered candidate hydration in the paired fixture.
Candidate preparation starts at 32,768 chunks; retrieval switches at 4 GiB of
published float vectors.

Private qualification compared the complete reader against the exact reader on
20 retained retrieval cases and representative queries at 1.35 million chunks.
All returned rankings, hydrated matches, provenance and coverage were identical,
including both hardware routing queries. The 20-case run measured median
reader time of 8.0 seconds for exact scans and 2.9 seconds with candidates.
A later hardware pair under heavier I/O measured 9.1–14.7 seconds exact versus
4.8–5.8 seconds with candidates. Native process counters in a separate hardware
pair measured 6.4–6.9 GB read for exact retrieval versus 1.1–1.7 GB with
candidates. Query inference is outside these reader timings.

Binary retrieval remains approximate; these cases establish measured recall,
not a guarantee for every future query.
The paired CI corpus exercises candidate preparation and publication while
retaining exact retrieval at its smaller size. A bounded native regression
also runs the production binary ranking and float reranking function against
retained SQLite vectors, comparing separated semantic matches with exact
search and checking original text, provenance and deletion.

## Durable task and source state

Task-history traversal yields to transport and timers before each bounded page.
Exhaustive searches and history reads retain their full results while unrelated
requests can proceed. The bridge regression checks response ordering against
real retained Pi history, without replacing storage or the runtime.

Tasks and source events use keyed Pi documents. Small control records retain
source cursors and triage ownership; indexed references locate task owners and
related event versions without decoding unrelated retained bodies. Pi still
owns the SQLite schema and atomic commit boundary.

Startup migrates legacy documents in batches of at most 32 records or 4 MiB,
retaining the exact old records. Each batch is independently durable and resumes
after interruption. A record larger than the budget owns its own batch.
The native insertion result distinguishes new records from existing ones, so
fresh records skip redundant index scans. Already handled originals do not
create pending-work indexes.
Migrated startup checks only the completion markers. The runtime regressions
close Pi during both migrations, reopen the file, and verify ownership,
versions, decisions and relinking. Cold point reads and hot admission also have
byte budgets against actual native SQLite reads and writes in CI.

The reusable growth fixture measures first migration, warm reopen, cold point
reads and repeated task/source admission at increasing retained sizes:

```sh
nix develop .#ci --command node runtime/dist/test/fixtures/state-growth.js
```

Set `AGENT_STATE_COUNTS`, `AGENT_STATE_SOURCE_BYTES`, `AGENT_STATE_TASK_BYTES`,
`AGENT_STATE_RUNS` and `AGENT_STATE_REPORT` to qualify a larger synthetic corpus
and retain JSON. Setup stays outside admission timing. The fixture records the
actual SQL emitted by Pi and runs `EXPLAIN QUERY PLAN` with its real parameters.
Point lookups use `documents_by_address`; document materialization and writes
use the existing document-revision indexes and primary keys. No SQLite schema
changes or extra indexes are required.

Local qualification with 4,096 tasks, 366 MiB of source text and 8 MiB of task
summaries measured:

| Operation | Legacy documents | Keyed documents |
| --- | ---: | ---: |
| Cold task and source-status read | 385 ms | 0.49 ms |
| Task admission, median | 86.7 ms | 5.1 ms |
| Task admission, p95 | 124.8 ms | 11.1 ms |
| Source admission, median | 12.4 ms | 4.9 ms |
| First readiness, including migration | 394 ms | 5,717 ms |

The one-time migration trades startup work for bounded normal reads and writes.
Subsequent Harness reopen took 0.93 ms, followed by the 0.49 ms point read.
These local observations are not CI latency thresholds. The CI regressions
verify bounded materialization and persistence, not machine-specific timings.
Migration cost depends on record count as well as retained bytes. Qualify a
consistent snapshot before activation and allow a maintenance window for the
first open. Later starts read only the completion markers.
