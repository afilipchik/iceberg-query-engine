# Prepared aggregate keys — September 8, 2026

Production grouped aggregation now prepares canonical keys once for shared routing
and state lookup when a bounded admission succeeds. This follows the
[component measurement](retained-key-component-2026-09-08.md); no complete-query
performance gain has yet been measured for this implementation.

`physical/morsel_agg/prepared_keys.rs` owns flat KeyRows and cached hashes and
borrows the exact evaluated RecordBatch. Private validation checks the combined
layout identity, batch address, group width and row extent before a consumer can
use it. The safe borrow prevents destruction/mutation of the batch during use;
an equivalent cloned batch is deliberately not interchangeable. Every group
lookup still compares full canonical bytes under the bound key layout.

Preparation uses a child pool limited to one eighth of currently available query
memory. Byte/offset/hash storage and encoding scratch use that child and therefore
the same parent query budget. This fraction is a headroom policy, not proof that
all downstream allocations fit. Each allocation retains the existing reservation
and old/new growth overlap checks. Only a typed admission refusal returns None;
partially prepared storage drops before ordinary routing. Invalid inputs and
other errors propagate. Fallback happens before routing/state changes and visits
already evaluated arrays; no source or volatile expression is replayed.

ParallelControllers retains the optional owner until every scoped worker stops.
Both routing and selected-row ingestion borrow the cached key/hash. GroupRows can
append a borrowed canonical KeyRef while preserving the existing key/state
transaction and exact first-unapplied-selection-position cursor. The prepared
owner remains valid across a worker's spill transitions. The single-controller
path remains the ordinary one-encoding path. Preparation time is included in the
existing routing span; it is not excluded from profiling or query timing.

Validation so far:

- First aggregate gate:125 passed, one existing explicitly selected-only component
  benchmark ignored,807 tests filtered,2.68s.
- Added exact bytes/NULL, duplicate key, batch/layout mismatch, empty-input,
  bounded refusal/cleanup, dictionary-codebook and NaN/signed-zero tests.
- Extended the independent decimal/count selection oracle to exercise prepared
  keys with duplicates/reversed selections in both actual-spill and non-spill
  cases. The original256KiB cases remain; prepared cases use1MiB and explicitly
  assert preparation succeeds. These larger cases do not replace low-budget gates.
- The test-only drop-order compile error is preserved in focused-02.log; corrected
  by dropping borrowed-key owners before their batches. Final focused gate:
  127 passed, zero failures, one ignored component benchmark,807 filtered,2.62s.

Broad process45262 is terminal101:994 library tests pass,11 ignored (ten existing
plus the explicit component benchmark). All14 selected expression, transition,
volatile-key and input-error tests pass. Native remains9pass/1fail: the existing
join requests383984bytes with used0 under262144bytes. No new failure is observed
in these gates. Formatting and whitespace checks pass. Release57568 completed
in8m43s with493 input hashes unchanged; frozen binary1a0ece71e7091ae4aa3db00036879773466ea82872c050e913d3d5c27b1d6c8b,
lance/gpu features,48GiB and one build job. Paired driver81974 is terminal1;
no heavy job remains active. Logs:
`.scratch/parallel-aggregate-input/prepared-key-*`. After the verified freeze,
run the same strict CPU-control and canonical paired measurements against3ff868c7.
Record low-budget completion,
spill behavior, protected regressions and provider/residency outcomes separately.
The full milestone and DuckDB leadership remain open.

Completed test evidence and the changed production modules are immutable under
`docs/benchmarks/2026-09-08-prepared-key-tests/`:14 files verified and493 source
inputs checked against the release-build input manifest. The release was still
active at archival time; this archive does not certify a finished binary.
Prepared comparisons are `measure_prepared_keys.py` (eight canonical SF10 queries)
and `measure_prepared_key_smoke.py` (custom memory-resident Q1/Q6), in repository
scratch. They require a valid reference and six measured samples per side before
computing a block ratio. Both launched sequentially under the48GiB wrapper.

Custom-memory comparison is terminal1. All64 completed engine outputs validate;
eight Q1 warmups exceed the fresh139.075–144.130ms ceilings, leaving48 dependent
measured samples not_run. Q6's four complete blocks produce ratio0.949883 with
95% paired-block interval0.888895–1.010787; no confirmed gain. This is a custom
float component, not canonical SF10 or GPU performance. No overall implementation
acceptance follows from the smoke run.

## Completed canonical paired result

Four fresh-process blocks per query, six measured pairs plus gated warmup,16
threads,4GiB query and12GiB process limits; before3ff868c7, after1a0ece71.
Startup and execution orders are independently balanced. Fresh DuckDB calibration
and typed oracle comparisons remain mandatory. Ratios below are geometric means
of complete block median ratios; intervals resample the four whole paired blocks.

| Canonical query | After / before |95% interval|
|---|---:|---:|
|Q1|0.99977|0.98918–1.00934|
|Q2|0.96357|0.91315–1.02070|
|Q5|1.01016|0.98960–1.04765|
|Q10|0.96030|0.94659–0.97461|
|Q19|0.98327|0.96388–0.99728|
|Q20|1.00510|0.97054–1.02750|

Q10 improves3.97% (interval2.54–5.34%); Q19 improves1.67% (0.27–3.61%). The other
four intervals include no change. No complete-query interval establishes a10%
regression in this component. Four blocks are limited evidence, not three
independent certification sessions. No valid ratio exists for Q12 or Q13.

All346 completed canonical engine outputs validate;339 also pass timing. Q12
records seven late completed requests and one timeout, stopping dependent samples;
Q13 records eight warmup timeouts across both binaries. There are93 not_run slots.
Do not collapse correct-but-late results, timeouts and dependent non-executions.
The shared cgroup peak is2,731,167,744bytes with max/oom/oom_kill all zero; the
earlier smoke snapshot is cumulative within this same scope, not per-query RSS.

Across the two distinct workloads, all410 completed engine outputs validate,
395 meet time gates,15 are late, nine time out and141 are not_run. Those totals
are bookkeeping, not a combined performance score. Preserve all original samples,
failure events, plans and source provenance under
`docs/benchmarks/2026-09-08-prepared-key-pairs/`.

The measured shared-code improvement is real within these comparison limits,
but is smaller than the standalone key-preparation saving. Full canonical/provider
screens, low-budget/concurrency gates, and the broader task007 acceptance remain
open. Next profile the remaining generic state-update cost and rerun the full
provider screens on this frozen candidate; do not claim the Q1/Q12/Q13 failures
are solved or reinterpret the component as DuckDB leadership.

Archive completion verified1,030 files and493 source inputs. Full provider screen
69779 completed: all22 canonical SF10 queries in raw Parquet, Native, Iceberg
and Lance, sequentially under48GiB with16 threads,4GiB query/12GiB process limits,
three samples and one session. It uses frozen1a0ece71 and the corrected reference-
retirement harness. Raw57/native60/Iceberg63/Lance57 valid pairs; all317 completed
measured/warmup outputs correct. No track completes. See
[provider evidence](prepared-key-provider-screen-2026-09-08.md). It is broader
validation, not a three-session certification run. The later calibration-stop
harness fix is separate and does not change these archived results.
