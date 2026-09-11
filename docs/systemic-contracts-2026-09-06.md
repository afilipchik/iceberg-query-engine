# Systemic contract hardening — 2026-09-06 UTC

This continues the approved realistic-benchmarks/DuckDB-leadership epic. Source
HEAD is 88849c4 with the preserved uncommitted audit, harness and prior correctness
work. This report distinguishes implemented contracts from outstanding certification.

## Reproduced failures and implementation

- Two new pre-fix regressions failed: CAST('bad' AS BIGINT) silently returned NULL;
  signed/UInt64 comparisons produced `[NULL, NULL, false]` instead of
  `[true, true, false]` at the i64/u64 boundaries. Before log:
  `.scratch/cast-contract-before.log`.
- `Expr::Cast` carries `CastMode::{Strict,Try}` through binding and rewrites.
  Display and native expression fingerprints preserve the policy. Shared conversions
  select Arrow's error mode for strict/implicit casts and nullable mode for TRY_CAST.
- Integer coercion represents both domains exactly, with Decimal128(20,0) for
  signed/UInt64 mixtures. CASE uses the same type contract and evaluates only chosen
  rows, avoiding errors and subquery work in untaken branches; simple CASE works.
- IN-list reduction uses Kleene OR. IN-subquery membership uses common-domain Arrow
  row encoding, preserves dictionary null values and observes empty-set identities.
  NOT IN no longer becomes an ordinary anti join. Positive IN decorrelation requires
  matching domains and an uncorrelated set. Correlated evaluation substitutes each
  outer row; unsupported nested grandparent references fail explicitly.
- `optimizer/properties.rs` derives ordinal keys from aggregation/DISTINCT and
  preserves them through identity projections and row-subset operations. Scan NDV
  cannot prove uniqueness. Group reduction no longer removes joins or defers joins
  using statistics. Eager LEFT COUNT requires proof on the actual left subtree;
  eager SUM rejects unsafe exact-arithmetic reassociation and nullable additive terms.
- Owned memory reservations enforce query/context/process ancestors transactionally.
  Failed growth leaves counters unchanged; drop/cancellation releases capacity.
  SQL calls have independent query IDs, peaks and spill counters. Reserved and locally
  observed peaks are distinct. This is admission infrastructure, not full ownership.

## Validation checkpoint

Every engine/build/test invocation used `scripts/claude-safe-build.sh`, repository
TMPDIR and finite cgroup memory. No commits or Rust dependency changes. The
repository scratch Python environment gained pinned PyIceberg for provider
conversion; its constraints and installation report are archived separately.

```bash
TMPDIR="$PWD/.scratch" SAFE_BUILD_MEM=48G SAFE_BUILD_JOBS=2 scripts/claude-safe-build.sh cargo test --locked --test cast_contract_tests --test membership_contract_tests --test semantic_proof_tests --test memory_reservation_contract --test systemic_numeric_tests --test partition_contract --test spill_tests --test sql_comprehensive
TMPDIR="$PWD/.scratch" PYTHONPATH="$PWD/scripts" SAFE_BUILD_MEM=4G scripts/claude-safe-build.sh .scratch/venv-lance/bin/python -m unittest discover -s scripts/benchmark/tests -v
```

Historical checkpoint logs: `.scratch/contracts-full-04.log` — 200 passed, zero ignored;
`.scratch/contracts-harness-02.log` — 40 passed. An earlier spill test adaptation
mistook the helper's ordering flag for a spill-assertion flag; corrected by retaining
the NOT IN result comparison separately. The NOT EXISTS test still asserts real spill.
Library gate then passed 498 tests with one pre-existing ignored disabled-rule test. After dense admission and physical API guards, `.scratch/contracts-resource-gate.log` passes 504 library + 206 integration tests, with the same one ignored test. Optional-feature validation remains pending.

Canonical SF10 preparation completed under a 16 GiB cap at
`.scratch/public-bench/tpch-sf10/dataset.json`. The fresh canonical SF1 run passed 66 paired samples at 2.305574× suite time.
The canonical SF10 run passed 660 paired samples across three sessions at
2.784506× suite time and 2.640705× geometric mean, with no time-gate failures or
OOM events. Full [SF10 evidence](benchmarks/2026-09-06-canonical-sf10/README.md)
is preserved; the measured snapshot predates the dense admission/API guards below.

## Outstanding work and acceptance boundaries

- Operator metadata, morsel accumulators, spill scratch, queues, caches and returned
  results still need reservation ownership. Retained Arrow slices must charge backing
  allocations, not just logical lengths. A guard on QueryResult alone is insufficient:
  its public batches can be moved or cloned and outlive it.
- Existing aggregate finalization can retain other partitions and outputs while
  processing a subpartition; skew and readback require recursive handling or named
  refusal. Do not equate observation telemetry with query-wide enforcement.
- Float equality still follows existing Arrow total-order behavior. Shared signed-zero
  and NaN semantics, complete nested correlation, all consumption boundaries and
  CAST/TRY_CAST conversion-domain coverage remain follow-up work.
- Packed group/join-key rules now use structural/type/predicate proofs rather
  than name-based footer bounds. New provider paths still require a trusted
  lineage contract before becoming eligible; their performance impact needs a
  separate baseline from the decimal experiment below.
- The provider harness additions require actual conversion and execution validation;
  accepting a CLI track is not provider certification. IPC and GPU require matched
  residency/control evidence. Full JOB/ClickBench, SF100, layout, cap/concurrency and
  three-session leadership gates remain required.
- The shared decimal accessor passed its isolated acceptance gate: Q1 improved
  20.9% in the alternating screen and its full measured snapshot reached 2.711019×
  suite time. Later source changes require separate evidence; this is not overall
  DuckDB leadership. Keep correctness, resource completion and speed independent.

## Subsequent resource and physical API checkpoint

Dense morsel aggregation now reserves its presence bitmap, fixed-width atomic
accumulators and vector headers before allocation, preserving the reservation until
those buffers are destroyed. Six tests cover admission, concurrent domains, error and
cancellation release, exact results and planning-time CTE propagation. Perfect/hash
states, provider allocations and copied results are still outside this boundary.

The public experimental DelimJoin execution is quarantined with NotImplemented
before consuming children: its hash-only equality, NULL/cardinality behavior and
partition omission require a shared exact dependent-join implementation. The SQL
flattening rule was already disabled. VectorSearch checks for one global fallback
partition before any provider/index work. Six API-boundary tests pass; see the
[partition audit](partition-boundary-audit-2026-09-05.md).

The first instrumented profile shows Q1/Q6 scan+process dominates merge (tens of
microseconds), while Q18 spends roughly 550–580 ms on merging 15M worker-local groups.
Existing counters did not isolate per-query scan, expressions and state updates, so
opt-in worker-time counters were added. They are process-global diagnostics for
serialized queries, never concurrent per-query accounting.

The second profile (`.scratch/public-bench/tpch-sf10-contracts-profile-02`) separates
summed worker time. Q1 median state/key handling is 7,017 ms, aggregate expressions
6,181 ms and scan/pushdown 5,058 ms. Q6 spends 5,698 ms in scan/pushdown and only
32 ms processing aggregation batches. Q18 spends 4,688 ms in state/key handling.
These are overlapping worker durations, not query wall times. A typed Decimal128
accessor experiment removes repeated type dispatch and scale-factor computation;
checked full-width sums, NULL handling and overflow remain mandatory. This was
the pre-measurement hypothesis; the accepted result is recorded below. The
accessor does not target Q6 scan costs.

Inspection also reproduced a separate correctness bug: the perfect-hash group
encoding mapped both signed `-1` and NULL to `u64::MAX`, merging two SQL groups.
The new regression failed with one output row instead of two. Exact hit validation
now covers the sentinel, with a passing regression for Int64, Int32 and Date32,
both insertion orders and separate batches. Logs:
`.scratch/null-sentinel-before.log`, `.scratch/null-sentinel-after.log`.

Real-source public extracts are prepared and readback-verified: JOB retains all
113 queries over 21 tables; ClickBench retains all 43 queries over one million
source rows. The eight initially recorded comparator gaps were subsequently
closed by complete-oracle typed slice validation. See
[public extraction evidence](benchmarks/2026-09-05-public-extracts/) and
`scripts/benchmark/PUBLIC_WORKLOADS.md`. These are development inputs, not full
workload certification.

## Completed decimal gate and subsequent grouping/proof fixes

The decimal-only measured snapshot passes another 660 paired SF10 samples:
suite 2.711019× DuckDB, geometric mean 2.601719×, no correctness/time failures or
OOM events. Q1 improves 20.9% against the saved alternating control; the suite's
engine median sum is 2.76% lower than the earlier canonical baseline, with no
query median regression over 10%. See the
[complete experiment](benchmarks/2026-09-06-decimal-aggregation/README.md).

After that snapshot, aggregation received explicit perfect-slot occupancy across
ingestion, stride changes, merging, draining and output. An all-NULL group exists
even when SUM sees no value or COUNT(value) remains zero. Dictionary accessors
now check selected dictionary-value validity, and small integer-range admission
uses checked subtraction with exact generic fallback on overflow. Six new unit
regressions cover those paths, including ordinary/combined dictionary handling.

Packed group/join rules now ignore statistics as semantic proofs. A shared helper
follows unambiguous identity lineage, integer type domains and explicit exact
integer predicates. Computed projections, joins and unsupported boundaries decline
packing; grouped inputs also require proven non-NULL. Checked radix arithmetic
rejects overflow. Tests include misleading statistics, derived aliases, outer-join
NULL extension, safe renamed/filtered keys and inexact predicate rejection.

The earlier combined gate (`.scratch/contracts-final-gate.log`) passes **521 library
and 206 integration tests**, with one pre-existing ignored disabled-rule test.
That validates these later source changes, whose performance is not represented
by the decimal-only snapshot. Optional features and public query execution remain
separate follow-up gates. Iceberg's exact eight-table conversion/readback smoke
passes; [provider evidence](benchmarks/2026-09-06-provider-smoke/README.md) records
physical decimal re-encoding without changing source schemas or values.

## Public workload contract repairs

The first real-data development runs exposed additional shared failures:
JOB had 26 wrong answers and six aggregate type errors, plus four independent
DuckDB identifier-quoting failures. ClickBench had one wrong AVG result and
twelve capability failures. See [preserved findings](public-workload-findings-2026-09-06.md).

Four minimal pre-fix aggregate regressions all failed. Legacy MIN/MAX paths
skipped unsupported arrays or emitted sentinels for all-NULL input; Int16 AVG
updates could be skipped. Shared typed state now handles MIN/MAX and ordinary
AVG, including mixed DISTINCT plans. Batch-boundary dictionary/string normalization,
full-width integer output and exact temporal count reconstruction keep physical
encodings from changing values. Temporal grouping retains the original unit/timezone
and adjacent nanosecond values. Arbitrary unsupported aggregate domains still refuse.

ORDER BY aggregate expressions are collected into the Aggregate plan and rebound
to scalar output references, including hidden sort aggregates. LENGTH counts Unicode
characters; STRLEN/OCTET_LENGTH explicitly count UTF-8 bytes. EXTRACT uses Arrow's
typed temporal kernels for supported fields, with timezone handling and explicit
refusal for unsupported fields or out-of-range inputs.

The selected combined gate passes 741 tests, followed by the separately compiled
positive-spill extrema regression: **742 total Rust tests passed**, one pre-existing
ignored test. Logs are `.scratch/public-contract-full-gate.log` and
`.scratch/aggregate-extrema-spill.log`. That expanded Python harness passed
**65 tests** after dialect quoting, failure-metadata and REGEXP_REPLACE adaptation.

The subsequent JOB development rerun passes **113/113 queries with zero issues**,
using the quoted-alias JOB04 metadata. ClickBench's second development run passes
**41/43**: q29 hits the regex timing ceiling, and q43 retains a grouped ORDER BY
binding failure. These results supersede the first-run counts for that measured
snapshot; the original failures and source archives remain preserved.

Further source fixes and focused tests now cover grouped ORDER BY output
references and bounded batch-local REGEXP_REPLACE evaluation. Three arguments
retain global replacement with dollar capture syntax; four arguments select
explicit options with backslash captures (empty options replace the first match,
`g` replaces all). ClickBench04 adds empty options generically to three-argument
calls, preserving original SQL and applying the same equivalent translation to
both engines and oracle. The completed final combined gate
(`.scratch/public-contract-final-gate.log`) passes **748 Rust tests**, with one
pre-existing ignored test; this includes four regex, five grouped ORDER BY and
eight aggregate encoding regressions. The Lance/GPU release build completed
(`.scratch/public-contract-final-release.log`). Final raw-Parquet development
reruns in `.scratch/public-bench/final-contracts-01/` validate all **43 ClickBench
queries across 129 pairs and all 113 JOB queries across 339 pairs**, with zero
issues. ClickBench's engine/DuckDB suite ratio is **2.234947×**, geometric mean
**1.671372×**; JOB's are **1.675876×** and **1.421927×**. These completed
measurements supersede the intermediate 41/43 ClickBench result for this new
source snapshot. Original failures remain archived. They close the observed
development-query failures, not full workload, provider or latency leadership
certification. That three-session SF10 gate subsequently completed: **660 pairs
correct**, suite **2.801267×** DuckDB and geometric mean **2.636274×**. This is
the final pre-dictionary-helper snapshot, not the subsequent candidate score.

## Consolidated current checkpoint

The [public-contracts evidence](benchmarks/2026-09-06-public-contracts/README.md)
preserves the final pre-dictionary SF10 gate above, JOB's 339 correct pairs and
ClickBench's 129 correct pairs. Current source validation totals **750 Rust tests
passed**, one pre-existing ignored rule test. The complete Python harness passes
**75 tests with zero skips**, including the enabled local Lance regression and
the subsequent aligned exact-comparison fast path;
the default harness invocation without `BENCHMARK_TEST_LANCE=1` does not cover
that optional test. Earlier totals in this report identify historical gates.

Provider smoke remains distinct from performance certification. Native's repeat
returns all 66 answers correctly but fails eight time ceilings (Q7 three, Q10 two,
Q19 three). The [optimizer audit](optimizer-overhead-investigation-2026-09-06.md)
shows shared optimization overhead against a faster native DuckDB reference;
Q19's optimization cost is almost identical in IPC, while Q10 also changes join
shape and optimization cost. Repeated enumeration and fixed-point churn remain
unproven hypotheses. No query-specific timing exception was introduced.

The [Lance reference diagnosis](benchmarks/2026-09-06-lance-reference-avg/README.md)
reproduces decimal AVG truncation in the extension's pushed-down reference path.
The explicitly recorded provider-wide `disabled_optimizers='extension'` setting
retains direct Lance scans and normal DuckDB aggregation; SQL and tolerances
remain unchanged. With this reference correction all **66 pairs pass**. This
repairs reference behavior, not production engine decimal semantics or full-scale
Lance certification.

The final custom floating-point GPU smoke and fresh same-binary CPU control each
pass **40 pairs**; **all 40 GPU samples actually execute on device**. This does
not certify canonical decimal GPU coverage, cold upload or full provider SF10.

### Dictionary candidate: completed development adoption gate

The [dictionary grouping investigation](dictionary-grouping-investigation-2026-09-06.md)
records the implemented `normalize_morsel_group_array` helper, which preserves
Dictionary(Int32, Utf8) group arrays while aggregate inputs and other group
domains retain the shared checked normalizer. Source changes, focused tests and
the release build are complete. An alternating matched ten-pair screen shows
Q1 **11.883% faster** (1,032.760 to 910.036 ms); Q6/Q9/Q14/Q18 controls remain
within approximately ±2%. Only the shared morsel aggregation source differs
between those production snapshots, with identical optional features.

The [complete candidate evidence](benchmarks/2026-09-06-dictionary-preservation/README.md)
now passes **660 canonical SF10 pairs**, suite **2.763338112×** DuckDB and
geometric mean **2.61140432×**. Q1's median is 914.626 ms versus 1,057.773 ms
for the immediate control; no query median regresses more than 10% against that
control. JOB's 339 pairs and ClickBench's 129 pairs also pass. This completes
the dictionary change's measured development adoption gate; it does not erase
the older Q9 regression against earlier snapshots or establish leadership.

Candidate adapter smokes pass 66 pairs each for decoded IPC, Iceberg and Lance.
Native retains eight timing failures with all 66 answers correct. Candidate GPU
and same-binary CPU control each pass 40 pairs; **39 of the 40 GPU samples execute
on device**, distinct from the earlier snapshot's 40/40 device count above.

The harness subsequently added an aligned exact-comparison fast path, with
**75 tests passing and zero skips**. Its 60-million-row synthetic validation
probe took 3.161 seconds at 267 MiB peak RSS. This is harness-only comparison
throughput, not real dataset preparation evidence or an engine performance gain.
Full decoded-IPC SF10 conversion now passes all eight tables (86,586,082 rows)
in `.scratch/public-bench/canonical-sf10-providers-01`. The remaining old converter
was intentionally interrupted, with its incomplete native output preserved.
A separately tested exact reordered-bag backend passes all **83 harness tests
with zero skips**, including optional Lance and actual-spill checks. It validated
the full immutable lineitem output in 118.600 seconds including staging, with
actual spilling and no result approximation. See
[exact comparator evidence](benchmarks/2026-09-06-comparator-exact-bag/README.md).
Fresh native, Iceberg and Lance conversions are running in
`.scratch/public-bench/canonical-sf10-providers-02`; their completion and provider
latency scores are not claimed yet. Engine source and its binary remain unchanged.

### Proposed follow-up work, not implemented by this checkpoint

- [Memory ownership proposal](next-memory-ownership-2026-09-06.md): bound queued
  batches and preserve reservations with their owners; current local telemetry
  and process caps do not establish query-wide ownership.
- [Join-key investigation](join-key-regression-investigation-2026-09-06.md):
  attribute the changed composite-key path with exact tuple equality. Do not
  restore estimated statistics as semantic packing proofs.
- [Optimizer attribution proposal](optimizer-overhead-investigation-2026-09-06.md):
  measure statistics collection, rule execution and full-plan formatting on the
  actual statistics-aware path before changing convergence or enumeration.
- [Comparator scaling work](benchmark-comparator-scaling-2026-09-06.md): the
  aligned exact fast path above is implemented; broader scaling work must retain
  typed multiplicity and complete-oracle eligibility. Faster validation is not
  engine speedup.

Full public workloads, provider SF10/SF100, cap/concurrency/layout matrices and
three-session leadership remain separate unmet acceptance requirements.


### Full provider conversion and systemic IPC failure follow-up

All four full SF10 provider conversions now validate all eight tables and
86,586,082 rows against the source. The first full decoded-IPC timing run then
failed: one Q5 warmup allocator abort left ten following samples unvalidated.
The matrix was stopped; 220 comparisons passed, but no complete provider score
is accepted. A fresh standalone IPC worker reproduces the abort while raw
Parquet completes. Evidence is preserved in
[the full provider checkpoint](../docs/benchmarks/2026-09-06-canonical-provider-sf10/README.md).

The IPC plan joined suppliers and customers on their low-cardinality nation key
before applying the more selective order/lineitem relationships. MemoryTable
lacked column statistics, and inner hash joins collected matching rows before
yielding. Current source repairs both shared contracts: lazy cached column
statistics for costing, and pull-driven inner joins with 4,096 candidate pairs
per step and cancellation on stream drop. Legacy candidate vectors now refuse
excessive per-batch growth. Build-side and wide-payload ownership remain open.
Eager aggregation also requires structural/schema evidence and preserves scalar
multiplication before floating SUM, where moving it can change zero into NaN.

The combined selected Rust gate passes **765 tests**, with one pre-existing
ignored test; formatting and whitespace checks pass. The harness's latest gate
is **83 passed, zero skips**. Release rebuild and fresh same-source timing are
pending; these changes have no accepted performance score yet. Full provider,
public workload and resource acceptance remain open.


### Fixed release: standalone SF10 reproduction now passes

The final `lance,gpu` release build completed in 8m18s. At unchanged 40 GiB
query / 48 GiB process limits and 16 threads, fresh raw and IPC workers both
return the exact five-row Q5 answer: raw **545.533 ms**, IPC **645.253 ms**.
Both satisfy independently calibrated 10× DuckDB ceilings. These instrumented
single-query diagnostics are not suite performance estimates.

The new IPC plan applies orders/lineitem restrictions before the supplier and
customer relationships, removing the reproduced nation-key intermediate. The
whole diagnostic scope peaks at **17,900,060,672 bytes**, with zero OOM events;
this is scope-wide memory, not a query reservation measurement. The original
allocator failure remains preserved.

Evidence: `docs/benchmarks/2026-09-06-ipc-systemic-repair/`, including source
hashes, build/test logs, binary identity and the complete diagnostic archive.
The fresh full provider matrix is running in
`.scratch/public-bench/canonical-sf10-provider-gates-02`; no complete score is
claimed until it finishes. Full current-source raw/public and resource gates
remain outstanding.
