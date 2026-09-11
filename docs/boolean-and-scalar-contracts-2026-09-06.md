# Boolean NULL correctness and scalar comparisons — 2026-09-06

A source review of decimal comparison allocation exposed an independent SQL
correctness bug: interpreted AND/OR and BETWEEN used strict-null Arrow Boolean
kernels. False AND NULL must be false; true OR NULL must be true. NOT BETWEEN
inherits the same distinction. Five new tests failed before the correction and
pass afterward, including direct evaluation, SQL projection/filter and compiled
predicate routes. Both existing compiled evaluator tests also pass.

The interpreter now uses Kleene kernels. The compiled evaluator previously
intersected all referenced-column validity once for the entire expression; this
cannot represent Boolean three-valued logic. It now declines nullable batches
for programs containing AND/OR, allowing the checked interpreter to evaluate
them. Null-free batches retain compiled execution. A future per-register validity
implementation may restore performance for nullable Boolean trees; this patch
makes no such claim. Cast/error propagation remains through the ordinary Result
path. No dependency changes.

A separate performance candidate retains comparison constants as one-element
Arrow Datum operands instead of full-length arrays. Only literal, alias and CAST
chains rooted in literals qualify. Coercion and CAST/TRY_CAST use the existing
numeric contract. Both-constant results expand to the current batch length;
empty batches retain existing empty evaluation, avoiding invalid-cast errors on
nonexistent rows. BETWEEN evaluates all three operands once in the existing
order, then combines comparisons with Kleene AND. Dictionary literal shortcuts
remain before normalization; LIKE, arithmetic, functions and CASE selection
retain their existing paths. No query IDs or workload-specific conditions exist.

The constant-chain evaluator avoids slicing unrelated input columns. A storage
invariant test verifies that a decimal CAST constant occupies one element for an
8,192-row batch; answer equality alone would not establish that allocation
property. Independent comparisons cover decimal scales, signed/unsigned limits,
all six operators and both operand directions, NaN/signed-zero ordering,
dictionary key/value NULLs, typed/untyped NULLs, casts, empty/scalar result lengths,
BETWEEN and untaken invalid CASE branches.

Validation is recorded separately from performance acceptance. The initial
combined expression gate passes 34 tests, zero skips. The final library gate passes 557 tests with one pre-existing ignored test;
all 12 Boolean/scalar integration tests pass, zero skips. A final dictionary
constant shortcut guard then passes 13 focused tests, including its new result-
length regression. The first release build was intentionally stopped before
measurement to include this edge case. Matched performance
screening passes all 220 executions with identical plans. Q6 improves 52.6% raw
and 75.3% IPC; Q14 improves 26.6%/64.0%. IPC Q18 is 2.6% slower; no screen query
regresses over 10%. Full provider validation is active; prior SF10 scores belong to the
frozen IPC repair binary, not these source changes.

Source: `src/physical/operators/filter.rs`, `src/physical/compiled_expr.rs`.
Tests: `tests/boolean_null_contract_tests.rs`,
`tests/scalar_comparison_contract_tests.rs`, existing cast/membership/numeric gates.


## Full native gate completed

The final binary (`0d5cf9ec2f387a004bc88736a357ad214cb579b873bf8011a156a7e73aff720c`)
passes all 660 native pairs (22 queries, ten samples, three sessions), zero
issues. Previous Q1/Q6/Q15 time-gate failures do not recur. Suite engine/DuckDB
ratio is 3.358422; geometric mean 2.998737, zero query wins. This establishes
completion/correctness/time-ceiling closure for the measured native workload,
not native performance leadership. Remaining provider gates are running in
`.scratch/public-bench/scalar-comparison-provider-gates-01`.


## Full decoded IPC gate completed

All 660 pairs pass with zero issues. Suite engine/DuckDB ratio is 0.524594,
geometric mean 0.421267, with 17/22 query wins. The worst query ratio is 3.045515,
so the strict per-query leadership requirement remains unmet despite the suite
win. This result belongs to the final scalar candidate and its matched Arrow
reference. Iceberg, Lance, raw control and GPU routing gates remain in progress.


## Full Iceberg gate completed

All 660 pairs pass, zero issues. Suite engine/DuckDB ratio is 0.315968,
geometric mean 0.289468, 21/22 query wins and worst ratio 1.338778. This is the
same pinned-snapshot warm persistent-provider comparison described in the
[boundary investigation](iceberg-benchmark-boundary-investigation-2026-09-06.md).
It does not certify cold open, object storage, evolution/deletes or resource
profiles. Lance, raw control and GPU routing are the remaining active tracks.

## Full Lance gate completed

All 660 pairs pass, zero issues. Suite engine/DuckDB ratio is 1.259869,
geometric mean 1.123707, seven of 22 query wins and worst ratio 3.395529.
The preceding frozen IPC-repair candidate scored 1.453202 on this same qualified
Lance comparison; this is a sequential full-suite comparison, not an alternating
causal estimate. The DuckDB Lance extension optimizer remains disabled globally
for its independently reproduced decimal AVG bug; do not label this a stock
pushdown score. Raw CPU control and GPU routing remain active.

## Completed-provider regression review

Across IPC, Iceberg and Lance, none of the 66 per-query engine medians regresses
more than 10% against the immediately preceding frozen IPC-repair candidate.
The largest observed increases are IPC Q11 +5.1%, Iceberg Q18 +6.5%, and Lance
Q21 +2.8%. These are sequential comparisons; fresh DuckDB median changes are
retained alongside engine changes in
[the regression artifact](benchmarks/2026-09-06-scalar-comparison/completed-provider-regression.json).
The failed native control cannot support an accepted full native regression score.

All physical-plan string sets match for these modes. Q19 optimized-plan strings
differ in conjunction order among duplicated implied IN predicates, matching
the already documented optimizer determinism/idempotence issue. Other optimized
plan sets match. Plan hashes check provenance, not SQL semantic equivalence.
The full raw control, GPU and public-workload gates remain required.

## Full raw CPU gate completed

The same-binary `gpu_control` track uses the ordinary raw-Parquet CPU path.
All 660 pairs pass, zero issues. Suite engine/DuckDB ratio is 2.257953,
geometric mean 2.043245, one of 22 query wins and worst ratio 5.361387.
The preceding frozen candidate scored 2.547248. No raw query median regresses
more than 10%; the largest increase is Q16 +1.5%, with Q9 effectively unchanged.
These sequential full-run comparisons supplement the earlier alternating screen
and do not establish overall DuckDB leadership. The regression artifact now
covers 88 query/mode medians; all remain below the 10% regression threshold.
GPU routing is the final active mode.

## Full six-mode matrix completed

The driver exits zero after all six modes pass 660 pairs each: 3,960 measured
pairs, zero correctness or time-gate issues. GPU routing's suite ratio is
2.254746× DuckDB, geometric mean 2.036180×, with one query win. All 660 GPU
mode samples report zero successful device runs, so the score measures CPU
execution/fallback, not acceleration. The fresh CPU control scores 2.257953×.

The matrix is complete for these matched warm conditions; resource ownership,
concurrency, cold/residency costs, full public workloads and overall leadership
remain unmet. The supported float GPU smoke and public development checks use
the same frozen scalar binary in subsequent isolated runs. Full artifacts are
retained in `.scratch/public-bench/scalar-comparison-provider-gates-01`; compact
reports are copied to the evidence directory, with full archival/checksums pending.

## Supported GPU smoke completed

The unchanged 600,000-row custom float Q1/Q6 fixture passes 40 CPU-control pairs
and 40 GPU pairs, with all 40 GPU samples executing on device. CPU/GPU medians
are 24.580/1.419 ms for Q1 and 6.912/0.628 ms for Q6. Four threads, affinity 0–3,
4 GiB query, 8 GiB process and 16 GiB cgroup match the prior supported-path check.
This is a warm supported-operator regression check. It does not certify decimal
acceleration, cold upload or the separate unresolved hard-VRAM-budget contract.

## Public development gates completed

ClickBench passes 129/129 pairs across all 43 queries on the existing one-million-
row extract: suite 2.155821× DuckDB, geometric mean 1.599572×. JOB passes 339/339
pairs across all 113 queries on its existing development extract: suite 1.567246×,
geometric mean 1.327974×. No correctness or time-ceiling issues occur; the driver
exits zero. These remain development extracts, not full public-workload or
resource/concurrency certification. No fixture or SQL changes were introduced.

The scalar candidate now has complete canonical six-mode, supported GPU, public
development and after CPU diagnostic evidence. It improves a shared expression
bottleneck with independent typed validation; it does not close overall DuckDB
leadership or the remaining resource-ownership contracts.
