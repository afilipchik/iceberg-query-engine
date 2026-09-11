# Fused aggregate budget overshoot — 2026-09-07

The existing `exact_decimal_spill_merges_match_an_independent_integer_oracle`
integration test refuses its256KiB query budget before reaching spill fallback
when run with multiple Rayon threads. This is a **spill-completion failure**,
not an OOM or silently wrong answer: the query pool correctly rejects admission.
No test data, result expectations or query budget was changed.

The fixture has20 batches of1000 rows,1000 repeated integer groups and exact
Decimal128(38,2) values with coefficient10^20+17. It requires positive actual
spill bytes and exact grouped sums. An older recorded653 default-feature run
passed it; that historical run alone does not establish the cause of the change.

## Controlled evidence

The new precision-bound helper passes24 decimal library tests,11 other tests in
`systemic_numeric_tests`, and24 tests across six additional integration targets.
None are ignored. The failing spill test remains failed, not skipped. The initial
driver exited101 and stopped; the remaining targets were run separately.

A matched full-target control restored only `src/planner/decimal.rs` to its
frozen662 bytes, ran the same12 integration tests, then ran them with the new
helper. **Both runs passed11 and failed the same spill test.** All632 recorded
inputs were hash-checked, and the candidate source was restored and verified.
Thus the precision lookup does not explain this failure. Optimized performance
for that lookup remains pending.

At the unchanged budget, a separate exact-test probe recorded:

| RAYON_NUM_THREADS | Outcome |
|---:|---|
|1|Pass, including actual spill and independent exact sums|
|4|Named query-memory refusal|
|16|Named query-memory refusal|

These are bounded probes, not a claim that every scheduler interleaving behaves
identically. The fused implementation clamps its worker count to at least2;
the Rayon1 setting must not be described as proof of one aggregate worker.

The four-thread trace confirms `execute_fused_streaming`, four input partitions,
and `disjoint=false`. At termination workers report0/2/0/0 fully processed
batches and256/1000/512/512 groups. A32768-byte request fails with235072 bytes
already used against262144. Earlier reproductions fail at other growth sizes,
consistent with competing partial-state growth. No spill-fallback trace occurs.

Commands run through `scripts/claude-safe-build.sh`,48GiB cgroup, one compiler
job, repository TMPDIR, locked Lance/GPU features. Drivers, commands, exact
source hashes, complete output and restoration checks are under
`.scratch/decimal-precision-bound-repair/{validation-01,integration-control-01,worker-probe-01}`;
`spill-workers4-trace.log` contains the diagnostic. The matched-control and
worker-probe drivers exited0 as orchestrators; their recorded individual failing
test exits are101 and must not be presented as successful tests.

## Mechanism and next repair

`SpillableHashAggregateExec::execute_fused_streaming` gives workers shared
input, so several workers may build overlapping groups. Its group-count budget
uses a fixed per-group estimate and is checked after `process_batch`, periodically
only every16 completed batches and again at termination. The admitted raw-state
arena can run out of query budget inside a batch first. Admission errors then
propagate after draining workers, as required; treating them as permission to
re-execute consumed input would introduce a separate correctness defect.

The repair must coordinate work granularity, real state-growth admission and
spill selection before consuming an unreplayable input. It must preserve all
input partitions, exact aggregate states, cleanup and terminal error propagation.
An arbitrary higher test budget, a query-specific shortcut, or retrying the input
after failure does not resolve the mechanism. Retain this fixture at256KiB and
test several worker counts, duplicates, NULL states and actual spill. Also retain
clean refusal where the necessary working set cannot fit.

The general input queue separately preadmits parallel output slots from the same
query pool; consumer headroom warrants review, but **it is not the traced cause
of this reproduction**, which fails earlier in fused execution. No production
resource-policy fix has been applied yet.

## State-transition implementation constraints

The existing `aggregate_with_spilling` stores raw input rows and later applies
the original aggregate expressions. Feeding it finalized worker outputs is
incorrect: COUNT would count partial rows, and AVG would average partial means
without their weights. The distributed planner already splits COUNT into a
sum of counts and AVG into sum/count, but that SQL rewrite is not a serializer
for the current in-process accumulator representation.

In particular, `AccumulatorState::SumDecimal` stores an optional fulli128
coefficient, a signed scale and a separate seen flag. Its coefficient can
temporarily exceed38 decimal digits while still fittingi128, then cancel back
into the final SQL range. A partial-state codec must preserve that intermediate
coefficient and sticky arithmetic-overflow status; validating it as a finalized
Decimal128(38,s) output during flush could introduce an incorrect early error.
`Avg` currently stores a floating sum and count, so its internal checkpoint must
preserve those fields rather than substitute the distributed decimal-SUM path.
This is source-derived reasoning, not a new executed codec regression.

The next implementation therefore needs two explicit interfaces:

1. A resumable ingestion result with the exact applied-row cursor and retained
   evaluated arrays. Admission failure must occur before mutating the next row;
   neither source rows nor volatile expressions may be evaluated again.
2. A bounded, query-owned partial-state flush/merge format carrying group types,
   NULL/seen state, exact coefficient/scale/overflow and AVG sum/count. Encoding
   scratch must be available while old state remains live, and state memory must
   be released incrementally or after a bounded flush. HAVING remains a final
   operation, never a reason to discard overflowing intermediate state.

An upfront spill route can handle capabilities without that state format, but it
must be selected before consuming input. Heuristic group counts or final output
types cannot substitute for these contracts. The lifecycle fix now prevents
error-to-replay and pending-input error hangs; it does not supply this transition.
