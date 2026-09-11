# Aggregate row transaction: debugger attribution

Native/resident Q1 remains slow after incremental native decoding, and resident
input already has16admitted slots. A debugger launched with the frozen native
query process now locates active work in the fixed aggregate row transaction.
This is attribution for the next shared implementation, not a query-specific
rewrite or proof of a prospective speedup.

## Reproduction

Frozen c20b0648,518source inputs verified before/after, canonical SF10 nativeQ1,
16threads CPU0–15, default disjoint ownership,4GiBquery/12GiBprocess, GPU0.
The debugger and child run inside the repository48GiB wrapper; no host profiling
settings changed. The host perf_event_paranoid value is4. GDB reads optimized
function symbols; this binary has no source-level debug info.

Initial53545 completed its engine child but captured no samples because the console
run occupied GDB until exit. The owned idle debugger was stopped and the protocol
failure preserved. A corrected launch first failed before execution due to a
Python-path typo (exit127); the actual corrected40031 is terminal0. Background
console run plus MI interrupts captures40all-thread stacks at approximately0.2s
of running time per sample, then lets the query finish. Its result independently
matches the typed oracle. No engine source or configuration policy changed.

The40snapshots contain30frame occurrences of StateRows::prepare_arrays_indexed
and34of IngestionController::ingest_inner. Engine leaf frames include10in
prepare_arrays_indexed,3in StateRows::begin,3in PreparedGroup::commit and3in
AccumulatorState::update. Many other threads are sleeping/waiting. These are
stopped-thread observations, not normalized CPU percentages: deterministic sample
spacing, stop rendezvous and optimized inlining limit statistical interpretation.
Full stacks and machine-readable symbol counts remain available.

## Source contract and next implementation

StateRows stores fixed numeric states as the general AccumulatorState enum, which
also represents selected ScalarValue aggregates. begin clones the old fixed row
into workspace; prepare_arrays_indexed resolves each input and updates scratch;
commit clones the complete fixed row back. The copies enforce atomicity: a later
slot failure cannot leave earlier updates committed, and spill retries resume at
the exact uncommitted input row. Removing that transaction is not an acceptable
performance shortcut.

Investigate a compact, fixed-only state representation selected by the existing
bound FixedStateCodec. Keep selected-value payloads and their reservations on
the current path. The design must avoid recreating the old per-input
serialization/decode round trip. Validate representation size and actual hot-loop
cost before attributing any expected gain to it; the snapshots alone do not prove
cloning is the dominant cost.

Required semantics include NULL-versus-zero SUM state, checked integer counts and
sums, unsigned exact coefficients, decimal scale and sticky overflow, float bits,
AVG count/sum, Welford variance, boolean NULL states, whole-row rollback and
identical spill frames. Prefer reusing established conversion/update semantics
outside the hot loop; do not duplicate subtly different numeric rules.

First add independent multi-slot rollback/overflow and mixed fixed/selected tests,
then change representation or batch-bound operations. Preserve exact row cursor,
new-group rollback, partial merge and detached output admission. Measure against
c20b0648 on low/high cardinality and skew at4and16threads. Existing partial
ownership remains opt-in until general costing and protected gates pass.

Native admission remains separate resource work. The phase data and stacks make
shared aggregate processing the next performance investigation, rather than
assuming additional scan concurrency solves a resident bottleneck.

[Previous matched measurements](native-incremental-measurement-2026-09-10.md),
[archive](benchmarks/2026-09-10-aggregate-row-transaction-attribution/manifest.json).
