# Budget-aware aggregate expression quantum — September 8, 2026

Frozen0ba65b55 closed three native256KiB completion failures, but its fixed1024-row
expression quantum coincided with a7.7% Q10 slowdown against9c868dc0. That earlier
comparison included several changes, so it did not isolate batching as the cause.
This candidate changes only the generic grouped evaluation scheduling policy;
the accompanying integration test expands independent value/NULL/key checks.

The row quantum is `clamp(pool.max() / max(256, 64 * evaluated_columns), 1, 8192)`,
with checked arithmetic. It accounts heuristically for expression width and the
configured pool limit, not momentary free memory. Three evaluated columns under
256KiB retain1024-row evaluation; a larger budget permits8192 rows. Wider
expressions reduce the selected quantum. These constants are a bounded experiment,
not calibrated bounds on expression allocations. Variable-width/intermediate
values can exceed the estimate and must still refuse through actual admission.

Source owners, full-backing input leases and producer permits remain held through
all slices. View metadata is reserved before slicing; expression allocations and
evaluation metadata retain their existing reservations. Refusals never trigger
re-evaluation/replay, so volatile inputs are not silently evaluated twice. Empty
batch behavior and profiling of source rows/batches are unchanged. The separate
scalar aggregate implementation is unaffected.

Tests:14 focused passes with zero skips, covering low/high-budget independent
COUNT/SUM/group-key values, nullable input and an empty prefix, volatile grouping,
exact Decimal spill transitions, and input errors. Full library992 passes,
10 existing ignores,16.30s. Native suite remains9 passes/1 failure at unchanged
256KiB; only the383984-byte join admission failure remains. Formatting and
whitespace pass. Commands use locked/offline lance,gpu, Rayon4, one build job,
repository TMPDIR and the48GiB safe-build wrapper.

Source comparison against0ba65b55's491-input manifest identifies only
src/physical/morsel_agg/live_spill.rs and tests/aggregate_expression_quantum.rs
as changed. This supports an isolated follow-up comparison, but proves no gain.
Logs are under `.scratch/parallel-aggregate-input/`:
budget-quantum-focused-01.log and budget-quantum-library-native-01.log.

Release29075 completed in8m44s, frozen at2026-09-08T18:27:05Z. Binary SHA256
`3ff868c77eca9340c517f492ce2f9adb1c66e5fc0d904dac7e9e5b503a7e4628`
verified all491 inputs before/after compilation. Paired benchmark49103 completed with exit1 at its final failure assertion.
No heavy job remains active; source hashes are unchanged.
measure_budget_quantum.py compares canonical Q2/Q5/Q10/Q12/Q19/Q20 against0ba65b55
with four balanced fresh blocks, six measured pairs plus gated warmups, typed
DuckDB oracles and strict10× query ceilings. It runs through the48GiB wrapper
with pinned Python/PYTHONPATH=scripts. Preserve all failures and intervals;
no current-source performance certification applies yet.

If the isolated comparison recovers Q10, protect the full canonical/provider/
resource/residency gates before accepting broader performance claims. If it does
not, use component traces to locate the remaining overhead instead of increasing
row quanta or budgets until one query happens to pass.


## Completed paired evidence

After/before geometric means of four block-median ratios, with whole-block
bootstrap95% intervals (six measured pairs per block plus gated warmups):

| Query | Ratio |95% interval |
|---|---:|---:|
| Q2 |0.97138|0.90607–1.04141|
| Q5 |1.01285|1.00725–1.01674|
| Q10 |0.92279|0.88987–0.95692|
| Q19 |0.99167|0.97842–1.00509|
| Q20 |1.00338|0.99668–1.01011|

Q10 improves7.72% against the fixed1024-row candidate, supporting evaluation
batch overhead as a material cost in this generic path. Q5 is1.29% slower in
this experiment; retain that tradeoff and recheck it in broader validation.
The other three intervals include1. Four blocks are limited component evidence;
this is not a new full-suite baseline or a DuckDB leadership claim. It does not
prove improvement against the pre-regression9c868dc0 binary in a direct paired run.

Q12 cannot produce a complete valid paired block: eight completed engine outputs
exceed their fresh ceilings. Across all six queries, all294 completed engine
outputs match the independent typed oracle;286 pass their elapsed-time gate.
42 dependent requests are explicitly not_run. The final assertion fails as
intended. Scope peak2830761984bytes; max/oom/oom_kill events zero. No timeout
status is substituted for late completed output.

Archive: `docs/benchmarks/2026-09-08-budget-quantum/`, including491-input frozen
source, build/test logs, all paired responses/plans/typed outputs, SQL/data/harness
hashes, reference calibration, lifecycle and scope evidence. No results were
rewritten from earlier candidates. Before the next full canonical/provider run,
fix the ordinary-runner calibration and warmup gaps documented in
benchmark-calibration-contract-gap-2026-09-08.md; paired runners already enforce
the required gates. The remaining native join refusal and wider resource/
residency/public-workload acceptance criteria remain open.
