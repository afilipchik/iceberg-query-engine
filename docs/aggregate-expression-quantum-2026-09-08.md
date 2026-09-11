# Bounded aggregate expression batches — September 8, 2026

The generic grouped aggregate evaluated every grouping and aggregate input over
the provider's entire batch before ingestion. A16384-row nullable input with a
computed SUM and COUNT(*) reproduced a131584-byte expression request while
171701bytes were already held under a262144-byte query limit. The prior accounting
fix preserved source backing storage correctly; full-batch expression allocation
was the next independent obstacle.

The live aggregate now evaluates at most1024 rows per slice, retaining the entire
incoming source owner, full-backing input reservation and producer demand permit
until all slices finish. View metadata is admitted before constructing a sliced
RecordBatch, including reported array metadata plus conservative per-column and
owner allowances. Evaluation retains its existing admitted-expression scope and
metadata reservation. Evaluated arrays are released after each controller ingest.
The controller's grouping/spill state spans all slices. Empty batches still run
one zero-row evaluation, and input profiling counts original provider batches,
not artificial slices. Volatile group expressions are evaluated once per slice
and those exact arrays drive both routing and ingestion. No source replay occurs.

This bounds rows per evaluation; variable-width values and complex expressions
can still refuse under their own admission limits. It is not a byte guarantee for
arbitrary expressions, nor a fix for source-side construction/queues. Smaller
slices can increase dispatch/schema overhead, requiring performance verification.
Scalar aggregation has a separate implementation and is unchanged.

## Reproduction and verification

`tests/aggregate_expression_quantum.rs` builds a nullable16384-row input with an
empty prefix and uses independent exact COUNT/computed-SUM expected values under
256KiB. The first attempt exercised the separate ungrouped route and passed;
the corrected grouped test reproduces the refusal on old source, then passes
with this change. It checks all final leases release. Existing tests separately
cover exact Decimal spill transitions, NULLs, volatile keys and consumed-input
errors without replay.

All commands use locked/offline lance,gpu, Rayon4, one build job, repository
TMPDIR and the48GiB safe-build wrapper. Logs are in
`.scratch/parallel-aggregate-input/`:

- expression-quantum-red.log: scalar-route control passes.
- expression-quantum-red-02.log: grouped allocation refusal reproduced.
- expression-quantum-focused-01.log:13 tests pass, zero skips (new regression,
  evaluated keys, spill transitions and input errors).
- expression-quantum-library-native-01.log:992 library passes,10 existing ignores;
  native9 passes,1 failure. No native test budget was changed.

The ordinary/filtered native aggregate and deletion-vector completion tests now
pass. The remaining native join still refuses383984bytes withused0 under262144.
This closes three previously reproduced completion failures, not the native gate
as a whole. Formatting and whitespace pass.

## Frozen performance result

Release74944 completed in8m43s. Frozen binary SHA256
`0ba65b552e23be7dc031ce3508da5b4d1d2956752a80480b3a56085239da3320`
verified all491 inputs before/after compilation. Frozen at2026-09-08T18:07:49Z.
Paired benchmark44625 completed with exit1 at its final failure assertion.
No heavy job remains active. Source inputs remained unchanged during measurement.

Prepared next driver: `.scratch/parallel-aggregate-input/measure_expression_quantum.py`
compares frozen9c868dc0 to the new release on canonical Q2/Q5/Q10/Q12/Q19/Q20.
It uses four independently balanced fresh blocks, six measured pairs plus a gated
warmup, fresh DuckDB calibration/oracles,16 threads,4GiB query/12GiB process and
strict10× ceilings. It refuses to overwrite results and checks binary/data hashes.
Running through the48GiB wrapper with pinned `.scratch/venv-lance/bin/python` and
PYTHONPATH=scripts after release74944 succeeded. This selected component is
not a substitute for the full canonical/provider/resource/residency gates.


Four fresh blocks, six measured pairs plus gated warmups per query, balanced
startup/execution order independently. Results are after/before geometric means
of block-median ratios, with whole-block bootstrap95% intervals:

| Query | Ratio | Interval |
|---|---:|---:|
| Q2 |0.97383|0.90210–1.05126|
| Q5 |1.00438|0.98425–1.01814|
| Q10 |1.07701|1.06557–1.09282|
| Q19 |0.99728|0.97644–1.01604|
| Q20 |0.98530|0.95450–1.01250|

Q10 regresses about7.7%; the other intervals include1. No broad speedup is
established. The comparison includes all changes since frozen9c868dc0 (projection
identity guard, native optional-prescan check, retained-input accounting and
expression quantum); it does not isolate an exclusive CPU cause. Smaller
expression batches are the leading source hypothesis for Q10 overhead, requiring
a bounded follow-up comparison rather than attribution by assumption.

Q12 has no valid complete block comparison. Eight completed executions exceed
fresh931.35–969.20ms ceilings (both candidates have failures);43 later engine
requests are explicitly not run across the suite. All293 completed engine
outputs validate independently;285 also pass their time gate. There are no
worker-timeout outcomes in this run: late completed results remain time-gate
failures. Scope peak2879852544bytes, memory.events max/oom/oom_kill zero.
This is component evidence with four blocks, not full suite certification.

Evidence is preserved in `docs/benchmarks/2026-09-08-expression-quantum/`, including
all samples, worker lifecycles, typed outputs, plans, fresh reference calibrations,
frozen491-file source snapshot and release provenance. The next performance
experiment should preserve bounded evaluation at small budgets while reducing
unnecessary slicing overhead where a larger quantum can be safely admitted.
Actual expression reservations stay authoritative; estimates must never bypass
admission. Preserve this regression and the remaining native join failure.
