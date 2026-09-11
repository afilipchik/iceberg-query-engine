# Compact fixed aggregate cells

The shared spillable aggregate row now stores fixed numeric accumulators in a
32-byte Copy cell instead of the64-byte general AccumulatorState enum. The change
halves fixed state and row-workspace storage per slot on this target. It does not
yet establish a throughput improvement: the first candidate deliberately reuses
the established numeric update/merge implementation through typed local state.
A measured comparison must determine whether conversion overhead offsets cheaper
row copies. No ownership default or query-specific rule changes.

## Representation and transaction

New `fixed_cell.rs` stores three native numeric words, the bound FixedStateCodec
and NULL/seen/overflow flags. Float values use exact bit conversion; decimal
coefficients preserve all128bits and a separate sticky-overflow flag. AVG retains
sum/count, variance retains count/mean/M2, and boolean state retains NULL separately
from false. The cell cannot contain selected ScalarValue payloads. This is a
numeric memory representation, not the spill serialization format.

StateRows and RowWorkspace use reserved FixedCell vectors. begin copies the entire
old fixed row into scratch, per-slot preparation updates only scratch, and commit
copies the prepared row back. Later-slot errors and dropped preparation tokens
preserve rollback, selected-payload cleanup, and new-group truncation. No row is
partly committed; spill retry keeps its exact uncommitted cursor. Selected min/max/
first values retain the current admitted payload representation.

Updates and merges use the same checked integer arithmetic, exact unsigned SUM
handling, decimal overflow/scale rules, floating arithmetic and finalization as
before. Cold restore/write paths retain the existing independently defined
FixedStateCodec wire frames. Input arrays do not regain the removed
serialization/decode round trip. Compact cells reconstruct only typed local
numeric state when calling the shared arithmetic helpers.

## Validation so far

Red27101 terminal101 reproduces64bytes per fixed slot against the32byte footprint
requirement. The new cell has a Copy trait check and independent frame-preservation
cases for integer bounds, signed zero, NaN payloads, decimal extreme coefficients,
negative scales, sticky overflow, AVG/variance internals and boolean NULL values.
Existing row tests retain their independent numeric/selected-value assertions and
failure-before-commit checks; internal inspections decode the compact cell only
for those assertions.

Initial library34326 is terminal101:1080passes/11ignored, two tests no longer spill
because their old fixtures now fit. The scheduler fixture grows from2048to4096
unique groups, and the retained-input fixture from1000to2000groups, at the same
256KiB budgets. Mandatory split/spill assertions and independently calculated
COUNT/decimal SUM/AVG/NULL results remain. This strengthens pressure coverage
rather than lowering its acceptance standard.

Corrected library26228 terminal0:1082passes/11ignored. Partial library51787 also passes1082/11ignored. Both-mode gate15164 is terminal1:
native/IPC/mutation/dictionary default58passes/2plan-assertion failures, partial
53passes/7failures (the two plan assertions, four native memory refusals and the
formatted floating-point comparison). These names were reproduced on preceding
source; they remain open. Each mode's remaining targets pass2decimal-expression,
3decimal-root-reuse and2parallel-input spill tests, with full spill8pass/6fail and
systemic numeric11pass/1fail. Allocation boundaries change with smaller states:
partial native requests163434at183961,163434at178841,161343at175558 and137344at183961
under262144bytes. Numeric default refuses4096at260377; partial1008at261922. These
are not successful resource gates. All initial failures and corrected logs remain.

Builds/tests use locked/offline features lance,gpu,48GiB containment, one build job
and repository TMPDIR. Performance remained unproven at this gate.

Against frozen c20b0648, existing source changes are state_rows.rs and the module
registration, plus pressure fixtures in ingestion_controller.rs and
partition_scheduler.rs; fixed_cell.rs is new. There is no dependency change.

Next: complete both-mode gates, freeze the exact candidate and compare low/high-NDV
and skewed aggregate workloads at4/16threads. Keep old failures visible and do not
promote the default ownership policy based on this representation change.

[Debugger attribution](aggregate-row-transaction-attribution-2026-09-10.md),
[previous frozen CPU comparison](native-incremental-measurement-2026-09-10.md).

Correctness archive verifies15files/519source inputs. Release64441 terminal0 in8m53s freezes8b6a82e9/519inputs; paired4982 is terminal0. No source changed during measurement.
[Archive](benchmarks/2026-09-10-compact-fixed-state/manifest.json).

## Prespecified optimized comparison

`run_compact_fixed_paired.py` is prepared for64fresh-process outputs: Q1/Q9/Q17/Q18,
raw16threads, native16threads, decoded resident16threads and a separate resident
4thread case, two reversed-order control/candidate blocks. Control is c20b0648;
candidate must come from the current source-verified optimized build. Raw/native
retain4GiBquery/12GiBprocess and CPU0–15; resident retains32/48GiB, using CPU0–15
or0–3 to match16or4workers. GPU stays disabled and preload is separate. Independent
typed oracles exist for every query/provider combination. The180second watchdog
is diagnostic, not the matched DuckDB10times acceptance ceiling. This screen will
not establish a protected confidence bound or full-suite leadership.

Driver and summarizer syntax, all12distinct oracle files and519matching build/
correctness-archive source hashes were checked before launch. The optimized build and paired4982 completed; source stayed unchanged.

The completed64output screen finds performance regressions despite the smaller
footprint: resident16thread Q18ratio1.169559 and rawQ1ratio1.132091. All answers
validate, but this is not an accepted speed candidate. See the [full result and
required direct-update follow-up](compact-fixed-measurement-2026-09-10.md).
