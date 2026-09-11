# Parallel resident aggregate reduction — September 9, 2026

## Problem and scope

The [frozen balanced-ownership diagnostic](balanced-aggregate-native-scaling-2026-09-09.md)
shows that distributing input across local aggregate owners improves ingestion,
but the single final merge reverses that gain for native canonical SF10 Q18.
At sixteen threads, its non-output finish grows from 40.348 to 3,642.100 ms.
This change targets that shared operator boundary. Default ownership remains
`disjoint`; the candidate is selected by the existing `QE_AGG_OWNERSHIP=partial`.
No query identifiers, dependency changes, or budget increases are involved.

## Implementation contract

`src/physical/morsel_agg/parallel_merge.rs` reserves up to sixteen reducers and
an 8,192-row routing window before input opens. Each resident partial-state key
routes by the existing canonical hash to one reducer. Full canonical equality
still resolves collisions. Prefix offsets place every selected source row once
in a flat admitted index buffer, preserving source order within each reducer.
Independent reducers execute through scoped Rayon tasks when there are at least
256 rows per active reducer. Sources and routing storage remain borrowed until
all tasks stop. COUNT, SUM, AVG and other supported aggregate states merge before
final values or HAVING are evaluated.

All allocations use the existing query pool. An optional startup admission denial
drops the abandoned reducer set. The serial consumer and its bounded run ledger
remain reserved before ingestion. If any local controller has spilled by finish,
the unused parallel reducers drop and original runs transfer to that serial
consumer. Resident-only inputs instead release the unused serial consumer and
merge through the parallel reducers. This choice happens before applying any
partial state; consumed input is never replayed.

A reducer encountering target pressure uses its prepared partial-row writer.
Before decoding any reducer's spill, all resident siblings spool through their
prepared writers and release resident capacities. Routing buffers drop first.
Final output remains sequential; reducer key domains are disjoint and therefore
complete reducer outputs may concatenate. Terminal errors poison the merge and
all remaining owners clean up on drop. This is bounded parallel resident reduction,
not parallel decoding of already-spilled local runs.

`QE_AGG_PROF=1` reports actual parallel reduction workers, rows, windows, dispatched
parallel windows, routing time and merging wall time. Serial reduction reports
whether local spill or startup admission selected it. These traces distinguish
requested experimental ownership from the reduction actually executed.

## Source comparison

Local DuckDB revision `1c27c54f27bea397d18d86f7c0d6a9f1d4aa85f8`,
`src/execution/radix_partitioned_hashtable.cpp`, assigns independently finalizable
partitions in `RadixHTGlobalSourceState::AssignTask` and combines a partition through
a thread-local aggregate table in `RadixHTLocalSourceState::Finalize` (around
lines 720–881). That supports separating ingestion and partition finalization as
independent scheduling decisions. Our admission, ownership and spill contracts
remain specific to this engine.

## Validation and measurements

Focused job85728 passes all 20 selected parallel tests. New component tests cover
multiple windows, 4/16 reducers, overlapping partials, NULL keys/values, exact
Decimal128 sums, AVG states, actual spool, admission denial, poisoned foreign
layouts, and output callback failure. Additional checks cover mixed resident/
spooled siblings, empty input and selection validation before prefix mutation.
The first broad command exits101 because abbreviated integration target names
were invalid; it ran no tests. Corrected broad2924 exits0 with 1,054 library
passes/11 ignored and 12 integration passes. Default15122 also passes 1,054/11.
Full spill6026 exits1: both modes retain eight passes and the same six known
failure names (each cargo exits101). Formatting and whitespace checks pass.

Release97747 exits0 in8m53s, freezing `bd689bf6` with 510 verified inputs.
Paired92740 and same-binary81286 both exit0: all 32 outputs and algorithm traces
validate. Q18 improves against serial reduction at both thread counts. Against
disjoint ownership it improves at16threads but regresses at4threads, so default
ownership remains unchanged. Each measurement archive verifies253files. See
[paired measurements](parallel-reduction-native-2026-09-09.md) and
[ownership tradeoff and next steps](parallel-reduction-ownership-2026-09-09.md).
The [11-file correctness archive](benchmarks/2026-09-09-parallel-aggregate-reduction/manifest.json)
verifies all510source inputs; four existing inputs changed and one module was added.
All jobs are terminal; the earlier `cd8098d5` binary remains immutable.

The prespecified paired diagnostic compares old serial and new parallel reduction
with the same partial ownership at 4/16 threads, canonical native SF10 Q1/Q18,
two reversed-order blocks, 4 GiB query/12 GiB process caps and CPU affinity 0–15.
Every completed output must match the existing independent typed oracle, and
candidate reduction traces must confirm parallel execution. Its 180-second
watchdog is diagnostic only, not the 10× DuckDB timing acceptance gate.
Full protected/provider/residency/resource/concurrency acceptance remains open.

The subsequent [four-provider screen](parallel-reduction-provider-screen-2026-09-09.md)
validates all331completed outputs,247/264required pairs. Iceberg completes this
single session; raw/native/Lance retain failures. Source/binary remain unchanged;
provider/residency/resource/concurrency leadership is not certified.
