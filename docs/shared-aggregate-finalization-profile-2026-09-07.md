# Shared aggregate route profile — frozen649, 2026-09-07

The contained diagnostic89923 completed36/36 canonical SF10 engine requests with
full typed DuckDB agreement and the fresh10x query-time ceiling. These runs enable
profiling and are not acceptance timings. Candidate source/binary and preceding
semantic/resource gates are in [the identity report](qualified-column-identity-2026-09-07.md).
No new production change was made for this profile.

## Reproduction and preserved evidence

Driver: `.scratch/qualified-column-identity-repair/diagnose-shared-cpu.py`.
Results: `.scratch/public-bench/qualified-identity-shared-cpu-01/`.
Analysis: `.scratch/qualified-column-identity-repair/analyze-shared-cpu.py` and
`shared-cpu-analysis.json`, including engine-log hashes and every stage observation.
The driver runs under the96GiB wrapper, CPU affinity0–15,16threads,40GiB query and
48GiB process budgets, using the same verified canonical SF10 dataset/providers.
Each case preserves a full typed oracle, three fresh DuckDB calibrations, one
engine warmup and three measured diagnostic requests. Engine logging enables
AGG_TIMING, QE_AGG_PROF, HJ_PROF and QE_SPILL_DEBUG. Process CPU/RSS observations
are sampled at5ms; pipe timing and CPU tick quantization limit their precision.

| Query | Provider | Engine diagnostic median ms | DuckDB calibration median ms |
|---|---|---:|---:|
| Q18 | native |1052.98|175.06|
| Q18 | raw Parquet |1138.90|292.10|
| Q18 | decoded IPC |950.93|810.97|
| Q13 | native |969.17|210.15|
| Q13 | raw Parquet |793.39|282.32|
| Q13 | decoded IPC |811.87|357.06|
| Q1 | native |745.62|107.45|
| Q1 | raw Parquet |685.81|212.74|
| Q1 | decoded IPC |648.22|1097.35|

These paired provider definitions matter: DuckDB's IPC/Parquet route is not its
native-storage route. Do not use the IPC ratios to claim general leadership.

## What actually executed

Native and IPC Q18's inner disjoint aggregation each finish15,000,000 state groups
and emit624 rows after HAVING. Their finalization intervals are314–323ms and
317–342ms respectively, including warmups. Those intervals include output
construction, predicate filtering and state destruction. The enclosing outer
aggregate mostly waits on its input and finalizes624groups in roughly0.2–0.3ms.
Do not add the nested intervals or label all outer time aggregate CPU.

Raw Parquet Q18 takes another route: morsel scan/process takes324–339ms, followed
by531–564ms for merge/output/HAVING. The raw merge log splits this into260–286ms
sharding and268–296ms merge/build. It reports about15,000,339 thread-local groups,
which still require semantically correct reconciliation; their near-equality to
the final group count is not a uniqueness proof.

Q13's1,500,000-group finalization is only about16–20ms across these providers.
Its large total gap therefore cannot be explained by the Q18 finalization result.
Q1's logged finalization is below0.5ms; its update/input path remains the target.
No traced fused aggregate aborted in these36requests. This rules out the logged
fallback/re-execution path for these requests only, not for every operator or
resource setting. Absent HJ_PROF output is not evidence that joins cost nothing.

## Next bounded CPU experiment

The immediate measured target is shared aggregate finalization, not query SQL or
identifier handling. Before changing its representation, split the existing
finalization interval into state preparation, output construction, filtering and
state destruction. Associate diagnostics with stable operator identities and
retain nested/parallel timing boundaries explicitly. Keep the frozen649 binary as
the uninstrumented performance control.

The source currently constructs a complete shard output before filtering it in
`build_filtered_output`. The raw path also materializes an entry-reference vector;
the general path collects group references and intermediate scalar values.
Those allocations are plausible avoidable work, not yet proven to explain the
whole measured interval. State destruction is included and must be measured too.

If output construction/filtering dominates, implement bounded output chunks that
apply the existing typed predicate before retaining results, using the same SQL
finalization/error behavior and explicit buffer ownership. Cover all current
state representations, NULL/empty/global aggregates, decimal overflow, multiple
aggregates and batches, and predicates that retain zero/some/all rows. Do not
introduce a Q18-specific threshold, assume disjointness from estimates, replace
exact decimal sums with floats, or bypass query memory admission. If state
construction/destruction dominates instead, investigate inline typed accumulator
storage with measured state-size/reservation accounting before adopting it.

A useful bounded change still needs a component improvement and end-to-end
balanced results without a protected regression. The current profile does not
prove the epic's broad replacement threshold and does not justify rewriting the
whole engine. Canonical full-provider latency screen20226 is running separately;
public workloads, holdouts, concurrency and full memory ownership remain open.


Prepared diagnostic source delta (not integrated or executed) is under
`.scratch/aggregate-finalization-attribution/`. `candidate.patch` adds opt-in
single-state phase traces for prepare/build_output/filter/state_drop, with IDs
for interleaved worker logs. `preparation.json` records exact base/candidate hashes;
rustfmt passes. The production tree remains frozen649 while screen20226 runs.
This first instrumentation scope covers the disjoint native/IPC single-state
finalizers, not raw sharding/merge. After the screen is terminal, apply only if
the base hash still matches, validate error/result behavior, build a separately
identified diagnostic binary, and retain the uninstrumented control. Do not treat
this unexecuted instrumentation as CPU improvement or completed attribution.


## Source comparison: aggregate state layout

A clean local DuckDB checkout at`e500d77864fe565f90e68f06d729c25b11e775c5`
provides a concrete architecture comparison. `AggregateObject` obtains each bound
aggregate's state size and aligns its payload; `TupleDataLayout` includes those
payload widths in each tuple's layout. Grouped aggregation updates state addresses
in vector batches and advances them by the aggregate payload size. Destruction
skips aggregate-destructor traversal when the layout has no destructor; otherwise
it traverses tuple-data chunks. Sources:
[bound aggregate payload](https://github.com/duckdb/duckdb/blob/e500d77864fe565f90e68f06d729c25b11e775c5/src/execution/operator/aggregate/aggregate_object.cpp),
[tuple layout](https://github.com/duckdb/duckdb/blob/e500d77864fe565f90e68f06d729c25b11e775c5/src/common/types/row/tuple_data_layout.cpp),
[grouped hash table](https://github.com/duckdb/duckdb/blob/e500d77864fe565f90e68f06d729c25b11e775c5/src/execution/aggregate_hashtable.cpp).
Exact inspected file hashes are in`duckdb-state-layout-sources.json` beside the
prepared diagnostic. This checkout is source evidence, not an assertion that the
benchmark wheel was built from this commit.

In this engine, the generic raw-key aggregate map stores a separate
`Vec<AccumulatorState>` per group. Exact decimal SUM uses that representation;
the enum must also accommodate unrelated aggregate variants. The existing single
Float64 SUM path demonstrates a direct-value map already works for one supported
shape. This is a concrete representation difference, not proof that changing it
will recover the measured gap. The prepared trace now records constant-time
representation counts/capacity and actual`size_of::<AccumulatorState>()` so the
executed decimal state path and destruction cost can be observed without adding
a per-group telemetry scan.

A future typed-state change must preserve sticky decimal overflow, NULL/seen
state, exact coefficient/scale through demotion and merge, raw-null groups and
mixed pre-/post-perfect-hash states. It must account for resized hash storage and
old-plus-new live memory, not just inline payload. A fast representation that
silently loses an earlier state or bypasses reservations is unacceptable.

[Research follow-up and distinguishing workload families](aggregate-research-follow-up-2026-09-07.md) evaluates longer-term alternatives without changing the spillable default or asserting an unmeasured speedup.


## Diagnostic scheduling checkpoint

Coordinator2907628 was paused with SIGSTOP; its already-running second IPC cell
(child3021401, after-first/offset1) was allowed to finish normally. Host process
inspection confirms the child exited0 with no descendants and the scope contains
only the stopped coordinator. No measured request was suspended. The cell summary
is complete; its coordinator status entry will be written when resumed. The
remaining predeclared cells are unchanged, with this between-cell interruption
recorded in `pause-boundary.json` and `pause-ready.json`.

The opt-in phase trace is now temporarily applied to`src/physical/morsel_agg.rs`
after verifying the frozen649 source hashes. Library test8130 is running with
QE_FINALIZATION_TRACE=1. It is a diagnostic, not a performance candidate. After
tests/build/attribution, restore the exact frozen source with the guarded
`restore-source.py`, verify the retained benchmark binaries, and invoke the guarded
`resume-coordinator.py`. Do not restart or discard session20226.

Source review identifies a narrower potential output cost: raw-key aggregate
output directly builds Int64/Float64 arrays but routes decimals through an
intermediate`Vec<ScalarValue>`. The trace must establish its contribution before
an output-builder change is selected; a typed-state redesign is not presumed
necessary. Any new builder must preserve exact conversion/errors and memory
ownership, not just remove an allocation in the common case.


Library8130 completed767passes with QE_FINALIZATION_TRACE=1; ten preexisting
ignored cases remain. Diagnostic release44481 is the only active heavy job.
`run-profile.py` brackets engine-log byte ranges per query and preserves the same
36-request typed diagnostic matrix. `freeze-binary.py` verifies every production
source hash before copying a separately named diagnostic binary. The source
manifest and patch hash are saved in`diagnostic-source.json`; no dependency change.
The original screen coordinator remains paused and must be restored/resumed as
specified above when attribution completes.


## Completed phase attribution and restored source

Diagnostic release44481 completed successfully in8m38s. Binary SHA256 is
`50ed8692640bce6bb6fac2cebf53091afd8cee1f64d7f35de9bd94baf4d66bfe`.
Session10043 completed normally: all36requests (Q18/Q13/Q1 × native/raw/IPC,
one warmup and three steady samples) passed independent typed DuckDB comparisons
and fresh three-calibration10× query ceilings. The library trace check passed767
tests with10existing ignores. These are diagnostic measurements, not acceptance
latencies. The archived build metadata's “build pending” status describes its
pre-build creation; the terminal build log and binary manifest establish completion.

The parser checked each sample's recorded log byte range, correlated worker IDs,
and required every captured single-state finalizer to contain exactly prepare,
build_output, filter and state_drop. All320captured states have complete phases.
The following values are medians across three steady requests of the **maximum
worker wall duration for each phase**, restricted to states with≥10,000groups.
Different phase maxima can belong to different workers; they must not be added
or described as exclusive CPU time or query wall time.

| Query/provider | Output construction ms | Filter ms | State destruction ms |
|---|---:|---:|---:|
| Q18 native | 210.214 | 7.218 | 111.411 |
| Q18 decoded IPC | 201.954 | 4.878 | 112.922 |
| Q13 native | 8.268 | <0.001 | 7.736 |
| Q13 raw Parquet | 8.334 | <0.001 | 7.500 |
| Q13 decoded IPC | 12.254 | <0.001 | 9.008 |

Q18 native/IPC traces contain15million aggregate groups per request. Their raw
hash state stores a Vec of64-byte AccumulatorState elements per group. Decimal
output currently materializes both a Vec<ScalarValue> and a Vec<&ScalarValue>
before constructing Arrow buffers. The measured output phase includes iteration,
finalization, conversions and allocation; the trace does **not** isolate those
vectors' contribution. State destruction is a separate substantial cost whose
allocation/free-thread explanation remains a hypothesis. Q18 raw takes the
untraced shard-merge route; Q1 has no single-state finalization traces. Missing
coverage is not zero cost. Q13's larger overall latency needs other attribution.

The first bounded experiment is direct exact Decimal128 output with reservation
owned buffers, removing scalar/reference vectors without changing aggregate
update order or moving HAVING before finalization. Preserve signed scales,
precision checks, sticky overflow, all-NULL/empty semantics, bound-field metadata,
and cleanup/lease lifetime under named memory refusal. Explicitly pass the shared
pool through worker finalization; thread-local scope does not propagate to Rayon.
Source inspection confirms MorselAggregateExec already has a memory_pool field and
with_memory_pool method; the missing wiring is at the shared finalization helpers,
not a missing operator pool. This corrects the earlier working-note hypothesis.
General and raw shard output paths must share conversion semantics. No production
optimization has been accepted from this diagnostic.

Frozen649 source was restored with hash guards. The original benchmark coordinator
2907628 was resumed with SIGCONT after verifying source and both retained benchmark
binaries. The first six cells independently audit2904successful typed/time-gated
requests; the sixth IPC cell has suite649/647=.999033 and no>10%flags. The earlier
Lance Q11 mean flag remains open. Fourteen predeclared cells remain at this checkpoint.

[Archived raw evidence and SHA256 manifest](benchmarks/2026-09-07-aggregate-finalization-phases/SHA256.json)
contains178member-verified files. Manifest SHA256:
`097ba06dadbbbc4e2297d80ceb9dbbf1905fa19946deb0f3796d4cf283e862e3`.
It preserves request/results, typed comparison records, process samples, logs,
plans, environments, source patch, binary provenance and analysis script. The
binary itself remains in repository scratch; no diagnostic binary is used by the
balanced latency coordinator. Formatting and diff whitespace checks pass.
