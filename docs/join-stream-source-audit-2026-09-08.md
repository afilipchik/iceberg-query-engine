# Active join stream and local upstream comparison

The next join experiment needs instrumentation on the active execution path.
Current `HashJoinExec::execute` returns an `InnerProbeStream` for inner joins
before reaching the collected probing implementation. The existing `HJ_PROF`
phase counters live inside that older implementation. Their historical numbers
cannot attribute the current canonical SF10 failures. Enabling the variable
alone does not provide coverage of the active inner stream.

This is a source finding, not a measured explanation of the remaining latency.
The scheduling candidate already reduces Q13 latency by 34.7%, but raw SF10 still
times out on Q5/Q9/Q10/Q12/Q13. Native still times out on Q1/Q13. These results
use the frozen scheduling binary, not a new join implementation.

## Inspected implementations

The local checkouts remain at DuckDB
`1c27c54f27bea397d18d86f7c0d6a9f1d4aa85f8` and ClickHouse
`a1b25f3f4beb3ba49aa3b73671cc244185331b86`. These January snapshots are distinct
from the benchmark's DuckDB 1.4.4 wheel. Neither checkout was edited or built. Exact inspected file hashes are in
[the source provenance](benchmarks/2026-09-08-batch-dispatch/.scratch/live-schema-boundary/join-stream-source-provenance.json).

| Concern | Observed implementation | Consequence for this engine |
|---|---|---|
| Bounded duplicate expansion | DuckDB `ScanStructure::NextInnerJoin` retains chain selections when the next matches would exceed `STANDARD_VECTOR_SIZE`; subsequent calls resume. | Preserve our current row/chain cursor and 4,096-candidate bound. Restoring a collected fast path would undo an existing resource contract. |
| Probe payload | DuckDB slices the probe vectors through selections and gathers only the required build columns. Its no-long-chain path constructs output directly. | Our ordinary gather copies nonidentity probe selections with Arrow `take`. Deferred selection is a possible representation change, not a drop-in optimization: downstream expressions, dictionaries, lifetimes and admission must support it. |
| Downstream pressure | ClickHouse `JoiningTransform::prepare` stops requesting input when the output port cannot push. `readExecute` retains `join_result` and advances it with `next()`. | Our pull-driven stream already has the corresponding basic lifecycle. Investigate the cost inside it rather than claiming streaming is absent. |
| Large single-key expansion | ClickHouse `HashJoinResult` retains `GenerateCurrentRowState`, including row offsets and optional row/byte limits, across output calls. | Output bounds must handle a single key with many duplicates. A bound on input rows alone is insufficient. These source limits do not prove strict query-wide byte admission. |
| Current gather overhead | Our active stream collects `(usize, usize)` build positions plus `usize` probe positions, then invokes `create_joined_batch`. A separate `create_joined_batch_u32` helper already exists in the collected path. | A bounded direct-index emission experiment may avoid repacking, but only after active-path measurements. Preserve filters, exact key equality, sentinel domains and multi-batch build offsets. |

Source entry points:

- [Current inner stream](../src/physical/operators/hash_join.rs), functions
  `HashJoinExec::execute`, `InnerProbeStream::next_batch`, `probe_vectorized`,
  `create_joined_batch` and `create_joined_batch_u32`.
- [DuckDB join output](/media/afilipchik/nvme6tb/src/duckdb/duckdb/src/execution/join_hashtable.cpp:993).
- [ClickHouse processor](/media/afilipchik/nvme6tb/src/clickhouse/ClickHouse/src/Processors/Transforms/JoiningTransform.cpp:63)
  and [retained output state](/media/afilipchik/nvme6tb/src/clickhouse/ClickHouse/src/Interpreters/HashJoin/HashJoinResult.h:12).

## Bounded next implementation

1. Add opt-in per-stream phase counters to the active inner stream: input wait,
   key preparation, candidate traversal, residual filtering, and output gather.
   Record input/output batches and rows, candidate counts and completion versus
   drop. Instrument build initialization separately. Distinguish nested and
   overlapping wall intervals from exclusive CPU; cooperative yields are not
   CPU work. Keep timers and per-row accounting off in ordinary latency runs.
2. Validate instrumented and ordinary outputs against the same typed oracle for
   the five unresolved raw queries, plus duplicate-heavy, selective and empty
   synthetic joins. Report which physical routes actually emit counters.
3. Select one measured shared cost. If index conversion/gather dominates, test
   bounded direct index emission within the existing cursor. If traversal
   dominates, test batched candidate resolution. Do not copy an upstream whole
   operator or infer dominance from source appearance.
4. Preserve nullable and dictionary keys, multi-column equality, residual ON
   filters, duplicate chains spanning output boundaries, multiple input
   partitions, cancellation and admission failure. Verify first output before
   complete probe consumption and no source replay.
5. Freeze the candidate and run balanced latency comparisons with profiling off,
   followed by the provider and resource gates. No full-suite score is valid
   while required queries time out or reference workers fail.

The active stream still uses fallible ordinary vectors for candidate scratch;
the fact that they have a row bound does not establish complete query-pool
accounting. Any change must preserve existing prepared-input envelope contracts
and identify direct execution coverage separately.

The source comparison itself changed documentation and benchmark evidence only.
A subsequent [instrumentation implementation](active-join-profile-2026-09-08.md)
now adds opt-in active-path counters and passes32 selected contract tests. Its
optimized build and phase measurements are complete; the subsequent
[bounded input candidate](parallel-aggregate-input-2026-09-08.md) repairs a
reproduced serial-polling bottleneck. The scheduling binary remains a distinct
historical control. See the
[current measurements](aggregate-batch-dispatch-2026-09-08.md) and
[earlier upstream comparison](local-engine-source-comparison-2026-09-07.md).

## Pipeline scheduling follow-up

Rechecked the local commits above on September8. DuckDB's
[`Pipeline::ScheduleParallel`](/media/afilipchik/nvme6tb/src/duckdb/duckdb/src/parallel/pipeline.cpp:101)
requires parallel capability from the sink, source and every intermediate
operator, then limits task count by each operator's state, the source, sink
and scheduler. It calls `LaunchScanTasks`; parallelism is a property of the
whole executable pipeline, not just the aggregate's hash table. ClickHouse's
[`JoiningTransform::prepare`](/media/afilipchik/nvme6tb/src/clickhouse/ClickHouse/src/Processors/Transforms/JoiningTransform.cpp:86)
sets inputs not-needed when the output port cannot push. These are source
contracts, not benchmark measurements or claims of exhaustive byte accounting.

Our diagnosed failure matches the first distinction: parallel aggregate owners
were fed by a serially polled frontier. The new input tasks produce actual CPU
overlap and large paired gains on Q5/Q10/Q20. Output demand permits remain held
through consumption, preserving downstream pressure. The remaining serial cases
are explicit capability boundaries:

| Next operator family | Contract that must precede parallel admission | Regression gates |
|---|---|---|
| Computed projections | Bound final copied buffers separately from intermediate expression allocations and query-pool dependencies; preserve once-only prepared streams. Fixed result types alone are insufficient. | Decimal arithmetic/overflow, date extraction, NULLs, dictionary values, prefix errors, two-slot admission and cancellation. |
| Variable-width scans | Validate payload extents and offsets; bound copied batches separately from decoder scratch and retained row groups. Never use sample string lengths as a bound. | A single oversized value, skewed lengths, slices, NULLs, dictionaries, malformed metadata, low budgets and source errors. |
| Outer join output | Resume bounded duplicate expansion and unmatched-row emission without replay; retain match state through residual ON filters and cancellation. | Duplicate chains crossing batches, no matches, NULL keys, false/NULL residuals, both build orientations and all partitions. |

Implement one family at a time after the current candidate's resource checks.
Prove its bound and lifecycle with small tests before measuring canonical SQL.
Re-run the unchanged serial cases as controls and require balanced typed runs
before updating provider scores. This order follows observed missing capability,
not special treatment of a query number. Upstream whole-operator replacement is
not justified by these measurements.

## September 8: collected outer-join duplicate traversal

Read-only inspection during release77024 finds another attribution boundary.
`HashJoinExec::execute` traverses each collected outer-join probe batch before
`probe_hash_table`: it evaluates keys, hashes/traverses the vectorized table and
counts candidates against1,048,576. The later Left/Right/Full probe evaluates keys
and traverses again. `t_probe` starts after the preflight, so it does not include
that earlier work. `VectorizedHashTable::probe_batch` separately checks the same
candidate count while appending and uses fallible vector growth.

This is a confirmed source-level duplicate traversal, not a measured fraction of
Q13 time or a reproduced volatile-expression wrong answer. The actual appending
limit means a changed volatile key does not simply bypass all candidate limits.
Do not remove the preflight on that observation alone: early refusal, parallel
in-flight buffers, scratch allocation and the incomplete query-pool ownership
contract still require review. Reusing evaluated keys or replacing the collected
outer path with an admitted bounded cursor must preserve residual ON filters,
both build orientations, unmatched-row tracking across partitions and cancellation.

A future diagnostic must include preflight separately from actual probe/filter/
gather and from input collection. The current direct fixed-input experiment is
kept separate so its before/after attribution remains meaningful.
