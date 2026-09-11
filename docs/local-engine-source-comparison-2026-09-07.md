# Local DuckDB and ClickHouse comparison: next shared bottlenecks

The local sources support continuing with shared execution contracts rather than
adding query-specific shortcuts. The current arena repairs allocation and
finalization cost, but does not yet give this engine typed batch aggregate
updates, a uniform incremental provider boundary, or an integrated spillable
state lifecycle. Those are separate changes; adopting another engine's data
structure alone would not supply them.

This is a source comparison and an interpretation of existing measurements.
Neither external engine was built or benchmarked for this comparison. The
[653/662 SF10 comparison](dense-domain-full-sf10-2026-09-07.md) is complete.
The subsequent decimal-bound experiment compiles and has59 selected passes;
an existing parallel spill-completion failure reproduces with both helpers.
Its optimized performance remains pending, and it is absent from both benchmark binaries.

## Exact sources

Both local checkouts had clean tracked-file status when inspected:

- DuckDB: `/media/afilipchik/nvme6tb/src/duckdb/duckdb`, commit
  `1c27c54f27bea397d18d86f7c0d6a9f1d4aa85f8` (2026-01-24).
- ClickHouse: `/media/afilipchik/nvme6tb/src/clickhouse/ClickHouse`, commit
  `a1b25f3f4beb3ba49aa3b73671cc244185331b86` (2026-01-25).

These are January source snapshots, not claims about the newest releases. The
DuckDB checkout is **not** the benchmark's pinned DuckDB 1.4.4 wheel and is also
different from the earlier `.scratch/duckdb` research checkout. File hashes and
profile-input hashes are in [the evidence archive](benchmarks/2026-09-07-local-engine-source-comparison/). The engine
comparison uses frozen662 production inputs and its archived component run.

## What the implementations actually do

| Concern | DuckDB source | ClickHouse source | Current engine and implication |
|---|---|---|---|
| Aggregate dispatch | `GroupedAggregateHashTable::UpdateAggregates` dispatches per aggregate over a batch; `AggregateExecutor::UnaryScatterLoop` is templated on input/state/operation and separates validity/selection cases. | `Aggregator::executeAggregateInstructions` calls batch aggregate operations; state sizes and aligned offsets are computed from the bound aggregate functions. Compiled functions are optional. | `TypedArrayAccessor::update_accumulator` still selects accessor and accumulator variants inside row updates. Pre-downcasting already exists; this is not a claim that every row allocates a scalar. Typed batch updates are a distinct possible improvement over the new allocation arena. |
| State and merge | Grouped state lives in a `TupleDataLayout` with partitioned tuple storage. The radix table coordinates thread-local state, memory reservations, repartitioning and finalization. | Aggregate states have function-specific sizes in an arena; larger structures can become two-level tables, enabling parallel merge and external aggregation. | `RawRows` owns a flat `AccumulatorState` array, but shared merge still ranges/shards worker state and constructs destination rows. Bare-float/general/nested ownership and spill integration remain separate gaps. |
| Decimal arithmetic | `BindDecimalMultiply` selects the physical integer operation and overflow policy during binding. Decimal SUM also binds by internal type. | SUM uses templated state/value operations and batch routines, including architecture-specific variants. Its overflow and NULL policies must be checked independently. | `planner/numeric.rs::arithmetic` delegates decimals to Arrow Decimal128 kernels, then validates output precision. The reserved primitive output path currently excludes decimals. Exactness and admission must remain intact if this is optimized. |
| Pipeline boundary | `PipelineExecutor::ExecutePushInternal` processes chunks through operators into a sink with blocked/finished states. Aggregation remains a blocking operator where required. | `AggregatingTransform` consumes chunks through processor ports and checks output readiness; merging/generation becomes a subsequent pipeline stage. | Streaming Parquet and bounded prepared joins already exist. However, the planner's ordinary provider fallback calls `scan_with_filter` and constructs `MemoryTableExec`; Lance's scan collects its stream. Native streaming is selected only under its specific memory-pressure/spill-coverage conditions. |

The corresponding local source entry points are:

- [DuckDB aggregate update](/media/afilipchik/nvme6tb/src/duckdb/duckdb/src/execution/aggregate_hashtable.cpp:533),
  [typed scatter](/media/afilipchik/nvme6tb/src/duckdb/duckdb/src/include/duckdb/common/vector_operations/aggregate_executor.hpp:107),
  [radix sink](/media/afilipchik/nvme6tb/src/duckdb/duckdb/src/execution/radix_partitioned_hashtable.cpp:529),
  [chunk pipeline](/media/afilipchik/nvme6tb/src/duckdb/duckdb/src/parallel/pipeline_executor.cpp:304),
  [decimal binding](/media/afilipchik/nvme6tb/src/duckdb/duckdb/src/function/scalar/operator/arithmetic.cpp:836).
- [ClickHouse state layout](/media/afilipchik/nvme6tb/src/clickhouse/ClickHouse/src/Interpreters/Aggregator.cpp:563),
  [batch updates](/media/afilipchik/nvme6tb/src/clickhouse/ClickHouse/src/Interpreters/Aggregator.cpp:1460),
  [two-level/external aggregation](/media/afilipchik/nvme6tb/src/clickhouse/ClickHouse/src/Interpreters/Aggregator.cpp:1815),
  [SUM implementation](/media/afilipchik/nvme6tb/src/clickhouse/ClickHouse/src/AggregateFunctions/AggregateFunctionSum.h:51),
  [processor backpressure](/media/afilipchik/nvme6tb/src/clickhouse/ClickHouse/src/Processors/Transforms/AggregatingTransform.cpp:776).
- Our [typed accessor](../src/physical/morsel_agg.rs),
  [shared numeric path](../src/planner/numeric.rs),
  [reserved primitive kernels](../src/planner/reserved_numeric.rs),
  [provider routing](../src/physical/planner.rs), and
  [Lance reader](../src/storage/lance.rs).

This comparison does not prove that a Rust enum branch is the dominant CPU cost,
that a C++ loop is inherently faster, or that either external engine is free of
resource/lifecycle problems. For example, ClickHouse's inspected processor has
an explicit outstanding early-output state-release concern. Copying overflow
behavior, statistics-based shortcuts, or lifetime assumptions without matching
this engine's contracts would be unsafe.

## What the existing measurements prioritize

Three steady instrumented candidate samples yield the following medians for Q1:

| Track | Query wall ms | Planning wall ms | Aggregate-expression worker ms | State/key worker ms |
|---|---:|---:|---:|---:|
| Raw Parquet | 721.0 | 0.09 | 4335.7 | 2904.7 |
| Iceberg | 687.8 | 0.08 | 4169.1 | 2803.0 |
| Lance | 862.7 | 247.95 | 4728.1 | 4970.9 |
| Extra IPC | 662.0 | 0.43 | 5303.4 | 5171.7 |

Worker values are cumulative, nested wall intervals across parallel workers,
not exclusive CPU. They cannot be added to query wall time or used to claim an
exact fraction of engine-versus-DuckDB excess. Native Q1 is absent because its
control timed out in this diagnostic; do not silently substitute another run.
The full source/profile provenance is preserved separately from latency gates.

These observations make aggregate-expression work at least as important to
investigate as state updates for this low-cardinality workload. Q1 finalization
is tiny, so the successful high-cardinality Q18 arena change should not be
expected to fix Q1. The measured Lance planning interval is consistent with
provider materialization in the source, but does not independently attribute
all of that interval to scanning. The uninstrumented first-order raw/native
suite results still trail DuckDB by 2.34/3.37 times. More finalization tuning is
not a sufficient general path forward.

## Bounded next experiments and implementation gates

1. **Separate decimal expression costs before another aggregate rewrite.**
   After the current performance gate reaches a terminal state, use the existing
   Q1/Q6/Q14 expression shapes plus generated independent decimal arrays. Measure
   arithmetic, casts, validity construction, precision validation and output
   allocation separately. Inspect optimized code before asserting that repeated
   `10^precision` calculation survives compiler optimization. The first candidate
   should retain current logical precision/scale and checked arithmetic; do not
   narrow SQL result types to imitate DuckDB's physical representation. Evaluate
   a query-owned Decimal128 output kernel with batch-level constants and direct
   admitted output, including all temporary casts and validity buffers.
2. **Only then test typed aggregate batch updates.** Separate group-index
   construction from aggregate updates; bind the operation and input/state type
   once per batch or plan. Start with a general supported aggregate family,
   never a query number, column name, or fixture range. Keep exact wide decimal
   sums, all-NULL/zero distinction, dictionary domains, duplicate groups, and
   overflow errors. Any typed state layout must cover growth, merge, destruction,
   spill serialization and query-pool ownership, not just the ingestion loop.
   Compare against the existing arena; a standalone C++/Rust layout model is not
   enough to accept the production change.
3. **Make incremental provider execution an explicit vertical slice.** Move
   data scanning out of the ordinary planning fallback and expose a snapshot-bound
   incremental source with projection, residual filters, cancellation and bounded
   queued buffers. Preserve native dense/GPU capabilities through explicit
   descriptors rather than relying on a materialized `MemoryTableExec` shape.
   Test first-output-before-full-input and slow consumers, then CTE sharing,
   deletes/evolution and spill. This is task006's contract, not a switch that
   blindly enables an existing streaming operator everywhere.

Run one production experiment at a time, preserving the current 662 control.
Require independent typed oracles, real admission/refusal and spill checks,
then balanced full provider comparisons. Preserve the native Q1 diagnostic
failure, the new first-order Lance Q20 flag, and the older Lance Q17 regression.
Do not remove decimal validation, use approximate results, change the time
boundary, or relax resource budgets to obtain a speedup. The component-replacement
trigger still requires measured attribution and failed bounded fixes; this
source comparison alone does not trigger a DataFusion adapter or an engine rewrite.


## Optimized-code follow-up: repeated decimal bounds are confirmed

A bounded disassembly of frozen662's `planner::numeric::arithmetic` confirms
that the compiler has **not** hoisted decimal precision-bound exponentiation
out of the coefficient loop. At `0x5ed85de` the loop initializes base10; the
multiplication/exponent loop spans `0x5ed8600` through `0x5ed8643`, returns to the
coefficient comparison at `0x5ed8542`, then advances to the next coefficient at
`0x5ed8556`. Thus the source-level repeated `10_u128.pow(precision)` is actual
optimized machine work, not merely a misleading source appearance.

The captured function belongs to benchmark binary SHA256
`8baa8eead1565c10997bb839165a40b9f5bac80f5f9ebd8a0e41b57b3f0062a9`.
The command, addresses, disassembly hash and interpretation are preserved in
`.scratch/engine-source-comparison/decimal-bound-codegen.json` and the adjacent
disassembly. This read-only observation launched no engine and changed no
benchmark settings. It proves redundant work exists; it does not quantify how
much query time it consumes.

The first decimal experiment should therefore isolate reusable precision bounds
before introducing a larger Decimal128 output kernel. A checked compile-time
bound table or one bound per array can preserve the existing scalar validation
contract, including invalid precision, signed extremes and exact positive/negative
overflow boundaries. Compare the optimized code and independent typed decimal
oracles, then measure actual expression and protected query costs. Preserve the
current frozen benchmark run before building or executing this candidate.


[The frozen disassembly evidence](benchmarks/2026-09-07-decimal-bound-codegen/)
is member-verified. A one-file candidate and independent decimal-digit boundary
test are prepared under `.scratch/decimal-precision-bound-repair/`. It changes
only the shared precision-bound calculation to a compile-time table; the existing
invalid-precision and unsigned-magnitude checks remain. Rustfmt and patch
applicability checks pass. The patch was subsequently **applied to the working tree** with its base hash
verified. Compilation and59 selected tests pass. The remaining spill-completion
test fails under its256KiB budget with both helpers, as documented in the
[controlled reproduction](fused-aggregate-budget-overshoot-2026-09-07.md).
Optimized performance remains pending; the completed full comparison uses the
immutable frozen662 executable. No full acceptance pass is claimed.


Historical source finding, now reproduced and partially repaired on2026-09-08:
the provider vertical slice must also cover shared prescan: shared scans
run during planning, and `provider.scan(...).ok()?` discards an error before the
ordinary path may attempt another scan. This is a source-level replay/error
contract risk, not a reproduced wrong result. Snapshot-bound repeatable file
providers and a consuming source are not interchangeable. The eventual adapter
must declare replayability, preserve the original error where retry is not
valid, and test partial consumption followed by failure. No cache policy or
provider implementation was changed during that frozen benchmark. The subsequent
[shared-prescan repair](shared-prescan-error-propagation-2026-09-08.md) now
propagates the original scan error without retry. Eager materialization, budgets
and replayability declarations remain open.

## Spill transition: implementation lessons from the local sources

The continuation's [source hashes and clean tracked status](benchmarks/2026-09-07-local-engine-source-comparison/spill-follow-up/source-provenance.json)
identify the same January snapshots listed above. No external checkout was edited.

The next implementation should treat in-memory aggregation and external
aggregation as phases of one stateful operator. Our reproduced failure is at
that boundary: `execute_fused_streaming` applies input, discards its accumulated
state on the group limit, and returns `Ok(None)` so the caller executes input
again. Both shared/disjoint consuming-source regressions fail with two executions
per partition. Separately, admission can fail inside a batch before the periodic
group-count check. Neither problem is solved by a faster hash table.

The local DuckDB implementation separates the lookup table from stored tuples.
[`GroupedAggregateHashTable::Abandon`](/media/afilipchik/nvme6tb/src/duckdb/duckdb/src/execution/aggregate_hashtable.cpp:129)
clears the pointer table and count while retaining partitioned tuples. Where
needed it first flushes/unpins the unpartitioned collection and repartitions it.
[`AcquirePartitionedData`](/media/afilipchik/nvme6tb/src/duckdb/duckdb/src/execution/aggregate_hashtable.cpp:108)
transfers stored data ownership and initializes a replacement collection.
Abandoning the index therefore does not discard the logical aggregate input.
Later combination reconciles duplicate partial groups.

[`MaybeRepartition`](/media/afilipchik/nvme6tb/src/duckdb/duckdb/src/execution/radix_partitioned_hashtable.cpp:460)
accounts for aggregate allocator bytes, partitioned tuple bytes and hash entry
capacity. It checks the temporary-memory reservation, attempts a reservation
update, then transfers/repartitions data for external execution when necessary.
[`MaxThreads`](/media/afilipchik/nvme6tb/src/duckdb/duckdb/src/execution/radix_partitioned_hashtable.cpp:667)
bounds source parallelism using reservation space and the largest partition.
Its explicit qualification matters: aggregate allocator memory itself cannot
be spilled there; it is retained and subtracted from usable memory. This source
is not evidence that every DuckDB aggregate supports arbitrarily small budgets.

DuckDB's HLL-driven adaptation can skip lookups when most keys appear unique.
It retains tuples and defers deduplication to the source phase. That is an
important distinction from our historical estimated-uniqueness rewrites:
estimates select an execution strategy, but do not prove SQL uniqueness or
permit dropping the eventual exact merge.

ClickHouse exposes the partial-state boundary more directly.
[`writeToTemporaryFile`](/media/afilipchik/nvme6tb/src/clickhouse/ClickHouse/src/Interpreters/Aggregator.cpp:1849)
uses a non-final header and writes the two-level aggregate structure, then
initializes replacement tables/arenas for continued ingestion.
[`writeToTemporaryFileImpl`](/media/afilipchik/nvme6tb/src/clickhouse/ClickHouse/src/Interpreters/Aggregator.cpp:2033)
converts each bucket with `final=false`, writes it, and transfers aggregate-state
ownership to `ColumnAggregateFunction` objects so the old container will not
destroy their state twice. Bucket serialization is distinct from final SQL output.
[`AggregateFunctionAvgBase`](/media/afilipchik/nvme6tb/src/clickhouse/ClickHouse/src/AggregateFunctions/AggregateFunctionAvg.h:131)
merges and serializes numerator and denominator; division occurs only when
inserting the final result. It does not average already-finalized partial averages.

ClickHouse's inspected threshold check follows batch execution, and external
flushing requires a suitable two-level representation. Its configurable
overflow modes can also truncate grouping. Those details are not an admission
guarantee to copy into this engine, whose default contract requires exact results
or a named refusal. Preserve the useful state/ownership design while implementing
our own pre-admission and cleanup guarantees.

### Concrete consequences for the pending fix

1. **Separate evaluated input, lookup metadata, and owned aggregate state.**
   Retain evaluated arrays and an exact applied-row cursor across a flush.
   Admission must occur before committing the next row. Dropping a lookup index
   must not discard state; memory-pressure recovery must not replay the source.
2. **Spill internal states with their merge algebra.** COUNT stores a count;
   AVG stores sum/count; decimal SUM stores the fulli128 coefficient, scale,
   seen flag and sticky overflow. A partial decimal coefficient may exceed the
   final38-digit precision and later cancel validly. Final SQL precision checks
   belong after merge. The existing raw-row Parquet spill format cannot simply
   consume final aggregate output.
3. **Admit a feasible working set, including the transition.** Reserve state,
   key/hash metadata, queued/evaluated arrays, codec scratch and merge headroom.
   Derive feasible worker/partition concurrency from that same query budget.
   A forced minimum of two workers is not evidence that two fit. Repartition
   oversized spill partitions instead of collecting all partials into memory.
4. **Make ownership and cancellation part of the design.** Spill files, partial
   states and reservations need one owner through flush, merge, error and
   cancellation. Preserve the repaired producer/worker supervision. Unsupported
   codec types must select a correct path before any input is consumed.
5. **Bind batch update functions only after this lifecycle is sound.** Both
   engines have typed batch operations and explicit state layout. This supports
   testing dispatch outside the row loop, but does not establish its performance
   benefit here. Retain the separate decimal-bound experiment and measure both.

A related source-level risk was found in disjoint routing: the drain task evaluated
group expressions to choose a worker, then the worker evaluates them again on
the routed batch. A volatile group expression could give different routing and
aggregation keys, invalidating disjoint finalization. The subsequent
[evaluated-input implementation](fused-evaluated-input-2026-09-07.md) reproduced
duplicate output groups and repaired the boundary by retaining evaluated arrays.
Its54 selected tests pass; the two memory-pressure replay regressions remain red.

The executable acceptance fixtures and sequencing are in
[the implementation contract](../.claude/epics/realistic-benchmarks-duckdb-leadership/updates/2026-09-07-fused-spill-implementation.md).
This continuation adds source evidence and corrects completed benchmark status;
it does not implement or certify the state-spill transition. The frozen662
full/protected measurements remain separate from current-source semantic tests.

## September11 follow-up: minimum working memory

The local DuckDB checkout at1c27c54f explicitly tracks minimum temporary-memory reservations. `TemporaryMemoryManager::DefaultMinimumReservation` uses the smaller of a per-thread allowance and a fraction of available managed memory; `UpdateState` combines each state's minimum, remaining size, other reservations, query limits and spill availability. These scheduling reservations are not by themselves proof that every allocation is covered by a hard cap.

`PhysicalHashJoin::PrepareFinalize` derives its minimum from the largest build partition plus pointer-table space and probe-side partitioning space. The latter depends on projected tuple width, thread count, radix partitions and block allocation size. `RadixHTGlobalSinkState` derives aggregate minimum memory from row width, sink capacity, partition buffers, hash entries and thread count; later stages adjust it for retained allocators and the largest partition. These are concrete examples of planning memory for simultaneous pipeline work, rather than assuming arbitrarily small budgets can progress by spilling.

For this engine, the next resource audit should similarly separate irreducible reader/output ownership, partition processing, retained upstream state and concurrency. The current admitted handoff's4096-byte-per-column envelope alone exceeds an8KiB budget for two columns once vector ownership is included. Reducing schema-only charges cannot eliminate that floor. Derive and validate allocation/lifetime bounds before changing allowances; preserve clean below-minimum refusal separately from feasible-budget actual-spill correctness. This recommendation does not copy DuckDB's reservation heuristics as hard memory-safety guarantees or as SQL proofs.

[Exact local source paths, revision and hashes](benchmarks/2026-09-11-duckdb-minimum-memory-source.json). Inspect `src/storage/temporary_memory_manager.cpp`, `src/execution/operator/join/physical_hash_join.cpp` around PrepareFinalize and `src/execution/radix_partitioned_hashtable.cpp` around global sink initialization in that checkout. No engine was executed for this source comparison.
