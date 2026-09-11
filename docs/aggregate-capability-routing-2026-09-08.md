# Aggregate capability routing — September 8, 2026

The latest equivalent-SQL SF10 experiment reproduces a performance discontinuity
between bare and wrapped aggregates. Both frozen candidates time out on all
wrapped-form warmups at the strict fresh DuckDB ceiling; the bare form completes.
See [measurement and limits](admitted-aggregate-frontier-2026-09-08.md). This is a
systemic routing problem worth addressing, not evidence that scheduling alone
explains the lost time or that a particular replacement has already succeeded.

## Source evidence

Our `src/physical/planner.rs::try_extract_parquet_source` recognizes Scan,
Filter directly over Scan, and recursively Project. It does not recognize
SubqueryAlias. `lower_aggregate_cpu` uses that extraction to select a direct-file
MorselAggregate; otherwise it constructs the generic aggregate pipeline. The
initialization-only probes show that the tested aliases/derived table/CTE select
the generic route. Neither source inspection nor these probes prove a wrong
answer from existing Project extraction; that remains a separate semantic audit.

The local DuckDB snapshot `1c27c54f27bea397d18d86f7c0d6a9f1d4aa85f8`, file
`src/execution/physical_plan/plan_aggregate.cpp`, makes the relevant distinction
explicit. `CanUsePartitionedAggregate` walks projections only when the required
expressions are bound references and remaps their indices at each level. It maps
scan projection IDs to base columns before consulting source partition metadata.
Unsupported operators return false. `ExtractAggregateExpressions` constructs a
projection for group/aggregate arguments and replaces them with bound references;
the selected aggregate consumes the resulting physical child. Thus ordinary
aggregate execution and source-specific eligibility have separate contracts.
This is a local source comparison, not a claim about the benchmark wheel's exact
implementation or current upstream. No third-party code was copied.

ClickHouse's demand/cancellation and DuckDB's dictionary-vector observations are
recorded separately in [encoded input review](encoded-input-source-review-2026-09-08.md).
Those ideas complement capability propagation; none establishes a measured
exclusive bottleneck in this engine.

## Implementation sequence and acceptance

1. Add small typed metamorphic regressions for equivalent bare, derived-table,
   single-use CTE and table-column-alias aggregates. Include reordered/duplicate
   projections, colliding names and qualified references, NULL groups, all-NULL
   values, empty input, multiple partitions, Decimal SUM/AVG and computed
   expressions. Verify output names/types as well as values against an independent
   oracle. Add negative cases containing LIMIT, DISTINCT, volatile/subquery
   expressions and filters that cannot safely move. Do not assert every query
   must fuse: assert semantic preservation and explicit eligibility.
2. Replace ad hoc source peeling with a structured extraction result carrying
   exact output-to-source bindings, required expressions, predicates and provider
   capability. Compose mappings through each transparent node; preserve aliases
   in the public output schema. Reject ambiguous/unsupported transformations
   before execution. Do not use names, cardinality estimates or sampled uniqueness
   as proofs. Audit all callers before changing the extraction contract.
3. Apply one eligibility implementation to planning and execution. Retain generic
   execution for genuinely non-fusible input. Avoid widening the direct-file path
   until its query-budget/spill semantics meet the same resource contract. The
   current generic frontier's admitted ownership remains independently useful.
4. Run focused semantic and forced-spill/admission tests, library/integration
   gates, then freeze a new binary with before/after input hashes. Rerun the exact
   four-form component with the same data, fresh DuckDB oracle, strict warmup and
   query ceilings, balanced blocks and typed outputs. Retain these old timeouts;
   compare completion first and timing only for completed valid measurements.
5. If wrappers still fail, use bounded component experiments to distinguish
   decode/flattening, batch handoff, grouping and spill setup. Sweep cardinality,
   selectivity, encoding and fragmentation; do not infer exclusive CPU time by
   summing overlapping producer spans. Broader generic-path work must improve
   non-fusible/provider input too, rather than only routing this example away.
6. Protect canonical SF10, Q2/Q19, Native/Iceberg/Lance, IPC/GPU residency and
   budget/concurrency gates before making any leadership claim. A component
   improvement or one passing bare aggregate does not certify the suite.

This task continues the existing realistic-benchmarks-duckdb-leadership epic.
The latest scheduling candidate has passed semantic tests but has not met its
performance acceptance criterion. Overall DuckDB leadership remains unachieved.
