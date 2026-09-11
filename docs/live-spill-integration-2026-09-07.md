# Production partial-state spill integration

The production SpillableHashAggregateExec now calls `morsel_agg/live_spill.rs` for
its supported simple grouped aggregation path. It binds exact key/state/output
types before input starts, opens each declared partition once, polls an owning
FuturesUnordered/SelectAll frontier, evaluates inputs once and passes retained
arrays to IngestionController. The old worker abort/discard/Ok(None) replay path
was removed. Pressure flushes committed partial states and resumes the same
batch; only unsupported capabilities can choose the ordinary path before input.

Both original tests in `fused_aggregate_budget_transition.rs` now pass unchanged:
shared/disjoint planner hints, duplicates, NULLs, exact decimal SUM, weighted AVG,
MIN/MAX and arbitrary selections, actual spill and one execution per partition.
The original `exact_decimal_spill_merges_match_an_independent_integer_oracle`
also passes unchanged with its256KiB query limit, actual spill,1000 groups and
independent exact decimal values. These three reproduced failures are closed for
the tested production route; wider resource/performance certification remains open.

## Execution and ownership

The route currently uses **one aggregation working set**, with concurrent input
partition polling and no per-worker scatter/coalescing queues. The disjoint hint
is retained as a planner hint but does not create separate state workers here.
This makes input ownership and budget transitions explicit; it is a provisional
CPU policy whose performance must be measured and improved. It is not a claim
that serial aggregation beats the preceding parallel implementation.

The group working limit uses the smaller of the operator threshold and one quarter
of the query pool, divided by the existing per-group estimate. Headroom is needed
for retained input, old-plus-new growth, merging and output. The estimate remains
advisory: actual allocations are independently admitted by the pool. The first
256KiB run completed in memory and therefore failed the unchanged test's actual-spill
assertion; that outcome is preserved. The generic headroom policy then exercises
and completes the required disk path without changing the query or its budget.

Frontier and evaluated-column metadata are admitted. Received batches receive a
consumer retention lease; this is after provider construction and does not certify
that construction's allocation path. Expression evaluation enters the existing
query-pool scope; kernels outside its admitted implementations still need audit.
Input-admission denial can flush resident state before retrying admission, without
pulling input or evaluating expressions again. Results are built in64-row ranges
into ReservedVec; its owning iterator retains collection admission until consumed.
Array payload/type/batch owners survive the controller and query state.

COUNT(*) and qualified wildcards bind an Int64 presence input and use the admitted
literal evaluator. The initial full library run exposed this missing binding in
two distributed tests. All11 distributed coordinator tests pass after correction.
Ingestion-run bytes are recorded through existing spill metrics; compaction and
repartition IO are not included in that byte counter.

Input execute/stream errors preserve their original classification. Input panics
become named query errors, and dropping the owning frontier cancels pending input
futures/streams. The old lifecycle fixture used a missing column as a worker
failure. Binding now rejects that before any input opens, so the test separates
that case (zero opens) from a real runtime division-by-zero failure (one open per
partition and pending-peer cleanup). Runtime failure coverage was retained and a
binding test was added. The two replay tests and256KiB decimal test were not edited.

## Current limits and evidence

Post-filters and unsupported normalized layouts select the ordinary route before
input consumption. Global queries still use the ordinary production route, although
the new controller independently supports empty global output. Provider construction,
all evaluator kernels, ordinary paths, wider memory/concurrency matrices, parallel
state policy and post-filter output admission are not certified by this change.

Final validation: **887 library passes,10 explicit ignores;25 integration passes,
zero failures**. The ignores are the prior flatten-dependent-join test, eight
separate real-CUDA tests, and the dedicated IPC sidecar test. Formatting passes.
The selected integration gate covers float extrema, both replay cases, eight
lifecycle cases and all12 systemic numeric tests. Earlier compilation, no-spill,
lifecycle-assumption and COUNT(*) failures are preserved rather than counted as
final coverage. [Evidence](benchmarks/2026-09-07-live-spill-integration/) contains
21 verified members and658 current source-input hashes. Manifest SHA256:
`cd8a7f92082b16da2f82f32ffd0e74a88cee383770c733f053a00670f2489f71`.

No optimized benchmark result is claimed here. The next required gate is a fresh
canonical smoke/performance run, followed by current SF10/provider/residency and
protected comparisons. The old frozen benchmark binaries do not contain this route.
