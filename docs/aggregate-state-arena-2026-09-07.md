# Aggregate state arena implementation — 2026-09-07

## Problem and implementation

The raw integer-key aggregation path allocated `Vec<AccumulatorState>` separately
for each group and carried those allocations through shared merges. Component
profiles and the standalone layout experiment motivated a contiguous arena;
see [the measured path](aggregate-state-layout-path-2026-09-07.md). This document
records the implementation candidate, not a measured engine speedup.

`physical/morsel_agg/raw_state.rs` now stores a raw key-to-index hash table and
fixed-arity contiguous state rows. Keys and accumulator payloads use
`ReservedVec`; the pinned hashbrown table's data/control layout has a conservative
pre-allocation bound, verified against actual allocation sizes during growth.
All capacity growth is fallible and covers simultaneous old/new allocations.
`MemoryPool::clone` shares the existing hierarchy, counters and admission lock.
`AggregationState::new_with_pool` carries the query owner into ingestion from
ordinary materialized aggregation, parallel morsels and fused streaming workers.

Raw sharding pre-admits flat destination arrays and moves rows through consuming
iterators that retain the source payload leases. It does not allocate a vector
per group. Shared raw merges keep flat rows, using either a reservation-owned
checked u32 direct-address index or the admitted hash index. Raw output replays
borrowed row iterators, avoiding the previous collected reference array. NULL
keys remain a separate group. Ordinary/specialized float reconciliation and
exact decimal accumulator semantics remain explicit.

`AggregationState::merge` and raw normalization/demotion/shard methods now return
`Result`; callers propagate errors. The infallible derived state Clone was removed;
there were no production consumers requiring it. Existing integer/date/raw-key
and mixed/general output semantics are retained. No SQL-text/query-ID dispatch,
dependency change, allocator-policy change or production memory-limit increase
was introduced.

## Errors must not bypass admission

The materialized morsel dispatcher previously fell through to a vector table
on any error. It now returns the eligible morsel attempt's error. Fused streaming
workers previously preserved processing errors only under debug tracing and
could return a fallback marker after consuming input. They now retain the first
original error independently of tracing, drain/join every worker, then propagate
it. The legacy group-count threshold restart remains separate; this candidate
does not claim to complete the broader replayability audit.

New physical tests force named query-budget refusal in both dispatchers, assert
that both input partitions ran exactly once, and verify complete reservation
cleanup. They run without enabling tracing. New arena unit tests cover exact
values beyond f64 precision, signed decimal scale, heap-bearing MIN state,
duplicate keys, zero-arity rows, pinned hash layout bounds, query/ancestor refusal,
shard ownership and partial-failure cleanup. The consuming-iterator test verifies
that exhausting an iterator does not release its payload lease before drop.

## Validation boundary and preserved failures

The initial focused run passed24tests and failed the former4MiB shard-output
success fixture after state storage became query-owned: it refused an additional
65536bytes with4148896already charged. The next full run passed725tests but
showed exact completion at that old budget after removing collected row
references and under different worker overlap. Both logs are preserved. The
fixture now validates exact completion OR a named clean refusal at4MiB, since
parallel overlap changes the peak. Its separate16MiB case must complete the same
60000-key independent integer oracle. The small4096-byte refusal remains
mandatory. This changes fixture expectations, not production limits.

The subsequent default run passed727library tests and45integration tests; two
existing library ignores remain (flatten_exists and isolated IPC). The final
compact checked u32 merge index passes789Lance/GPU library tests and45integration
tests. The isolated IPC test and all8CUDA tests also pass individually:843unique
selected passes, with only the existing flatten_exists ignore outstanding.
No current-source release/performance result is available yet. Full logs and
source backups are in `.scratch/aggregate-state-arena-repair/`.

## Remaining scope

Only outer raw arena/key/hash and dense merge-index allocations are newly covered.
Nested scalar/string allocations, perfect/general-key state, specialized bare
float SUM maps/shard buffers, merge orchestration, nondecimal output arrays,
provider buffers and collected results still require further ownership work.
The mixed GroupKey compatibility merge can still materialize per-group vectors;
normal raw-only shard/merge does not. This is not complete query-wide reservation
coverage or DuckDB leadership.

Next freeze the candidate after tests, verify its optimized typed oracles and
real-spill caps, and measure the existing cross-provider component and balanced
protected/full-workload screens. The647/649 LanceQ17 regression remains open;
an unrelated state-layout gain cannot by itself clear it.


## Frozen source checkpoint

The658-file member-verified source archive has SHA256
`7028b3efb2ee4bc34c2f4cee51aacfeeffeaef667fc19206efdb5b60ee0d0cb7`;
manifest SHA256 is
`1b59c0fb04c1e531ff2985369d0b4d7fb7ba25fb44d34c8c854129a16416d0ac`.
The optimized Lance/GPU benchmark and cap build is session90785 under64GiB/jobs1.
A live process/cgroup observation is preserved beside the source metadata.
No release result is inferred from a running compiler. After successful build,
verify the binary snapshot, run the prepared optimized/resource drivers and
render the component template with the actual new binary hash before running it.
The paired component control is frozen653, not649or651.


## Additional domain gates rejected frozen658

Release build completed in10m56s. Benchmark binary SHA256:
`4f0fbbbf5b0ecd8ce6d55134957111ae198afe2c23b43679fb89a6cb751ad930`;
cap binary SHA256:
`4d1dbc2a2f6a30bb1fc8417b3d475cd524b908a628b2fdca2937159284eabb99`.
Selected optimized semantics,18decimal comparisons/two overflow refusals and
both DISTINCT spill caps pass. New probes then reproduced pre-existing extreme
key-domain and dense NULL-semantics bugs;658is not accepted for broad correctness
or performance. The current source includes their shared fixes, documented in
[the domain/NULL follow-up](dense-aggregate-domain-and-null-semantics-2026-09-07.md).
The prepared658component driver was not run. The next candidate must pass these
new gates before timing claims.
