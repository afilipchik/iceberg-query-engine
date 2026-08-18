# Execution Status — duckdb-parity

Updated: 2026-08-18T00:55Z

## In progress
- 001 baseline pin: parquet sweeps done (91.9s cold-ish, **89.3s warm**, 22/22
  MATCH @ HEAD ad3881a). DuckDB same-files per-query bench running.
  Lance sweep pending (needs `--features lance` build after measurements).

## Queue discipline
All measurements serialized on the idle box. Order:
1. duckdb_files_bench_sf100.py (running)
2. lance-feature build (CPU-saturating, not a measurement)
3. engine lance sweep
4. write baseline.md, close 001
5. Attribution runs: Q18 (HJ_TIMING+AGG_TIMING), Q9 (HJ_TIMING), Q4 (HJ_TIMING, RT_DISABLE A/B)

## Notes for 002 design (from code reading)
- Fused streaming agg ALREADY exists: `SpillableHashAggregateExec::
  execute_fused_streaming` (spillable.rs:726) — workers + channels, disjoint
  mode. So 2a's win is INSIDE the probe: skip joined-batch construction,
  feed thread-local `AggregationState` at the probe site (hash_join.rs
  probe_vectorized Inner arm, line ~2360 par_iter over probe batches).
- `create_joined_batch` (hash_join.rs:3268): row-store row-wise gather or
  shared take-index per column; probe side has identity fast path.
- HashJoinExec::execute materializes a partition's whole probe result Vec
  before streaming (stream::iter) — no intra-partition pipelining.
- HashAggregateExec (non-spillable path) fully materializes ALL input
  partitions before aggregating; spillable fused path does not.
- `AggregationState::process_batch(&batch, group_by, agg_inputs)` evaluates
  exprs by name against the batch — fused sink must present a batch whose
  schema resolves those names (minimal schema of referenced columns).
