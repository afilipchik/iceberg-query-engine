# Task 004 — Streaming native-table scan into spilling consumers (stream D)

Status: in progress (started 2026-08-29)

## Design (decided before code)

Mirrors epic Architecture Decision 4: a NEW operator, not a rewrite of
`NativeTable::scan()`.

- `NativeStreamingScanExec` (`src/physical/operators/native_scan.rs`):
  segment-at-a-time lazy streaming over `ipc_cache::read_row_group` (mmap
  zero-copy), applying BOTH `filter_deleted_rows` (deletion vectors) and
  `segment_might_match` pruning via new public helpers on `NativeTable`
  (`streaming_segment_ids`, `read_segment_batches`).
- Planner gate (`src/physical/planner.rs`, Scan arm): the streaming path
  engages ONLY when (a) the provider is a `NativeTable`, (b) its
  `scan_budget_exceeded()` (i.e. the materializing scan WOULD refuse), and
  (c) the Scan node has an Aggregate ancestor in the logical plan (pre-pass
  `collect_agg_covered_scans`, keyed by ScanNode address). In-budget tables
  take the EXACT pre-existing path — zero behavior/perf change, so
  dense-direct + GPU eligibility invariants hold trivially.
- Why "Aggregate ancestor" and not "Aggregate or Join": a bare
  `SELECT * FROM a JOIN b` materializes the join output into the
  QueryResult at the root — streaming the scans would trade a clean
  refusal for a possible OOM at result collection. An aggregate ancestor
  bounds what reaches the root. The criterion's join-consumer shape
  (aggregate over join) passes through both and streams.
- `try_extract_native_dense_source` declines when over budget so the
  dense-direct route (whose execution-time `scan_with_filter` would refuse)
  falls through to the generic spillable aggregate over the streaming scan.
- `check_scan_budget` stays untouched as the guard for genuinely
  materializing shapes (raw `SELECT *`, filter/project-only, ORDER BY-only)
  — boundary documented in CLAUDE.md + pinning tests.

## Log

- 2026-08-29: read 004.md, epic AD4, native_table.rs, planner.rs Scan arm,
  spillable.rs fused-streaming path (read-only — task 007's file),
  oom_cap_harness.{rs,sh}. Design above fixed.
