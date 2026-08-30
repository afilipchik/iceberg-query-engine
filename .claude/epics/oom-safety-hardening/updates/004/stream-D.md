# Task 004 — Streaming native-table scan into spilling consumers (stream D)

Status: CLOSED 2026-08-30 — all acceptance criteria met (see 004.md's
Outcome section for the full evidence table).

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
- 2026-08-29 (impl): NativeStreamingScanExec + NativeTable helpers +
  planner gate + dense-source decline landed (commit 981ffcb); boundary
  docs (commit d7f33a0). 3 operator tests + 7 e2e tests green;
  sql_comprehensive + native insert/delete/update suites green.
- 2026-08-29 (evidence, scenario 3): `.scratch/oom001/harness_20260829_152639`
  — native-scan COMPLETES both levers: cgroup exit 0 peak 170MB (was
  refusal at 27MB), rlimit exit 0 peak 165MB; 3 group rows, 0.10s wall,
  5.64GB table at 512MB memory_limit under 2G cap. Independently
  re-confirmed by task 007's post-fix sweep (0/143MB + 0/152MB,
  `.scratch/oom007/harness_postfix007`).
- 2026-08-29 (evidence, join consumer): re-verified tests green on the
  post-002/003/005/007 tree; join-consumer harness run in flight
  (`QE_HARNESS_SCAN_SQL` self-join of lineitem on (l_orderkey,
  l_linenumber), GROUP BY l_returnflag; QE_SPILL_DEBUG traces show
  execute_spill_path engaged: 63 spilled build partitions, 60M probe
  rows; logs `.scratch/oom004/join_evidence/`).
- 2026-08-30 (close-out): join-consumer run COMPLETED both levers
  (exit 0, peaks 2097/2048MB pinned at the 2G cap by reclaimable page
  cache, spill confirmed on both). Serve spot-check on pristine
  tpch-10gb-native: 22/22 OK, TOTAL 5513.76ms vs recorded
  5324/5667ms band — no regression. Outcome appended to 004.md,
  status: closed.
