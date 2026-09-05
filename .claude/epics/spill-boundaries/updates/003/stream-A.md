---
issue: 003
stream: boundaries
started: 2026-09-05T07:10:00Z
status: in_progress
---
## Scope
LEFT/RIGHT/FULL outer-join spill (spillable.rs), on top of task 002's filter.

## Progress (2026-09-05)
- Design: the outer joins reuse the SEMI/ANTI bitmaps for their PRESERVED
  side(s). `SpillJoinCtx` gained `preserve_probe()` / `preserve_build()`
  (LEFT: probe when build = right, build when build = left; RIGHT: build =
  right always via `execute`'s forced swap; FULL: both), `probe_bitmap()` /
  `build_bitmap()` (SEMI/ANTI's orientation OR a preserved side),
  `emit_pairs()` (INNER + outer), and `probe_emit()` / `build_emit()` ->
  `RowEmit::{None, Matched, Unmatched, UnmatchedNullExtended}`.
- ONE per-batch probe routine, `probe_batch_against_table`, replaces
  `probe_partition` / `probe_match_flags` / `accumulate_probe_matches` /
  `mark_build_matches_atomic`: a single `visit_candidate_pairs` pass (key
  equality + ON filter) emits the INNER pairs, marks the build bitmap
  (resident partition across the whole probe stream / read-back chunk) and
  the probe bitmap (per probe batch, carried across chunks) as the join
  needs. `emit_probe_rows` / `emit_build_rows` emit SEMI's matched, ANTI's
  unmatched, or the preserved side's unmatched rows NULL-extended
  (`null_side_columns` typed from the new unpruned `full_schema`, then the
  `retained` mask, then `batch_with_actual_types`).
- Phase A: resident partitions emit pairs + the probe-side rows at once (a
  probe row's status is final against its resident partition); a partition
  with NO build rows emits the piece whole for ANTI and NULL-extended for a
  preserved probe side (the task-004 dropped-batch hazard, generalized).
  Phase A': every resident build bitmap emits per `build_emit`. Phase B (K-way
  unchanged, budget unchanged): per chunk, pairs + both bitmaps in one pass;
  a preserved build side emits per chunk (chunks partition build rows
  disjointly); a preserved probe side emits in the final pass after the LAST
  chunk (a row unmatched in chunk 1 may match in chunk 2). Both bitmaps live
  with the partition's job.
- Refusal narrowed to CROSS/SINGLE/MARK by name ("supports INNER, LEFT,
  RIGHT, FULL, SEMI and ANTI joins only").
- Tests (row-for-row vs `naive_join_rows`, spill path AND the in-memory
  delegate): `outer_join_spill_is_cell_exact` (10 fixtures: LEFT x both
  orientations x dense/sparse, RIGHT dense/sparse, FULL x both x dense/sparse;
  NULL keys throughout), `outer_join_spill_with_on_filter_is_cell_exact` (9:
  compiled + gathered filters), `outer_join_spill_with_retained_mask_is_cell_exact`
  (7: keys-dropped and keys+payloads-dropped masks, with/without filter — a
  mask dropping a FILTER column is unreachable: the planner force-keeps filter
  columns and the delegate prunes its build batches before evaluating),
  `outer_join_spill_processed_in_parallel_is_cell_exact` (5, K=3),
  `cross_single_mark_spill_refused_by_name` (replaces the outer-join refusal
  pin). `tests/spill_tests.rs::outer_join_spill_matches_in_memory` replaces
  `left_join_spill_fails_loudly_not_wrong`: orders LEFT JOIN lineitem
  (build = preserved) @256KB, lineitem LEFT JOIN orders with an ON filter
  (probe = preserved) @8KB, RIGHT @8KB, FULL with an ON filter @8KB — each
  with COUNT(*) vs COUNT(other key) so an inner-shaped answer cannot pass;
  `left_join_unmatched_build_rows_preserved` untouched and green.
- Acceptance: `sweep22.py qe_003a parquet256M_003 --data data/tpch-100gb 256M
  3600 20` (MemoryMax=32G / QE_MEM_CAP=32G / QE_SPILL_DEBUG=1): **Q20
  33,065ms, 40,196 rows, CELL-EXACT, join_spill_starts=1, hash-check-ok=118,
  HASH-MISMATCH=0** (`.scratch/sb/logs/sweep_parquet256M_003.log`). Refused
  by name ("LEFT/RIGHT/FULL outer joins are not spillable") on 2026-09-04.
- Chaos (`.scratch/sb/run_chaos.sh`, harness built from the 003 tree,
  MemoryMax=8G / QE_MEM_CAP=6G): 200 @seed 20260905 tpch-10mb (179
  genuine-disk) + 100 @seed 777 tpch-100mb (92 genuine-disk) = **300/300, 0
  disk-expected-but-missing, 16,656 + 9,398 hash-check-ok, 0 HASH-MISMATCH**
  (`.scratch/sb/chaos/batch_003a_{a,b}.log`).
- Gates (debug profile): `cargo test --lib -- spillable hash_join` **55/55**;
  `spill_tests` **13/13**; `native_dictionary_semi_anti` **4/4**; fmt clean.
- SF=10 native band (`native_bench_compare.py --no-duckdb --no-iceberg --sf 10
  --binary qe_003a --memory-limit 40G --iterations 2`, MemoryMax=48G /
  QE_MEM_CAP=44G, quiet machine, load ~2.7): **22/22 OK, TOTAL 5,113.42ms**
  (`.scratch/sb/logs/sf10_native_003a.log`) — under the 5,288-5,667ms band's
  lower edge; nothing spills at 40G, so the changed join code is not on this
  path (Q09 1,161ms, Q13 648ms).
- Task-001 follow-up found by the gates: `native_streaming_scan_tests::
  other_materializing_shapes_still_refuse` pinned "ORDER BY-only refuses
  (ExternalSortExec is not a gate yet)" — the boundary 001 was told to move.
  Rewritten to the new contract: filter-only and LIMIT-only still refuse by
  name; ORDER BY-only and a bare join over an over-budget table now complete
  cell-exact (2 new tests, 9/9).
