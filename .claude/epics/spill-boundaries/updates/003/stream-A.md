---
issue: 003
stream: boundaries
started: 2026-09-05T07:10:00Z
status: completed
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

## 2026-09-05 follow-up — harness left-join / filtered-join memory (coordinator's finding)
- Coordinator's task-004 harness legs (40M-row build, 20M-row probe, 256MB
  budget, default 1G cap): pre-fix 8/8 clean refusals; HEAD 8f38273 on the
  cgroup lever: left-join build_right=0 killed (137, 1,024MB), filtered-join
  eq killed both orientations, left-join build_right=1 1,006/1,041MB,
  filtered ne 609/593MB.
- BEFORE measurement (pinned `.scratch/sb/bin/oom_cap_harness_head`, 8G scope
  so nothing is killed, QE_SPILL_DEBUG=1 + a 1s VmRSS sampler with
  timestamped traces: `.scratch/sb/trace_legs.sh` → `.scratch/sb/harness_trace/
  before_*.log`), same machine state, one leg at a time:
  | leg | phase-A peak | phase-B peak | K / chunk budget |
  |---|---|---|---|
  | left-join br0 | 292MB | **1,180MB** | 2 / 107MB |
  | left-join br1 | 309MB | **992MB** | 2 / 107MB |
  | filtered-join eq br1 | 286MB | **1,045MB** | 2 / 107MB |
  | filtered-join eq br0 | ~290MB | **1,046MB** | 2 / 107MB |
  | semi-join br0 (control) | 286MB | 653MB | 2 / 107MB |
  | semi-join br1 (control) | 268MB | 670MB | 2 / 107MB |
  In every leg RSS jumps to its plateau within ~2s of the `phase B:` trace
  line and stays there until DONE; phase A (pool of 3, per-call) is unchanged
  vs SEMI. The +350..530MB is therefore in phase B's emissions, which SEMI/
  ANTI do not have: `create_joined_batch` (pairs; 16 probe batches per group
  probed in parallel on the GLOBAL rayon pool, K=2 jobs) and `emit_build_rows`
  (a preserved build side's unmatched rows per chunk: ~790k-row chunks, 3/4
  unmatched). Both go through `gather_column`, which allocated ONE 1-row
  Arrow array PER OUTPUT ROW (`compute::take` of a single row, then a
  `concat` of all of them): ~1.2M ~200-byte allocations per chunk per column
  on rayon threads, freed elsewhere — the exact allocator-retention shape
  join-spill-streaming 003 measured for phase A. The `ne` control (no pair
  passes → no gather) staying at 609MB is the one-variable confirmation.
- Fix (spillable.rs): `gather_column` = one `compute::interleave` (or one
  `take` when all indices come from one batch) per column; every gathered
  emission (pairs, NULL-extended build rows, SEMI/ANTI build rows, resident
  phase-A' emission) sliced to `SPILL_EMIT_ROWS` = 8,192 rows per batch
  (`QE_SPILL_EMIT_ROWS` override) so the output channel's 8-batch bound is a
  byte bound too. `QE_SPILL_GATHER=legacy` keeps the old gather for the A/B.
- Controlled A/B (fixed binary `.scratch/sb/bin/oom_cap_harness_fu`, built
  from the fix into `.scratch/sb/target` — the shared release lock was held by
  the task-004 suites — same sampler, 8G scope; `.scratch/sb/harness_trace/
  {after,ab_legacygather,ab_noslice}_*.log`):
  | leg | before | legacy gather + slicing | new gather, no slicing | fix (both) |
  |---|---|---|---|---|
  | left-join br0 | 1,180MB | 1,112MB | 798MB | **776MB** |
  | left-join br1 | 992MB | — | — | **766MB** |
  | filtered-join eq br1 | 1,045MB | 1,049MB | 805MB | **780MB** |
  | filtered-join eq br0 | 1,046MB | — | — | **774MB** |
  | semi-join br0 / br1 (control) | 653 / 670MB | — | — | 661 / 663MB |
  Varying one thing at a time: slicing alone recovers ~70MB (the gather is
  the cause), the gather alone recovers ~380-410MB, both together another
  ~25MB. Phase A is untouched (per-call pool of 3 for a 256MB budget); K and
  the chunk budget are unchanged (2 / 107MB).
- Driver at the DEFAULT 1G caps, both levers (`scripts/oom_cap_harness.sh`,
  `.scratch/sb/harness_fu/br{1,0,1_ne}_driver.log`): **18/18 PASS** —
  left-join 763/791MB (br1), 807/816MB (br0); filtered-join eq 794/807MB
  (br1), 774/825MB (br0); semi-join 662/674 (br1), 615/591 (br0); anti-join
  679/717 (br1), 661/618 (br0); filtered ne 660/602. Before: 4 of the 8
  left/filtered cgroup+rlimit legs completed only on the rlimit lever
  (1,006-1,361MB), 3 were killed at 1G.
- Chaos on the fixed harness: 120/120 @seed 20260906 tpch-10mb (108
  genuine-disk, 9,968 hash-check-ok, 0 HASH-MISMATCH).
- After the measurements the probe-side emissions (`take_probe_rows`,
  `emit_probe_rows`) were sliced the same way (a skewed sparse phase-A piece
  can exceed 8,192 rows) and `run_spillable_join_opts` now pins every
  spill-path batch to `<= SPILL_EMIT_ROWS` rows; spillable+hash_join 55/55.
- Re-verified on a harness built from the FINAL tree (`oom_cap_harness_fu2`,
  incl. the probe-side slicing): the 8 target legs at the default 1G cap —
  left-join 797/803MB (br1), 795/799MB (br0); filtered-join eq 785/802MB
  (br1), 807/842MB (br0) — **8/8 PASS** on both levers
  (`.scratch/sb/harness_fu2_br{1,0}_driver.log`); chaos 100/100 @seed
  20260907 tpch-100mb (91 genuine-disk, 8,290 hash-check-ok, 0 mismatch).
  Committed as the `003 follow-up` commit together with the coordinator's
  `left-join` / `filtered-join` harness scenarios.
