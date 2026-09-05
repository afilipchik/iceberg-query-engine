---
issue: 002
stream: boundaries
started: 2026-09-05T06:40:00Z
status: completed
---
## Scope
ON-clause filter on the join spill path (spillable.rs), reusing hash_join's
`CompiledFilter` / `create_combined_batch` (made `pub(crate)`).

## Progress (2026-09-05)
- Removed the `filter.is_some()` refusal in `finish_via_spill`.
- `PairFilter` (spillable.rs): compiled once per `execute_spill_path` call from
  the build/probe child schemas + `swapped` (so the combined schema is exactly
  the planner's left ++ right); per batch it picks hash_join's `CompiledFilter`
  fast path only when BOTH compared columns' ACTUAL array types are
  Int64/Int32/Float64/Utf8 (a Dictionary column or a Date would make
  `evaluate` return false for every pair) and rejects NULL slots itself
  (`evaluate` reads a NULL slot's placeholder as data); otherwise the candidate
  pairs are gathered in 65,536-pair chunks into a combined batch, evaluated
  once with `evaluate_expr`, and a pair passes iff `valid && value`.
- `visit_candidate_pairs`: THE single candidate-pair enumeration (key equality
  + filter) used by INNER pair emission, probe-side SEMI/ANTI (first passing
  pair decides, already-flagged rows skipped) and build-side SEMI/ANTI marking,
  in phase A (resident partitions) and phase B (chunked read-back) alike. The
  non-atomic `mark_build_matches` (dead) was removed.
- Tests vs a new ROW-level naive oracle (`naive_join_rows`: key equality on
  column 0, optional filter closure, NULL never matches; fixtures gained an
  `lv`/`rv = i % 7` value column): `filtered_inner_spill_is_cell_exact` (7
  fixtures: both orientations x dense/sparse x compiled `lv < rv` / gathered
  `lv + 1 < rv`) and `filtered_semi_anti_spill_is_cell_exact` (14 fixtures:
  SEMI/ANTI x both orientations x dense/sparse x both filter paths) — spill
  path AND the in-memory delegate both equal the oracle row-for-row.
  `tests/spill_tests.rs::filtered_semi_anti_join_spill_matches_in_memory`:
  EXISTS with `l_suppkey <> o_custkey` (compiled shape) and NOT EXISTS with
  `l_extendedprice > o_totalprice / 4` (gathered shape) at 8KB, cell-exact vs
  unlimited.
- Acceptance: `sweep22.py qe_002a parquet256M_002 --data data/tpch-100gb 256M
  3600 21` (MemoryMax=32G / QE_MEM_CAP=32G / QE_SPILL_DEBUG=1):
  **Q21 74,485ms, 100 rows, CELL-EXACT, join_spill_starts=3, hash-check-ok=334,
  HASH-MISMATCH=0** (`.scratch/sb/logs/sweep_parquet256M_002.log`,
  `.scratch/sb/parquet256M_002/`). Refused by name on 2026-09-04.
