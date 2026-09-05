---
issue: 001
stream: boundaries
started: 2026-09-05T06:36:53Z
status: completed
---
## Scope
Tasks 001 (planner routing) -> 002 (ON-filter spill) -> 003 (outer-join spill) as one stream; 004 by the coordinator.

## Progress
- Starting

## 2026-09-05 (task 001 — planner routing)
- Read: CLAUDE.md rules, PRD/epic/001-003, the restructured join spill path
  (spillable.rs: BuildDecision::Spill, execute_spill_path, phase A/B, SEMI/ANTI
  helpers, tests), planner.rs Scan/Join/Sort/Aggregate lowering, hash_join.rs
  CompiledFilter / filter_candidate_pairs / null-sentinel emitters,
  jss-003 memory discipline, sjc3-004 orientation facts.
- Root cause of the five native-1G refusals confirmed with a live Q11 probe
  (`.scratch/sb/probe/q_probe.py`, PLAN_DEBUG + CTE_DEBUG + QE_DEBUG_SCAN_BUDGET,
  serve --tables data/tpch-100gb-native --memory-limit 1G): Q11's HAVING subquery
  is CSE'd into a twice-referenced CTE (`__having_total_cse_0`) and
  `materialize_shared_ctes` lowers CTE plans through `create_physical_plan_inner`
  BEFORE the old `collect_agg_covered_scans` pre-pass ran — so partsupp was
  refused even though it sits under an aggregate. Q15's `revenue` CTE is the
  same mechanism. Q02/Q10/Q20 are the "no aggregate ancestor" shapes
  (Sort+LIMIT above joins; Q10's customer decoration join sits ABOVE the
  aggregate after GroupKeyReduction).
- Implemented `collect_spill_covered_scans` (planner.rs): top-down walk with a
  `CoverWalk { covered, blocked }` state — Aggregate/Distinct cover and reset
  `blocked` (bounded output); Join/Sort cover when lowered spillable
  (`use_spillable()`), else block; Window/DelimJoin/VectorSearch block; a
  twice-referenced CTE root is a fresh, blocked root (its output is
  materialized); Filter/Project/Limit/Union/plain alias pass through. A Scan is
  covered iff `covered && !blocked`. The pre-pass now runs BEFORE
  `materialize_shared_ctes`. Field renamed `agg_covered_scans` ->
  `spill_covered_scans`; doc comments in native_scan.rs / native_table.rs
  repointed.
- Planner unit tests (4 new, debug profile, `physical::planner::tests`): raw
  dump / filter-project-only / LIMIT-only / Window-over-scan / Window-over-join
  uncovered; aggregate / join both sides + sort + limit / sort / Window over
  aggregate covered; UNION ALL branch is address-keyed; twice-referenced CTE
  root blocks unless it has its own aggregate; once-referenced alias passes
  through; without a memory config only aggregates cover. 9/9 green.
- Real-run verification (binary `.scratch/sb/bin/qe_002a` = working tree at
  this point, incl. the in-flight task-002 filter code; sweep22.py, oracle
  `.scratch/sb/oracle/`, MemoryMax=32G / QE_MEM_CAP=32G / QE_SPILL_DEBUG=1 /
  QE_DEBUG_SCAN_BUDGET=1, `--tables data/tpch-100gb-native --memory-limit 1G`),
  log `.scratch/sb/logs/sweep_native1G_001.log`, artifacts
  `.scratch/sb/native1G_001/`:
  Q02 324ms CELL-EXACT (100 rows) · Q10 1,922ms CELL-EXACT (20) · Q11 209ms
  CELL-EXACT (100) · Q15 892ms CELL-EXACT (1) · Q20 17,569ms CELL-EXACT
  (40,196 rows; spill traces present, no join-spill START — it did not reach
  the LEFT-join refusal at 1G). 5/5, ENGINE TOTAL 20.9s. Every one of these
  refused by name on 2026-09-04 (`.scratch/jss/sweep_native1G.log`).
