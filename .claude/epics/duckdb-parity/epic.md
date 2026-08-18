---
name: duckdb-parity
status: completed
created: 2026-08-18T00:32:07Z
updated: 2026-08-18T00:32:07Z
progress: 100%
prd: .claude/prds/duckdb-parity.md
github: (will be set on sync)
---

# Epic: duckdb-parity

## Overview

Close the SF=100 like-for-like gap: parquet 87.1s → ≤60s (DuckDB same
files: 39.4s), lance 98.3s → ≤88s (DuckDB lance: 75.7s). The prior epic
attributed the gap; this one implements the mechanisms: fused
probe→aggregate (2a), deferred join gather (2b scoped to the Q9 shape),
Q4 attribution+fix, and Lance IN-list pushdown for Q19.

## Architecture Decisions

- **2a as a fused execution mode inside the existing join/agg machinery,
  not a parallel operator stack.** PARITY-PLAN round 35 proved the
  output-repartitioning variant loses; fuse INTO the probe. Reuse
  `AggregationState::process_batch` — the probe hands it (group-key
  columns, agg-input columns) per probe batch instead of building joined
  RecordBatches. Planner detects `Aggregate(Join)` where referenced
  columns ≤ small bound; anything else takes today's path.
- **2b scoped, not generalized.** Only the join-probe gather path: when a
  join's parent is another join (on keys) or a fused aggregate, emit
  (probe_row_id, build_row_id) + key columns; gather payload columns once
  at the sink. No engine-wide (batch, sel) contract this epic.
- **Lance pushdown stays whitelist + re-applied filter.** Extending the
  renderer to `IN (list)` and OR-of-renderables cannot cause wrong
  answers because FilterExec above the scan re-evaluates the full
  predicate; a bad push is only a perf bug. The cost gate logic stays;
  Q19's IN-lists are on low-NDV dictionary-class string columns where
  selectivity IS estimable from value-count heuristics — if not, gate on
  the diagnostic measurement.
- **Attribution before optimization, always** (HJ_TIMING / AGG_TIMING /
  PLAN_DEBUG diff vs parquet; the Q9 and Lance-Q09 lessons).
- **Measurements serialized** on the idle box; cell-exact validation
  after every lever (row counts are not answers).

## Technical Approach

### Engine (shared parquet+lance wins)

1. **JoinAggregate fusion (2a)** — `src/physical/planner.rs` detects
   `Aggregate(HashJoin)`; probe path in `hash_join.rs` gains a fused sink
   feeding `AggregationState` (from `morsel_agg.rs`/`hash_agg.rs`).
   Targets Q18 (9.1s), Q21, Q13, Q3, Q10 — and helps lance identically.
2. **Deferred probe gather (2b-lite)** — `create_joined_batch` today
   gathers all output columns via `take`. Add a probe mode emitting
   selection indices + minimal columns for join-above-join chains; final
   gather at the sink. Q9 gate ≤16s.
3. **Q4 attribution** — HJ_TIMING + plan diff vs DuckDB EXPLAIN, then
   the indicated fix (likely semi-join probe drain or scan overlap).

### Lance-specific

4. **`expr_to_lance_sql` IN-list + OR rendering** (`src/storage/lance.rs`)
   with the existing correctness invariant; measure Q19 with
   `QE_LANCE_PUSH=all` first to bound the win, then gate properly.
5. **Lance Q10/Q18 plan-diff attribution** at SF=100 — cheap, uses
   PLAN_DEBUG on both paths; fixes ride on whatever it names (likely 2a).

### Infrastructure

- None new. Benchmarks: `scripts/sf100_full_benchmark.sh`,
  `benchmark-lance`, `scripts/duckdb_files_bench_sf100.py`; validation
  against `data/sf100_duckdb_results`.

## Implementation Strategy

Phase 1 (parallel): 2a implementation; Q4 attribution; Lance Q19
diagnostic measurement + rendering. Phase 2: 2b-lite (builds on 2a's
sink plumbing). Phase 3: full QA sweep both formats both scales, docs,
close-out. Each lever: implement → cell-exact validate → benchmark →
commit-or-revert.

## Task Breakdown Preview

- 001: Baseline pin — one serialized SF=100 sweep (parquet+lance) +
  per-query gap table committed to the epic dir. [quick]
- 002: 2a JoinAggregate fusion — core implementation + tests.
- 003: 2a rollout — enable on Q18/Q21/Q13/Q3/Q10 shapes, SF=10+100
  validation, per-query gates.
- 004: Q4 attribution → fix (gate ≤2.5s) or documented negative.
- 005: Lance Q19 pushdown — IN-list/OR rendering + gating + measurement.
- 006: 2b-lite deferred probe gather — Q9 gate ≤16s.
- 007: Lance Q10/Q18 attribution + ride-along fixes.
- 008: QA close-out — full suites both modes, SF=10+SF=100 cell-exact
  both formats, CLAUDE.md/PARITY-PLAN updates, epic close.

## Dependencies

- 003 depends on 002. 006 depends on 002 (shared sink plumbing), and its
  measurement runs must serialize with 003/004. 007 partially depends on
  003 (2a may be the fix). 008 depends on all.
- 002, 004, 005 are parallelizable (disjoint files: join/agg internals vs
  profiling vs lance.rs).

## Success Criteria (Technical)

- G1: SF=100 parquet ≤ 60s warm; Q9 ≤ 16s; Q18 ≤ 6s.
- G2: SF=100 lance ≤ 88s; Q19-lance ≤ 2.5s.
- G3: SF=10 parquet ≤ 7.7s; 22/22 cell-exact both scales both formats;
  full suite green both modes; memory safety intact.

## Tasks Created
- [ ] 001.md - Baseline pin — SF=100 parquet+lance sweep and gap table (parallel: false)
- [ ] 002.md - 2a core — fused probe→aggregate (JoinAggregate fusion) (parallel: true)
- [ ] 003.md - 2a rollout — SF=100 gates on Q18/Q21/Q13/Q3/Q10, both formats (parallel: false)
- [ ] 004.md - Q4 attribution → fix (EXISTS semi-join, +2.6s at SF=100) (parallel: true)
- [ ] 005.md - Lance Q19 — IN-list/OR pushdown rendering (parallel: true)
- [ ] 006.md - 2b-lite — deferred probe gather (Q9 ≤ 16s) (parallel: false)
- [ ] 007.md - Lance Q10/Q18 attribution + ride-along fixes (parallel: true)
- [ ] 008.md - QA close-out — full suites, cell-exact both scales/formats, docs (parallel: false)

Total tasks: 8
Parallel tasks: 4
Sequential tasks: 4
Estimated total effort: 28-45 hours (measurement wall time dominates several)

## Estimated Effort

- 2a is the largest lever (M-L, touches the two hottest operators).
  2b-lite L (highest risk, gated behind 2a). Others S-M.
- Total: one long working session with serialized measurement windows.

## Epic close-out (2026-08-18)

**SF=100 parquet: 89.3 → 72.7s warm, 22/22 cell-valid every sweep.
Like-for-like vs DuckDB on the same files: 2.23x → 1.81x. vs native:
1.08x.** SF=10: 7.63s parquet / 7.36s IPC, ALL 22 CELL-EXACT (G3 MET).
Lance: sweep totals carry ±5-8% cache variance on this box; warm A/Bs:
Q9 29.0→20.8s, Q19 5.5→3.3s, Q10 2.37s ≈ duck-lance parity.

Per-query (final sweep, ms): Q1 3367, Q2 458, Q3 3976, Q4 1878, Q5 3746,
Q6 953, Q7 3060, Q8 2919, Q9 18738, Q10 2697, Q11 276, Q12 1389,
Q13 2973, Q14 1036, Q15 844, Q16 1875, Q17 2363, Q18 7563, Q19 1308,
Q20 5038, Q21 5484, Q22 747.

Gates: G1 (≤60s, Q9≤16, Q18≤6) NOT met — the residue is named and
program-level (Q9 is CPU-saturation-bound; radix-partitioned joins /
selection vectors, PARITY-PLAN 2b). G2 (lance ≤88s, Q19≤2.5) partially:
Q19 halved to 3.3s; sweep variance prevents an honest total claim.
G3 MET.

Beyond the numbers, the epic's attribution work found and fixed FOUR
correctness bugs: the ad3881a Q5 SF=10 cross-join planning failure
(PackedJoinKeys inside the fixpoint loop), the perfect-hash rehash
invalidating the combined path's per-batch cache (latent wrong answers),
the DictString 7-vs-8-byte raw-key pack (group splits on mixed batches),
and the Null-fallthrough in extract_group_value/extract_join_key for
dictionary arrays (silent one-group collapse class). Plus the ops
lesson: systemd-oomd kills the terminal scope under benchmark pressure —
scripts/oomsafe.sh now isolates every heavy run.

Commits: df17bf8 (scaffolding+baseline), 170217c (RT caps + optimizer
fix), cb6f2ba (dictionary gather), a5c9635 (lance sampled stats),
6c41285 (raw-sum merge).
