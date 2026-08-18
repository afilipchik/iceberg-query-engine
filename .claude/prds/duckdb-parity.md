---
name: duckdb-parity
description: Close the like-for-like gap to DuckDB on both parquet (2.21x) and lance (1.30x) at SF=100 via fused probe-aggregate, deferred join gather, Q4 attribution, and Lance predicate pushdown
status: completed
created: 2026-08-18T00:32:07Z
---

# PRD: duckdb-parity

## Executive Summary

The engine runs the full TPC-H suite at SF=100 in 87.1s over parquet and
98.3s over lance. DuckDB over the SAME files runs 39.4s (parquet) and
75.7s (lance). The goal is to close those like-for-like ratios — 2.21x
and 1.30x — toward 1.0x, using the mechanisms the prior epic
(`close-parquet-gap`) attributed but scoped out as "a rewrite epic, not
stories": fused probe→aggregate (PARITY-PLAN 2a), deferred/selection-
vector join gather (2b), the never-finished Q4 attribution, and the
Lance-only Q19 pushdown gap.

## Problem Statement

Engine-level rule tuning has asymptoted. The prior epic's attribution
work proved the remaining parquet gap is concentrated in **join-probe
gather and per-row probe cost**:

- **Q9 (the whale)**: 20.4s engine vs ~9.2s DuckDB-parquet. The 80M-entry
  partsupp composite-key probe was fixed by PackedJoinKeys; the remaining
  ~11s is probe-side gather (take-based `create_joined_batch`).
- **Q18-class drain**: +4.6s at SF=100 is join-probe drain into an
  aggregate — the exact shape 2a (JoinAggregateExec) removes.
- **Q4**: +2.6s, worst ratio after Q13, EXISTS semi-join — attribution
  started in the prior epic but never done (tasks 002/005 left open).
- **Lance Q19**: 5.3s vs 1.2s on parquet — the OR-of-IN-lists predicate
  is refused by the Lance pushdown whitelist, so the scan full-decodes;
  parquet's RowFilter + runtime filters eliminate nearly everything.
- **Lance Q10/Q18**: 5.5 vs 3.9s and 11.5 vs 9.1s — unattributed; plan
  diffs vs parquet (the proven method) not yet taken at SF=100.

## User Stories

**As the engine developer**, I want join-heavy aggregate queries (Q18,
Q21, Q13, Q3, Q10) to stop materializing full joined batches when the
only consumer is an aggregate, so probe output feeds accumulators
directly.
- AC: `Aggregate(Join)` shapes fuse when group-by + agg inputs reference
  a small column set; cell-exact results; measured wins on Q18/Q21 at
  SF=100.

**As the engine developer**, I want the hash-join probe to defer wide
payload-column gather (emit row ids / selection, gather at the sink), so
Q9's ~11s of gather cost shrinks.
- AC: Q9 SF=100 ≤ 16s (the prior epic's unmet gate) without regressing
  any other query beyond noise.

**As the engine developer**, I want Q4's 2.6s attributed and fixed.
- AC: written attribution (profile + plan diff vs DuckDB), then a fix
  with gate Q4 SF=100 ≤ 2.5s, or a documented negative result naming the
  mechanism.

**As the engine developer**, I want Lance scans to push IN-list and
OR-of-IN predicates so Q19-on-lance stops full-decoding.
- AC: Q19 lance SF=100 within 2x of the parquet path's Q19; correctness
  guaranteed by the existing re-apply-filter-above-scan invariant.

## Functional Requirements

1. Fused probe→aggregate operator (2a): planner fuses `Aggregate(Join)`
   when its column footprint is small; falls back cleanly otherwise.
2. Deferred gather / selection-vector probe path (2b, scoped): joins
   whose parents need few columns emit keys + row-ids, gathering payload
   late. Start with the Q9 shape (join feeding join feeding aggregate).
3. Q4 attribution and fix.
4. Lance `expr_to_lance_sql` whitelist extension: `IN (list)` and OR of
   renderable conjuncts, still gated by the cost model; diagnostic env
   override respected.
5. Lance Q10/Q18 attribution via PLAN_DEBUG diff vs parquet at SF=100.

## Non-Functional Requirements

- Memory-safe always; spillable operators unaffected or extended, never
  bypassed. No OOM at any scale.
- 22/22 cell-exact vs DuckDB at SF=10 AND SF=100 after every change
  (row counts are not answers).
- Full test suite green in both modes (default + `QE_IPC_CACHE=1`),
  lance feature suite green.
- `cargo fmt` before every commit; commit-or-revert per lever.
- Benchmark timeout rule: 10x DuckDB per query.

## Success Criteria

- **G1 (parquet)**: SF=100 suite total ≤ 60s warm (from 87.1s), i.e.
  ≤ 1.55x like-for-like, with Q9 ≤ 16s and Q18 ≤ 6s.
- **G2 (lance)**: SF=100 lance total ≤ 88s (from 98.3s) with Q19-lance
  ≤ 2.5s.
- **G3**: no SF=10 regression beyond noise (parquet ≤ 7.7s warm).
- **Stretch**: like-for-like parquet ≤ 1.3x (≈51s). Full 1.0x parity is
  the program exit criterion, not this epic's gate.

## Constraints & Assumptions

- Single dev box (i9-13900KF hybrid, no NUMA), warm page cache, measured
  serialized on an idle machine; run-to-run noise ~±3%.
- DuckDB baselines from `scripts/duckdb_files_bench_sf100.py` (parquet
  39.4s / lance 75.7s, duckdb 1.4.4) are current as of 2026-08-17.
- Lance pinned at 0.23.x (arrow 53); no format-level statistics exist,
  so Lance-side wins must come from pushdown rendering and shared
  engine mechanisms, not zone maps.
- 2b full generalization (every operator passes (batch, sel)) is out of
  reach in one epic; scope to the join-probe/sink path.

## Out of Scope

- Distributed M3 (shuffle).
- Window functions, new SQL surface.
- Bespoke parquet decoders (Rewrite 3 option 3).
- Making the IPC sidecar cache default-on.
- DuckDB-native-storage parity (owned-format program work).

## Dependencies

- Existing: HJ_TIMING/AGG_TIMING/PLAN_DEBUG diagnostics, PackedJoinKeys,
  fused-agg disjoint mode, `scripts/duckdb_files_bench_sf100.py`,
  `data/sf100_duckdb_results`, `data/tpch-100gb-lance`.
- No external dependencies.
