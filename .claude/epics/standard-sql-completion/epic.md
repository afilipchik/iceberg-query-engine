---
name: standard-sql-completion
status: backlog
created: 2026-08-21T15:16:42Z
updated: 2026-08-21T15:16:42Z
progress: 0%
prd: .claude/prds/standard-sql-completion.md
github: (will be set on sync)
---

# Epic: standard-sql-completion

## Overview

Implement the SQL-standard window function suite (the architecture gap behind
21 of the probe's 34 failures) plus the grouping extensions and missing
expression forms. The window design follows the sketch already recorded in
`trino-function-implementation.md` Priority 5: `WindowExpr` in the expression
tree, a `Window` logical node, a `WindowExec` physical operator that
sorts-then-evaluates per partition. Everything is validated against DuckDB.

## Architecture Decisions

1. **Binder strategy: extract-and-project.** A SELECT with window expressions
   binds to `Project(final exprs) <- Window(window exprs) <- input`. Each
   distinct `(function, args, partition, order, frame)` becomes one output
   column of the Window node (named `__w0`, `__w1`, ...); the final Project
   rewrites the original expressions to reference them. Window functions can
   therefore appear inside arbitrary scalar expressions for free, and
   multiple different windows in one SELECT are N columns of one node
   (grouped by shared sort requirement at execution).
2. **Execution: sort-partition-evaluate, one window group at a time.**
   `WindowExec` sorts its input by (partition keys, order keys) via the
   existing sort machinery (spill-capable, memory-tracked), then walks
   partitions and evaluates each function over the partition with frame
   logic. Functions implement one trait (`WindowEvaluator`) with fast paths:
   rank-family needs only order-key change detection; LAG/LEAD are offset
   lookups; framed aggregates keep running accumulators for the common
   accumulate-only frames and recompute per-row only for shrinking frames
   (MIN/MAX with sliding start). v1 is single-threaded across partitions —
   correctness first, the morsel treatment is a later perf epic.
3. **Frames**: ROWS exact per standard; RANGE limited to the shapes the
   standard's default and common usage need (UNBOUNDED PRECEDING ..
   CURRENT ROW / UNBOUNDED FOLLOWING, plus numeric offsets over a single
   numeric/date order key — peers determined by order-key equality).
   Unsupported RANGE shapes are refused BY NAME, never silently approximated.
4. **Grouping sets rewrite, not a new operator.** `GROUP BY GROUPING SETS /
   ROLLUP / CUBE` desugars in the binder to a UNION ALL of ordinary
   aggregates, each padding the absent group columns with typed NULLs and a
   constant `GROUPING()` bitmask column. Reuses the whole existing aggregate
   path (morsel, spill, distributed gather) with zero new physical code.
5. **Small gaps live in the binder/evaluator**: IS [NOT] DISTINCT FROM as a
   null-safe comparison op; ANY/SOME/ALL desugared to EXISTS/aggregate
   subquery forms the engine already runs; OVERLAY as a scalar function;
   DATE ± INTERVAL via arrow's date arithmetic kernels in the cast/coercion
   layer. GROUP BY ordinal fixed where ordinals resolve in the binder.
6. **Distributed**: no new work — gather mode ships whole tables and runs the
   ORIGINAL statement locally on the initiator; scatter's plan-probe already
   rejects unsupported shapes, and the Window node makes the probe say
   "window" by name. A cluster test proves it.

## Technical Approach

### Backend Services

- `src/planner/logical_expr.rs`: `WindowFunction` enum (11 + agg carrier),
  `WindowExpr { func, args, partition_by, order_by, frame }`, `WindowFrame`
  types; Display + data_type.
- `src/planner/binder.rs`: replace the `Not implemented` guard (line ~1765)
  with real binding: collect window exprs from the projection, resolve named
  WINDOW clauses, build Window node, rewrite projections. GROUP BY ordinal
  fix. GROUPING SETS/ROLLUP/CUBE desugar. IS DISTINCT FROM, ANY/ALL,
  OVERLAY, interval arithmetic binding.
- `src/planner/logical_plan.rs`: `Window(WindowNode)` variant (input,
  window_exprs, schema).
- `src/optimizer/rules/*`: projection pushdown treats Window like Aggregate
  (needs its inputs, passes the rest); predicate pushdown may push only
  predicates on partition keys through a Window (standard-safe), everything
  else stops below it.
- `src/physical/operators/window.rs` (new): `WindowExec` + `WindowEvaluator`
  implementations; sort reuse; memory-pool reservations for partition
  buffers.
- `src/physical/planner.rs`: Window lowering.

### Frontend Components

None. CLI/REPL benefit automatically.

### Infrastructure

- `scripts/sql_gap_probe.py` promoted from `.scratch` into `scripts/` as the
  feature-coverage gate (window/grouping/expression sections must pass).
- New `tests/window_functions.rs` DuckDB-validated suite (oracle CSVs or live
  comparison via the established generate-expected-results pattern).

## Implementation Strategy

Land the expression/plan plumbing first (task 1), the executor next (2), then
function families in parallelizable slices (3-5), then the non-window gaps
(6-7), then validation + docs (8-10). Every function lands with its DuckDB
comparison in the same task — no "tests later".

## Task Breakdown Preview

1. Window plumbing: WindowExpr/WindowNode/binder extraction/schema,
   optimizer pass-through, physical lowering to a correctness-first
   WindowExec that handles ROW_NUMBER only (proves the pipe end to end).
2. WindowExec partition/sort/frame engine: partition boundaries, peer
   groups, ROWS/RANGE frame resolution, memory-tracked buffering.
3. Ranking family: RANK, DENSE_RANK, PERCENT_RANK, CUME_DIST, NTILE. [dep 2]
4. Navigation family: LAG, LEAD (offset+default), FIRST_VALUE, LAST_VALUE,
   NTH_VALUE with frame interaction. [dep 2]
5. Aggregates over windows: COUNT/SUM/AVG/MIN/MAX, running and sliding
   frames, default-frame semantics. [dep 2]
6. Grouping extensions: GROUPING SETS / ROLLUP / CUBE desugar + GROUPING().
7. Expression gaps: IS [NOT] DISTINCT FROM, ANY/SOME/ALL, OVERLAY,
   DATE ± INTERVAL, GROUP BY ordinal fix (+ LATERAL if contained).
8. DuckDB-validated window/grouping suite (>=60 cases) + probe promotion.
   [dep 3,4,5,6]
9. Distributed + Flight verification of window queries on a 3-node cluster;
   scatter probe rejects windows by name. [dep 5]
10. Docs: CLAUDE.md, README, trino-function-implementation.md status flip.
    [dep 8]

## Dependencies

- Existing sort/memory/aggregate infrastructure; DuckDB oracle in `.venv`;
  sqlparser 0.52's existing OVER/WINDOW/GROUPING SETS parse support
  (verified by probe error shapes).

## Success Criteria (Technical)

- All PRD probe items green via the committed probe script.
- >=60-case DuckDB-validated window/grouping suite green; existing 971
  tests green; TPC-H 22/22 unchanged.
- Window query correct through Flight + HTTP on 3-node cluster (gather).
- `cargo fmt` clean; no new clippy errors; lance build green.

## Estimated Effort

10 tasks. The frame engine (task 2) and aggregate frames (task 5) carry the
most correctness risk; DuckDB comparison per-function keeps it honest.

## Tasks Created
- [ ] 001.md - Window plumbing end to end (ROW_NUMBER only) (parallel: true)
- [ ] 002.md - WindowExec partition, peer and frame engine (parallel: false)
- [ ] 003.md - Ranking family: RANK, DENSE_RANK, PERCENT_RANK, CUME_DIST, NTILE (parallel: true)
- [ ] 004.md - Navigation family: LAG, LEAD, FIRST_VALUE, LAST_VALUE, NTH_VALUE (parallel: true)
- [ ] 005.md - Aggregates over windows: COUNT, SUM, AVG, MIN, MAX (parallel: true)
- [ ] 006.md - Grouping extensions: GROUPING SETS, ROLLUP, CUBE, GROUPING() (parallel: true)
- [ ] 007.md - Expression gaps: IS DISTINCT FROM, ANY/ALL, OVERLAY, date+interval, ordinal fix (parallel: true)
- [ ] 008.md - DuckDB-validated window and grouping suite + probe promotion (parallel: false)
- [ ] 009.md - Distributed + Flight verification of window queries (parallel: true)
- [ ] 010.md - Docs: CLAUDE.md, README, plan status flip (parallel: true)

Total tasks: 10
Parallel tasks: 8
Sequential tasks: 2
Estimated total effort: 54 hours
