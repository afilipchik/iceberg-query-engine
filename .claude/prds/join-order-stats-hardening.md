---
name: join-order-stats-hardening
description: Surface missing/degenerate join-key statistics loudly instead of a silent bad fallback, and fix native-table statistics going stale after mutation
status: backlog
created: 2026-08-27T17:23:53Z
---

# PRD: join-order-stats-hardening

## Executive Summary

A go/no-go investigation for full adaptive join-order re-optimization
(`.claude/plans/research/2026-08-27-modern-olap-research-synthesis.md`,
and the follow-on live repro recorded in `CLAUDE.md`'s "Adaptive
join-order re-optimization: investigated, NOT pursued yet" section)
found the large investment isn't justified today — every production
table-registration path already carries real statistics and picks
correct join orders. But it surfaced two real, narrow, cheap problems
worth fixing on their own: (1) when a relation's join-key statistics
are missing or degenerate, `JoinReorder`'s DPsize cost model silently
falls back to a near-arbitrary default that can misorder joins
catastrophically (the repro: a >60,000x cardinality misestimate turned
a 1.13s query into one that never completes) — with no visibility into
why; (2) native tables' per-column statistics are computed once at
write time and never recomputed by DELETE/UPDATE, so they can silently
drift from reality as a table mutates.

## Problem Statement

1. `JoinReorder`'s DPsize enumerator (`src/optimizer/rules/
   join_reorder.rs`) computes join selectivity from per-column NDV
   estimates. When a relation has no recorded column statistics (e.g.
   registered via `ExecutionContext::register_table`/`MemoryTable`, a
   real, shipped code path), the cost model falls back to a generic
   default with no signal to anyone that this happened — a query can
   silently get a wildly wrong plan with no diagnostic trail explaining
   why.
2. Native table `ColumnStats` (`src/storage/native_manifest.rs`) are
   computed once per segment at write time; NDV is derived at query time
   from `min_i64`/`max_i64` and a row count. DELETE/UPDATE never
   recompute or adjust these — after significant deletion, the derived
   estimate can diverge from the table's real, current data
   distribution, potentially in the dangerous direction (overestimating
   true post-deletion NDV understates true join selectivity, the same
   failure class as problem 1).

## User Stories

**As someone debugging a query that's mysteriously slow or hanging**, I
want the engine to tell me when it's making a join-order decision on
missing or degenerate statistics, so I don't have to reverse-engineer
the cost model to find out.
- Acceptance: a relation with no recorded column statistics feeding a
  join edge produces a visible signal (log/trace, not silent) naming
  the relation and column, without needing a special debug flag to
  discover a hang was caused by this.

**As someone running a native table that's been mutated (DELETE/UPDATE)
many times**, I want its statistics to still be trustworthy for join
planning, or to be honestly treated as uncertain rather than
confidently wrong.
- Acceptance: after a DELETE/UPDATE that materially changes a native
  table's data distribution, its statistics either reflect the current
  live data or are recognized as stale/degraded by the cost model
  (reusing problem 1's own new "missing/degenerate" handling) rather
  than being trusted at face value indefinitely.

## Functional Requirements

1. In `JoinReorder`'s cost model, detect when a relation's join-key
   column has no usable statistics (or a genuinely degenerate one — the
   task should investigate what "degenerate" means precisely given the
   current fallback logic) and emit a visible signal — tracing at a
   level that shows up in normal operation, not gated behind a
   debug-only env var, since the whole point is diagnosing a
   catastrophic bad plan someone didn't know to look for.
2. Investigate exactly how NDV is derived for native tables at query
   time (`min_i64`/`max_i64`/row count — confirm which row count:
   physical or live) and design a fix so that DELETE/UPDATE either (a)
   keeps this derivation accurate against the table's current live data,
   or (b) causes the derived estimate to be treated as degraded/
   untrustworthy once mutation has invalidated it materially, reusing
   requirement 1's own handling rather than a second, separate
   mechanism.
3. Neither fix should require a full table rescan on every mutation
   (that would fight this program's own "no compaction, bounded mutation
   cost" design philosophy established by the native-tables-mutation
   epic) — prefer incremental/cheap adjustments or honest degradation
   over expensive recomputation.

## Non-Functional Requirements

- **No regression** to the DPsize cost model's existing, already-correct
  behavior on real (non-degenerate) statistics — every currently-passing
  cell-exact/performance benchmark must stay unregressed.
- **No regression** to native table mutation's own performance
  characteristics (the mutation epic's own measured costs) — a
  statistics fix must not reintroduce a full-rescan-per-mutation cost.
- Every build through `scripts/claude-safe-build.sh`.

## Success Criteria

- G1: the go/no-go repro (`examples/adaptive_reopt_ndv_repro.rs`) run
  after this PRD's fix shows a visible warning/trace identifying the
  missing-statistics relation, rather than a silent bad plan.
- G2: a real test demonstrates native-table statistics staying accurate
  (or honestly degraded) after a DELETE/UPDATE that would previously
  have left them silently stale.
- G3: no regression to existing join-order/cost-model tests or to
  native-table mutation performance.
- G4: full suite green.

## Constraints & Assumptions

- Builds on the go/no-go investigation's own findings
  (`CLAUDE.md`'s "Adaptive join-order re-optimization" section,
  `examples/adaptive_reopt_ndv_repro.rs`) — read before starting, don't
  re-derive.
- This PRD explicitly does NOT attempt full adaptive join-order
  replanning (that was the investigated-and-deferred large option) —
  scope is strictly the two narrow fixes above.

## Out of Scope

- Full adaptive/runtime join-order re-optimization (deferred, per the
  go/no-go investigation).
- Computing real NDV for `MemoryTable`/`register_table` — that
  registration path staying statistics-free is accepted; the fix is
  making that fact visible, not eliminating it.
- Extending the runtime-filter-bitmap mechanism (a separate,
  independently-identified next step, not bundled here).

## Dependencies

- `src/optimizer/rules/join_reorder.rs`, `src/optimizer/cost.rs`.
- `src/storage/native_manifest.rs`, `native_delete.rs`, `native_update.rs`.
- `examples/adaptive_reopt_ndv_repro.rs`, `CLAUDE.md`'s adaptive
  re-optimization section — the load-bearing investigation to build on.
- `scripts/claude-safe-build.sh` for every build.
