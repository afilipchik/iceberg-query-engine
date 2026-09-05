---
name: spill-boundaries
description: Close the three remaining clean-refusal boundaries — native-table scans feeding joins, ON-clause-filter join spill, and LEFT/RIGHT/FULL outer-join spill — so every TPC-H query completes at every budget on parquet and native
status: active
created: 2026-09-05T01:39:08Z
---

# PRD: spill-boundaries

## Executive Summary

The 2026-09-03 SF=100 certification left exactly three named, clean
refusals: (1) on NATIVE tables at 1G, an over-budget native scan that
feeds a JOIN refuses at the scan admission check (Q02 part, Q10
customer, Q11/Q20 partsupp, Q15 lineitem) — the streaming scan operator
exists but the planner only routes it under aggregate-covered shapes;
(2) at 256M on parquet, Q21's join carries an ON-clause predicate the
spill path cannot evaluate; (3) at 256M on parquet, Q20's decorrelated
LEFT join has no spill path. Each is a safe refusal today; each is work
the engine can do. This epic makes all three complete by spilling.

## Problem Statement

"Slow but correct on larger-than-memory data" has three holes with a
query each. They are the last places a budget costs coverage rather
than speed.

## User Stories

**As someone querying native tables at a tight budget,** I want a table
larger than the budget to stream into a join the same way it already
streams into an aggregate.
- Acceptance: native SF=100 @1G goes from 17/22 + 5 refusals to 22/22
  cell-exact; the harness native-scan scenario still passes; the SF=10
  native band is unchanged.

**As someone whose join has an ON-clause predicate,** I want the spill
path to evaluate it per candidate pair rather than refuse.
- Acceptance: Q21 SF=100 parquet @256M completes cell-exact through the
  spill path; filtered INNER/SEMI/ANTI spill tests vs naive truth.

**As someone whose query needs an outer join over a build side that
does not fit,** I want NULL-extended results, not a refusal.
- Acceptance: Q20 SF=100 parquet @256M completes cell-exact; LEFT, RIGHT
  and FULL spill tests vs naive truth in both build orientations,
  including chunk-straddling duplicates and NULL keys.

## Functional Requirements

1. **Native scans feeding spill-capable consumers stream.** Widen the
   planner's `collect_agg_covered_scans` to "spill-covered": a Scan
   whose path to the root passes through a spillable pipeline breaker
   (spillable hash join — either side —, spillable aggregate, external
   sort) gets `NativeStreamingScanExec`. Raw materializing shapes keep
   the admission refusal. Depends on the probe side streaming
   (`join-spill-streaming`), otherwise a streamed 600M-row probe would
   be re-materialized by the join.
2. **ON-clause filter on the spill path.** INNER: gather candidate pairs
   as today, evaluate the filter on the joined batch, keep true rows.
   SEMI/ANTI (both orientations) and outer joins: a candidate counts as a
   match only if the filter is true for that (build row, probe row)
   pair — evaluate per candidate via the combined-schema batch (reuse
   hash_join's `CompiledFilter` fast path where it compiles; fall back
   to expression evaluation on a gathered pair batch). Remove the
   `filter.is_some()` refusal.
3. **Outer-join spill.** LEFT/RIGHT/FULL with the same partition +
   chunked read-back machinery: match bitmaps for the PRESERVED side —
   probe-side preserved (bitmap per probe batch across chunks, emit
   unmatched rows NULL-extended after the last chunk) and build-side
   preserved (per-resident-partition bitmaps across the probe stream +
   per-chunk bitmaps on read-back, emit unmatched NULL-extended) — and
   FULL = both. Reuse hash_join's null-sentinel emission
   (`create_joined_batch` with `usize::MAX`, `create_build_only_batch`).
   Retained-column masks apply (outer joins DO get `retained`).
4. **Certification**: SF=100 parquet 256M → 22/22; native 1G → 22/22;
   harness gains `left-join` and `filtered-join` scenarios.

## Non-Functional Requirements

- Cell-exact vs naive truth and DuckDB; every existing test, chaos,
  four suites, M1/M2 green; SF=10 bands unchanged.
- Refusals that remain (if any) named precisely; none expected on TPC-H.

## Success Criteria

- G1: native SF=100 @1G 22/22 cell-exact (Q02/Q10/Q11/Q15/Q20 complete).
- G2: parquet SF=100 @256M 22/22 cell-exact (Q20, Q21 complete through
  spill paths with real spill activity and 0 hash mismatches).
- G3: dedicated tests: filtered INNER/SEMI/ANTI spill; LEFT/RIGHT/FULL
  spill × orientations × dense/sparse × NULL keys, all vs naive truth;
  harness left-join + filtered-join scenarios COMPLETED under caps.
- G4: no regression (bands, suites, chaos ≥300, M1/M2); CLAUDE.md's three
  boundary bullets rewritten as closed; certification note refreshed.

## Constraints & Assumptions

- Ordered after `join-spill-streaming` (requirement 1's dependency and
  shared operator code).
- Files: `spillable.rs` (join section), `planner.rs`
  (`collect_agg_covered_scans`), harness example + driver.

## Out of Scope

- Single/Mark join types on the spill path.
- Native-scan spill-awareness for raw dumps (`SELECT *` to the client).

## Dependencies

- `join-spill-streaming` merged.
