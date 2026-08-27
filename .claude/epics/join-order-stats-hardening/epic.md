---
name: join-order-stats-hardening
status: in-progress
created: 2026-08-27T17:23:53Z
updated: 2026-08-27T17:23:53Z
progress: 0%
prd: .claude/prds/join-order-stats-hardening.md
github: (will be set on sync)
---

# Epic: join-order-stats-hardening

## Overview

Two narrow, cheap fixes surfaced by the adaptive-join-order-
re-optimization go/no-go investigation, which found the large
replanning investment isn't justified today but that missing/degenerate
join-key statistics can still silently produce catastrophically bad
plans (the repro: a >60,000x cardinality misestimate turned a 1.13s
query into one that never completes), and that native tables' own
statistics can silently go stale after mutation.

## Architecture Decisions

- **Visibility over correction.** Neither fix attempts to make the cost
  model magically right when statistics are missing/wrong — the goal is
  making that condition VISIBLE (so a human or a future adaptive
  mechanism can act on it), not silently guessing better.
- **No full rescans.** Native-table statistics staleness after mutation
  gets fixed via incremental adjustment or honest degradation, not a
  full column rescan per mutation — that would fight the native-tables-
  mutation epic's own established "bounded mutation cost" discipline.
- **One shared "untrustworthy statistics" signal, not two.** The
  native-table staleness fix (task 002) should route through the same
  visibility mechanism task 001 builds, rather than inventing a second,
  parallel way to say "don't trust this number."

## Technical Approach

### Missing/degenerate statistics visibility (task 001)
`JoinReorder`'s cost model, when a relation's join-key column has no
usable statistics, currently falls back silently. Add tracing that
fires in normal operation (not debug-gated) naming the relation/column,
so a bad plan caused by this is diagnosable without already knowing to
look.

### Native-table statistics staleness (task 002)
Investigate exactly how NDV is derived at query time for native tables
(`min_i64`/`max_i64`/row count) and whether DELETE/UPDATE leave it
stale in the dangerous direction (overestimating post-deletion NDV
understates true join selectivity). Fix via incremental adjustment where
cheap, or by routing a materially-invalidated estimate through task
001's own degraded-statistics signal.

### Validation (task 003)
Re-run the go/no-go repro and confirm the visibility fix fires; a real
mutation-then-query test proving native-table statistics stay accurate
or honestly degraded; no regression to existing cost-model/mutation
tests; full suite; docs.

## Task Breakdown Preview

- 001: Missing/degenerate join-key statistics visibility in `JoinReorder`
  (parallel: false, entry point — task 002 reuses its signal)
- 002: Native-table statistics staleness after mutation (parallel: false,
  depends on 001's signal mechanism)
- 003: Validation, no-regression check, full suite, docs, epic close
  (parallel: false, depends on 001 and 002)

Total tasks: 3
Estimated total effort: S-M overall — both fixes are narrow by design.

## Dependencies

- `src/optimizer/rules/join_reorder.rs`, `src/optimizer/cost.rs`.
- `src/storage/native_manifest.rs`, `native_delete.rs`, `native_update.rs`.
- `examples/adaptive_reopt_ndv_repro.rs`, `CLAUDE.md`'s adaptive
  re-optimization section.
- `scripts/claude-safe-build.sh` for every build.

## Success Criteria (Technical)

- G1: the go/no-go repro shows a visible warning identifying the
  missing-statistics relation.
- G2: a real DELETE/UPDATE-then-query test proves native-table
  statistics stay accurate or honestly degraded.
- G3: no regression to existing cost-model or mutation tests/performance.
- G4: full suite green.

## Estimated Effort

- 001: S.
- 002: S-M.
- 003: S.

## Tasks Created
- [ ] 001.md - Missing/degenerate join-key statistics visibility (parallel: false)
- [ ] 002.md - Native-table statistics staleness after mutation (parallel: false)
- [ ] 003.md - Validation, no-regression check, full suite, docs, epic close (parallel: false)

Total tasks: 3
Parallel tasks: 0
Sequential tasks: 3
Estimated total effort: S-M
