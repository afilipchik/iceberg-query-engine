---
name: native-table-pruning
status: in-progress
created: 2026-08-27T07:44:44Z
updated: 2026-08-27T09:30:00Z
progress: 50%
prd: .claude/prds/native-table-pruning.md
github: (will be set on sync)
---

# Epic: native-table-pruning

## Overview

Wire native tables' already-computed per-segment min/max statistics into
actual scan-time segment skipping. The write-side half of this mechanism
(`ColumnStats`, computed at write time, already feeding the cost-based
optimizer) has existed since the foundation epic; the read-side half
(`NativeTable::scan_with_filter`) has never been implemented — it's the
`TableProvider` trait's default, which silently ignores any filter and
decodes every segment in full. Identified as the single cheapest,
best-evidenced gap in this session's modern-OLAP research synthesis.

## Architecture Decisions

- **Mirror `row_group_pruning.rs`'s pattern, don't reinvent it.** Parquet
  already solves the identical problem (AND-of-simple-comparisons/
  BETWEEN against per-chunk min/max) — reuse or directly adapt its
  predicate-recognition logic rather than building a parallel dialect
  that could subtly diverge in what it considers "provably unsatisfiable."
- **Skip-on-certainty only.** A segment is only skipped when the
  predicate is PROVABLY unsatisfiable against its stats. Absent stats
  (string columns today), an unrecognized predicate shape, or any
  ambiguity means the segment is scanned — pruning can only make a query
  faster by skipping correctly, never wrong by skipping incorrectly. The
  existing `FilterExec` above the scan is untouched and still re-applies
  the full predicate to whatever segments ARE read — this is the same
  belt-and-suspenders correctness argument Parquet's own pruning relies
  on, not a new invariant.
- **String/binary columns don't prune yet, and that's fine.** `ColumnStats`
  has no min/max for them today; extending the manifest format is a
  separate, larger PRD, not bundled here.

## Technical Approach

### Read-path pruning
`NativeTable::scan_with_filter` implemented for real: evaluate the pushed
predicate against each active segment's `ColumnStats` before calling
`ipc_cache::read_row_group`, skip segments that can't match. First task
must read the actual current caller-side wiring for Parquet's own
`scan_with_filter` (who decides a predicate is pushable, where that
decision is made) and confirm/extend it so native tables become eligible
the same way — don't assume this is already provider-agnostic.

### Validation
Cell-exact with pruning on vs. off, and vs. an independent DuckDB oracle,
across varied predicate shapes (single-column range, equality,
multi-column AND, a predicate on a column with no stats). Real
before/after measurement on the specific Q4/Q12/Q13 regression named in
`CLAUDE.md`, reported honestly whatever the result.

## Task Breakdown Preview

- 001: Implement segment-level pruning in `NativeTable::scan_with_filter`,
  wire the caller-side eligibility so native tables actually reach it
  (parallel: false, the epic's core piece)
- 002: Validation — cell-exact across predicate shapes, the Q4/Q12/Q13
  before/after measurement, no-regression check on unfiltered queries,
  full suite, docs, epic close (parallel: false, depends on 001)

Total tasks: 2
Estimated total effort: S-M — this is "use data the engine already
computes," not new infrastructure.

## Dependencies

- `src/storage/native_table.rs`, `native_manifest.rs`.
- `src/storage/row_group_pruning.rs`, `parquet.rs`'s `read_file_with_filter`.
- `scripts/claude-safe-build.sh` for every build.

## Success Criteria (Technical)

- G1: at least one segment measurably skipped (traced, not inferred) for
  a range/equality-filtered query against a multi-segment native table.
- G2: cell-exact identical results, pruning on vs. off, vs. DuckDB.
- G3: real before/after numbers on the named Q4/Q12/Q13 regression.
- G4: full suite green; no regression to unfiltered-query performance.

## Estimated Effort

- 001: S-M.
- 002: S.

## Tasks Created
- [x] 001.md - Implement segment-level scan pruning (parallel: false) — CLOSED 2026-08-27
- [ ] 002.md - Validation, before/after measurement, QA close-out (parallel: false)

Total tasks: 2
Parallel tasks: 0
Sequential tasks: 2
Estimated total effort: S-M

## Task 001 close-out summary (2026-08-27)

`NativeTable::scan_with_filter` (`src/storage/native_table.rs`) now really
prunes: for each active segment, `segment_might_match` evaluates the pushed
predicate (AND/OR/NOT/BETWEEN/InList, mirroring `row_group_pruning.rs`'s
own recursive shape and reusing its `flip_op`/`eval_range`/`eval_range_f64`
helpers verbatim, made `pub(crate)`) against that segment's `ColumnStats`;
a segment PROVABLY unable to match is never passed to
`ipc_cache::read_row_group` at all. The caller-side wiring
(`PhysicalPlanner`'s Scan arm calling `provider.scan_with_filter(...)`
unconditionally for any non-streaming-Parquet provider) was confirmed
ALREADY provider-agnostic by reading `src/physical/planner.rs` in full —
zero changes needed there. Deletion vectors remain fully respected
(pruned-out segments are simply never read; segments that ARE read still
go through the unchanged `filter_deleted_rows` step). Tracing via
`QE_DEBUG_NATIVE_PRUNING=1`, matching this codebase's established
env-gated diagnostic convention.

Real, traced confirmation: `examples/native_pruning_check.rs` against
real on-disk multi-segment native tables (`data/tpch-1gb-native/orders`,
2 segments; `.../lineitem`, 6 segments) shows real segment skips for
range/equality/AND/BETWEEN predicates (e.g. an AND-of-two-comparisons on
`lineitem.l_orderkey` skips 5 of 6 segments), cell-exact both against an
independent in-process unpruned baseline and against a fresh DuckDB
oracle over the same source parquet. 12 new hermetic unit tests (10
`segment_might_match` cases + 2 end-to-end `scan_with_filter` cases,
one of which composes pruning with a real deletion vector) all pass.
Full suite green in all four feature combinations, each exactly the
prior baseline + these 12 new tests, zero regressions; `cargo fmt --all
-- --check` clean. Full detail, every command, and the complete Outcome
section: `001.md`.

Not attempted by this task (explicitly task 002's job per the task
breakdown above): the Q4/Q12/Q13 before/after re-measurement (G3).
