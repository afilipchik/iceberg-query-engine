---
name: native-table-pruning
description: Wire native tables' already-computed per-segment min/max stats into actual scan-time segment skipping
status: completed (2026-08-27) — mechanism shipped and validated (G1/G2/G4 met); G3 measured honestly and did NOT close the named Q4/Q12/Q13 regression (root cause is join-spill cost/gaps, not scan pruning, for this dataset — see .claude/epics/archived/native-table-pruning/epic.md's close-out)
created: 2026-08-27T07:44:44Z
---

# PRD: native-table-pruning

## Executive Summary

Native tables already compute per-segment `ColumnStats` (min/max/null-count)
at write time and already feed them into the cost-based optimizer's join
reordering — but `NativeTable::scan()` never uses them to skip segments at
read time. Every query decodes every active segment in full, relying
entirely on a post-scan `FilterExec` for correctness. This is the single
cheapest, best-evidenced gap identified by this session's modern-OLAP
research synthesis (`.claude/plans/research/2026-08-27-modern-olap-research-
synthesis.md`, §2.2, §3 item 2): fine-grained, out-of-band min/max pruning
is a 25+ year, universally-validated technique this engine already has half
the mechanism for. `CLAUDE.md` itself already attributes a real, measured
regression (Q4/Q12/Q13 pushing larger post-filter join build sides into the
spill threshold at scale) to this exact gap.

## Problem Statement

`NativeTable::scan_with_filter` (`src/storage/native_table.rs`) is not
overridden — it's the `TableProvider` trait's default, which just calls
`scan(projection)` and ignores any filter entirely. Every query against a
native table decodes every byte of every active segment, then filters
in-memory via `FilterExec`. Parquet's own reader (`src/storage/parquet.rs`,
`row_group_pruning.rs`) already does the equivalent pruning against
row-group statistics — native tables are missing the read-side half of a
mechanism whose write-side half (segment `ColumnStats`) already exists.

## User Stories

**As someone querying a filtered subset of a large native table**, I want
the engine to skip segments that provably cannot contain matching rows,
so that a selective query doesn't pay to decode data it will immediately
discard.
- Acceptance: a query with a numeric/date range or equality predicate on a
  column with segment-level min/max stats measurably decodes fewer
  segments and runs faster than before, with byte-for-byte identical
  results to the unpruned path.

**As the engine's maintainer**, I want pruning to be strictly a performance
optimization, never a correctness risk.
- Acceptance: pruning only SKIPS a segment when the predicate is PROVABLY
  unsatisfiable against that segment's stats (mirroring
  `row_group_pruning.rs`'s own conservative logic); every segment that is
  scanned still passes through the existing `FilterExec` unchanged, so a
  wrong pruning decision could only ever make a query slower (by failing
  to skip), never wrong (by skipping a segment it shouldn't have).

## Functional Requirements

1. `NativeTable::scan_with_filter` implemented for real: given a pushable
   predicate (AND-of-simple-comparisons/BETWEEN, matching what
   `row_group_pruning.rs` already recognizes for Parquet — reuse or
   directly adapt that logic/pattern rather than inventing a new
   predicate-recognition dialect), evaluate it against each active
   segment's `ColumnStats` (`min_i64`/`max_i64`/`min_f64`/`max_f64`/
   `null_count`) and skip segments that cannot match before calling
   `ipc_cache::read_row_group` on them.
2. Whatever caller/eligibility mechanism determines when a predicate gets
   pushed down to `scan_with_filter` for Parquet must be checked and, if
   necessary, extended so native tables become eligible the same way —
   read the actual current wiring before assuming it's provider-agnostic
   already.
3. String/binary columns have no min/max in `ColumnStats` today (only
   `null_count`) — predicates on those columns simply never prune (no
   segment is skipped for them), not an error, not a correctness risk.
   Extending `ColumnStats` to carry string bounds is explicitly out of
   scope for this PRD (see Out of Scope).
4. Deletion vectors (`Segment::deleted_rows`, from the mutation epic)
   remain fully respected — pruning decides whether to READ a segment at
   all; the existing deletion-vector filter inside `scan()` still applies
   unchanged to whatever segments ARE read.

## Non-Functional Requirements

- **Correctness always preserved.** A pruning decision must never cause a
  row that could match the predicate to be silently dropped. When in
  doubt (stats absent, predicate not recognized, comparison inconclusive),
  scan the segment — never skip on uncertainty.
- **Cell-exact validated**, the way every mechanism in this codebase is —
  compare full results with pruning on vs. off, and against an independent
  DuckDB oracle, not row-count-only.
- **No regression** to unfiltered/full-scan queries, to the write path, or
  to any other table type's behavior.
- Every build through `scripts/claude-safe-build.sh`.

## Success Criteria

- G1: a numeric/date-range-filtered query against a native table with
  multiple segments measurably skips at least one segment when stats
  prove it can't match, confirmed via tracing/instrumentation, not
  inferred from wall-clock time alone.
- G2: cell-exact identical results with pruning on vs. off, across varied
  predicate shapes, and vs. an independent DuckDB oracle.
- G3: the named Q4/Q12/Q13 regression (`CLAUDE.md`'s "Current
  limitations" section) is re-measured with pruning enabled — report the
  real before/after numbers, whatever they are, including if pruning
  alone doesn't fully close the gap (the join-spill correctness/
  performance work is a separate, parallel effort — see
  `spill-join-correctness-2`).
- G4: full suite green; no regression to unfiltered-query performance.

## Constraints & Assumptions

- Builds on the existing `row_group_pruning.rs` pattern for Parquet —
  reuse its predicate-recognition logic/shape rather than reinventing an
  equivalent for native tables from scratch, unless a native-table-specific
  reason to diverge is found and justified.
- `ColumnStats` (`src/storage/native_manifest.rs`) is the existing,
  unchanged data source — this PRD is about USING it at read time, not
  about collecting new statistics.

## Out of Scope

- Extending `ColumnStats` to carry string/binary min/max or dictionary-
  level NDV — real, valuable, and named as a candidate in the research
  synthesis, but a separate, larger change to the manifest format itself.
- Bloom filters or any other non-min/max pruning index — named in the
  research synthesis as a separate, valuable technique, not bundled here.
- Any change to the write path, the manifest format's own statistics
  computation, or Parquet/Iceberg/Lance's own (already-working) pruning.
- Fixing the still-open `spill-join-correctness` wrong-answer bug or its
  slowness — a separate, parallel effort (`spill-join-correctness-2`);
  this PRD may reduce how often large post-filter build sides reach the
  spill threshold in the first place, but does not touch the spill
  mechanism itself.

## Dependencies

- `src/storage/native_table.rs`, `native_manifest.rs` (`ColumnStats`).
- `src/storage/row_group_pruning.rs` (the pattern to mirror).
- `src/storage/parquet.rs`'s `read_file_with_filter` (the caller-side
  wiring to understand and extend).
- `scripts/claude-safe-build.sh` for every build.
