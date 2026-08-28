---
name: spill-size-estimate-fix
description: Fix SpillableHashJoinExec's build-side size estimator massively overcounting Dictionary-typed (and similar) columns, causing unnecessary spills
status: backlog
created: 2026-08-28T19:02:39Z
---

# PRD: spill-size-estimate-fix

## Executive Summary

A diagnostic investigation into TPC-H Q12's ~20x slowdown over native
tables (3.14s vs ~154ms over Parquet, from the SF=10 six-way benchmark,
`.claude/plans/research/2026-08-28-sf10-sixway-benchmark.md`) found the
cause is not pruning, join order, or genuine memory pressure — it's a
real bug in `SpillableHashJoinExec`'s build-side size estimator.
`estimate_batch_size` (`src/physical/operators/spillable.rs`) reports
Dictionary-typed columns' size via Arrow's `get_array_memory_size()`,
which returns the size of the ENTIRE underlying mmap buffer a column's
Arrow `Buffer` points into — not the column's actual logical content.
Native table segments are read as one large mmap'd Buffer per segment
file, so a Dictionary column (e.g. `l_shipmode`) drags the WHOLE
segment's on-disk size along as its "size," roughly 3x over (keys
buffer, offsets buffer, values buffer each independently reporting the
full mmap capacity). Measured: Q12's real build side is ~42MB; the
estimator reports ~167.7GB — a ~4,000x overestimate — which crosses the
spill threshold and forces an unnecessary, expensive spill.

## Problem Statement

`estimate_batch_size`'s fallback branch (`_ => c.get_array_memory_size()`)
correctly handles Utf8/Binary columns with a content-aware calculation,
but Dictionary-typed columns (and potentially other non-primitive types
reachable by the same fallback) fall through to the buggy generic path.
This affects any native-table query where a Dictionary-coerced column
feeds a hash join's build side — Q12 is the one currently measured and
confirmed, but the bug is general, not query-specific.

## User Stories

**As someone running a native-table query whose build side includes a
Dictionary-coerced column**, I want the join to spill only when the
build side is genuinely too large for memory, not because of a
measurement artifact.
- Acceptance: `estimate_batch_size` reports a Dictionary column's size
  from its actual logical content (keys array size + real dictionary
  values bytes), matching the existing correct pattern already used for
  Utf8/Binary columns. Q12 over native tables no longer spills for its
  real ~42MB build side, and its wall time closes most of the gap to
  the Parquet leg's ~154ms.

## Functional Requirements

1. Fix `estimate_batch_size`'s Dictionary-column handling to compute
   size from actual logical content, not `get_array_memory_size()`'s
   mmap-capacity artifact — mirror the existing Utf8/Binary branch's
   approach.
2. Audit the SAME function for any other type reaching the buggy
   generic fallback that could have the identical issue (e.g. any other
   Arrow array type whose `Buffer`s could be mmap-backed and larger than
   their logical slice) — fix or explicitly document as safe, don't
   leave a sibling instance unexamined.
3. Sweep for other TPC-H queries (or other realistic query shapes) whose
   native-table build side includes a Dictionary column and is currently
   spilling unnecessarily because of this bug — report what's found,
   whether or not every instance gets fixed in this PRD's own scope.

## Non-Functional Requirements

- **Cell-exact correctness preserved** — this is a size-estimation fix
  only; it must not change which rows end up in the join's output, only
  whether/when spilling engages.
- **No regression** to genuine spill cases — a build side that IS
  actually too large for memory must still spill correctly (this fix
  must not accidentally suppress spilling when it's really needed,
  which would reintroduce an OOM risk).
- Every build through `scripts/claude-safe-build.sh`.

## Success Criteria

- G1: Q12 over native tables at SF=10 no longer spills for its real
  build side (confirmed via `QE_SPILL_DEBUG=1`), and wall time closes
  most of the gap to the Parquet leg.
- G2: cell-exact correctness preserved, validated against a DuckDB
  oracle.
- G3: a genuinely oversized build side (a real stress case) still
  spills correctly — no OOM regression.
- G4: full suite green, including existing spill-path tests.

## Constraints & Assumptions

- Builds directly on the diagnostic investigation already performed
  this session (recorded in this conversation and
  `examples/spill_size_estimate_check.rs`) — read it before starting,
  don't re-derive the root cause.
- This PRD is narrowly scoped to the size-ESTIMATION bug. It does not
  revisit the spill decision's own threshold/percentage logic, the
  spill file format, or native-table pruning — those were already
  investigated and correctly ruled out as the cause for this specific
  problem.

## Out of Scope

- Sort-on-write / clustering for native table segments (a different,
  real, independently-valuable idea from the earlier strategy outline,
  already shown not to be the cause here).
- A lighter-weight spill file format (also independently valuable, but
  unrelated to this specific bug — fixing this bug should make most
  Dictionary-column spills unnecessary in the first place).
- Extending the runtime-filter mechanism to native-table probe sides.

## Dependencies

- `src/physical/operators/spillable.rs` (`estimate_batch_size`,
  `compute_build_decision`).
- `examples/spill_size_estimate_check.rs` (the diagnostic that found
  and confirmed this) — reusable as a starting point for validation.
- `scripts/claude-safe-build.sh` for every build.
