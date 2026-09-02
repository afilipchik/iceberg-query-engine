---
name: spill-size-estimate-fix
status: completed
created: 2026-08-28T19:02:39Z
updated: 2026-08-29T23:59:00Z
progress: 100%
prd: .claude/prds/spill-size-estimate-fix.md
github: (will be set on sync)
---

# Epic: spill-size-estimate-fix

## Overview

A diagnostic investigation into Q12's native-table slowdown found a
real, precisely-located bug: `SpillableHashJoinExec`'s build-side size
estimator (`estimate_batch_size`, `src/physical/operators/spillable.rs`)
reports Dictionary-typed columns' size via Arrow's
`get_array_memory_size()`, which returns the whole underlying mmap
buffer's size, not the column's actual logical content — a ~4,000x
overestimate for Q12's real ~42MB build side, which spuriously crosses
the spill threshold. Fix it.

## Architecture Decisions

- **Fix the estimate, not the threshold or the spill mechanism.** The
  spill decision's own logic (byte-size check, 80% of memory_limit) is
  correct; only the INPUT to that check is wrong. No change to
  `compute_build_decision`'s own threshold/percentage logic.
- **Mirror the existing correct pattern.** `estimate_batch_size` already
  handles Utf8/Binary columns correctly (content-aware, not
  capacity-aware) — the fix for Dictionary columns should follow the
  same shape, not invent a new approach.
- **Correctness over aggressiveness.** The fix must not cause a
  genuinely oversized build side to stay in memory and OOM — validate
  with a real stress case, not just the now-fixed Q12 case.

## Technical Approach

### The fix (task 001)
`estimate_batch_size`'s Dictionary-column handling: compute size from
`keys.len() * key_width + dictionary_values_actual_bytes`, not
`get_array_memory_size()`. Audit the same function's fallback branch for
any other type that could have the identical mmap-capacity-vs-logical-
content problem.

### Validation (task 001, continued)
Re-run the established Q12 native-table repro with `QE_SPILL_DEBUG=1`,
confirm it no longer spills, confirm cell-exact correctness, confirm
wall time improves. Construct a real oversized-build-side stress case
and confirm it still spills correctly (no OOM regression).

### Broader sweep + QA close-out (task 002)
Check other TPC-H queries for the same Dictionary-column-feeds-a-
spilling-join shape over native tables; report findings. Full suite,
docs, epic close.

## Task Breakdown Preview

- 001: Fix `estimate_batch_size`'s Dictionary-column handling, validate
  against Q12 and a real oversized-build-side stress case (parallel:
  false, entry point)
- 002: Broader sweep for other affected queries, full suite, docs, epic
  close (parallel: false, depends on 001)

Total tasks: 2
Estimated total effort: S — precisely diagnosed, narrowly scoped.

## Dependencies

- `src/physical/operators/spillable.rs`.
- `examples/spill_size_estimate_check.rs` (the diagnostic that found
  this).
- `scripts/claude-safe-build.sh` for every build.

## Success Criteria (Technical)

- G1: Q12 over native tables no longer spills for its real build side;
  wall time closes most of the gap to Parquet.
- G2: cell-exact correctness preserved.
- G3: a genuinely oversized build side still spills correctly.
- G4: full suite green.

## Estimated Effort

- 001: S-M.
- 002: S.

## Tasks Created
- [x] 001.md - Fix estimate_batch_size + validate against Q12 and a real stress case (parallel: false) — CLOSED 2026-08-29; the stress-case criterion lives on in oom-safety-hardening task 001 (formal handoff, see 001.md's Outcome part 2)
- [x] 002.md - Broader sweep, full suite, docs, epic close (parallel: false) — CLOSED 2026-08-29 jointly with oom-safety-hardening 006 (shared QA close-out)

Total tasks: 2
Parallel tasks: 0
Sequential tasks: 2
Estimated total effort: S

## Epic close-out (2026-08-29 — COMPLETED)

Task 001 closed 2026-08-29 (fix + Q12 validation; stress-case criterion
formally handed off to `oom-safety-hardening` 001/007, where it was
root-caused and FIXED — the unbudgeted spill-path hash tables are now
budgeted, both adversarial repros complete under real caps). Task 002
closed jointly with `oom-safety-hardening` task 006's QA close-out (one
shared final-HEAD run set; see that task's Outcome and 002.md's own).

### G1-G4 verdicts

- **G1 — MET.** Q12 over native tables no longer spills (zero
  `QE_SPILL_DEBUG` join-spill traces at 40G); the ~4,000x Dictionary
  mmap-capacity overestimate (~167.7GB claimed vs ~42MB real) is fixed
  content-aware via `ArrayData::get_slice_memory_size()`. Wall time
  0.18s — Q12 native (177.6ms in the full sweep) is now FASTER than the
  parquet leg (216ms), i.e. the gap to Parquet is fully closed.
- **G2 — MET.** Cell-exact 3/3 at final HEAD (`MAIL,353822,529784` /
  `SHIP,352224,530051`), plus the committed unit regression test.
- **G3 — MET.** Genuinely oversized build sides still spill and now
  survive: the fix never under-counted (both adversarial repros still
  entered the spill path), and after `oom-safety-hardening` 007 the
  500MB-limit Int32 repro completes at 555MB peak under a 3G cap and
  the 30MB-limit Dictionary repro at 106MB under 2G — no OOM, no spill
  suppression. Harness join-consumer scenario shows real observed spill
  end to end.
- **G4 — MET.** Full suite green at HEAD in all four combos (1317/1382/
  1326/1320, 0 failures, fmt clean).

Broader-sweep finding (task 002's charter): Q12 was the ONLY affected
query — no other TPC-H query's spill behavior changed, confirmed by a
zero-spill-trace 22-query sweep at the same premise.

Directory archival to `.claude/epics/archived/` at branch merge.
