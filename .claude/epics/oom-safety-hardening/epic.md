---
name: oom-safety-hardening
status: in-progress
created: 2026-08-29T21:30:20Z
progress: 14%
prd: .claude/prds/oom-safety-hardening.md
github: (will be set on sync)
---

# Epic: oom-safety-hardening

## Overview

Make the Memory Safety Rule true everywhere: with a configured
`--memory-limit`, every code path either completes by spilling to disk or
refuses cleanly by name — never gets OOM-killed and never aborts at the
new in-binary `RLIMIT_DATA` cap. The engine now has hard containment
(`enforce_process_memory_cap()`, `QE_MEM_CAP`, shipped 2026-08-29): an
overshoot kills only the engine. This epic closes the overshoots
themselves, in the three known gaps (aggregate/sort collect-then-decide,
native-table scan refuse-instead-of-stream, un-admission-checked
INSERT/CTAS), plus the size-estimate bug that feeds every spill decision.

## Architecture Decisions

1. **Estimate first.** `estimate_batch_size`'s ~4,000x Dictionary-column
   overestimate (`spill-size-estimate-fix` epic, currently 0%) corrupts
   every spill decision downstream. Its two tasks are executed FIRST, in
   their own epic (kept separate so its history stays coherent), and this
   epic's operator work builds on the corrected estimate. No duplicate
   implementation here.
2. **One reference pattern, no new inventions.** G1's fix for
   `SpillableHashAggregateExec` and `ExternalSortExec` mirrors
   `SpillableHashJoinExec::execute_spill_path`'s already-shipped streaming
   two-phase reservation (`compute_build_decision` +
   `stream_merge_input_partitions`, `spill-join-correctness-2` task 002)
   — read batch-by-batch with a running size total, decide to spill DURING
   ingestion.
3. **Harness before fixes.** The adversarial harness (patterned on
   `examples/spill_join_oom_repro.rs`) is built first and run against
   CURRENT code to record failing pre-fix evidence per operator — the same
   pre-fix/post-fix discipline that epic used. It exercises BOTH cap
   levers: `systemd-run -p MemoryMax=` (RSS, kernel SIGKILL) and
   `QE_MEM_CAP` (mapped anonymous bytes, engine abort). "Pass" = complete
   or refuse cleanly under BOTH.
4. **Streaming native scan is a new operator, not a rewrite of `scan()`.**
   Mirror the parquet split: `ParquetTable::scan()` materializes,
   `StreamingParquetScanExec` streams. A `NativeTableScanExec`-style
   streaming path (segment-at-a-time via the existing
   `ipc_cache::read_row_group` reader) feeds spilling consumers;
   `check_scan_budget` stays as the guard for shapes that genuinely must
   materialize (raw `SELECT *` dumps) — that boundary gets documented, not
   silently narrowed.
5. **Write-path admission mirrors `check_scan_budget`.** Same
   `memory_limit * spill_threshold` formula, same named-refusal error
   style, placed in `create_table_as_select`/`insert_into_native_table`
   before expensive work. The estimate is the source's on-disk bytes
   scaled by the bounded-merge concurrency — deliberately conservative;
   false-refusal risk is called out and tested.

## Technical Approach

### Backend Services

- `src/physical/operators/spillable.rs`: streaming two-phase reservation
  for `SpillableHashAggregateExec::execute` and
  `ExternalSortExec::execute`, replacing
  `collect_input_partitions_concurrently` on those paths.
- `src/storage/native_table.rs` + `src/physical/planner.rs`: streaming
  native-table scan path for spill-capable consumers; budget check
  retained for materializing shapes.
- `src/execution/context.rs`: formal admission check on INSERT/CTAS.
- `examples/` + `scripts/`: one reusable adversarial cap harness.

### Infrastructure

Every build/test/repro through `scripts/claude-safe-build.sh` (hook-
enforced); every adversarial run under a real cgroup or `QE_MEM_CAP` cap
with OOM-kill absence verified via exit codes and `journalctl -k`.

## Implementation Strategy

Phase 0 (dependency): finish `spill-size-estimate-fix` (its own epic).
Phase 1 (diagnose): harness + pre-fix evidence + root-cause the
2026-08-28 107.2G incident (G4).
Phase 2 (fix, parallel where files allow): aggregate/sort streaming
(same file — sequential), native streaming scan, write admission.
Phase 3 (prove): post-fix harness sweep, full suite in all four feature
combos, perf non-regression re-runs, docs.

## Task Breakdown Preview

- [ ] 001: Adversarial cap harness + pre-fix evidence + 2026-08-28
      root-cause (parallel with spill-size-estimate-fix)
- [ ] 002: Streaming two-phase reservation for SpillableHashAggregateExec
- [ ] 003: Streaming two-phase reservation for ExternalSortExec
- [ ] 004: Streaming native-table scan into spilling consumers
- [ ] 005: Formal admission check for INSERT/CTAS write path
- [ ] 006: QA close-out: post-fix harness sweep, full suite, perf
      non-regression, docs

## Dependencies

- `spill-size-estimate-fix` epic (external, runs first — tasks 002/003
  build on the corrected estimate).
- `SpillableHashJoinExec::execute_spill_path` as the reference pattern.
- `examples/spill_join_oom_repro.rs` as the harness pattern.
- `enforce_process_memory_cap()` / `QE_MEM_CAP` (shipped 2026-08-29) as
  the second cap lever.

## Success Criteria (Technical)

G1-G6 as in the PRD, with G1/G6 evidence required to be
hardware-backed (real caps, real exit codes) and pre-fix-failing /
post-fix-passing, and G5 requiring all four feature combinations green
plus unregressed recorded native-table and RSS numbers.

## Tasks Created
- [x] 001.md - Adversarial cap harness + pre-fix evidence + 2026-08-28 root-cause (parallel: true) — CLOSED 2026-08-29
- [ ] 002.md - Streaming two-phase reservation for SpillableHashAggregateExec (parallel: false, conflicts: 003)
- [ ] 003.md - Streaming two-phase reservation for ExternalSortExec (parallel: false, conflicts: 002)
- [ ] 004.md - Streaming native-table scan into spilling consumers (parallel: true)
- [ ] 005.md - Formal admission check for INSERT/CTAS write path (parallel: true)
- [ ] 006.md - QA close-out: post-fix harness sweep, full suite, perf non-regression, docs (parallel: false)
- [ ] 007.md - Budget the join spill path's hash-table memory — the confirmed fourth gap from 001 (parallel: false, conflicts: 002/003, runs FIRST in the spillable.rs queue)

Total tasks: 7
Parallel tasks: 3
Sequential tasks: 4
Estimated total effort: 47 hours

Note (2026-08-29): task 001 closed — root cause of the accounting hole
AND the 2026-08-28 incident both named with evidence (unbudgeted spill-
path hash tables, ~10-20x amplification; incident was a bare uncapped
repro run pre-hook, 58.4G at kill / 107.2G was scope lifetime peak).
Task 007 added per 001's fourth-gap rule.

## Estimated Effort

6 tasks + 2 external dependency tasks; the two spillable.rs rewrites are
the risk center (L each); harness and admission check are S-M. Rough
total: 30-45 focused hours.
