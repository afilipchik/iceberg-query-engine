---
name: oom-safety-hardening
status: completed
created: 2026-08-29T21:30:20Z
progress: 100%
updated: 2026-08-29T23:59:00Z
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
- [x] 002.md - Streaming two-phase reservation for SpillableHashAggregateExec (parallel: false, conflicts: 003)
- [x] 003.md - Streaming two-phase reservation for ExternalSortExec (parallel: false, conflicts: 002)
- [x] 004.md - Streaming native-table scan into spilling consumers (parallel: true)
- [x] 005.md - Formal admission check for INSERT/CTAS write path (parallel: true)
- [x] 006.md - QA close-out: post-fix harness sweep, full suite, perf non-regression, docs (parallel: false)
- [x] 007.md - Budget the join spill path's hash-table memory — the confirmed fourth gap from 001 (parallel: false, conflicts: 002/003, runs FIRST in the spillable.rs queue)

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

## Epic close-out (2026-08-29, task 006 — COMPLETED)

All 7 tasks closed. Everything below re-verified at FINAL HEAD by task
006 (do-not-trust-mid-epic-results discipline); full evidence in
`006.md`'s Outcome and `.scratch/oom006/`.

### G1-G6 verdicts

- **G1 — MET.** `SpillableHashAggregateExec` (task 002) and
  `ExternalSortExec` (task 003) now stream input with a running
  `estimate_batch_size` total and decide to spill MID-STREAM (the join's
  two-phase reservation pattern, mirrored), with accounted finalize /
  streamed merge delivery. Hardware-backed before/after: harness agg and
  sort scenarios were FAIL 137 (cgroup kill) / FAIL 134 (rlimit abort) at
  1.0-2.0GB peaks pre-fix (task 001's recorded battery); at final HEAD
  both complete exit 0 under the SAME 1G caps — agg 406/408MB peak with
  groups=1000003 exact, sort 845/858MB with 250M rows globally ordered.
- **G2 — MET.** Aggregate AND join consumer shapes over a native table
  exceeding `check_scan_budget` now COMPLETE via
  `NativeStreamingScanExec` + real spilling (task 004): harness
  native-scan flipped clean-refusal → completed (exit 0, 164/148MB peak
  on a 5.64GB table at 512MB limit); join consumer verified with
  observed `execute_spill_path` spill on both levers. The remaining
  raw-materialization boundary (`SELECT *`/filter-only/ORDER BY-only
  dumps still refuse by name) is deliberate, documented in CLAUDE.md +
  `native_table.rs`, and pinned by tests.
- **G3 — MET.** `check_insert_write_admission` (task 005): harness
  insert scenario is a clean named refusal (exit 2, ~30MB peak, both
  levers) citing check name, exact estimate/budget bytes, and both
  knobs; admitted-side calibration held (2GiB limit → completed under a
  real 2G cap). RSS band unregressed at final HEAD: sql-mode INSERT of
  60M rows peaked 1,670,048 KB (~1.59GB, band ~1.6-1.7GB).
- **G4 — MET.** Task 001 NAMED the root cause with profiler evidence:
  `execute_spill_path`'s per-partition hash tables were built with ZERO
  memory accounting (~10-20x amplification over budgeted batch bytes) —
  a distinct fourth gap, fixed inside this epic as task 007 (measured +
  predicted table budgeting, eviction, chunked read-back). The
  2026-08-28 incident itself: a bare uncapped repro run pre-hook (58.4G
  at kill). Also fixed en route: two latent self-deadlocks in the
  `QE_ALLOC_PROFILE` diagnostic allocator.
- **G5 — MET.** Cell-exactness pinned everywhere touched (Dictionary
  agg/sort/join spill tests, chunk-straddling duplicate-key test,
  streaming-scan vs independent recomputation, Q12 3/3 cell-exact).
  Full suite green in all four combos at HEAD: default 1317/0, lance
  1382/0, gpu 1326/0, pulsar 1320/0 (pre-epic 1285/1350/1294/1288 —
  +32 accounted-for new tests each), `cargo fmt` clean. Perf
  unregressed: SF=10 native sweep 22/22, 5288ms (vs 8.20s pre-fix,
  better than the 5324-5667ms band; Q12 177.6ms); parquet cache-off
  7.29-7.37s (22/22, historical 7.03-7.40s range); M1/M2 distributed
  gates PASS.
- **G6 — MET.** `examples/oom_cap_harness.rs` +
  `scripts/oom_cap_harness.sh` (task 001): one documented, reusable
  harness, machine-readable verdicts, journal kill evidence, BOTH cap
  levers, run pre-fix (001) and post-fix per task (002/003/004/005/007)
  and finally 006's full sweep: **8/8 PASS, zero 137s, zero 134s**.

### Remains OPEN by design (tracked, not regressions)

1. **The ~0.34% spilling-INNER-join duplicate-counting bug**
   (spill-join-correctness epic) — out of this epic's scope, still
   open, still needs its own root-cause task. Task 007's evidence adds:
   nothing implicates the (now-budgeted) table build/chunked read-back —
   all write-vs-read checksums `hash-check-ok`.
2. **The rollup last-ULP float flake** — proven PRE-existing by task 003
   (`.scratch/oom003/pre003_rollup_flake_repro.log`); did not manifest
   in 006's four-combo sweep; needs its own small task (tolerance or
   deterministic accumulation order).
3. **Task 004's documented refusal boundary**: raw materializing shapes
   over an over-budget native table refuse by name (streaming them would
   move the OOM to the QueryResult root). Deliberate; pinned by tests.
4. **RLIMIT_DATA coarseness caveat**: `QE_MEM_CAP` counts virtual
   private-anonymous mappings (mimalloc ~1GiB arena + tokio stacks up
   front), so it needs ~1GB headroom and cannot enforce tight sub-GB
   budgets — documented in CLAUDE.md's Memory Safety Rule; the cgroup
   lever is the precise one.
5. Pre-existing, separately documented: Q4 SEMI-join spill unsupported
   (clean refusal), Q13 SF=100 spill temp-file-rename error, spill-dir
   collision between co-located serve processes.

Archival of this epic directory to `.claude/epics/archived/` happens at
branch merge, per repo convention.
