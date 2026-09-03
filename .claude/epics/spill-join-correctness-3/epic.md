---
name: spill-join-correctness-3
status: in-progress
created: 2026-09-02T15:05:55Z
updated: 2026-09-03T18:54:09Z
progress: 80%
prd: .claude/prds/spill-join-correctness-3.md
github: (will be set on sync)
---

# Epic: spill-join-correctness-3

## Overview

Make the spill path provably correct at scale: settle the ~0.34%
duplicate-counting bug against the rewritten (oom-safety-hardening)
spill code with a high-trial recalibration, fix the two known SF=100
spill failures (Q4's SEMI-join refusal, Q13's temp-file rename error),
then certify TPC-H SF=100 cell-exact under budgets that force real
spilling.

## Architecture Decisions

1. **Measure before hunting.** The dup-counting bug's entire evidence
   base predates the spill-path rewrite. Task 001 recalibrates on
   today's code (≥5k chaos trials + ≥200 full-query repro trials)
   before any root-causing; the no-guess-fixes rule stands. A
   conditional task (002) exists only if a wrong answer reproduces.
2. **SEMI first, ANTI if free.** Partition routing already co-locates
   build/probe by key hash, so existence semantics survive partitioning;
   the risk is chunked read-back (a probe row must match at most once
   across chunks for SEMI, and only be emitted after ALL chunks for
   ANTI). Mechanism: per-probe-row match bitmaps carried across chunks;
   SEMI emits on first match, ANTI emits survivors after the last
   chunk. Outer joins stay refused (documented boundary).
3. **Q13's rename error is treated as a lifecycle bug, not rewritten
   around.** The prior epics' rename bugs (carried-run deletion) are the
   pattern library; find the exact double-use/early-delete, fix it
   minimally, pin it.
4. **SF=100 certification reuses existing validators** (safe_benchmark /
   sf100 validate scripts, DuckDB oracle, native_bench_compare) — no new
   harness shapes; the oom-cap harness gets SF=100-class inputs via its
   existing knobs.
5. **File discipline**: tasks 002/003/004 all live in spillable.rs —
   strictly sequential. Task 001 (measurement) and 005 (certification)
   touch no operator source.

## Technical Approach

### Backend Services
- `src/physical/operators/spillable.rs`: SEMI/ANTI spill support
  (`execute_spill_path`, `process_spilled_partition`, probe emission),
  Q13 rename-lifecycle fix, dup-bug fix if confirmed.
- Tests: dedicated SEMI/ANTI spill cell-exactness tests, deterministic
  regression tests for each fix.

### Infrastructure
- All runs wrapped + capped; SF=100 runs under generous-but-capped
  scopes (`SAFE_BUILD_MEM`/`MemoryMax` sized to the run, QE_MEM_CAP
  set); chaos/repro batches logged with preserved artifacts for any
  wrong trial.

## Implementation Strategy

Wave 1 (parallel): 001 recalibration (no source changes) + 003 Q13
rename root-cause/fix (spillable.rs).
Wave 2: 004 SEMI/ANTI spill (spillable.rs, after 003).
Wave 3: 002 dup-bug fix (only if 001 found a wrong answer; else closed
as verdict-only), then 005 SF=100 certification + close-out.

## Task Breakdown Preview

- [x] 001: Recalibrate the duplicate-counting bug on the rewritten
      spill path (≥5k chaos + ≥200 repro trials, verdict + CI)
- [x] 002: Conditional — root-cause + fix + regression-pin the dup bug
      (only if 001 reproduces it)
- [x] 003: Q13 SF=100 temp-file rename error: reproduce, root-cause, fix
- [x] 004: SEMI/ANTI join spill support (Q4 SF=100 completes cell-exact)
- [ ] 005: SF=100 certification + epic close-out (22/22 cell-exact
      parquet+native under spilling budgets, harness at scale, suites,
      docs)

## Dependencies

- Merged oom-safety-hardening spill code (main @ 0659d3e).
- `data/tpch-100gb` (+ native conversion, regenerate if stale).
- Chaos/oom-cap harnesses, DuckDB oracle scripts.

## Success Criteria (Technical)

G1-G5 as in the PRD; every fix carries a deterministic regression test;
every claim carries preserved run artifacts.

## Estimated Effort

5 tasks; 003/004 are the risk center (spillable.rs surgery), 001/005
are long-running but mechanical. Rough total: 28-40 focused hours plus
SF=100 machine time.

## Tasks Created
- [x] 001.md - Recalibrate the duplicate-counting bug on the rewritten spill path (parallel: true)
- [x] 002.md - Conditional: root-cause + fix + regression-pin the dup bug (parallel: false, after 001/003/004)
- [x] 003.md - Q13 SF=100 temp-file rename error: reproduce, root-cause, fix (parallel: true, conflicts: 002/004)
- [x] 004.md - SEMI/ANTI join spill support, Q4 SF=100 (parallel: false, after 003)
- [ ] 005.md - SF=100 certification + epic close-out (parallel: false, last)

Total tasks: 5
Parallel tasks: 2
Sequential tasks: 3
Estimated total effort: 28-40 hours + SF=100 machine time
