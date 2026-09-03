---
name: spill-join-correctness-3
status: completed
created: 2026-09-02T15:05:55Z
updated: 2026-09-03T21:04:50Z
progress: 100%
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
- [x] 005.md - SF=100 certification + epic close-out (parallel: false, last)

Total tasks: 5
Parallel tasks: 2
Sequential tasks: 3
Estimated total effort: 28-40 hours + SF=100 machine time

## Close-out (2026-09-03)

All five tasks closed. Commits on `epic/spill-join-correctness-3`:
306dc15 (PRD/epic/tasks), 9f8b5e6 / 8a71139 / 4b98b1b / b89ee2f (001),
740e8f4 (003), 47658ff / 570a8cb (bookkeeping), 67f7cea + d378744 (004
operator, tests, harness), 7607f34 (004/002 close + CLAUDE.md), and the
005 close-out commit. Evidence per task in `00N.md` "Outcome" +
`updates/00N/`; artifacts under `.scratch/sjc3-00N/`.

### G-verdicts (PRD Success Criteria)

- **G1 — duplicate-counting verdict, ≥5,200 trials: MET.** 0 wrong /
  6,041 verified trials on the rewritten spill path (5,600 chaos + 441
  cold Q12-class full-query trials at genuinely spilling budgets),
  89,212 hash-check-ok / 0 mismatch; pooled 95% CI [0%, 0.061%]
  (Q12-class leg alone [0%, 0.833%]) — stated as a bound, not proof. 002
  closed verdict-only. Task 004 added 300 chaos trials (25,188 / 0) and
  task 005 added SF=100 spilling sweeps with 1,932 further hash checks and
  0 mismatches.
- **G2 — Q4 SF=100 via a spilling SEMI join, cell-exact; SEMI/ANTI spill
  tests: MET.** Q4 native SF=100 completes cell-exact at 64M (61.2s) and
  16M (65.6s) through `execute_spill_path` (62/64 partitions spilled,
  122-128 hash-check-ok / 0 mismatch); the historical 100G premise no
  longer refuses even pre-fix (80M is the pre-fix floor). SEMI and ANTI
  both shipped, in BOTH build orientations, each pinned against an
  independent naive-join ground truth (dense/sparse, chunk-straddling
  duplicates, Dictionary keys, NULL keys, the two ANTI hazards); harness
  semi-join/anti-join flipped 4/4 REFUSED → 8/8 COMPLETED (12/12 more at
  SF=100-class in 005).
- **G3 — Q13 SF=100 cell-exact, rename mechanism named: MET.** Root-caused
  with a live reproduction at the pruning epic's failing commit (the
  spillable AGGREGATE's `merge_parquet_files` rename window on a PID-less
  shared spill root — never the join); doubly removed on main; pinned by
  an adversarial-deletion test; Q13 SF=100 cell-exact at 100G (13.5s in
  003; 10.2s in 005's sweep).
- **G4 — SF=100 certification, 22/22 parquet + native, real spill on
  heavy queries, zero OOM, harness at scale: MET, with the two boundaries
  stated.** Parquet: 22/22 at 64G, 8G and **1G (6 spilling queries, join
  spill on Q09/Q16, 0 mismatches)**; at 256M 20/22 + Q20/Q21 clean named
  refusals at the documented LEFT-join / ON-filter boundaries. Native:
  22/22 at 100G; 1G: 17/22 cell-exact + 5 clean native-scan admission refusals at the documented native-scan boundary (Q02/Q10/Q11/Q15/Q20: over-budget native scans feeding joins), 5 spilling queries incl. join spill on Q09/Q16, 366 hash-check-ok / 0 mismatch, zero OOM. Harness 12/12 at SF=100-class.
  Zero kernel kills / rlimit aborts in any engine run.
- **G5 — four suites green, no regression, CLAUDE.md updated: MET.**
  default 1326/0/2, lance 1391/0/3, gpu 1335/0/2, pulsar 1329/0/2 (each =
  baseline + 9 new, +1 ignored); SF=10 harness 8/8; native 5,524ms
  (band 5288-5667); parquet cache-off 7.29s (7.03-7.40); INSERT RSS
  1.57GB (~1.6-1.7); M1/M2 PASS; Q16/Q22 unchanged. CLAUDE.md: Q4 + Q13
  limitation bullets closed, duplicate-counting status updated with the
  bound, SF=100 certification note with every premise, plus two newly
  documented items.

### Found on the way (recorded, not hidden, not this epic's scope)

1. **Two pre-existing spill-path bugs fixed in 004** (both INNER-
   reachable): Dictionary join keys were `JoinValue::Null` on the spill
   path (a spilling join on a Dictionary key returned zero rows); an
   empty partition evicted when the first batch crossed the threshold
   became a file-less `SpilledPartition` (read-back ENOENT with sparse
   keys).
2. **OPEN — in-memory `HashJoinExec`** (hash_join.rs): build-side-output
   SEMI/ANTI with `Dictionary(Int32,Utf8)` keys and repeated build keys
   marks one build row per distinct key (SEMI 20 vs 30,000; ANTI 59,980
   vs 30,000; Utf8 control correct). Pinned `#[ignore]`d in
   `in_memory_hash_join_findings_from_task004_fixtures`. Needs its own
   task.
3. **Documented boundary — the join spill path materializes the whole
   probe side** before probing; correct but the probe side is unbounded
   by the budget (Q4@64M needed a 32G cap; the SF=100-class harness join
   peaked at 8GB for a 300M-row probe). Together with single-threaded
   spilled-partition processing this is why Q09 takes ~1,400s at 1G.
   Natural next join-spill task.
4. **Boundaries confirmed on TPC-H**: LEFT/RIGHT/FULL spill (Q20 at
   256M) and ON-clause-filter spill (Q21 at 256M) refuse by name; on
   NATIVE tables at 1G, over-budget native scans feeding joins (Q02, Q10,
   Q11, Q15, Q20) refuse by name at the native-scan admission check
   (`NativeTable::scan()` streams only into aggregate-covered consumers)
   — the parquet provider streams, so the same queries complete at 1G on
   parquet. Three named boundaries, all clean refusals.

Archive deferred to merge (`.claude/epics/archived/` on `main`).
