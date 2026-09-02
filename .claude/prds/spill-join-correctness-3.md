---
name: spill-join-correctness-3
description: Settle the ~0.34% duplicate-counting bug on the rewritten spill path, close the two known SF=100 spill failures (Q4 SEMI-join refusal, Q13 rename error), and certify TPC-H SF=100 under tight memory budgets
status: active
created: 2026-09-02T15:05:55Z
---

# PRD: spill-join-correctness-3

## Executive Summary

The oom-safety-hardening epic (merged 2026-09-02, commit 0659d3e) made
every covered operator spill or refuse cleanly under a configured
`--memory-limit`, and in doing so **rewrote most of `SpillableHashJoinExec`'s
spill path** (direct hash-table budgeting + eviction, chunked read-back;
task 007) alongside new streaming ingestion for aggregate/sort. Three
things remain open, all in or near that same file, and together they are
the difference between "memory-safe at SF=10" and "provably correct and
memory-safe at SF=100":

1. **The ~0.34%-rate duplicate-counting wrong-answer bug** in the
   spilling INNER join (open across `spill-join-correctness` /
   `spill-join-correctness-2`; root cause never confirmed; 1 wrong
   result in 290 pooled trials, 95% CI [0.01%, 1.91%]). The code it
   lived in has now substantially changed — the bug may be dead, fixed
   incidentally, or still alive with a different rate. Nobody knows,
   and CLAUDE.md calls it P0.
2. **Q4 at SF=100 refuses**: its `EXISTS` compiles to a SEMI join whose
   build side exceeds the budget, and the join spill path supports only
   INNER joins ("SEMI join build side exceeds the memory budget, but the
   join spill path currently supports only INNER joins") — a safe
   refusal, not a crash, but a real coverage hole at scale.
3. **Q13 at SF=100 fails** with a `SpillableHashJoinExec` temp-file
   rename error (pre-existing; recorded by the native-table-pruning
   epic) — never root-caused.

This epic settles #1 with evidence (recalibrate first, root-cause only
if it still reproduces, no guess fixes — the standing discipline for
this bug), fixes #2 and #3, and closes with an SF=100 certification:
all 22 TPC-H queries cell-exact under tight memory budgets, plus the
oom-cap harness at SF=100-class scale.

## Problem Statement

The engine's spill path is now memory-safe but not certified correct at
scale. A known, low-rate wrong-answer bug hangs over the spilling INNER
join with an unknown post-rewrite status; two named SF=100 queries
cannot complete through the spill path at all. Until these are settled,
"slow but correct on larger-than-memory data" is a claim verified only
at SF=10.

## User Stories

**As someone running TPC-H-class workloads at SF=100 with a bounded
`--memory-limit`,** I want every query to complete with correct answers
(by spilling) or refuse cleanly — including Q4 and Q13 — so the memory
budget never silently costs me coverage or correctness.
- Acceptance: all 22 queries at SF=100 complete cell-exact vs a DuckDB
  oracle under a budget that forces real spilling on the heavy queries,
  with zero OOM kills and zero wrong answers.

**As someone who saw "1 wrong answer in 290 trials" in the record,** I
want a current, evidence-backed verdict on that bug against today's
code: dead (with trial counts that meaningfully bound the rate), or
alive (with a confirmed root cause and a fix) — not a stale number
carried forward.
- Acceptance: a high-trial recalibration (≥5,000 chaos-harness trials
  plus repro-class sweeps) on the post-rewrite spill path, reported with
  a binomial CI; if any wrong answer appears, the mechanism is caught
  with the now-working instrumentation (checksums, profiler, traces)
  and fixed, with a regression test.

## Functional Requirements

1. **Recalibrate the duplicate-counting bug** on the rewritten spill
   path: ≥5,000 `spill_chaos_harness` trials (mixed forced-spill
   patterns) plus ≥200 Q12-class full-query repro trials at spilling
   budgets, all order-independent-checksum verified. Report rate + CI.
   Bug-hunting discipline unchanged: no fix without a confirmed
   mechanism; every wrong trial's artifacts preserved.
2. **Root-cause + fix** (conditional): if any wrong answer reproduces,
   instrument (QE_SPILL_DEBUG checksums, alloc profiler, targeted
   tracing), confirm the mechanism, fix it, and pin it with a
   deterministic regression test. If zero wrong answers in the full
   battery, close the bug's tracking with the new bound stated honestly
   (a bound, not a proof of absence) and record what was ruled out.
3. **SEMI/ANTI join spill support**: extend `SpillableHashJoinExec`'s
   spill path beyond INNER to at least SEMI (Q4's shape) and ANTI if
   the same mechanism covers it safely (Q16/Q22's shapes at larger
   scales). Existence semantics under partitioned spill must be exact —
   a probe row matches if its key matches ANY build row, so partition
   routing must guarantee build/probe co-location (it already does by
   key hash); chunked read-back must not double-emit SEMI matches or
   drop ANTI ones (per-probe-row match flags across chunks, or
   equivalent). Q4 at SF=100 under its previous refusing budget must
   complete cell-exact.
4. **Q13 SF=100 rename error**: reproduce, root-cause, fix. Suspected
   territory: spill temp-file lifecycle under the (now epoch-old)
   multi-file layout vs task 002's open-writer rewrite; treat the prior
   epics' fixed rename bugs (`multi_pass_merge` carried-run deletion) as
   the pattern library, not the answer.
5. **SF=100 certification**: full 22-query sweep at SF=100 over parquet
   AND native tables under budgets that force real spilling on the
   heavy queries (record which queries actually spilled via
   QE_SPILL_DEBUG), cell-exact vs DuckDB; oom-cap harness scenarios
   re-run at SF=100-class inputs; all wrapped, zero kernel kills.

## Non-Functional Requirements

- Cell-exact everywhere; no tolerance changes.
- No regression to the merged epic's numbers: harness 8/8, SF=10 native
  5288ms band, parquet cache-off 7.03-7.40s range, suites
  1317/1382/1326/1320.
- Every command wrapped (`claude-safe-build.sh` / `systemd-run
  MemoryMax` + `QE_MEM_CAP`) — hook-enforced; SF=100 runs sized
  generously but capped.
- The no-guess-fixes rule from the prior two spill epics applies to the
  duplicate-counting bug specifically.

## Success Criteria

- G1: duplicate-counting verdict delivered with ≥5,200 total verified
  trials on post-rewrite code: either 0-wrong with the CI stated, or
  root-caused + fixed + regression-pinned.
- G2: Q4 SF=100 completes cell-exact via a spilling SEMI join under the
  budget that previously refused; SEMI (and ANTI if shipped) spill is
  cell-exact vs unlimited-memory runs in dedicated tests.
- G3: Q13 SF=100 completes cell-exact; the rename error's mechanism is
  named in the fix's doc comment.
- G4: SF=100 certification sweep: 22/22 cell-exact on parquet and
  native, real spill activity recorded on the heavy queries, zero OOM
  kills; harness green at scale.
- G5: full suite green in all four feature combinations; no regression
  to the merged epic's recorded numbers; CLAUDE.md updated (the Q4/Q13
  limitations bullets, the duplicate-counting bug's status, SF=100
  certification section).

## Constraints & Assumptions

- Builds directly on the merged oom-safety-hardening code (hash-table
  budgeting, chunked read-back, streaming agg/sort) — do not revert or
  work around it.
- SF=100 data exists (`data/tpch-100gb`, ~97GB parquet; native
  conversion may need regeneration via `write-native` if stale — check
  manifests first, budget disk accordingly, 65GB).
- SF=100 runs are expensive (minutes/query when spilling); size trial
  counts and iteration counts to what a session can actually complete,
  and say so where a target is cut.
- The rollup last-ULP flake (aggregate merge order) is OUT OF SCOPE
  (own small future task), as are native-table compaction and M3
  shuffle.

## Out of Scope

- M3 distributed shuffle; distributed-path SF=100 certification.
- Native-table compaction / deletion-vector density.
- Rollup last-ULP flake.
- Extending spill support to Left/Right/Full outer joins (refusals
  stay; documented boundary — outer-join spill needs match-tracking
  across the full build side, a materially bigger design).

## Dependencies

- `src/physical/operators/spillable.rs` (post-rewrite) — the center of
  gravity for tasks 2-4.
- `examples/spill_chaos_harness.rs`, `examples/oom_cap_harness.rs`,
  `scripts/oom_cap_harness.sh`, `scripts/safe_benchmark.sh`,
  `scripts/sf100_engine_validate.py`-class validators, DuckDB oracle
  scripts.
- `data/tpch-100gb` (+ native conversion), `data/tpch-10gb` for
  cheaper repro classes.
