---
issue: 004
title: SEMI/ANTI join spill support (Q4 SF=100 completes cell-exact)
analyzed: 2026-09-03T14:43:06Z
estimated_hours: 10
parallelization_factor: 1.5
---

# Parallel Work Analysis: Task 004

## Overview

`SpillableHashJoinExec::finish_via_spill` (spillable.rs ~692) refuses every
non-INNER join. Everything downstream of that guard — `probe_with_spilling`,
`process_spilled_partition`, `probe_partition`, `create_joined_batch` — is
written for INNER only (`probe_partition` literally ignores `join_type`; the
`create_joined_batch` it calls gathers build++probe columns against
`output_schema`, which for SEMI/ANTI is the LEFT schema alone, so even
lifting the guard would produce a schema mismatch, not a wrong-but-plausible
answer). The task is to make SEMI and ANTI first-class on the spill path,
keep Left/Right/Full refused, and prove it on Q4 at SF=100.

## Facts verified against the code (2026-09-03) — the design must honor these

1. **Orientation is NOT fixed.** `planner.rs:1563` sets `build_right_for_left`
   for Semi/Anti only when `left_rows > 2 * right_rows` (or `> 2 * right/10`
   when the right side is an aggregate). So the spill path sees BOTH:
   - `swapped == true`  (build = right = subquery side; probe = LEFT = the
     OUTPUT side). Emission = probe rows. A probe row's decision is complete
     within its own partition (see fact 2), so in-memory partitions decide
     per probe batch; SPILLED partitions need a per-probe-row match bitmap
     carried across the chunked read-back (007's mechanism) and emit AFTER
     the last chunk: SEMI = rows with bit set, ANTI = rows with bit clear.
   - `swapped == false` (build = LEFT = the OUTPUT side; probe = right).
     Emission = build rows, exactly like `hash_join.rs`'s
     `create_semi_anti_batch` path. In-memory partitions need a per-partition
     `build_matched` bitmap accumulated across ALL probe batches (the probe
     is streamed partition-by-partition through `probe_with_spilling`) and
     emitted after the probe loop ends. Spilled partitions: read-back chunks
     partition the build rows DISJOINTLY, so a per-chunk `build_matched`
     probed against the whole probe partition file is exact — emit per
     chunk, no cross-chunk state needed.
   Q4 at SF=100 (orders filtered by date ≈ 5.7M rows vs lineitem filtered by
   `l_commitdate < l_receiptdate` ≈ 380M rows) is expected to take the
   `swapped == false` branch (build = orders = output). Stream B confirms via
   the physical plan. Implement BOTH orientations; if one turns out to be
   genuinely unreachable from the planner, still implement it (the operator
   is constructed directly by tests/harnesses) or refuse it by name with a
   precise message — never silently fall through to INNER logic.
2. **A partition is wholly resident or wholly on disk.**
   `evict_build_partition_to_disk` `take()`s `partitions[idx]` and sets
   `spilled[idx]`; `build_with_partitioning` never re-adds to an evicted
   partition. Partition routing (`partition_batch_by_hash`, fixed-seed xxh64
   on the `JoinKey`) is identical for build and probe, so every build row
   that could match a probe row lives in the probe row's partition. Existence
   semantics therefore survive partitioning without cross-partition state.
3. **Silently-dropped probe batches are an ANTI hazard.** In
   `probe_with_spilling`, a probe batch whose partition has neither a hash
   table nor a spill file is dropped. Correct for INNER and SEMI; for
   swapped ANTI those probe rows have NO build rows in their partition and
   MUST be emitted. Check what an empty-but-never-evicted partition looks
   like after `budget_partition_hash_tables` (line ~2310 takes both
   `resident` and `spilled`) — an empty `Some(BuildPartition)` with an empty
   table also yields "no match" and must emit for ANTI.
4. **Build partitions that receive zero probe rows are the mirror hazard**
   for `swapped == false` ANTI: every build row in such a partition is
   unmatched and must be emitted (in-memory: emit all rows of the partition;
   spilled with `probe_file == None`: read back and emit all rows).
5. **NULL keys.** Non-spill semantics (`probe_batch_semi`,
   `probe_one_semi_anti_batch`, `vectorized_hash::has_null`): a NULL probe
   key never matches (SEMI drops it, ANTI keeps it); a NULL build key never
   matches (build-side SEMI drops it, build-side ANTI keeps it).
   `probe_partition` already `continue`s on NULL probe keys. Verify
   `build_hash_table`/`partition_batch_by_hash` route NULL build keys
   somewhere they can still be emitted for build-side ANTI. Pin this with a
   test that compares spill vs unlimited on data containing NULL keys on both
   sides.
6. **ON-clause filters stay refused** (`filter.is_some()` guard). Q4's
   `l_commitdate < l_receiptdate` is a subquery-local predicate pushed to
   the lineitem scan, not a join filter — stream B confirms from the plan.
7. **`retained` is never set for Semi/Anti** (`set_retained` gate) — emit
   the full left schema; do not thread the mask into the new emission.
8. **Hash-table budgeting (007) applies unchanged** — SEMI/ANTI tables are
   charged identically; chunk sizing in `process_spilled_partition` stays.
9. **Existing test to update**: `tests/spill_tests.rs::left_join_spill_fails_loudly_not_wrong`
   asserts the refusal message contains "INNER". Keep that word in the new
   outer-join refusal (e.g. "supports only INNER, SEMI and ANTI joins") or
   update the assertion in the same commit.
10. **Q4@SF=100 may no longer refuse at 100G.** The Dictionary
    `estimate_batch_size` fix moved every spill boundary (task 003 found
    Q13 no longer spills at 100G; task 001 found Q12 needs ≤16M). The
    acceptance budget is "the largest budget at which HEAD still refuses
    with the SEMI message" — stream B measures it before stream A needs it.

## Parallel Streams

### Stream A: Operator implementation + tests
**Scope**: SEMI/ANTI on the spill path, both orientations; outer-join
refusal message; unit tests in spillable.rs's test module; integration
tests in tests/spill_tests.rs; INNER non-regression (chaos ≥100 trials,
spill_tests, spillable unit tests, default suite); Q16/Q22 SF=10 check.
**Files**: `src/physical/operators/spillable.rs`, `tests/spill_tests.rs`,
`CLAUDE.md` (operator docs only, at the end).
**Can Start**: immediately
**Estimated Hours**: 7
**Dependencies**: none for the code; needs stream B's measured Q4 budget +
oracle for the final SF=100 acceptance run.

### Stream B: SF=100 baseline, oracle, budget probe, harness scenario
**Scope**: (1) fresh DuckDB Q4 oracle over `data/tpch-100gb`; (2) pinned
HEAD binary built from a detached worktree with its own CARGO_TARGET_DIR
(so stream A's in-flight edits never enter the baseline); (3) Q4 at SF=100
native at the pruning epic's settings (`--memory-limit 100G`) — record
refuse/complete/spill; if it completes, step the budget down until the
SEMI refusal appears and record the LARGEST refusing budget; capture the
physical plan (join orientation, `build_right`, filter presence); (4) add a
`semi-join` scenario (and `anti-join` if cheap) to
`examples/oom_cap_harness.rs` + `scripts/oom_cap_harness.sh::cap_for`,
constructing `SpillableHashJoinExec` directly with a build side far above
the budget; run it through the shell driver — expected verdict pre-fix is
REFUSED (exit 2), which must be recorded so the post-fix flip to COMPLETED
is a measured change.
**Files**: `examples/oom_cap_harness.rs`, `scripts/oom_cap_harness.sh`,
`.scratch/sjc3-004/**` (artifacts, gitignored).
**Can Start**: immediately
**Estimated Hours**: 3
**Dependencies**: none

### Stream C (sequential, after A and B): acceptance + close-out
Run Q4 SF=100 on the fixed binary at stream B's refusing budget with
`QE_SPILL_DEBUG=1`, cell-exact vs the oracle; run the harness scenario
(expect COMPLETED); write the Outcome section in 004.md; commit.

## Coordination Points

### Shared Files
None between A and B. Both commit to `epic/spill-join-correctness-3` in the
same checkout — stage only your own files (`git add <path>`), retry on a
transient `index.lock`, never `git add -A`, never stash or touch the other
stream's files.

### Sequential Requirements
- Stream C needs A's fix merged into the working tree AND B's budget/oracle.
- Stream B's pinned baseline binary must be built from HEAD BEFORE A's
  first commit lands, or from a detached worktree at the task-start commit
  (the latter is mandatory — it removes the race entirely).

## Conflict Risk Assessment
Low. The only physical resource contention is memory: B's 100G-limit SF=100
leg needs `MemoryMax=110G` on a 125G box. B runs that leg first (while A is
still reading), checks `free -g` shows ≥100G available before launching it,
and runs all later step-down probes under small caps sized to the budget
(e.g. `MemoryMax=16G`). A builds with `SAFE_BUILD_MEM=48G SAFE_BUILD_JOBS=8`.

## Parallelization Strategy
A and B in parallel; C after both. B's results are consumed by C, not by A's
implementation, so A never blocks on B.

## Expected Timeline
- With parallel execution: ~7h wall (A) + ~1h (C)
- Without: ~11h
- Efficiency gain: ~27%
