---
name: hash-join-dictionary-semi-anti-fix
description: Fix the in-memory HashJoinExec wrong answer for build-side-output SEMI/ANTI with Dictionary keys and repeated build keys; give Dictionary keys the vectorized fast path
status: active
created: 2026-09-05T01:36:45Z
---

# PRD: hash-join-dictionary-semi-anti-fix

## Executive Summary

`spill-join-correctness-3` task 004 found, while validating the spill
path against an independent naive-join ground truth, that the IN-MEMORY
`HashJoinExec` returns wrong answers for SEMI/ANTI joins whose build side
is the LEFT (output) side when the join key is `Dictionary(Int32, Utf8)`
and build keys repeat: it emits ONE build row per distinct matched key.
On a 60k-row build over 40 values probed by 2k rows over 20 of them, SEMI
returns 20 rows (truth 30,000) and ANTI 59,980 (truth 30,000). The same
fixture with plain `Utf8` or `Int64` keys is correct, as is the
probe-side orientation. Root cause, read from the code (2026-09-04):
`VectorizedHashTable::build` rejects Dictionary keys
(`can_vectorize_arrays` has no Dictionary arm), so Dictionary-keyed joins
fall back to `probe_semi_anti_parallel`'s generic `HashMap` path, whose
entry loop `break`s after marking the FIRST build entry — correct when
the probe row is the output (its status is decided), wrong when the build
rows are the output (every build row sharing the key must be marked).
The vectorized path (`probe_one_semi_anti_batch`) marks every candidate
and is why Utf8/Int64 keys are right.

## Problem Statement

A wrong answer in the default in-memory join for a planner-reachable
shape: a Semi/Anti join whose left side is not estimated at >2x its right
(`build_right_for_left` false) with a native table's Dictionary column as
the key — e.g. `... WHERE l_shipmode IN (SELECT ... )` with a comparably
sized subquery. Cell-exactness is this engine's first rule; this is the
top-priority open item from the 2026-09-03 roadmap addendum.

## User Stories

**As anyone running Semi/Anti joins over native tables' string columns,**
I want every matched (SEMI) / unmatched (ANTI) left row returned, not one
per distinct key.
- Acceptance: the pinned fixture (`in_memory_hash_join_findings_from_
  task004_fixtures`) passes un-ignored, both join types, and a SQL-level
  test over a real native table with a Dictionary key column agrees with
  the same query over plain parquet and with DuckDB.

**As a performance-minded operator,** I want Dictionary-keyed joins to
take the vectorized fast path like Utf8 keys do, not the generic map.
- Acceptance: `VectorizedHashTable` builds for Dictionary(Int32, Utf8)
  keys (decoded once at build/probe), verified by a test and a measured
  Dictionary-keyed join at SF=10 native that is no slower than before.

## Functional Requirements

1. Correctness fix in `probe_semi_anti_parallel` (and any sibling
   generic path with the same pattern — audit `probe_hash_table`'s
   sequential Semi/Anti handling): when `!swapped`, mark EVERY matching
   build entry; `break` only when `swapped`. Doc comment names the
   mechanism. No change to swapped behaviour.
2. Regression tests: the task-004 findings test converted to a hard,
   un-ignored assertion (Dictionary + Utf8 controls, SEMI + ANTI,
   build-side orientation); a SQL-level integration test over a small
   native table (written in-test from the tpch-10mb parquet via the
   native write path) with a Dictionary key column, compared cell-by-cell
   with the identical query over the parquet source.
3. Dictionary keys on the vectorized path: `VectorizedHashTable::build`
   and every probe entry point decode Dictionary key arrays to their value
   type ONCE per batch (the same `compute::cast` the spill path and the
   aggregate already use), so `can_vectorize_arrays` accepts them and the
   generic-map fallback is no longer reached for this shape.
4. Every change measured: suite green, SF=10 native band unchanged, a
   Dictionary-keyed join timed before/after.

## Non-Functional Requirements

- No behaviour change for Int64/Utf8 keys (existing tests + M1/M2 gates).
- Every command wrapped/capped (hook-enforced); fmt clean.

## Success Criteria

- G1: the findings fixture passes un-ignored (SEMI 30,000 / ANTI 30,000
  for Dictionary AND Utf8), and the SQL-level native-table test is
  cell-exact vs parquet and vs DuckDB.
- G2: Dictionary keys build a `VectorizedHashTable` (asserted), and a
  Dictionary-keyed SEMI join at SF=10 native is measured no slower than
  the pre-fix generic path.
- G3: default suite + spill_tests green; SF=10 native sweep within band;
  M1/M2 PASS; CLAUDE.md's open-finding bullet rewritten as closed.

## Constraints & Assumptions

- Sole source file for the fix: `src/physical/operators/hash_join.rs`
  (+ `vectorized_hash.rs` for the Dictionary arm); tests may touch
  `spillable.rs`'s test module (the pinned fixture lives there).
- Assumes the root cause above; task 001 must CONFIRM it (a targeted
  test that fails before and passes after the one-line change) before
  widening scope.

## Out of Scope

- Spill-path changes (all covered by `spill-join-correctness-3`).
- Dictionary-keyed INNER/outer join fast paths beyond what requirement 3
  gives for free.

## Dependencies

- `in_memory_hash_join_findings_from_task004_fixtures` (spillable.rs
  tests) — the executable pin; `naive_join_count` ground truth helper.
- `data/tpch-10mb`, native write path (`native_write::write_from_parquet`).
