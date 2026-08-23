---
issue: 002
stream: main
started: 2026-08-23T01:17:37Z
status: completed
completed: 2026-08-23T02:10:00Z
---
## Scope
See .claude/epics/duckdb-parity-2/002.md

## Progress

- Read `disjoint_group_hint` (`src/physical/planner.rs:351-390`), its call
  site (`planner.rs:1059`), `SpillableHashAggregateExec::with_disjoint_groups`
  / `execute_fused_streaming`'s disjoint branch (`spillable.rs:664-1012`),
  and the merge path it bypasses (`morsel_agg.rs::merge_raw_states_to_batches`,
  `finalize_disjoint_states`).
- Built `examples/disjoint_merge_bench.rs` (same idiom as `radix_bench.rs`):
  reimplements the production dense range-shard merge algorithm and the
  disjoint hash-scatter algorithm as synthetic, self-contained pipelines,
  timed with `Instant`. Swept range in {500K, 1M, 1.5M, 2M, 3M, 5M} at
  TPC-H's fixed 10:1 orders:customer multiplicity, plus a secondary sweep
  of mult in {3,5,10,20,40} at range=1.5M fixed.
  - Calibration: at range=15M/mult=10 (SF=100 shape) the bench predicts
    128.8M overlapping partial entries from first principles vs the doc
    comment's cited "126M partial slots for 15M real groups" — within
    2.2%, validating the overlap model. Absolute SF=10 merge-step timing
    (~150-200ms in the bench) is consistent with the real, already-measured
    223ms `[raw-merge]` number — the bench's verdict at the untested 1.5M
    point can be trusted.
  - Verdict: disjoint mode won EVERYWHERE tested (1.55x-2.85x net faster,
    scatter cost included), with no sign of a crossover approaching from
    below. At SF=10's exact shape (range=1.5M, mult=10): ~1.9-2.0x.
- Measurement supported the fix: lowered `disjoint_group_hint`'s range
  floor from 2,000,000 to 1,000,000 (`planner.rs:384`), chosen to stay
  inside the directly-tested bracket rather than extrapolate below the
  500K point. No change needed in `spillable.rs` — the threshold lives
  entirely in `planner.rs`; `with_disjoint_groups` already just accepts
  whatever bool the planner computes, so nothing there needed wiring.
- Correctness: `disjoint_aggregation_matches_plain_aggregation_exactly`
  green; full `cargo test --release` green (988 tests, 0 failed, 1 ignored
  — unchanged from before this task); `tests/duckdb_validated.rs` green
  (175 passed, cell-exact validation unaffected).
- Q13 SF=10 end-to-end re-measured before/after in this exact checkout
  (git-stash isolated my one-line change, same branch state otherwise —
  task 005's hash_agg.rs work was already committed to this branch and is
  included on both sides):
  - BEFORE (floor=2,000,000, Q13 stays on the shared channel): avg
    413.1ms/iteration (5 iterations), inner-aggregate merge (`[raw-merge]`)
    ~197-281ms (~223ms typical, matching the task's cited baseline almost
    exactly).
  - AFTER (floor=1,000,000, Q13 now qualifies for disjoint): avg 246.9ms/
    iteration (5 iterations), inner-aggregate finalize ~30ms (no
    `[raw-merge]` at all — confirms the shared merge path is no longer
    reached).
  - **~1.67x faster end to end (413.1ms -> 246.9ms, -40.2%); merge phase
    itself ~7.1x faster (223ms -> 30.9ms typical).**
- SF=100 spot-check (`data/tpch-100gb`, 2 iterations): unaffected by this
  change by construction — range=15M was already inside the OLD floor
  (2M-64M) and remains inside the new one, so before/after are identical
  here; this run is a regression check, not an improvement measurement.
  Result: Q13 still correct, ~2.83s avg. Surfaced a SEPARATE, out-of-scope
  finding recorded below for task 006.
- `cargo fmt --all -- --check` clean.

## Out-of-scope finding for task 006 (not fixed here — outside this
   task's file scope, `morsel_agg.rs`)

At SF=100, `finalize_disjoint_states` (`morsel_agg.rs:166`) still prints
repeated `[raw-merge] ~469053 groups dense=false` lines — one per disjoint
worker state. Cause: each worker's ~469K-group state exceeds
`PARALLEL_MERGE_MIN_GROUPS` (65,536), so `merge_states_to_batches_filtered`
(called by `finalize_disjoint_states` with a length-1 `states` vec) takes
the "raw shard merge" branch meant for MULTI-state merging — sharding and
rebuilding via a HashMap even though there is only one state and nothing to
merge. `dense=false` because a single disjoint worker's keys are scattered
by HASH across the full 15M range relative to only ~469K of its own
entries, so the range-vs-total ratio looks sparse even though the true key
domain is dense. Net effect: SF=100's disjoint finalize costs ~205ms/
iteration, not the ~0.1ms the original doc comment cites (that number is
stale, predating some now-shipped optimizations — see the updated doc
comment in `planner.rs`). At SF=10 this doesn't fire (46,875 groups/worker
< 65,536), which is why this task's SF=10 result is clean. A cheap direct
fix would special-case `finalize_disjoint_states`'s single-state build to
skip the shard/merge machinery entirely (call `state.build_output`
directly) — left for whoever picks this up next (task 006 or a follow-up),
since `morsel_agg.rs` is out of this task's scope.

## Files changed
- `src/physical/planner.rs` — `disjoint_group_hint`: floor 2,000,000 ->
  1,000,000, doc comment updated with the measurement and its evidence.
- `examples/disjoint_merge_bench.rs` — new microbenchmark (see above).
