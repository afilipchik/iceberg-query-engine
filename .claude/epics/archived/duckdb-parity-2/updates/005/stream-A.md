---
issue: 005
stream: main
started: 2026-08-23T01:17:37Z
status: completed
---
## Scope
See .claude/epics/duckdb-parity-2/005.md

## Progress

- Starting implementation
- Read `src/physical/operators/hash_agg.rs` end to end for every
  `distinct_set` construction/usage site (not just the ones named in the
  task's "Technical Details" line numbers, which had drifted slightly —
  actual sites were the field decl (~315), 3x `get_or_insert_with` in
  `update_accumulator` (CountDistinct ~2183, SUM(DISTINCT) ~2192,
  ApproxDistinct ~2445), the union merge in `merge_accumulator_states`
  (~1369), and 4 read sites in `build_agg_array`).
- Found 3 *unrelated* `std::collections::HashSet<_>` locals in a
  different function (`aggregate_scalar_simd`, the no-GROUP-BY SIMD path,
  lines ~1938/1941/1944) that are not part of `AccumulatorState` at all —
  confirmed out of scope (a separate, short-lived per-call set, not the
  `distinct_set` field) and left untouched.
- Change: `use hashbrown::{HashMap, HashSet};` (was just `HashMap`);
  `distinct_set: Option<HashSet<GroupValue>>` (was
  `Option<std::collections::HashSet<GroupValue>>`); the 3
  `get_or_insert_with(std::collections::HashSet::new)` sites now build
  `HashSet::new` (hashbrown's). Verified every other API used on the
  field (`.iter().cloned()`, `.extend()`, `.clone()`, `.as_ref()`,
  `.len()`, `.insert()`) is identical surface on both `std` and
  `hashbrown` — no behavior-affecting API differences found.
  `GroupValue` already derives plain `Hash`/`Eq` (`core::hash::Hash`),
  which both hash-table crates consume identically, so no trait-impl
  changes were needed.
- `cargo check --lib` clean. Formatted with `rustfmt --edition 2021` on
  just this one file (not `cargo fmt --all`, since this is a live shared
  checkout with other agents' uncommitted edits in-flight elsewhere —
  wanted to avoid reformatting files outside my scope). Follow-up
  `cargo fmt --all -- --check` (read-only) confirmed the whole workspace
  clean.
- Full default `cargo test --release` (via `scripts/claude-safe-build.sh`,
  queued behind a concurrent sibling agent's build on the same `target/`
  lock, ~9m40s once it started): **988/988 passed, 0 failed**, including
  `spill_tests::count_distinct_spill_matches_in_memory` (spill_tests: 7/7
  passed) and `sql_comprehensive::test_count_distinct` /
  `test_distinct_*` (sql_comprehensive: 129/129 passed).
- Q16 SF=10 (`benchmark-parquet --path ./data/tpch-10gb --query 16
  --iterations 5`) post-change: 228.4 / 164.9 / 160.9 / 155.0 / 161.0 ms
  (iteration 1 pays cold-cache cost; steady state ~155-165ms). Did **not**
  isolate a clean before/after via `git stash`: this checkout had other
  agents' `cargo build`/`cargo test` actively running throughout this
  session (confirmed via `ps aux` twice), and this task is explicitly
  barred from creating a worktree, so there was no way to get a "before"
  binary without briefly reverting `hash_agg.rs` on disk while a sibling
  agent's compile might be reading it — a real corruption risk to their
  build, not just mine. Used the task's own sanctioned fallback
  ("otherwise just after"). Consistent with the task's own prior
  estimate, this is not expected to move Q16 dramatically by itself — the
  anti-join parallelism gap (task 004) and general aggregation overhead
  dominate; this fix is justified on its own (free, zero-risk,
  zero-new-dependency consistency fix), not on a large measured Q16 delta.
- Committed as `eba694d` ("Task 005: distinct_set switches
  std::collections::HashSet to hashbrown::HashSet"), touching only
  `src/physical/operators/hash_agg.rs` (5 insertions, 11 deletions across
  16 changed lines). `git status --short` after commit shows no trace of
  this file; remaining working-tree entries belong to other concurrent
  agents (`src/physical/planner.rs`, `examples/disjoint_merge_bench.rs`).
- Done.
