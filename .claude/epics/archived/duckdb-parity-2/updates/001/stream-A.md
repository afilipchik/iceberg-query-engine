---
issue: 001
stream: main
started: 2026-08-23T01:17:37Z
status: completed
---
## Scope
See .claude/epics/duckdb-parity-2/001.md

## Progress

- Read `src/storage/ipc_cache.rs`: confirmed the doc-comment/code mismatch
  described in the task — `Mode::Auto` (the actual default when
  `QE_IPC_CACHE` is unset) silently uses an existing fresh sidecar and
  never builds one, but the module doc claimed "default off until the
  gauntlet passes."
- Fixed the doc comment to match the code (lower-risk option per the
  task's own guidance — doesn't change behavior for any existing user of
  this repo's pre-built `.qeipc` sidecars). Added a `Display` impl for
  `Mode` so there is one canonical, unambiguous label for each of the
  three premises (off/auto/build), reused by both the doc and the new
  benchmark output.
- Added premise labeling in two places (both in scope, both applied since
  they cover different invocation paths):
  - `src/main.rs`'s `BenchmarkParquet` handler now prints
    `IPC cache mode: <Display>` at startup — visible to anyone running
    `benchmark-parquet` directly, and captured in `safe_benchmark.sh`'s
    per-query log-on-failure output.
  - `scripts/safe_benchmark.sh`'s header block now prints
    `Cache premise: ...` (derived from `$QE_IPC_CACHE`) once per sweep —
    visible on the console and in the saved log for every run, including
    successful ones (the per-query subprocess stdout is otherwise
    discarded on PASS).
- Searched the whole repo (not just CLAUDE.md) for the stale Q9 "IPC loses
  to parquet, accepted trade-off" caveat described in the task. No live
  copy exists anywhere — not in CLAUDE.md, not in README.md, not in the
  ipc-default epic's own files (`.claude/epics/ipc-default/*.md`), not
  anywhere else in the tree. The phrase only appears inside the
  duckdb-parity-2 planning docs (001.md/epic.md/prd) that *describe* the
  stale claim as something to go find and remove — there is nothing left
  to remove. Nothing changed for this criterion; documented as verified-
  absent rather than silently skipped.
- Re-baselined CLAUDE.md's SF=10 section on today's binary (post the
  2026-08-22 dependency-modernization epic):
  - Built release via `scripts/claude-safe-build.sh cargo build --release`.
  - `QE_IPC_CACHE=0 scripts/safe_benchmark.sh --data ./data/tpch-10gb
    --iterations 3` → 22/22 pass, **7.40s** total.
  - `QE_IPC_CACHE=1 scripts/safe_benchmark.sh --data ./data/tpch-10gb
    --iterations 3` → 22/22 pass, **5.88s** total.
  - DuckDB native reference unchanged (3.32s, from `safe_benchmark.sh`'s
    own `DUCKDB_MS` table).
  - Like-for-like DuckDB (same parquet files, `read_parquet` views,
    best-of-2, `duckdb_rebaseline.py`'s `tpch_queries()` helper — the
    exact `duckdb_files_bench_sf100.py` pattern pointed at SF=10, run as
    a one-off `.scratch/duckdb_files_bench_sf10.py`, gitignored, not
    committed) → **4.18s**.
  - Ratios: cache-off 2.23x native / **1.77x like-for-like**; cache-on
    1.77x native / **1.41x like-for-like**.
  - Replaced CLAUDE.md's stale 2026-08-17/18 SF=10 figures with a table
    mirroring the SF=100 four-way matrix's format (premise | engine total |
    DuckDB comparison-with-stated-premise | ratio), explicitly noting the
    `Mode::Auto` default's dependence on sidecar pre-existence.
  - SF=100 re-measurement was explicitly "time permitting" in the task
    and was skipped: it requires the much larger `data/tpch-100gb`
    dataset, historically takes far longer per premise, and four other
    agents are concurrently active in this same (non-worktree) checkout —
    not worth the wall-clock/contention risk for an optional stretch goal
    when the required SF=10 deliverable was fully completed.
- Verification:
  - `scripts/claude-safe-build.sh cargo build --release` — clean.
  - `scripts/claude-safe-build.sh cargo fmt --all -- --check` — clean for
    every file in scope. The only outstanding fmt diff in the tree
    (`examples/disjoint_merge_bench.rs`) belongs to a concurrent agent's
    untracked, in-progress file, explicitly out of this task's scope —
    left untouched.
  - `scripts/claude-safe-build.sh cargo test --release` — 988 passed, 0
    failed, 1 ignored across every test binary (lib 237, main 24,
    cli_tests 18, distributed_cluster 19, duckdb_validated 175,
    flight_tests 8, function_tests 98, function_validation_tests 225,
    lance_tests 0 (feature-gated off), partition_contract 16,
    spill_tests 7, sql_comprehensive 129, tpch_queries 23,
    vector_search_tests 0 (feature-gated off), window_functions 9).
  - Verified all three premises (`off`/`auto`/`build`) print distinct,
    correct labels via a direct `benchmark-parquet` invocation.
- Committed as a single commit: `Task 001: fix ipc_cache Auto-mode doc,
  label cache premise, re-baseline SF=10` (`d9aeef4`), touching exactly
  `CLAUDE.md`, `scripts/safe_benchmark.sh`, `src/main.rs`,
  `src/storage/ipc_cache.rs`. Confirmed `git status --short` post-commit
  shows only concurrent agents' unrelated files
  (`src/physical/operators/hash_agg.rs`, `src/physical/planner.rs`,
  `examples/disjoint_merge_bench.rs`) — nothing of mine left uncommitted.

## Status: completed

All in-scope acceptance criteria satisfied. See `001.md` frontmatter/
Definition of Done for the itemized checklist.
