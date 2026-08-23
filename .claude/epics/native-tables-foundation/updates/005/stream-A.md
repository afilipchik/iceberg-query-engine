---
issue: 005
stream: main
started: 2026-08-23T20:19:44Z
status: in_progress
---
## Scope
See .claude/epics/native-tables-foundation/005.md

## Progress

### Findings (confirmed by reading the code, not assumed)

- `disjoint_group_hint` (`src/physical/planner.rs`) needed ZERO changes,
  exactly as 001/004's notes predicted — confirmed by reading it in full.
- `try_execute_dense_direct` (`src/physical/operators/morsel_agg.rs`) has
  TWO real dependencies on a parquet-backed source, not one:
  1. Key-bounds computation opens each file directly via
     `parquet::file::reader::SerializedFileReader` (the one the task file
     called out).
  2. The scan+accumulate loop itself drives a `ParallelParquetSource` over
     `self.files: Vec<PathBuf>` — a native table has no parquet files
     backing its segments (they are Arrow IPC, read via
     `ipc_cache::read_row_group`), so this had to be generalized too, not
     just the bounds block, or a native table could never reach this
     operator's scan step even after fixing (1).
  3. `MorselAggregateExec` is only ever CONSTRUCTED when
     `try_extract_parquet_source` matches (gated on
     `provider.parquet_files().is_some()`), which is `None` for
     `NativeTable` by design (task 004's Outcome) — so the routing gate in
     `src/physical/planner.rs::lower_aggregate_cpu` also needed a native
     counterpart, not just the operator internals.

### Implementation

- **`src/physical/operators/scan.rs`**: `TableProvider::as_any(&self) ->
  &dyn std::any::Any` (new required method, one-line impl added to all 6
  implementors: `MemoryTable`, `ParquetTable`, `LanceTable`, `PulsarTable`,
  `ShardedParquetTable`, `NativeTable`) — a type-erased downcast escape
  hatch so the planner can recognize a `NativeTable` specifically without
  widening the dense-direct fast path to every non-Parquet provider
  (Lance was investigated and rejected for this path; see CLAUDE.md).
- **`src/physical/operators/morsel_agg.rs`**:
  - `MorselAggregateExec` gained `native_provider: Option<Arc<dyn
    TableProvider>>` (new field, `with_native_provider` builder) alongside
    the existing `files: Vec<PathBuf>` (stays empty in native mode) —
    "alongside", per the task's own suggested wording.
  - Extracted `dense_direct_shape` (group-by/aggregate-kind eligibility,
    source-agnostic, was already source-agnostic logic, just inline) and
    `accumulate_dense_batch` (the per-batch accumulation body) as free
    functions so the SAME code runs for both a Parquet row group and a
    native table's scanned batches — not a reimplementation.
  - New `dense_direct_key_bounds(provider, key_name_lower)`: the
    `column_stats[key_name].{min_i64,max_i64}` lookup `disjoint_group_hint`
    already uses, width-capped at 64,000,000 the same way the parquet
    footer loop is.
  - `try_execute_dense_direct` now branches: native mode uses
    `dense_direct_key_bounds` + `provider.scan_with_filter(...)` (rayon-
    parallelized accumulation over the returned batches); the ORIGINAL
    parquet footer-opening loop and `ParallelParquetSource` loop are
    UNCHANGED code, just moved into the `else` arm of that branch (verified
    byte-for-byte equivalent by diffing before/after).
  - `execute()`: if a native-mode instance ever reaches past
    `try_execute_dense_direct` returning `None` (should be unreachable —
    the planner pre-checks eligibility), it returns a clear `Err` rather
    than falling through to the parquet-only generic tier over an empty
    file list (which would have silently produced zero rows).
- **`src/physical/planner.rs`**: new `try_extract_native_dense_source`
  (mirrors `try_extract_parquet_source`, but: no `Filter` arm/refuses any
  scan-level filter since `NativeTable::scan_with_filter` has no pushdown
  and this path never re-evaluates one; only matches a provider that
  downcasts to `NativeTable`). `lower_aggregate_cpu` tries it as an `else
  if` after the parquet extractor, re-validates full eligibility via
  `dense_direct_shape` + `dense_direct_key_bounds` (the SAME functions
  `try_execute_dense_direct` uses) BEFORE constructing a native-mode
  `MorselAggregateExec` — required because native mode has no fallback
  tier, so the planner must be exactly as strict as the executor, not an
  approximation.
- **Tests**: added
  `native_table_matches_source_on_a_dense_direct_group_by_cell_exact` to
  `tests/native_table_validation.rs` (GROUP BY the orders PK, the exact
  dense-direct shape) — cell-exact vs the Parquet source. All 11 tests in
  that file pass (10 pre-existing + 1 new).
- **`examples/native_dense_direct_check.rs`** (new): builds a native table
  from `data/tpch-1gb/lineitem.parquet` (SF=1, ~6M rows) projected to
  `(l_orderkey, l_quantity)`, runs `GROUP BY l_orderkey` (Q18's own group
  key) against both the native copy and the parquet source. Meant to be
  run with `AGG_TIMING=1` to capture the real routing evidence the task
  asks for.

### Verified

- `cargo check` clean on default build AND `--features lance` AND
  `--features pulsar` AND `--features gpu` (all four compile — the new
  `as_any` required trait method reaches every implementor, including the
  two feature-gated ones).
- `cargo fmt --all -- --check` clean.
- `cargo test --release --test native_table_validation`: 11/11 passed.
- Large-scale (`data/tpch-1gb`, 6,000,000-row lineitem, 6 segments)
  `AGG_TIMING=1` run of `examples/native_dense_direct_check` — REAL
  measurement, both legs same process/binary, `GROUP BY l_orderkey` (Q18's
  own group key), 1,498,929 groups both sides:

  ```
  [AGG_TIMING] dense-direct scan+accumulate: 33.547199ms (key=l_orderkey, width=1498929, aggs=2, files=1, threads=6, projection=Some([0, 4]), filter_pushed=false)
  [AGG_TIMING] dense-direct output: 6.719659ms (1498929 rows)
  parquet source (lineitem_src   ):  1498929 groups in 41.670361ms

  [AGG_TIMING] dense-direct scan+accumulate (native): 6.149006ms (key=l_orderkey, width=1498929, aggs=2, batches=95, projection=None)
  [AGG_TIMING] dense-direct output: 4.878705ms (1498929 rows)
  native table   (lineitem_native):  1498929 groups in 12.858442ms
  ```

  The `(native)` tag on the second `[AGG_TIMING] dense-direct
  scan+accumulate` line is the direct evidence: the native-table query took
  `try_execute_dense_direct`'s new native branch, not a fallback tier (which
  would either be a plain `[agg-path]`/generic-morsel print with no "dense-
  direct" text at all, or — had my planner pre-check/executor eligibility
  drifted apart — a hard `Err` from `MorselAggregateExec::execute()`'s
  native safety net). The untagged first line is the SAME, byte-for-byte
  UNCHANGED parquet leg, proving no regression to the existing fast path in
  the same run. Both legs agree on group count (1,498,929) — a real
  correctness cross-check on top of the dedicated cell-exact test. G4 (this
  epic's own success criterion) is met.

### Full suite: GREEN

`cargo test --release` (default build, through `scripts/claude-safe-build.sh`):
every binary "0 failed" --

| binary | tests |
|---|---|
| lib | 295 (+1 ignored) |
| main bin | 24 |
| cli_tests | 18 |
| distributed_cluster | 19 |
| duckdb_validated | 177 |
| flight_tests | 8 |
| function_tests | 98 |
| function_validation_tests | 225 |
| lance_tests (feature off, 0 collected) | 0 |
| native_table_validation | 12 (10 pre-existing + 2 new task-005 tests -- re-verified with an isolated re-run after a mid-build edit, see below) |
| partition_contract | 16 |
| spill_tests | 7 |
| sql_comprehensive | 129 |
| tpch_queries | 23 |
| vector_search_tests (feature off, 0 collected) | 0 |
| window_functions | 9 |

Total ~1060 passing, 0 failed, 0 unexpected. `native_table_validation`
specifically re-run in isolation right after the full-suite run (a test I
added mid-build didn't land in that specific run's cached binary) --
12/12 green, confirming both new task-005 tests
(`native_table_matches_source_on_a_dense_direct_group_by_cell_exact`,
`native_table_matches_source_on_a_filtered_group_by_cell_exact`) pass.
`cargo fmt --all -- --check` clean (had to fix formatting on the new
example file once). `cargo check` clean on default + `--features lance` +
`--features pulsar` + `--features gpu`.

### Coordination note

This session is NOT worktree-isolated — a sibling agent (task 006) is
concurrently, actively editing `src/storage/native_table.rs` (and
`src/execution/context.rs`) in this same working tree, adding a `scan()`
admission-control memory budget. Watched their diff grow live (65 -> 219
lines) while working. My own touch to that file is a single, isolated
6-line `as_any` method with no interaction with their `memory_budget_bytes`
mechanism. Per task 004's precedent for this same situation ("both sibling
agents committed their own files first... hand-splitting shared hunks"),
I will NOT commit `src/storage/native_table.rs` until task 006's changes
there land in a commit of their own, so my commit's diff to that file stays
exactly my own hunk. `git status --short` will be re-checked immediately
before committing.

## Next
- Read back the `AGG_TIMING=1` example output once the background run
  finishes; confirm the `(native)` tag appears.
- Run the full default-build test suite (`cargo test --release`).
- Wait for task 006 to commit `native_table.rs`/`context.rs`, then extract
  and commit only my own hunks.
- Close out: update 005.md frontmatter + Outcome section.
