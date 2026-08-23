---
issue: 004
stream: main
started: 2026-08-23T19:20:39Z
status: completed
---
## Scope
See .claude/epics/native-tables-foundation/004.md

## Summary

Implemented in full and committed (`f8391fe`, "Task 004: native table
TableProvider, CREATE TABLE AS SELECT, cell-exact validation"). Full
details in `004.md`'s Outcome section (closed).

## Progress

- Read 004.md in full, plus 001.md's and 002.md's closed Outcome sections
  (manifest schema, module layout decision, SQL DDL sizing/plan).
- `src/planner/binder.rs`: `Statement::CreateTable` arm in `Binder::bind()`.
  `require_supported_create_table_shape()` refuses, by name, all 57
  non-name/query fields of sqlparser 0.62's `CreateTable` struct
  (mechanically verified exhaustive against the actual struct definition —
  57 required, 57 checked, zero missing, zero typo'd) plus a
  `query.is_none()` columns-only-DDL refusal. `create_table_target_name()`
  extracts the target name without duplicating validation.
- `src/storage/native_table.rs` (new file): `NativeTable` `TableProvider`
  impl — schema/scan/statistics/distributed_splits/shard_by_splits/
  identity. Reads via `ipc_cache::read_row_group` unchanged. One real
  split per segment. `shard_by_splits` scopes statistics + `identity()`
  correctly (returns `None` for a shard — impl-nt-007-gpu's flagged
  GPU-cache-aliasing risk, closed with a dedicated test).
- `src/storage/mod.rs`: `pub mod native_table; pub use
  native_table::NativeTable;` (unavoidable registration).
- `src/execution/context.rs`: `native_table_root` field (default
  `./native_tables`, overridable), `register_native_table`,
  `create_table_as_select(&mut self, sql) ->
  Result<CreateTableAsSelectResult>` — streams the physical plan directly
  into task 003's `native_write::write_batches` (never through `sql()`'s
  materializing wrapper). `sql()` now explicitly refuses `CREATE TABLE`.
- `src/main.rs`: `--tables` auto-detection extended; REPL routes
  `CREATE TABLE` to `create_table_as_select`.
- `tests/native_table_validation.rs` (new): cell-exact CTAS-vs-source
  validation across plain-scan/filter/aggregate/join shapes.

## Two real bugs found and fixed during validation (not assumed away)

1. **Qualified column names from bare `SELECT *`.** This engine's binder
   preserves `relation` (table qualification) for `SelectItem::Wildcard`
   but not for explicit column lists — a genuine, pre-existing asymmetry
   (confirmed empirically: `SELECT * FROM orders` vs `SELECT o_orderkey
   FROM orders` produce differently-shaped schemas). Harmless for a
   transient `QueryResult` but wrong to persist forever into a table's
   column names. Fixed locally in `create_table_as_select`
   (`output_schema_for_native_write`) — never touched the general
   wildcard-binding behavior other callers rely on. impl-nt-003-write
   independently hit and fixed the same latent gap in `write-native --sql`.
2. **`Dictionary` as a logical/catalog type broke GROUP BY.** Task 003's
   writer correctly declares a dictionary-coerced column's MANIFEST type
   as `Dictionary(Int32, Utf8)` (describing what the segment file
   physically contains). Surfacing that as `NativeTable::schema()`'s
   logical type broke `AggregationState::build_group_array`
   (`src/physical/morsel_agg.rs`, shared by `HashAggregateExec` AND
   `MorselAggregateExec` — verified by reading both call sites), which has
   no `Dictionary` match arm, because no existing provider (Parquet, Lance)
   ever exposed one at the logical level — Parquet's own dictionary
   encoding is an invisible physical optimization, never a logical type.
   Fixed by having `NativeTable::schema()` report the decoded value type
   while `scan()` still returns the real Dictionary-encoded array
   unchanged (zero-copy) — mirrors Parquet's own model exactly, pinned by
   a dedicated unit test.

## Coordination

- Messaged impl-nt-003-write early for their real `write_batches`
  signature before guessing; coded directly against their answer, zero
  rework once `native_write.rs` landed.
- Implemented `TableProvider::identity()` on `NativeTable` at
  impl-nt-007-gpu's request, with the shard-safety guard they flagged
  (aliasing risk for the GPU resident cache) — confirmed by them reading
  the final code.
- Coordinated commit ordering in the shared (non-worktree-isolated)
  working tree: both sibling agents committed their own files first
  (`a61e8a1` task 003, `5c12923` task 007), each hand-splitting shared
  `main.rs`/`storage/mod.rs` hunks so my subsequent commit (`f8391fe`)
  diffed cleanly to only my own files — verified via `git status --short`
  before committing.

## Final verification

Full default-build suite green end to end: `cargo test --release` — lib
290/0/1-ignored, `duckdb_validated` 177, `sql_comprehensive` 129,
`tpch_queries` 23, `distributed_cluster` 19 (M1 gate unaffected),
`native_table_validation` 10/10, plus `cli_tests`/`flight_tests`/
`function_tests`/`function_validation_tests`/`partition_contract`/
`spill_tests`/`window_functions` — zero failures anywhere.
`cargo fmt --all -- --check` clean. `git status --short` showed only this
task's files before commit.
