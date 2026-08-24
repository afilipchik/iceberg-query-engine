---
issue: 002
stream: main
started: 2026-08-24T05:10:00Z
status: completed
---
## Scope
See .claude/epics/native-tables-mutation/002.md
## Progress
- Read 001.md's Outcome (all 6 decisions) plus the real source it builds
  on (`native_write.rs`, `native_manifest.rs`, `native_table.rs`,
  binder.rs's `CreateTable` arm, `create_table_as_select`,
  `register_native_table`) before writing any code.
- `src/storage/native_manifest.rs`: added `write_manifest_atomic` — the
  new single-FILE atomic-publish primitive (Decision 4), sibling to
  `publish_table_dir`. Purely additive; two new tests confirm it never
  touches sibling segment files and refuses an invalid manifest without
  publishing.
- `src/storage/native_write.rs`: added `NativeWriteMode::Append`,
  `TableWriteLock`/`lock_path_for`/`lock_table_for_write` (Decision 5,
  `std::fs::File::try_lock()`), the non-publishing write core
  (`write_append_segments` + `AppendSegmentWriter` +
  `cast_batch_to_target`), the non-locking publish core
  (`publish_manifest_update`), and the self-publishing entrypoint
  (`append_to_native_table`). `write_batches_with_options` dispatches
  `Append` to `append_to_native_table` for CLI/`write-native --mode
  append` parity, with zero changes to the existing Create/Overwrite
  path. 17 new unit tests (schema mismatch, segment-id continuation,
  dictionary inheritance, zero-row no-op, lock contention/release,
  direct composition of the two non-publishing building blocks without
  the self-publishing wrapper).
- `src/planner/binder.rs`: `insert_target_name` (mirrors
  `create_table_target_name`) and `require_supported_insert_shape`
  (mechanically checked against the real 26-field `ast::Insert` struct,
  including the `TableObject::TableQuery` variant sqlparser 0.62 added
  that the task's own struct-field count didn't separately call out —
  found by `cargo check`'s own exhaustiveness error, not missed). New
  `Statement::Insert` arm in `bind()`, same shape as `CreateTable`'s (no
  new `LogicalPlan` variant). 8 new unit tests (a 9th, refusing MySQL
  `INSERT ... SET`, was dropped: that shape does not even parse under
  GenericDialect, matching task 001's own precedent for MySQL's
  multi-table DELETE form — the `assignments` refusal check stays as
  defensive code, just not independently testable via real SQL text).
- `src/execution/context.rs`: `sql()` gained the same "refuse and point
  elsewhere" guard for INSERT that it already had for CREATE TABLE
  (necessary now that `bind()` accepts `Statement::Insert` — otherwise
  `sql()` would silently execute only the source query and insert
  nothing). New `insert_into_native_table` entrypoint + `InsertResult`.
- `src/main.rs`: REPL dispatches `INSERT INTO ...` to
  `insert_into_native_table` alongside the existing CREATE TABLE
  routing.
- **Real bug found and fixed, in scope**: `LogicalPlan::Values`'s
  physical planning was an unimplemented stub ("For now, return empty")
  that silently made EVERY `VALUES (...)` clause anywhere in the engine
  a permanent no-op — not previously caught because no existing test
  anywhere in the suite exercised literal `VALUES` SQL text (confirmed
  by grep before fixing). This directly blocked `INSERT ... VALUES`,
  which 001.md's Outcome explicitly recommended keeping (found via a
  genuinely failing integration test, not by inspection). Fixed in
  `src/physical/planner.rs` by evaluating each row's expressions via the
  existing `evaluate_expr` against a 1-row/0-column dummy batch (the
  SAME trick already used one arm above for `LogicalPlan::EmptyRelation`
  / bare `SELECT <literal>`), then concatenating per-column. No existing
  test relied on the old always-empty behavior (verified before fixing).
- New tests: `tests/native_insert_tests.rs` (9 end-to-end SQL-level
  tests — cell-exact vs an independently-computed reference, wildcard
  `SELECT *` positional correctness, empty-insert no-op, schema-mismatch
  clean error + table left untouched, `INSERT ... VALUES` end to end,
  `statistics()`/`distributed_splits()` post-insert, `sql()`'s refusal
  guard, unregistered/non-native-table error paths).
- New example: `examples/native_append_memory_check.rs` — measures real
  peak RSS (`/usr/bin/time -v`) for appending SF=10 `lineitem` (60M
  rows) to a native table, two modes reported separately
  (`QE_MEM_CHECK_MODE=direct|sql`):
  - `direct` (apples-to-apples with task 003's own methodology:
    `native_write::append_to_native_table` called directly, fed by the
    same `StreamingParquetReader` construction `write_from_parquet`
    uses) — **328MB peak RSS**, BETTER than task 003's own 406MB
    CREATE-mode baseline for the identical row count. This is the
    direct confirmation of this task's own acceptance criterion.
  - `sql` (the full `INSERT INTO t SELECT * FROM lineitem_src` via
    `ExecutionContext::insert_into_native_table`) — **~5.3GB peak
    RSS**, much higher. Root-caused, not just observed:
    `StreamingParquetScanExec::output_partitions()` returns one
    partition per row-group work item, and `insert_into_native_table`
    (identical structure to `create_table_as_select`, confirmed by
    reading both) merges every partition's stream via
    `futures::stream::select_all` with no backpressure before the
    single-threaded Append writer drains them. Confirmed this is a
    PRE-EXISTING, CTAS-shared characteristic (not introduced by this
    task's own Append write core) — named honestly in CLAUDE.md as a
    residual risk for a future task, not fixed here (would mean
    changing the engine's generic multi-partition scan/stream-merge
    pattern, shared by CTAS, outside this task's charter).
- `cargo fmt --all -- --check` clean.
- Full default-feature suite (release profile, through
  `scripts/claude-safe-build.sh`): **1096 passed, 1 pre-existing
  ignored (`flatten_dependent_join::tests::test_flatten_exists`,
  unrelated to this task), 0 failed**, across all 18 test binaries.
  Exit code 0. Touched-module unit/integration counts within that
  total: native_write.rs 32 (17 new), native_manifest.rs 25 (2 new),
  binder.rs 18 (8 new), native_insert_tests.rs 9 (new file).
- No regression to `CREATE TABLE ... AS SELECT` or the CLI write path:
  `Create`/`Overwrite` code paths are untouched by the `Append` branch
  added to `write_batches_with_options` (an early, separate return
  before any Create/Overwrite-specific logic runs), and the full suite
  above includes `tests/native_table_validation.rs` (phase 1's own CTAS
  cell-exact suite) green.
