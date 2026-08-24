---
issue: 003
stream: main
started: 2026-08-24T06:00:00Z
status: completed
---
## Scope
See .claude/epics/native-tables-mutation/003.md

## Progress

- Read 003.md in full (Technical Details, populated by task 001's design
  spike) and task 002's Outcome (exact reusable API: `publish_manifest_
  update`, `lock_table_for_write`, `write_manifest_atomic`).
- `src/storage/native_manifest.rs`: added `Segment::deleted_rows: Vec<u32>`
  (`#[serde(default)]`, sorted/deduplicated LOCAL positions), `Segment::
  live_row_count()`, and bounds/sort validation in `NativeManifest::
  validate()`. Fixed every existing `Segment { .. }` literal across the
  crate (native_manifest.rs x6, native_write.rs x2, native_table.rs x2) to
  set `deleted_rows: Vec::new()`. `Segment.row_count`/`Snapshot.row_count`
  deliberately left meaning the PHYSICAL count, per task 001's Decision 1.
- `src/storage/native_delete.rs` (NEW module, registered in
  `src/storage/mod.rs`): the row-identification + deletion-vector-editing
  core.
  - `identify_matching_rows(dir, target, predicate: Option<&Expr>,
    materialize_rows: bool) -> Result<MatchedRows>` — bespoke loop (NOT
    the generic LogicalPlan/PhysicalOperator pipeline), reads segments via
    `ipc_cache::read_row_group`, evaluates the predicate via
    `physical::operators::evaluate_expr`, tracks a running local-row
    offset across a segment's batches. `materialize_rows` is the
    load-bearing design choice for task 004 reuse: DELETE calls it
    `false` (positions only), UPDATE is expected to call it `true`
    (matched rows' CURRENT values for SET evaluation).
  - `apply_deletions(target, matches) -> Vec<Segment>` — BTreeSet union
    into existing `deleted_rows` (idempotent by construction), drops a
    wholly-tombstoned segment (task 001's Decision 3).
  - `delete_from_native_table(table_dir, predicate) ->
    Result<NativeDeleteResult>` — self-publishing entrypoint (lock, read,
    identify, apply, publish via task 002's `publish_manifest_update`
    UNCHANGED). Refined during implementation: `rows_deleted` is the NET
    newly-tombstoned count (not the gross predicate-match count), so a
    fully-redundant repeat DELETE is a TRUE no-op (no manifest write, no
    version bump) — not just "doesn't corrupt," genuinely idempotent.
- `src/storage/native_table.rs`: `scan()` gained a deletion-filtering step
  (new free fn `filter_deleted_rows`, per-batch fast path that skips
  masking entirely when no deleted position falls in a batch's range;
  segments with empty `deleted_rows` skip the step entirely). Since
  `scan_with_filter`'s default impl is `self.scan(projection)` and
  `NativeTable` doesn't override it, every read path (dense-direct-address
  via `morsel_agg.rs`, the generic `MemoryTableExec` path, the prescan
  cache) gets deletion filtering for free — confirmed by reading every
  call site, not assumed. `statistics()` row_count now sums `segment.
  live_row_count()`; `total_byte_size`/column-stats rollup deliberately
  untouched (task 001's Decision 1), which is also why `check_scan_
  budget`'s memory formula needed zero changes.
- `src/planner/binder.rs`: `delete_target_name` (extraction, mirrors
  `insert_target_name`), `require_supported_delete_shape` (mechanically
  checked against sqlparser 0.62's real `ast::Delete`/`TableFactor::Table`
  structs), `bind_delete` (validates + binds WHERE via the same
  `bind_expr` `bind_select` uses, supports table aliasing, `None`
  selection binds to `None` predicate, refuses a WHERE subquery by name
  at bind time since the bespoke evaluation loop has no `SubqueryExecutor`).
  New `Statement::Delete` arm in `bind()`: validates shape then always
  errors pointing at `delete_from_native_table` (DELETE has no
  `LogicalPlan` to return).
- `src/execution/context.rs`: `DeleteResult`, `ExecutionContext::
  delete_from_native_table` (mirrors `insert_into_native_table`'s shape,
  calls `Binder::bind_delete` directly — no physical plan involved),
  `sql()` DELETE-refusal guard (mirrors the CREATE TABLE/INSERT guards).
- `src/main.rs`: REPL routes `DELETE FROM <native table> ...` to
  `delete_from_native_table`, alongside the existing CREATE TABLE/INSERT
  dispatch.
- Tests: `src/storage/native_manifest.rs` (+5 tests), `src/storage/
  native_delete.rs` (16 tests, new module), `src/storage/native_table.rs`
  (+6 tests), `src/planner/binder.rs` (+13 tests including the subquery
  refusal), `tests/native_delete_tests.rs` (NEW, 10 integration tests).
  53 new tests total.
- `CLAUDE.md`: new "Mutation: DELETE" section (mirrors "Mutation: INSERT"
  style, including the subquery-refusal note); "Current limitations"
  bullet updated (DELETE no longer listed as unimplemented; UPDATE is now
  the sole remaining gap, with task 001's Decision 2 composition
  summarized for task 004).

## Final verification (real, not assumed)

- `cargo check --tests` (debug): 0 errors, 0 new warnings in any file this
  task touched.
- `cargo fmt --all -- --check`: clean.
- Full test suite (`scripts/claude-safe-build.sh cargo test --release
  --lib --tests` — the lib unit tests + all 17 integration test files;
  examples excluded, matching how task 002's own Outcome measured "full
  suite" as its 18 test binaries, not the ~50 example/benchmark binaries
  this workspace also has): **18/18 binaries green, 1145 passed, 0
  failed, 1 pre-existing ignored test (unrelated to this task, same one
  task 002 already reported), exit code 0.** Grew from task 002's own
  1096-passed baseline by the 53 new tests plus 1096 unaffected
  pre-existing tests staying green — zero regressions anywhere, including
  `tests/native_table_validation.rs` (phase 1's CTAS suite) and
  `tests/native_insert_tests.rs` (task 002's INSERT suite, all 9 tests
  still passing byte-for-byte).
- Live DELETE demonstration: temporarily instrumented
  `delete_a_subset_matches_the_independently_computed_complement_cell_exact`
  with `eprintln!`, ran it in isolation with `--nocapture`, captured real
  output, then reverted the instrumentation and re-ran the FULL suite
  once more to confirm the revert left everything exactly as green as
  before (identical 1145/0/1 counts). Real result captured:
  `DELETE FROM orders_native WHERE o_orderkey <= 500` against a
  1500-row/1-segment table → `DeleteResult { rows_deleted: 500,
  segments_dropped: 0, total_rows: 1000, version: 2, elapsed: 338.939µs
  }`, cell-exact against the independently-computed reference.
- `git status --short`: only the intended 9 modified files + 2 new files
  (`src/storage/native_delete.rs`, `tests/native_delete_tests.rs`).

Status: all acceptance criteria met. Closing out.
