---
issue: 004
stream: main
started: 2026-08-24T07:00:00Z
status: completed
---
## Scope
See .claude/epics/native-tables-mutation/004.md

## Progress

- Read 004.md in full plus task 001/002/003's Outcome sections for their
  exact final APIs (`native_write::write_append_segments`/
  `lock_table_for_write`/`publish_manifest_update`/`now_ms`;
  `native_delete::identify_matching_rows`/`apply_deletions`).
- Implemented `src/storage/native_update.rs` (new sibling module to
  `native_write.rs`/`native_delete.rs`/`native_manifest.rs`):
  `update_native_table(table_dir, predicate, assignments)` — the single
  atomically-published composition per task 001's Decision 2 (ONE lock
  acquisition, ONE manifest read, ONE `identify_matching_rows` call with
  `materialize_rows: true`, evaluate SET expressions, ONE
  `apply_deletions` + ONE `write_append_segments`, ONE combined
  `Vec<Segment>`, ONE `publish_manifest_update` call, then unlock).
- Found and fixed a real correctness gap during implementation (not
  anticipated in 001/003's own analysis): `identify_matching_rows`
  deliberately does not consult `Segment::deleted_rows` (harmless for
  DELETE's idempotent union). For UPDATE this would silently RESURRECT
  already-tombstoned rows on an overlapping second UPDATE (or an UPDATE
  after a DELETE). Added `live_matched_rows` to filter matched rows down
  to only currently-live positions before any SET expression evaluates
  them. Confirmed load-bearing: wrote the "overlapping sequential
  UPDATEs" adversarial test FIRST against the naive (unfiltered)
  composition, watched it fail with duplicate/wrong rows, then added the
  fix and watched it pass.
- `src/planner/binder.rs`: `update_target_name`,
  `assignment_target_column_name`, `require_supported_update_shape`
  (mechanically checked against sqlparser 0.62's real 11-field
  `ast::Update` struct, `src/ast/dml.rs`), `Statement::Update` arm in
  `bind()` (validates shape, always refuses pointing at the real
  entrypoint — mirrors the DELETE arm), and `bind_update` (validates +
  binds every SET assignment's value expression + WHERE predicate,
  refuses a subquery anywhere by name at bind time). 15 new unit tests.
- `src/execution/context.rs`: `UpdateResult`, `update_native_table`
  entrypoint (mirrors `delete_from_native_table`'s shape), `sql()`
  refusal guard.
- `src/main.rs`: REPL dispatch for `UPDATE ...` alongside the existing
  CREATE TABLE/INSERT/DELETE routing.
- `src/storage/mod.rs`: `pub mod native_update;`.
- Tests: 11 unit tests in `native_update.rs` (self-referential SET,
  zero-match, all-rows, dictionary round-trip via the real write path,
  overlapping sequential updates, UPDATE-after-DELETE, multi-segment
  targeting, lock contention, missing-table/unknown-column errors) + 12
  integration tests in `tests/native_update_tests.rs` (cell-exact against
  independently-computed CASE-expression references over the ORIGINAL
  TPC-H source; the highest-priority no-partial-state-visibility test —
  a real concurrent reader on a genuine `multi_thread` tokio runtime
  racing 60 back-to-back UPDATEs, verified 5x with zero flakes; a
  version-delta test that would catch a two-publish regression with zero
  timing dependence; SQL surface / error-path / no-regression checks).
- Two test-authoring bugs found and fixed while getting these green
  (documented so task 005/006 don't need to rediscover them): (1) my
  first draft of the concurrency test read back via
  `native_write::read_back`, which does NOT apply deletion filtering (by
  design — it is explicitly documented as "not the production read
  path") and so saw tombstoned-but-still-on-disk old rows alongside the
  new ones; switched to the real `TableProvider::scan` path via
  `NativeTable`. (2) the concurrency test initially ran on the default
  `current_thread` tokio runtime and hung indefinitely, since neither the
  writer's nor the reader's I/O truly yields (`std::fs` blocking calls
  wrapped in async fns) — fixed via `#[tokio::test(flavor =
  "multi_thread", worker_threads = 2)]` plus a `yield_now().await` in the
  reader loop.
- Full suite green: `cargo test --release` — 1182 passed, 1 pre-existing
  ignored test (unrelated), 0 failed, across all 20 test targets
  (lib + 19 integration files), exit code 0. Confirmed twice (before and
  after `cargo fmt --all`).
- `cargo fmt --all -- --check` clean.
- CLAUDE.md updated: new "Mutation: UPDATE" section (mirrors the INSERT/
  DELETE sections' depth), "Current limitations" bullet updated (the
  "UPDATE still not implemented" note removed — UPDATE now exists).
- `git status --short` shows only the intended changes: `src/execution/
  context.rs`, `src/main.rs`, `src/planner/binder.rs`, `src/storage/
  mod.rs` modified; `src/storage/native_update.rs`,
  `tests/native_update_tests.rs` new; plus this epic's own tracking
  files and CLAUDE.md.

## Status: completed
