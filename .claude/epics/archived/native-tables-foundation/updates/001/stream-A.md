---
issue: 001
stream: main
started: 2026-08-23T18:35:37Z
status: completed
---
## Scope
See .claude/epics/native-tables-foundation/001.md

## Progress
- Read the task, PRD (`.claude/prds/native-tables.md`), and epic
  (`epic.md`, Architecture Decisions) in full for context.
- Re-read `src/planner/binder.rs` in full: confirmed `Binder::bind()`
  still matches only `Statement::Query`; everything else is
  `NotImplemented`. Zero DDL/DML handling anywhere in the file.
- Read `src/storage/iceberg.rs`, `src/storage/ipc_cache.rs`,
  `src/storage/lance_write.rs`, `src/physical/operators/scan.rs`
  (`TableProvider` trait) in full.
- Confirmed `sqlparser = "0.62"` (`Cargo.toml:19`), matching the
  `dependency-modernization` epic's pin. Verified the `CreateTable`/
  `Insert` struct definitions directly in the pinned crate source
  (`~/.cargo/registry/.../sqlparser-0.62.0/src/ast/{ddl,dml}.rs`).
- Ran a throwaway spike (`.scratch/sqlparser_spike/`, its own Cargo
  project, gitignored, never touched `src/`) proving `sqlparser` 0.62
  under the engine's own `GenericDialect` already parses
  `CREATE TABLE t AS SELECT ...` (including joins/aggregates) into
  `Statement::CreateTable { query: Some(Box<Query>), .. }` — the exact
  same `Query` type `Binder::bind_query()` already fully handles — and
  `INSERT INTO t SELECT ...` into the analogous `Statement::Insert
  { source: Some(Box<Query>), .. }`.
- Traced `write-lance --sql` (`src/main.rs`, `Commands::WriteLance`) as
  the existing "run SELECT, write result" precedent, and found a real,
  previously-undocumented gap: `ExecutionContext::sql()` fully
  materializes every partition's stream into `Vec<RecordBatch>` before
  returning — not the streaming/non-materializing pattern the epic's
  Architecture Decisions require. Flagged for tasks 003/006.
- Read `src/physical/operators/morsel_agg.rs::try_execute_dense_direct`
  and `src/physical/planner.rs::disjoint_group_hint` in full: found
  `disjoint_group_hint` is ALREADY provider-agnostic (reads via
  `TableProvider::statistics()`), so it needs no changes; only
  `try_execute_dense_direct` has the parquet-footer dependency task 005
  must fix — narrowed exactly where and how.
- Read `src/physical/gpu.rs`'s `pid()`/`plan_gpu_agg` to confirm task
  007's description of the `parquet_files()`-based identity/eligibility
  gate matches the real code exactly.
- Read `src/storage/mod.rs`, `src/execution/context.rs`'s `register_*`
  family, and `src/main.rs`'s `--tables` auto-detection block (verified
  the `1238-1243` line reference in task 004 is still accurate).
- Made and documented three decisions with full evidence:
  1. **SQL DDL scope**: build `CREATE TABLE ... AS SELECT` in this epic
     (S, ~8-16h), attached to tasks 003 (write entrypoint + CLI command)
     and 004 (binder wiring + registration) — not deferred to
     CLI-only-first, since the incremental lift is small and proven.
  2. **Manifest format**: JSON sidecar (`_manifest.json`), not an Arrow
     IPC/Parquet manifest table, not Iceberg's own Avro format — full
     tradeoff analysis in 001.md's Outcome section.
  3. **Module layout**: three new sibling flat files under
     `src/storage/` (`native_manifest.rs` [002], `native_write.rs`
     [003], `native_table.rs` [004]), mirroring the existing
     `lance.rs`/`lance_write.rs` split, NOT evolving `ipc_cache.rs` in
     place and NOT one merged file — keeps 003/004 fully separable.
- Propagated all three decisions into `002.md` through `008.md`'s
  Technical Details (003.md and 004.md also gained one new Acceptance
  Criteria bullet each for the concrete new deliverables the SQL DDL
  decision added).
- Appended the full Outcome section to `001.md`, checked its
  Acceptance Criteria and Definition of Done, set `status: closed`.
- No `src/` changes. `.scratch/sqlparser_spike/` is gitignored and does
  not appear in `git status`.

## Final status
Complete. All three decisions made, evidenced, and propagated. Tasks
002-008 can proceed without re-deriving any of this.
