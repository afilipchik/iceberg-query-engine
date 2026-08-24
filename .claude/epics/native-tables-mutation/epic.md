---
name: native-tables-mutation
status: in-progress
created: 2026-08-24T04:45:55Z
updated: 2026-08-24T04:45:55Z
progress: 0%
prd: .claude/prds/native-tables.md
github: (will be set on sync)
---

# Epic: native-tables-mutation

## Overview

Phase 2 of the `native-tables` program. Phase 1 (`native-tables-foundation`,
archived) delivered a real, working, but strictly bulk-load/replace-only
table format: `CREATE TABLE ... AS SELECT` and a CLI write path both
produce one complete new snapshot every time, via `NativeWriteMode::
{Create, Overwrite}` — there is no `Append`, and no row-level mutation of
any kind. This epic adds `INSERT`, `DELETE`, and `UPDATE`. Unlike phase 1
— which was substantially "expose an already-proven mechanism as a
table" — this epic is genuinely new ground: the engine has zero DML
infrastructure of any kind today (phase 1 explicitly did not build
`Statement::Insert`, `Statement::Update`, or `Statement::Delete` handling;
all three currently hit the binder's catch-all `NotImplemented`), and the
storage format's segments are immutable Arrow IPC files by design — there
is no existing "modify a row in place" mechanism anywhere in this
codebase to extend.

## Architecture Decisions

**Deferred to task 001's design spike, not decided here** — matching
phase 1's own discipline (its task 001 decided SQL DDL scope and manifest
format before anything else proceeded; this epic's design stakes are
higher, so the same discipline applies with more force):

- **Deletion mechanism**: a per-table or per-segment deletion vector
  (bitmap or sorted row-id list) consulted at read time — the
  industry-standard "merge-on-read" pattern every modern lakehouse format
  (Iceberg, Delta Lake, Hudi) uses, and one this codebase already has
  read-side precedent for via `src/storage/iceberg.rs` — versus rewriting
  affected segments in place on every delete. Leaning toward deletion
  vectors (bounded read-time cost, no write amplification on every
  delete), but task 001 must weigh this against this engine's specific
  segment format and decide with evidence, not by default.
- **UPDATE semantics**: modeled as DELETE + INSERT (the standard
  simplification essentially every mutable lakehouse format uses,
  reusing both mechanisms rather than inventing a third), unless task
  001 finds a concrete reason this engine's shape makes true in-place
  update meaningfully better.
- **Compaction**: whether reclaiming space from accumulated deletes is
  in scope for this epic or explicitly deferred to a later one. A
  deletion vector alone is CORRECT indefinitely (reads stay right; they
  just do increasing amounts of filtering work as deletes accumulate) —
  compaction is a performance concern, not a correctness one, so
  deferring it is a legitimate, smaller-first-slice option task 001
  should weigh explicitly rather than assume either way.
- **Atomic publish for incremental changes**: phase 1's full-replace
  writes are publish-atomic by construction (a new complete snapshot
  either exists or doesn't — read the manifest field names/staging
  pattern in the archived `native-tables-foundation/002.md` and `003.md`
  Outcome sections before assuming how this worked). An `Append`/delete/
  update must preserve that property for the pieces it touches — a
  reader must never observe a manifest that references a segment that
  isn't fully written, or a deletion vector that's been partially
  updated. Task 001 must state the concrete mechanism (e.g., write new
  segment(s) + write updated manifest to a staging path, then atomic
  rename over the published manifest — mirroring the existing pattern)
  before task 002 implements anything.

**Not deferred — fixed by this epic's own non-negotiables:**

- **No new opt-out on memory safety.** Task 006 in the foundation epic
  found and fixed a real OOM (`NativeTable::scan()` was not spill-aware).
  Mutation introduces new unbounded-growth surfaces (a deletion vector
  that grows with every delete; concurrent-write buffering) that must be
  bounded by design, not discovered as a bug after the fact this time —
  though a dedicated verification task still exists here precisely
  because "by design" claims in this program have been wrong before and
  were only caught by adversarial testing, never by inspection alone.
- **Single-writer assumption stays explicit, not implicit.** This engine
  has no lock manager, no WAL, no MVCC. This epic does not build any of
  those. If concurrent writers to the same native table need to be
  prevented, task 001 must name the mechanism (even a simple advisory
  lock file) rather than leave it as an unstated assumption a future bug
  report discovers.

## Technical Approach

### SQL surface
`INSERT INTO <table> SELECT ...` (source is a `Box<Query>`, same shape
CTAS already binds via the existing `bind_query()` — task 001 should
confirm this holds for `Insert` the same way phase 1's own spike
confirmed it for `CreateTable`, not assume it transfers). `DELETE FROM
<table> WHERE <predicate>`. `UPDATE <table> SET <assignments> WHERE
<predicate>`. All three need new `Binder::bind()` match arms (currently
absent — everything but `Statement::Query` and `Statement::CreateTable`
hits `NotImplemented`) and new `ExecutionContext` entrypoints following
`create_table_as_select`'s established shape (`&mut self`, streaming,
not `sql()`'s materializing path).

### Storage
`NativeWriteMode::Append` (new) writes additional segments and merges
their stats into the manifest's `table_stats` rollup (the existing
`NativeManifest::rollup`/stats-merge functions from phase 1 are the
direct template — read their exact names in the archived task 002/003
Outcome sections before reinventing). Deletion vector mechanism per task
001's decision, most likely a new manifest-adjacent artifact (its own
small file or a `NativeManifest` field) versioned alongside
`snapshot.version`.

### Read path
`NativeTable::scan()`/`scan_with_filter()` gain a deletion-vector
consultation step if that's task 001's chosen mechanism — every row a
segment yields gets filtered against pending deletes before reaching the
rest of the query. This must compose correctly with task 005's
dense-direct-address fast path from phase 1 (which reads segment/table
stats directly) and with distributed splits (`shard_by_splits`) — a
deletion vector scoped to the wrong granularity could silently
miscount rows for a shard.

### Memory safety
Re-verify, don't assume: adversarially test INSERT/DELETE/UPDATE the same
way phase 1's task 006 found its real OOM — large deletion vectors, many
small mutations in sequence, concurrent-ish access patterns — under
`scripts/claude-safe-build.sh`'s memory-capped execution model.

### QA / benchmarking
Cell-exact validation: mutate a native table, independently recompute the
expected result (e.g., apply the equivalent SQL to the original parquet/
Iceberg source via DuckDB, or re-derive via a fresh `CREATE TABLE ... AS
SELECT` with the mutation's net effect expressed as a query), compare.
Per this program's standing convention: both DuckDB comparison premises
and a CPU/GPU split where relevant (mutation itself is not a GPU-offload
target, but post-mutation aggregate reads are, and should keep working).

## Implementation Strategy

1. Design spike (deletion mechanism, UPDATE semantics, compaction
   in/out, atomic-publish mechanism, single-writer enforcement, SQL lift
   sizing) — gates everything else, mirroring phase 1's own task 001.
2. INSERT (`Append` mode + SQL wiring) — the simplest, most
   self-contained piece; delivers standalone value (native tables can
   grow incrementally) even before DELETE/UPDATE exist.
3. Deletion vector mechanism + DELETE (shipped together — the mechanism
   has no independent value without the SQL surface that exercises it).
4. UPDATE (built on INSERT + DELETE's mechanisms, per task 001's
   semantics decision).
5. Memory safety + concurrency/crash-safety adversarial verification.
6. QA close-out (cell-exact, full suite, benchmarks, CLAUDE.md, archive).

Every lever: implement → cell-exact validate → benchmark → commit-or-revert,
through `scripts/claude-safe-build.sh`, matching this program's standing
discipline.

## Task Breakdown Preview

- 001: Design spike — deletion mechanism, UPDATE semantics, compaction
  scope, atomic-publish model, SQL lift sizing (parallel: false, gates
  everything)
- 002: INSERT — `NativeWriteMode::Append` + SQL wiring (parallel: false,
  depends on 001)
- 003: Deletion vector mechanism + DELETE (parallel: false, depends on
  001; can start alongside 002 only if task 001 confirms no shared-file
  conflict — default to sequential unless proven safe)
- 004: UPDATE (parallel: false, depends on 002 and 003)
- 005: Memory safety + concurrency/crash-safety adversarial verification
  (parallel: false, depends on 002/003/004)
- 006: QA close-out (parallel: false, depends on everything)

Total tasks: 6
Estimated total effort: likely the largest single epic in this program so
far — genuinely new infrastructure, not an extension of a proven
mechanism the way phase 1 substantially was.

## Dependencies

- Everything phase 1 built: `src/storage/native_manifest.rs`,
  `src/storage/native_write.rs`, `src/storage/native_table.rs`,
  `src/planner/binder.rs`'s `CreateTable` arm (the template for the new
  `Insert`/`Update`/`Delete` arms), `ExecutionContext::
  create_table_as_select` (the template for the new entrypoints).
- The general `HashJoinExec` multi-round bug phase 1's task 008 found and
  fixed — mutation's own testing may exercise re-execution paths in new
  ways; treat any anomaly as a possible new instance of a similar class
  of bug before assuming it's mutation-specific.
- No new external crate dependency identified as required (a roaring
  bitmap crate is a plausible addition if task 001 picks that deletion-
  vector representation — not assumed here).

## Success Criteria (Technical)

- G1: `INSERT INTO`, `DELETE FROM ... WHERE`, and `UPDATE ... SET ...
  WHERE` all work end-to-end through SQL, cell-exact vs an independently
  computed reference.
- G2: No performance cliff for the still-dominant read-only query shapes
  — phase 1's benchmarks (SF=10 1.23x, dense-direct-address firing,
  GPU-eligible shapes) must not regress for a table that has never been
  mutated.
- G3: Memory safety holds under adversarial testing (large deletion
  vectors, many sequential mutations) — not assumed, tested the way
  phase 1's task 006 tested it.
- G4: Full suite green in all feature combinations; M1/M2 distributed
  gates unaffected (mutation need not work inside a distributed cluster
  yet, matching phase 1's own G5 scope boundary for reads).
- G5: The single-writer assumption (or whatever task 001 decides) is
  enforced, not just documented — a concurrent-write attempt fails
  cleanly and namedly, never silently corrupts state.

## Estimated Effort

- 001: S-M (1-2 days — a spike, but a genuinely harder design question
  than phase 1's).
- 002: M (2-4 days).
- 003: M-L (3-5 days — new read-path mechanism, not just a write-side
  addition).
- 004: M (2-4 days, mostly composition of 002+003 per task 001's
  semantics decision).
- 005: M (2-4 days — adversarial testing, matching phase 1's task 006's
  own real find).
- 006: S-M (1-2 days).
- Total: likely 3-5 focused working sessions, the largest epic in this
  program so far.

## Tasks Created
- [ ] 001.md - Design spike — deletion mechanism, UPDATE semantics, compaction scope, atomic-publish model (parallel: false)
- [ ] 002.md - INSERT — Append write mode + SQL wiring (parallel: false)
- [ ] 003.md - Deletion vector mechanism + DELETE (parallel: false)
- [ ] 004.md - UPDATE (parallel: false)
- [ ] 005.md - Memory safety + concurrency/crash-safety adversarial verification (parallel: false)
- [ ] 006.md - QA close-out — cell-exact, full suite, benchmarks, docs, epic close (parallel: false)

Total tasks: 6
Parallel tasks: 0
Sequential tasks: 6
Estimated total effort: 84-152 hours (3-5 focused working sessions)
