---
name: native-tables-mutation
status: completed
created: 2026-08-24T04:45:55Z
updated: 2026-08-24T09:00:00Z
progress: 100%
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

**Decided by task 001's design spike** (`.claude/epics/
native-tables-mutation/001.md`'s Outcome section has the full evidence
behind every decision below — read it before implementing 002-006;
this section states the conclusions only) — matching phase 1's own
discipline (its task 001 decided SQL DDL scope and manifest format
before anything else proceeded; this epic's design stakes are higher,
so the same discipline applied with more force):

- **Deletion mechanism — DECIDED: a per-segment sorted deletion vector**
  (`Vec<u32>` of local row positions, a new field inline on the existing
  `Segment` struct in `_manifest.json`), consulted inside
  `NativeTable::scan()`/`scan_with_filter()` as a single choke point —
  merge-on-read, not copy-on-write. **Correction to this section's own
  prior claim**: `src/storage/iceberg.rs` is NOT existing read-side
  precedent for deletion-vector consultation — read in full, it
  REFUSES any Iceberg table containing delete files outright
  (`data_files_of`: `content != 0` is a hard `NotImplemented`, "row-level
  deletes are not supported — compact the table first"), never consults
  them. This epic's deletion-vector read-side logic is new, with no
  prior in-repo implementation to reuse. Representation is a plain
  `Vec<u32>`, not the `roaring` crate — segments are capped at
  ~1,000,000 rows by construction, below the scale where `roaring`'s
  compression wins over a flat structure, and a wholly-tombstoned
  segment is dropped from the manifest outright (see Compaction below),
  bounding the pathological case a plain list handles worst. No new
  Cargo dependency. Full reasoning, including composition with phase
  1's dense-direct-address fast path (verified needs ZERO changes) and
  the memory-budget mechanism (verified needs ZERO changes, with one
  named residual risk for task 005 to adversarially test): task 001's
  Outcome, Decision 1.
- **UPDATE semantics — DECIDED: DELETE + INSERT, confirmed**, no
  evidenced reason found for true in-place update to be better on this
  engine's immutable-segment shape. One load-bearing refinement beyond
  the "standard simplification" framing: task 004 must NOT implement
  this as two sequential calls to task 002/003's own public,
  self-publishing entrypoints (each would independently publish,
  leaving a real half-done window) — task 002 and task 003 must each
  expose a lower-level, non-publishing building block (write segments
  without publishing; identify+tombstone matched rows without
  publishing) that task 004 composes into ONE manifest edit and ONE
  atomic publish. Full reasoning: task 001's Outcome, Decision 2.
- **Compaction — DECIDED: explicitly deferred to a future epic**, not
  built here. A deletion vector alone is CORRECT indefinitely (reads
  stay right; they just do increasing amounts of filtering work as
  deletes accumulate) — confirmed, not just asserted. Honest, named
  downside: segment count grows by at least one per Append/INSERT
  forever (no merging of small segments), and disk space from deleted
  rows is never physically reclaimed — except one narrow, in-scope
  exception: a segment tombstoned to 100% (`deleted_rows.len() ==
  row_count`) is dropped from the manifest outright by task 003, which
  is not full compaction but does bound the deletion vector's own
  worst case. A future compaction task needs nothing this epic doesn't
  already build: candidate selection is its own new logic, but reading
  survivors (the existing deletion-aware `scan()`), writing them as
  fresh segments (the existing segment-writer), and publishing
  (the existing atomic single-file manifest rename + single-writer
  lock) are all direct reuse. Full reasoning: task 001's Outcome,
  Decision 3.
- **Atomic publish for incremental changes — DECIDED: single-FILE
  atomic rename of a freshly-written manifest**, generalizing (not
  reusing unchanged) phase 1's single-DIRECTORY rename
  (`native_manifest::publish_table_dir`). Confirmed by reading the
  actual implementation: phase 1's mechanism does
  `remove_dir_all(final_dir)` then `rename(staging, final_dir)` — safe
  only when the staging directory is a complete, self-sufficient copy
  of the whole new table state (true for Create/Overwrite, NOT true for
  Append/DELETE/UPDATE, which must preserve most of an existing
  directory's segment files unchanged). Confirms this section's own
  hint: "phase 1's atomicity only worked because full-replace writes
  have no partial-update window to protect against" — true, evidenced.
  The new mechanism: write new segment file(s) directly into the LIVE
  table directory under fresh non-colliding ids (harmless orphans if a
  crash happens before publish — no manifest references them yet),
  construct one new `Vec<Segment>` in memory, call the EXISTING
  `NativeManifest::build()` unchanged (it already derives
  `row_count`/`table_stats` fresh from whatever segments it's given —
  no new merge function needed), write the new manifest to a temp file
  in the same directory, then `rename()` it over the live
  `_manifest.json` — one atomic file-level rename, the same POSIX
  guarantee already trusted for directory-level renames elsewhere in
  this exact codebase. Explicit scope boundary: this provides
  read-atomicity and process-crash-safety, not `fsync`/power-loss
  durability beyond the OS's own page-cache behavior — matching, not
  narrowing, phase 1's own established scope (its own publish path
  doesn't `fsync` either). Full reasoning: task 001's Outcome, Decision 4.

**Not deferred — fixed by this epic's own non-negotiables:**

- **No new opt-out on memory safety.** Task 006 in the foundation epic
  found and fixed a real OOM (`NativeTable::scan()` was not spill-aware).
  Mutation introduces new unbounded-growth surfaces (a deletion vector
  that grows with every delete; concurrent-write buffering) that must be
  bounded by design, not discovered as a bug after the fact this time —
  though a dedicated verification task still exists here precisely
  because "by design" claims in this program have been wrong before and
  were only caught by adversarial testing, never by inspection alone.
- **Single-writer assumption stays explicit, not implicit — DECIDED:
  `std::fs::File::try_lock()`.** This engine has no lock manager, no
  WAL, no MVCC, and this epic does not build any of those. Task 001
  decided the concrete mechanism: `std::fs::File::{try_lock, unlock}` —
  STABLE standard-library methods (confirmed against this toolchain's
  shipped std source; this repo pins `rustc 1.93.0`, well past the
  1.89.0 stabilization), wrapping `flock(2)` on a sibling lock file per
  table directory, held for a mutation's full read-identify-write-publish
  span. Zero new Cargo dependency. Verified with a real, live
  cross-process test (not just documentation trust): a second process
  gets `TryLockError::WouldBlock` while the first holds the lock, and —
  critically for crash-safety — can immediately acquire it after the
  first is `SIGKILL`'d, with zero manual cleanup, because the kernel
  releases a `flock` the instant the holding process dies for any
  reason. This is why `File::try_lock()` was chosen over a simpler
  `create_new`-marker-file lock, which would NOT self-release after a
  crash. Readers never lock (writer-vs-writer only). Full reasoning and
  the live test transcript: task 001's Outcome, Decision 5.

## Technical Approach

### SQL surface
`INSERT INTO <table> SELECT ...` (source is a `Box<Query>`, same shape
CTAS already binds via the existing `bind_query()` — task 001 confirmed
this holds for `Insert` via a fresh spike, not assumed to transfer from
CreateTable's; `INSERT ... VALUES (...)` binds through the identical
path too, for free, since `Binder::bind_set_expr` already has a
`SetExpr::Values` arm). `DELETE FROM <table> WHERE <predicate>`.
`UPDATE <table> SET <assignments> WHERE <predicate>`. All three need new
`Binder::bind()` match arms (currently absent — everything but
`Statement::Query` and `Statement::CreateTable` hits `NotImplemented`)
and new `ExecutionContext` entrypoints following
`create_table_as_select`'s established shape (`&mut self`, streaming,
not `sql()`'s materializing path). DELETE/UPDATE's `WHERE`/`SET`
expressions bind via the existing `Binder::bind_expr` (the same
predicate-binding `bind_select`'s WHERE clause already uses) but are
NOT executed via the generic `LogicalPlan`/`PhysicalOperator` pipeline
CTAS/INSERT use for their source query — that pipeline has no way to
carry a matched row's (segment, local position) back out, which DELETE
and UPDATE both need. They use a bespoke per-segment scan+evaluate loop
instead (task 001's Outcome, Decision 1 and Decision 2).

### Storage
`NativeWriteMode::Append` (new) writes additional segments directly into
the live table directory (fresh, non-colliding segment ids continuing
from the existing max, not restarting at 0) and constructs one new
`Vec<Segment>` (existing + new) that the EXISTING `NativeManifest::build`
rolls up unchanged — no new merge function needed, confirmed by reading
`build`'s existing implementation (task 001's Outcome, Decision 4).
Append must inherit the target table's ALREADY-DECIDED dictionary
encoding and validate the source's schema (name+type, not just column
count — `SegmentWriter::accept` today checks only count) against the
target's existing manifest schema, not rediscover either from the new
data (task 001's Outcome, Decision 6). The deletion vector is a new
`deleted_rows: Vec<u32>` field inline on the EXISTING `Segment` struct
in `_manifest.json` — NOT a separate file or artifact (task 001's
Outcome, Decision 1 — corrects this section's own earlier "most likely
a new manifest-adjacent artifact" framing).

### Read path
`NativeTable::scan()`/`scan_with_filter()` gain a deletion-vector
consultation step (task 001's Outcome, Decision 1) — every row a
segment yields gets filtered against `deleted_rows` before reaching the
rest of the query, as a single choke point inside `scan()` itself, so
every existing caller (generic path, dense-direct-address, a future
distributed shard) composes correctly with zero changes at the call
site. Verified (not assumed) to compose correctly with task 005's
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
- [x] 001.md - Design spike — deletion mechanism, UPDATE semantics, compaction scope, atomic-publish model (parallel: false)
- [x] 002.md - INSERT — Append write mode + SQL wiring (parallel: false)
- [x] 003.md - Deletion vector mechanism + DELETE (parallel: false)
- [x] 004.md - UPDATE (parallel: false)
- [x] 005.md - Memory safety + concurrency/crash-safety adversarial verification (parallel: false)
- [x] 006.md - QA close-out — cell-exact, full suite, benchmarks, docs, epic close (parallel: false)

Total tasks: 6
Parallel tasks: 0
Sequential tasks: 6
Estimated total effort: 84-152 hours (3-5 focused working sessions)

## Epic close-out (2026-08-24)

All 6 tasks shipped and validated on branch `epic/native-tables-mutation`
(commits `864a9d7`..`efe1fc4` for tasks 001-005, plus this task's own
fix/docs/archive commits — see Commits below). Full suite green in
**all four feature combinations** (default 1188/0/1, lance 1253/0/2,
gpu 1188/0/1, pulsar 1191/0/1 — passed/failed/ignored, zero failures
anywhere), `cargo fmt --all -- --check` clean, M1 + M2 distributed gates
PASS via real 3-process clusters.

### Headline: what this epic actually delivered

`INSERT INTO`, `DELETE FROM ... WHERE`, and `UPDATE ... SET ... WHERE`
against native tables — the first mutation capability this table format
has ever had, turning phase 1's bulk-load/replace-only format
(`native-tables-foundation`) into a genuinely incrementally-writable one.
Deletion is merge-on-read (a per-segment sorted `deleted_rows: Vec<u32>`
consulted at a single choke point inside `scan()`/`scan_with_filter()`);
UPDATE is DELETE+INSERT composed into ONE atomically-published manifest
edit, never two sequential publishes; a new single-FILE atomic-rename
mechanism (`native_write::publish_manifest_update`) generalizes phase
1's whole-directory rename for every incremental write mode
(Append/DELETE/UPDATE); single-writer enforcement via
`std::fs::File::try_lock()`, verified live under a real `kill -9`.
Compaction is explicitly out of scope (task 001's Decision 3), with one
narrow in-scope exception (a 100%-tombstoned segment is dropped from the
manifest).

| check | result |
|---|---|
| Cell-exact INSERT+DELETE+UPDATE, real SF=10 scale (14,507,798 rows, `orders`) | **PASS** — 0 cells different vs an independent DuckDB DML reference |
| Never-mutated table vs phase 1's own recorded numbers | **1.27x vs DuckDB-parquet** (phase 1: 1.23x) — within noise |
| Mutated table (10% deletion vector, spread broadly) | **2.65x slower** than the same never-mutated warehouse — real, bounded, root-caused (deletion-vector consultation overhead, not query selectivity) |
| Dense-direct-address post-mutation | fires correctly, exact match vs an independent cross-check |
| GPU offload post-mutation | still engages, still correct — but its ~18x warm speedup is FULLY MASKED by deletion-vector overhead once a table is mutated |
| M1 / M2 distributed gates | **PASS** (mutation itself does not participate in distributed execution — matches phase 1's own scope boundary) |

### Per-task attribution

- **001** (design spike): decided all six open questions before any code
  was written — deletion vector mechanism (sorted `Vec<u32>`, inline on
  `Segment`, not roaring/copy-on-write/a sibling file), UPDATE semantics
  (DELETE+INSERT via non-publishing building blocks composed into ONE
  atomic publish — a load-bearing refinement the epic text itself did not
  spell out), compaction (confirmed out of scope, with the narrow
  100%-tombstone exception as the concrete floor), atomic-publish
  mechanism (single-file rename, generalizing not reusing phase 1's
  directory rename), single-writer enforcement (`File::try_lock()`,
  verified live with a real SIGKILL test), and SQL grammar sizing
  (confirmed a non-issue for all three statements via a fresh sqlparser
  spike). Corrected the epic's own inaccurate claim that
  `src/storage/iceberg.rs` was read-side precedent for deletion vectors
  (it REFUSES delete files outright; this epic's read-side logic was new).
- **002** (INSERT): `NativeWriteMode::Append`, SQL wiring, cell-exact at
  real scale. **Found and fixed a real, pre-existing bug**:
  `LogicalPlan::Values`'s physical planning was an unimplemented stub
  that always returned an EMPTY batch — no test anywhere in the engine
  had ever exercised literal `VALUES` SQL text before this task's own
  `INSERT ... VALUES` integration test caught it. Fixed for the whole
  engine, not just INSERT. Also flagged (not itself a fix): the
  SQL-path `INSERT INTO ... SELECT` shared CTAS's own unbounded
  concurrent-partition-merge pattern, measured at 5.3GB peak RSS for a
  60M-row source — carried forward to task 005.
- **003** (DELETE): the deletion-vector mechanism itself
  (`native_delete.rs`) plus the SQL surface — genuinely new machinery,
  not a thin wrapper (DELETE has no `LogicalPlan`/`PhysicalOperator`
  pipeline to reuse; row-identification is a bespoke scan+evaluate loop).
  Verified the dense-direct-address fast path needs ZERO code changes
  (deletion filtering happens before it ever sees a batch) — re-confirmed
  against the actual implementation, not just carried over from task
  001's design-time analysis.
- **004** (UPDATE): composed 002+003's non-publishing building blocks
  into one atomic DELETE+INSERT per task 001's decision. **Found and
  fixed a real correctness bug**: two overlapping UPDATEs (or an UPDATE
  following a DELETE) could RESURRECT already-tombstoned rows, because
  `identify_matching_rows` deliberately doesn't consult `deleted_rows`
  (correct for DELETE's own idempotent-union semantics, wrong for
  UPDATE's "read current value, write a new live row" semantics without
  an extra filter). Fixed with `live_matched_rows`, caught by a
  purpose-built adversarial test written FIRST, confirmed failing
  without the fix. Also verified no-partial-state-visibility with a real
  concurrent-reader test racing 60 UPDATEs against a genuine
  multi-threaded read loop — every single poll (hundreds, 0 flakes) saw
  either the fully-pre- or fully-post-update state, never a mix.
- **005** (memory safety + crash-safety): six adversarial scenarios, each
  given a real, evidenced verdict. Quantified (not just designed for) two
  real residual risks for a future compaction epic — deletion-vector JSON
  density at very large segment counts (~131MB extrapolated for task
  001's own "1000 segments x 1M rows x 1% deleted" worry, larger than
  its original "tens of MB" guess) and O(N²) cumulative manifest-rewrite
  cost across a long mutation sequence (~0.44ms/op near-empty to
  ~8.8ms/op at 2500+ segments). Verified the single-writer lock under a
  REAL external `kill -9`, 6/6 runs, zero flakes. **Found and fixed a
  real kernel-confirmed OOM** shared with the CTAS/INSERT SQL path
  (task 002's own carried-forward finding): unconditionally concurrent
  `futures::stream::select_all` over many partition streams, confirmed
  via `journalctl -k` under a real cgroup cap; fixed with
  `bounded_partition_merge` (concurrency-limited `flatten_unordered`),
  **70-71% peak RSS reduction, wall time neutral-to-faster**.
- **006** (this task, QA close-out): full suite re-verification in all 4
  feature combinations, cell-exact SF=10 real-scale validation (INSERT+
  DELETE+UPDATE composed, independently verified against DuckDB DML),
  never-mutated and mutated benchmark comparisons, M1/M2 re-confirmation,
  CLAUDE.md documentation, and this close-out. **Found and fixed THREE
  real, pre-existing bugs** (one root cause, one fix pattern, all
  reproduce on phase 1's own never-mutated fixtures — see "Mutation: QA
  close-out" in CLAUDE.md for the full story): a Dictionary-vs-declared-
  schema mismatch that crashed `ExternalSortExec`'s and
  `SpillableHashJoinExec`'s SPILL paths outright whenever a large-enough
  sort or join carried a Dictionary-coerced column (the ordinary case for
  native tables' low-cardinality string columns), and a genuinely
  separate k-way-merge staleness bug in the same file that could silently
  misplace or panic on rows once fixed enough to run. **Found — but
  deliberately did NOT fix — a FOURTH, deeper bug**: once the crash was
  fixed, TPC-H Q12's spilling join at SF=10 was revealed to complete but
  return a WRONG (2x-inflated) answer after ~320 seconds, a separate
  duplicate-counting bug in the partition/spill algorithm this task
  judged too large to root-cause and fix safely under time pressure —
  documented in detail (including the reasoning for NOT reverting the
  three schema fixes as a false safety net) rather than hidden or
  scope-crept into. **Independently re-verified by the orchestrating
  session before this merge**: the extreme slowness reproduced twice
  from scratch (~150s both times); the 2x-inflated wrong answer did NOT
  reproduce in either attempt (fresh table, and the same table after a
  real DELETE) — narrows but does not clear the risk; full reproduction
  matrix and reasoning in CLAUDE.md's own note on this finding. Treated
  as a live P0 for follow-up, not stood down.

**Three real bugs found and fixed across this epic's SIX tasks — two
genuine correctness bugs (task 002's `Values` stub, task 004's UPDATE
resurrection) and one genuine memory-safety bug (task 005's kernel-
confirmed OOM) — plus, in this final task, THREE MORE real bugs found
and fixed (one root cause, three call sites: the Dictionary/schema
spill-path crash) and one real bug found and knowingly left unfixed with
full documentation** (the join-spill duplicate-counting bug). Every task
in this epic that touched adversarial or real-scale validation found
something real — a strong, repeated signal that this program's
"implement → validate at real scale → benchmark" discipline keeps
paying for itself, not a coincidence of any one task.

### G1-G5 (this epic's own success criteria): ALL MET

- **G1** (`INSERT INTO`, `DELETE FROM ... WHERE`, `UPDATE ... SET ...
  WHERE` all work end-to-end through SQL, cell-exact vs an independently
  computed reference) — **MET**. Validated at both the small scale each
  task used during development AND, by this task, at real SF=10 scale
  (14,507,798 rows, all three statements composed in one realistic
  sequence) — independently verified against DuckDB DML over the same
  source parquet, 0 cell mismatches.
- **G2** (no performance cliff for the still-dominant read-only query
  shapes — phase 1's benchmarks must not regress for a table that has
  never been mutated) — **MET**. 1.27x vs DuckDB-parquet at SF=10,
  matching phase 1's own recorded 1.23x within this program's
  established noise band; dense-direct-address and GPU offload both
  re-confirmed firing at their pre-epic numbers on a pristine table.
- **G3** (memory safety holds under adversarial testing — large deletion
  vectors, many sequential mutations) — **MET**. Task 005's six
  scenarios each given a real, evidenced verdict; two residual risks
  quantified with concrete numbers (not hidden), one real OOM found and
  fixed with a measured 70-71% RSS reduction.
- **G4** (full suite green in all feature combinations; M1/M2 gates
  unaffected) — **MET**. All 4 combinations green (1188/1253/1188/1191
  passed, 0 failed anywhere, final state including this task's own 4
  fix commits and 4 new regression tests); M1 GATE PASS + M2 GATE PASS
  via real 3-separate-process clusters, re-confirmed with the FINAL
  default binary.
- **G5** (the single-writer assumption is enforced, not just documented
  — a concurrent-write attempt fails cleanly and namedly) — **MET**.
  `std::fs::File::try_lock()`, verified live: a real cross-process
  contention test (task 001) AND a real external `kill -9` mid-mutation,
  6/6 runs, zero flakes, kernel auto-release confirmed (task 005).

### Residues (named as one class, matching this program's convention)

1. **The join-spill duplicate-counting bug (found, NOT fixed — the
   single most important residue of this epic)** — see "Mutation: QA
   close-out" in CLAUDE.md and this task's own `006.md` Outcome section
   for the full investigation. Not native-table-specific: reachable by
   ANY sufficiently large spilling INNER join, on any table type. Was
   ALWAYS present (confirmed: reproduces on phase 1's own pristine,
   never-mutated fixture) but was previously masked end-to-end by a
   crash this task fixed as a side effect of unrelated real-scale
   validation. Recommend treating this as a P0 correctness bug for
   whichever future epic owns the join spill path, not merely folding it
   into the pre-existing "streaming rewrite" performance framing.

   **Update (`spill-join-correctness` epic, closed 2026-08-25):** a
   dedicated follow-up epic investigated this bug directly. It remains
   **OPEN** — root cause still not confirmed, despite real
   instrumentation and a controlled chaos-test experiment that directly
   DISPROVED the leading hypothesis (non-idempotent join-child
   re-execution) for the wrongness specifically (it fully explains the
   bug's severe slowness, which is a separate, now-FIXED issue — see
   below). This entry's own "not native-table-specific" claim above is
   now EMPIRICALLY confirmed, not just inferred from reading the code:
   plain parquet forced into the identical spill code path was
   statistically indistinguishable from native (0/80 wrong each,
   matched trial counts). Also newly confirmed: reachable via
   distributed (scatter) execution, where each node independently runs
   the identical join-spill code over its own shard (40/40 distributed
   trials came back correct, but the spill path DOES engage there — a
   confirmed exposure, not a confirmed-safe path). Rate estimate
   refined by pooling every trial run across that epic's own
   investigation (290 total, still only the one original wrong
   observation): 0.34% (95% CI [0.01%, 1.91%]), tighter than and not in
   conflict with this epic's own standalone 4.8% (1/21) — NOT evidence
   of a fix, just a tighter bound on the same low, real rate.
   Separately, that epic DID fix a confirmed, independent O(n²)
   `append_to_parquet` spill-write slowness that had been inflating
   this bug's own slow runs too (140-291s -> 3-6s, ~40-90x, on the
   identical repro) — that mechanism is now closed; the wrong-answer
   mechanism is not. Three further new, distinct bugs were found (a
   concurrent-`serve`-process spill-directory collision, a
   LIMIT-not-enforced-under-spill bug, a sort-spill run-file crash),
   none fixed. Full detail: `.claude/epics/archived/
   spill-join-correctness/epic.md`.
2. **Deletion-vector JSON density at very large segment counts** (task
   005, quantified not just flagged) — ~131MB extrapolated for a
   1000-segments x 1,000,000-rows x 1%-deleted table, larger than task
   001's original design-time "tens of MB" guess. Not urgent at this
   program's current real-scale fixtures (sub-2MB in every measured
   case), but a real number for a future compaction epic to size
   against. Task 001's own named forward-compatible escape hatch (a more
   compact `deleted_rows` on-disk encoding) needs its own backward-
   compatibility migration story, judged too large for a same-task fix.
3. **O(N²) cumulative manifest-rewrite cost across a long mutation
   sequence** (task 005) — every single Append/Delete/Update
   unconditionally re-reads and re-writes the WHOLE `_manifest.json`;
   measured ~0.44ms/op near-empty growing to ~8.8ms/op at 2500+
   segments across a real 3000-operation mixed sequence. The direct,
   expected mechanical consequence of this epic's own already-justified
   "no compaction" design decision (task 001), not a bug — a real
   ceiling for a table accumulating thousands of small mutations over
   its lifetime without ever compacting.
4. **INSERT's SQL-path memory ceiling is narrower than the read path's**
   (task 002 found, task 005 fixed the common case) — even after task
   005's `bounded_partition_merge` fix (5.3GB → 1.6GB), the write path
   still has no formal pre-flight admission check consulting
   `--memory-limit` the way `NativeTable::scan()`'s `check_scan_budget`
   does; an extreme sub-1GB configuration can still OOM. Named, not
   fixed — a reliable memory-need estimate for a write path is harder to
   derive correctly than the read path's clean "total on-disk bytes"
   proxy, and a wrong heuristic risks a new class of bug (false refusals
   of legitimately-small operations).
5. **Carried forward unchanged from phase 1**: no scan-level filter/
   row-group pruning for native tables (`NativeTable::scan_with_filter`
   has none at all); `NativeTable::scan()` is not incrementally streaming
   (task 006 of phase 1's admission-control cap is a ceiling, not a true
   streaming rewrite) — the same architectural gap `LanceTable::scan()`
   already has. Neither is mutation-specific; both remain open,
   unclaimed follow-up work this epic did not attempt to close.

### Deferred to phase 3/4 (per the `native-tables` PRD — explicitly NOT
attempted this epic, so the next epic starts from an accurate picture)

- **Phase 3 — GPU/RAM/disk tiering.** Unbuilt, as planned. This epic's
  own contribution here is confirming (not building) that GPU offload's
  identity/cache-invalidation mechanism (phase 1 task 007) correctly
  handles mutation: `identity()` = `table_id` + `version` bytes, and
  every mutation bumps `version`, so a post-mutation query can never
  silently serve stale pre-mutation GPU-cached columns — verified live,
  not just argued from the code.
- **Phase 4 — Materialized rollups.** Entirely unbuilt, as planned. No
  query-rewrite/substitution mechanism of any kind exists anywhere in
  this codebase (unchanged from phase 1's own finding).
- **Compaction** (within mutation itself, not a PRD phase but explicitly
  epic-out-of-scope per task 001's Decision 3) — a deletion vector is
  correctness-preserving indefinitely; segment count grows forever and
  disk space from partial deletes is never physically reclaimed (except
  the narrow 100%-tombstone exception). Every building block a future
  compaction task needs already exists or was built for other reasons
  in this epic (deletion-aware `scan()`, the segment writer, the atomic
  single-file publish + single-writer lock) — compositional reuse, not a
  blocked-on-something-hard future task.
- **Distributed participation for mutation.** Confirmed (M1/M2 gates)
  that nothing this epic touched broke EXISTING distributed behavior for
  other table types — but `INSERT`/`DELETE`/`UPDATE` themselves are
  single-process, single-`ExecutionContext`-session operations only, not
  reachable from `serve`'s HTTP/Flight surface at all, matching phase
  1's own identical boundary for `CREATE TABLE ... AS SELECT`.

### Commits

`864a9d7` (001) → `bfc855f` (001 done) → `584c1a1` (002) → `38f032a`
(002 done) → `c846da3` (002 done, 003 starting) → `67a63a1` (003) →
`95a9f5c` (003 done, 004 starting) → `40bc074` (004) → `847d438`
(004 done) → `be00057` (004 done, 005 starting) → `f15b02d` (005) →
`efe1fc4` (005 done, 006/final QA starting) → this task's fix/docs/
archive commits.

### Archival

Epic moved to `.claude/epics/archived/native-tables-mutation/` as this
task's final step, mirroring `native-tables-foundation`/
`duckdb-parity-2`/`dependency-modernization`'s archival pattern
(`git mv`, this session). Not merged to `main` — that decision and
action is left to the user/orchestrating session per this task's own
instructions.
