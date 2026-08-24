---
issue: 001
stream: main
started: 2026-08-24T04:45:55Z
status: completed
---
## Scope
See .claude/epics/native-tables-mutation/001.md

## Progress

- Read phase 1 in full: `.claude/epics/archived/native-tables-foundation/
  epic.md` and all 8 tasks' Outcome sections (001-008), per this task's
  own first acceptance criterion, before deciding anything.
- Read the current, real source this design builds on:
  `src/storage/native_manifest.rs`, `src/storage/native_write.rs`,
  `src/storage/native_table.rs`, `src/planner/binder.rs` (the
  `CreateTable` arm + `require_supported_create_table_shape`),
  `src/execution/context.rs` (`create_table_as_select`,
  `register_native_table`), `src/physical/operators/scan.rs`
  (`TableProvider` trait, `as_any`/`identity`),
  `src/physical/operators/morsel_agg.rs` (dense-direct-address native
  branch), `src/storage/ipc_cache.rs` (`read_row_group`),
  `src/storage/iceberg.rs` (delete-file handling — found it REFUSES
  deletes rather than consulting them, correcting the epic's own stated
  precedent claim).
- Ran a throwaway `.scratch/mutation_sqlparser_spike/` (gitignored,
  never touched `src/`) confirming `sqlparser` 0.62's real parsed AST
  shapes for `INSERT INTO ... SELECT`/`... VALUES`, `DELETE FROM ...
  WHERE`, `UPDATE ... SET ... WHERE`, including which optional clauses
  parse but must be refused by name (Hive `INSERT OVERWRITE`, Postgres
  `UPDATE ... FROM`, `ON CONFLICT`, etc.).
- Ran a live, real cross-process test of `std::fs::File::try_lock()`
  (two small binaries, `lock_holder`/`lock_contender`, in the same
  scratch crate) confirming: (a) a second process is refused with
  `TryLockError::WouldBlock` while the first holds the lock, and (b) a
  SIGKILL'd holder's lock is available again immediately with zero
  manual cleanup — the concrete evidence behind the single-writer
  enforcement decision.
- Decided all six questions with evidence (deletion mechanism, UPDATE
  semantics, compaction scope, atomic-publish model, single-writer
  enforcement, SQL lift sizing) — full reasoning in `001.md`'s own
  Outcome section.
- Propagated every decision into `002.md`-`006.md`'s Technical Details
  sections and corrected/sharpened `epic.md`'s Architecture Decisions
  section (including the `iceberg.rs` precedent correction).
- No `src/` changes. `.scratch/mutation_sqlparser_spike/` is gitignored
  and confirmed absent from `git status --short`.

## Outcome

Six decisions, each with evidence grounded in this engine's actual code:

1. **Deletion mechanism**: per-segment sorted `Vec<u32>` of local row
   positions, a new field inline on the existing `Segment` struct in
   `_manifest.json` — merge-on-read, consulted inside `NativeTable::
   scan()`/`scan_with_filter()` as a single choke point. Not `roaring`
   (no new dependency justified at this format's segment-capped scale),
   not a sibling/separate file (would break the single-file atomic-
   publish model).
2. **UPDATE semantics**: DELETE + INSERT, confirmed — but implemented by
   sharing task 002/003's lower-level, non-publishing building blocks
   composed into ONE atomic manifest edit + publish, not two sequential
   self-publishing calls.
3. **Compaction**: explicitly deferred to a future epic. One narrow,
   in-scope exception: a 100%-tombstoned segment is dropped from the
   manifest by task 003 (bounds the deletion vector's worst case, not
   full compaction).
4. **Atomic publish**: single-FILE atomic `rename()` of a freshly-
   written manifest onto the live `_manifest.json`, generalizing (not
   reusing unchanged) phase 1's whole-directory `publish_table_dir` —
   confirmed by reading the code that the whole-directory mechanism is
   unsafe to reuse unchanged for incremental mutations.
5. **Single-writer enforcement**: `std::fs::File::try_lock()` (stable
   std since Rust 1.89, this toolchain is 1.93.0 — zero new dependency),
   held for a mutation's full span, verified live cross-process AND
   SIGKILL-safe.
6. **SQL lift sizing**: grammar is a solved non-issue for all three
   statements (confirmed fresh via spike, not assumed from CTAS parity)
   — effort is in refusal-by-name boilerplate and each statement's
   execution mechanism, not parsing.

See `.claude/epics/native-tables-mutation/001.md`'s Outcome section for
the full evidence behind each decision.
