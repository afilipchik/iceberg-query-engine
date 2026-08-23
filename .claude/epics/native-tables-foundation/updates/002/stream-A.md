---
issue: 002
stream: main
started: 2026-08-23T18:53:45Z
status: completed
---
## Scope
See .claude/epics/native-tables-foundation/002.md

## Progress
- Read the task file in full, plus 001.md's Outcome section (the JSON
  sidecar decision, module layout decision, and concrete starting schema)
  and 003.md/004.md/005.md's Technical Details (to match field/function
  names downstream tasks already expect, so they need no translation
  layer and don't have to read this module's source).
- Read `src/storage/ipc_cache.rs` in full, `src/storage/iceberg.rs` in
  full (the `TableMetadata`/`Snapshot`/`open_table`/`is_iceberg_dir`
  structural precedent), `src/physical/operators/scan.rs` (`TableProvider`,
  `TableStatistics`, `ColumnStatistics`), `src/storage/parquet.rs`'s
  `compute_statistics` and `src/storage/lance.rs`'s
  `compute_column_stats` (the two existing "derive footer-less stats"
  precedents this task's `column_stats_for_array` mirrors), and
  `src/storage/mod.rs`.
- **Load-bearing finding, confirmed by reading `ipc_cache::read_row_group`
  and its private `rg_path` helper in full, then proven empirically by a
  dedicated test**: `read_row_group(dir, rg_idx, ..)` does NOT accept a
  path — it computes one itself, hard-coded to `rg_{rg_idx:05}.arrow`.
  Segment files MUST be named exactly that (using the segment's `id`),
  NOT the `seg_00000.arrow` example name earlier planning notes used
  illustratively. `Segment::expected_file_name` is the single source of
  truth; `NativeManifest::validate` rejects any manifest whose declared
  `path` disagrees. This needed ZERO changes to `ipc_cache.rs` — its
  existing `pub fn`s are sufficient exactly as task 001 predicted; the
  constraint is satisfied entirely by this module choosing the right
  file name. Confirmed empirically, not just by inspection: two of this
  module's own tests write real Arrow IPC segment files (one plain, one
  with a `Dictionary(Int32, Utf8)` column) in the manifest's required
  layout and read them back through the actual, unmodified
  `ipc_cache::read_row_group`/`ipc_cache::sidecar_dict_cols`.
- Implemented `src/storage/native_manifest.rs` (new file): `NativeManifest`/
  `Snapshot`/`Segment`/`ManifestField`/`ManifestDataType`/`ColumnStats`,
  JSON (de)serialization (`write_manifest`/`read_manifest`), semantic
  validation (`NativeManifest::validate`, covering format_version,
  duplicate/mismatched segment ids and paths, row_count consistency,
  unknown-column stats references, and a rollup-consistency check),
  identity/versioning helpers (`generate_table_id`, `existing_table_id`,
  `next_version`), statistics computation straight from Arrow arrays
  (`column_stats_for_array`, `compute_batch_stats`, `ColumnStats::merge`,
  `merge_stats_into`, `NativeManifest::rollup`), and atomic publication
  (`staging_dir_for`, `publish_table_dir`, mirroring
  `ipc_cache.rs::build_sidecar`'s staging-dir-then-rename pattern
  generalized to a whole table directory).
- One unavoidable addition outside the new file: `pub mod native_manifest;`
  in `src/storage/mod.rs` (a new module is otherwise never compiled, never
  seen by `cargo fmt`/`cargo test` — this is standard Rust module
  registration, not a scope expansion into another task's logic).
  `ipc_cache.rs` itself was NOT touched.
- Deliberate refinement over the spec text's `HashMap`: `column_stats`/
  `table_stats` use `BTreeMap<String, ColumnStats>` instead, for
  deterministic (sorted, diffable, greppable) JSON output — matching this
  module's own "human-readable/greppable" design goal from 001.md's
  Outcome. Converting `BTreeMap` -> `HashMap` (task 004's
  `TableStatistics::column_stats`) is exactly as trivial as `HashMap` ->
  `HashMap` would have been, so this costs nothing on the
  "no translation layer" requirement.
- Wrote 23 unit tests (not just happy-path): schema<->Arrow round trip
  across every supported type + an explicit unsupported-type error test;
  statistics correctness on a known synthetic RecordBatch (including
  null handling and confirming int/float stats never cross-populate);
  Date32/Date64 zone-map coverage; rollup folding (including a
  non-monotonic-ranges case and a "one segment has no entry for this
  column" identity case); a dedicated NaN test proving the
  rollup-consistency check uses bit-pattern comparison rather than
  `f64::PartialEq` (which would spuriously flag legitimate NaN data as
  corrupt); full manifest JSON round-trip; corrupted JSON, missing
  manifest file, missing required field, and unsupported format_version
  all as clear `Err`s, not panics; three internal-consistency corruption
  checks (row_count mismatch, segment path/naming mismatch, unknown
  column in stats); an empty-table (zero segments) validity case;
  identity/versioning helper behavior; atomic publish (fresh publish,
  and replace-wholesale-including-stray-file-cleanup); and the two
  `ipc_cache` compatibility proofs described above.
- Fixed two test-fixture bugs found by the first `cargo test` run (both
  in my own test code, not the library logic): a shared test schema
  declared a column non-nullable while a test batch put a null in it,
  and a rollup test's hand-picked "deliberately inverted" segment stats
  had an incorrect expected value (min/max-of-mins/maxes across
  literal recorded values, not a self-correcting operation — the test's
  premise, not the merge logic, was wrong). Corrected both; all 23 pass.
- `cargo fmt --all` found 8 formatting issues in the new file (long
  lines wrapping differently than rustfmt wants); ran `cargo fmt --all`
  to apply, then `cargo fmt --all -- --check` confirmed clean.
- Verified builds clean (through `scripts/claude-safe-build.sh`) under:
  default features (`cargo build --lib`), `--features pulsar`, and
  `--features lance` (via the repo's vendored `.scratch/tools/protoc`) —
  zero warnings attributable to the new file in any of the three.
- Ran the FULL test suite in release profile (`cargo test --release`,
  matching this repo's actual CI-equivalent gate): exit code 0 across
  all 15 test binaries. Lib unittests: 265 passed, 0 failed, 1
  pre-existing ignored (266 total, up from 243 pre-existing + the 23
  new ones = 266, confirming no other lib test was affected).
  `tests/duckdb_validated.rs` (the cell-exact validation suite): 177
  passed, 0 failed. Every other integration suite (cli_tests,
  distributed_cluster, flight_tests, function_tests,
  function_validation_tests, partition_contract, spill_tests,
  sql_comprehensive, tpch_queries, window_functions, doc-tests): all
  green, 0 failed. `ipc_cache.rs` has no dedicated unit-test file of its
  own (confirmed: 0 `#[test]` functions inside it) — its behavior is
  exercised through this full suite (parquet-scan paths,
  `QE_IPC_CACHE`-gated benchmarks/integration tests), all of which
  stayed green throughout, and it was not modified at all.
- `git status --short` shows exactly the two intended changes:
  `M src/storage/mod.rs` (one `pub mod` line) and
  `?? src/storage/native_manifest.rs` (new file). `ipc_cache.rs`,
  `iceberg.rs`, `lance_write.rs`, `src/physical/`, `src/planner/` all
  untouched.
- Appended the Outcome section to `002.md` (final manifest schema,
  field-by-field, precise enough that tasks 003-007 don't need to read
  this module's source), checked its Acceptance Criteria and Definition
  of Done, set `status: closed`.
- Committed as `Task 002: ...` (this repo's commit-message convention).

## Final status
Complete. Manifest format implemented, tested (23 new unit tests, all
passing), documented for downstream tasks, full existing suite (all 15
binaries, release profile) green with zero regressions, `ipc_cache.rs`
untouched (its own read mechanism reused wholesale, confirmed
sufficient with zero refactor — proven by two tests that exercise it
directly against manifest-described segments), `cargo fmt --all --
--check` clean. Tasks 003-007 can proceed against
`src/storage/native_manifest.rs`'s public API without reading its
source.
