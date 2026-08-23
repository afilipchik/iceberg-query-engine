---
issue: 003
stream: main
started: 2026-08-23T19:20:39Z
status: completed
---
## Scope
See .claude/epics/native-tables-foundation/003.md

## Progress
- Read the task file in full, plus 002.md's Outcome section (the exact
  final `NativeManifest`/`Segment`/`ColumnStats` schema, and CRITICALLY
  its finding that `ipc_cache::read_row_group` hardcodes segment files to
  `rg_{id:05}.arrow` via a private helper — every segment this task writes
  uses `native_manifest::segment_full_path`/`Segment::expected_file_name`,
  never an invented name) and 001.md's Outcome (module name
  `src/storage/native_write.rs`; `ExecutionContext::sql()` fully
  materializes via `try_collect()`, so the CTAS-shaped entrypoint must
  drive a `RecordBatchStream` directly instead).
- Read `src/storage/lance_write.rs` (direct template), `src/storage/
  ipc_cache.rs` in full (dictionary-encoding pattern to mirror: cast
  candidate Utf8 columns, demote back to plain if the resulting
  dictionary is wide), `src/physical/operators/scan.rs` (`TableProvider`,
  `RecordBatchStream`), `src/storage/parquet.rs` (`StreamingParquetReader`
  — reused directly, not reimplemented), `src/storage/iceberg.rs`
  (`open_table` resolves to a `ParquetTable`), `src/storage/lance.rs`
  (`fragment_infos`/`shard_with_fragments` — reused for a genuinely
  fragment-at-a-time Lance read), `src/execution/context.rs` in full
  (confirmed the materializing `sql()` gap firsthand), and `src/main.rs`'s
  `WriteLance`/`LoadLance` handlers (CLI template).
- Implemented `src/storage/native_write.rs` (new file, 1354 lines incl.
  17 tests): `write_batches(stream: RecordBatchStream, schema, out_dir,
  mode) -> Result<NativeWriteResult>` is the core entrypoint — takes a
  STREAM, not `Vec<RecordBatch>`, specifically to give a future `CREATE
  TABLE ... AS SELECT` a genuinely streaming path task 001 flagged
  `ExecutionContext::sql()` cannot provide. Internally: a private
  `SegmentWriter` buffers incoming batches up to
  `NativeWriteOptions::target_rows_per_segment` (default 1,000,000) rows,
  then flushes ONE segment: concatenates the buffered batches, decides
  dictionary encoding (see below), computes `native_manifest::
  compute_batch_stats` on the flushed batch itself (never re-reading the
  written file), writes the Arrow IPC file at `native_manifest::
  segment_full_path`, and records a `Segment`. `NativeManifest::build` +
  `write_manifest` + `publish_table_dir` finalize the table atomically
  once the stream ends.
- Dictionary encoding: every plain `Utf8` column is a candidate; the
  FIRST segment actually flushed decides (cast to `Dictionary(Int32,
  Utf8)`, keep it only if <= `dict_max_cardinality` [default 4096,
  matching `ipc_cache.rs`'s own threshold] distinct values), and that
  decision is LOCKED for every later segment — a deliberate difference
  from `ipc_cache.rs`'s own per-row-group re-decision, forced by the fact
  a native table's manifest declares ONE Arrow type per column for the
  whole table (unlike a parquet-shadowing sidecar, where `sidecar_dict_cols`
  only ever consults row group 0 and tolerates disagreement elsewhere).
  Tested explicitly (`dictionary_decision_is_locked_after_the_first_segment`).
- `write_from_parquet`/`write_from_iceberg`/`write_from_lance` (feature
  `lance`) convenience wrappers, each `(source, out_dir, mode) ->
  Result<NativeWriteResult>` (plus a `_with_options` variant for all
  three and the core entrypoint): parquet streams via the existing
  `StreamingParquetReader`; Iceberg resolves to a `ParquetTable`
  (`storage::iceberg::open_table`) and reuses the identical streaming
  reader; Lance streams ONE FRAGMENT AT A TIME via `LanceTable::
  fragment_infos`/`shard_with_fragments` + `scan` (both already `pub`,
  zero changes to `lance.rs`) inside a `futures::stream::unfold` state
  machine, run through `tokio::task::spawn_blocking` per fragment — a
  genuine improvement over `LanceTable::scan()`'s own contract (which
  always fully materializes the whole dataset), bounding peak memory to
  one fragment.
- Safety hardening beyond the literal spec, found while implementing:
  `Overwrite` mode refuses (rather than silently `remove_dir_all`-ing,
  which `native_manifest::publish_table_dir` does unconditionally once
  invoked) a destination that exists, is non-empty, and is NOT already a
  native table — protects against pointing `--out` at the wrong
  directory. An empty existing directory is still adopted silently. Every
  error path leaves `out_dir` completely untouched (writes go to a
  `native_manifest::staging_dir_for` staging directory first, published
  atomically only on full success; staging is best-effort cleaned up on
  any error).
- `write-native`/`load-native` CLI commands in `src/main.rs`:
  `write-native --from-parquet|--from-iceberg|--from-lance|--sql ... --out
  <dir> --mode create|overwrite`; the `--sql` arm builds a physical plan
  via `ctx.physical_plan()`, executes every partition, merges them with
  `futures::stream::select_all`, and streams the result straight into
  `write_batches` — never through `ctx.sql()`. `load-native --path <dir>
  [--query <sql>]` prints the manifest and, for `--query`, reads the
  whole table back (`native_write::read_back`, NOT a `TableProvider` —
  explicitly documented as a CLI validation convenience, not the
  production read path) into a `MemoryTable` for ad hoc SQL.
- Mid-implementation finding from cross-task coordination (flagged by
  impl-nt-004-provider, who hit it in their own CTAS validation): a bare
  `SELECT *`'s physical schema carries QUALIFIED field names
  ("orders.o_orderkey") — a pre-existing binder property
  (`SelectItem::Wildcard` preserves `field.relation`), not introduced by
  either of us. Fixed in `write-native --sql`'s own arm (added
  `unqualified_schema()` in `main.rs`, stripping any `table.` prefix from
  both the schema and every streamed batch before they reach
  `write_batches`) so a native table written via `--sql "SELECT * FROM
  t"` gets normal column names. `native_write.rs` itself is unaffected —
  it writes whatever schema/stream it is given, faithfully; this is a
  caller-side fix in the CLI convenience path only, mirroring the
  layering the peer used in their own DDL entrypoint.
- 17 new tests, all in `src/storage/native_write.rs`'s own `#[cfg(test)]
  mod tests` (16 in the default build, +1 gated on `--features lance`,
  mirroring `native_manifest.rs`'s own all-inline precedent): round trip
  via an in-memory `RecordBatch` stream (the CTAS-shaped case); low- and
  high-cardinality dictionary encoding, and the locked-decision-across-
  segments case; segment splitting at a small `target_rows_per_segment`
  with correct per-segment AND table-rollup stats; a same-data-different-
  batching invariance check (many tiny batches vs one big batch must
  agree on stats/segment count/schema); zero-rows refusal (both an empty
  stream and a stream of one zero-row batch) leaving no trace on disk;
  `Create`-mode-refuses-existing, `Overwrite`-bumps-version-preserves-
  identity-replaces-wholesale, `Overwrite`-refuses-a-non-native-non-empty-
  destination, `Overwrite`-adopts-an-empty-directory; a mid-stream error
  propagating and publishing nothing; Date32/Int32 zone-map stats; and
  COUNT+SUM checksum validation (source vs. written-back, mirroring
  `scripts/iceberg_gen.py`'s own discipline, `math.isclose`-equivalent
  tolerance) against real `data/tpch-1mb/orders.parquet`,
  `data/tpch-1mb-iceberg/orders`, and (lance feature) `data/tpch-1mb-
  lance/orders.lance` fixtures.
- Git coordination (uncommitted shared working tree, no worktree
  isolation — `src/main.rs` and `src/storage/mod.rs` both had my hunks
  interleaved with impl-nt-004-provider's uncommitted `native_table.rs`
  work): extracted exactly my own hunks via `git diff`/`git apply --check
  --cached` on hand-built patches (not `git add -p`, which needs a tty)
  so my commit is self-contained and compiles standalone (verified: zero
  references to `native_table` in anything I staged, only a doc-comment
  mention of the filename), leaving the peer's uncommitted hunks
  untouched in the working tree for their own commit. Committed as
  `6df5703 "Task 003: write path — bulk-load from parquet/Iceberg/Lance/
  query results"`. A second, small follow-up commit added the
  `unqualified_schema` fix (found after the first commit, via the peer's
  report).
- Verified builds clean (through `scripts/claude-safe-build.sh`) under
  default features (`cargo check`, both lib and bin) and `--features
  lance` (`cargo check`, via the repo's vendored `.scratch/tools/protoc`)
  — zero warnings attributable to the new file or the `main.rs`/`mod.rs`
  hunks in either.
- Ran the native_write-scoped test suite in DEBUG mode first for fast
  iteration: 16/16 passed (default), 17/17 passed (`--features lance`,
  including the Lance checksum test) — both green on the first real run.
- Ran the FULL lib suite in RELEASE mode (`cargo test --release --lib`,
  matching this repo's actual gate; used a private `CARGO_TARGET_DIR`
  under `.scratch/` to avoid lock contention with two other agents'
  concurrent `cargo test --release` runs in the shared `target/`):
  **289 passed, 0 failed, 1 ignored** — the COMBINED suite including task
  004's `native_table.rs`/`context.rs`/`binder.rs` work, confirming
  cross-task integration (their `TableProvider` reading tables this task
  writes) holds under release optimization too, not just debug.
  `cargo fmt --all -- --check` clean.
- **Real-data validation at true SF=10 scale** (`data/tpch-10gb`,
  `data/tpch-10gb-iceberg`, both explicitly named as available in the
  task), via the built release binary and an INDEPENDENT DuckDB oracle
  (`read_parquet`/`iceberg_scan` — not the engine validating itself):
  - `orders` (15,000,000 rows), from parquet: written in 2.55s, 15
    segments. COUNT and every integer SUM (`o_orderkey`, `o_custkey`)
    bit-exact vs. DuckDB; `SUM(o_totalprice)` (Float64) matches to
    ~1.4e-15 relative — floating-point summation-order noise, not a
    discrepancy.
  - `orders`, from Iceberg (`data/tpch-10gb-iceberg/orders`, single
    snapshot, 15,000,000 records per its own metadata.json): written in
    2.05s, 15 segments. Same result: COUNT and integer sums bit-exact vs.
    DuckDB's `iceberg_scan` on the same snapshot; float sum within
    ~8.6e-15 relative.
  - `lineitem` (60,000,000 rows, the largest TPC-H table; 2.8GB
    compressed parquet source), from parquet: written in **12.28s**,
    58 segments, 5.3GB on disk. **Peak RSS during the write: 415,428 KB
    (~406 MB)** (`/usr/bin/time -v`) — direct, measured proof of the
    bounded-per-segment-buffer design, not an assertion: converting a
    2.8GB source used well under half a gigabyte of resident memory.
    COUNT and all four integer SUMs (`l_orderkey`, `l_partkey`,
    `l_suppkey`, `l_linenumber`) bit-exact vs. DuckDB; all four float
    SUMs (`l_quantity`, `l_extendedprice`, `l_discount`, `l_tax`) match
    to <=1.4e-13 relative.
  - Schema output confirms dictionary encoding fired as designed:
    `o_orderstatus`/`o_orderpriority`/`o_clerk`/`o_comment` all came back
    `Dictionary(Int32, Utf8)`.

## Final status
Complete. Core streaming entrypoint (`write_batches`) plus three
source-specific convenience wrappers implemented and tested; dictionary
encoding for low-cardinality strings applied at write time (locked-once
design, documented and tested); `write-native`/`load-native` CLI commands
exercise the whole thing end-to-end. Checksums match the source exactly
(source vs. written-back, mirroring `iceberg_gen.py`'s own discipline) at
BOTH small unit-test scale (all three source types) AND true SF=10 real-
data scale (parquet + Iceberg, against an independent DuckDB oracle) —
including a direct, measured (`/usr/bin/time -v`) proof that converting
the 2.8GB `lineitem` table peaks at ~406MB RSS, confirming the "streaming,
no double materialization" requirement empirically, not just by design.
Full release-mode lib suite (289 tests, including task 004's concurrently-
landed work) green; `cargo fmt --all -- --check` clean. `git status
--short` shows only this task's intended changes plus other agents' own
in-progress work in shared files (verified via `git diff --cached`
containing zero references to `native_table` before committing).
