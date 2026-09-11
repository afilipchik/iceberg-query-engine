# Metadata-cache file identity fix, September8

## Reproduced systemic failure

The production metadata cache keyed freshness only by pathname and modification
time. A deterministic regression warmed plain and schema-override entries,
replaced a two-row Parquet file with a five-row file preserving mtime, and read
cached metadata. Before the fix (60780), the cache returned2 rows instead of5.
This can supply stale row counts/statistics/schema or offsets for current data.
Reader builders also reopened the pathname after metadata lookup/parse, allowing
a replacement to pair one file's metadata with another file's handle.

## Change

`src/storage/metadata_cache.rs` now opens the file first and fingerprints that
handle: length/mtime plus Unix device/inode/ctime with nanoseconds. Plain and
schema-override entries share a per-path/version cache. Fingerprints are checked
before and after metadata acquisition; a detected change returns an explicit
error. Cache hits stay on the read-lock path. Builders consume the same handle
used for lookup/parse, and `cached_open_file` exposes that matched pair to the
pending admitted-reader integration.

Schema-identity entries explicitly retain the originating SchemaRef, preventing
address-key reuse if the reader library normalizes the schema. A changed version
drops all obsolete schema variants from that path's cache entry; active readers
retain their own metadata/handle. Non-Unix platforms bypass reuse because this
implementation has no strong file identity there. This has not been certified
on non-Unix hardware.

The fix is not a lock/snapshot against later in-place writes. Readers still
require immutable file contents during consumption. Footer parse admission,
query-wide metadata admission and ownership remain open at this identity checkpoint.
The retention and live-scan follow-up below supersedes the unbounded cache policy. It
would be incorrect to describe this identity fix as full metadata memory safety.

## Verification

- Red60780: preserved-mtime replacement returned2 instead of5; log retained.
- Focused96511 then85952: final3 tests pass, zero skips. Coverage includes the
  preserved-mtime replacement, same-size/same-mtime replacement with refreshed
  statistics, old builder reading its pinned original file, new builder reading
  current data, unchanged-file reuse and retained schema-key ownership.
- Full library91595:962 passed,0 failed,10 existing ignored,29.05s.
- Integration35500: partition_contract17, qualified_column_identity12 and
  shared_prescan_errors3 passed;32 total, zero skips. Test sources have no
  fixture-missing early-success branches for these selected cases.
- Formatting and whitespace checks pass. No new benchmark binary/SF10 result.

All engine/tests ran with repository TMPDIR, Rayon4/one build job and the48GiB
safe-build scope, locked/offline lance,gpu features. Commands select
`--lib storage::metadata_cache -- --nocapture`, `--lib -- --test-threads=4`, or
`--test qualified_column_identity --test shared_prescan_errors --test partition_contract`.
Evidence/source hashes: `docs/benchmarks/2026-09-08-metadata-cache-identity/`.


## Retention and live scan follow-up

The global cache now retains at most 256 schema variants and 256 MiB of reported
metadata estimates, evicting least recently used entries. Oversized entries stay
uncached without evicting unrelated entries. Hits update an atomic age while
holding a read lock. The size estimate includes Parquet's `memory_size`, Arrow
schema storage and owner allowances; it is not exact RSS or pre-admission of
footer parsing. Metadata retained by active readers is outside this cache cap.

A contained diagnostic loaded all eight canonical SF10 table footers, then
checked Arc identity on a second pass. All eight remained cached. Parquet's
metadata estimates total 3,880,020 bytes; lineitem accounts for 3,138,458 bytes.
These estimates exclude the separately charged Arrow schema/owner allowances.
This establishes reuse for this eight-file working set, not throughput or a
fragmented-data cache-hit guarantee. Library retention gate: 963 passed, zero
failed, 10 existing ignored, 30.52 seconds. Probe exited zero. Evidence is frozen
in `benchmarks/2026-09-08-metadata-retention/`; the diagnostic is
`examples/metadata_cache_probe.rs`.

The live raw-Parquet scan previously parsed metadata for pruning, then reopened
and selected cached metadata independently for each row group. A pathname
replacement could therefore mix one version's selected row-group indices with
another version's contents. `ParquetSnapshot` now retains the exact metadata and
file fingerprint used for planning. Row-group work shares that owner. Execution
opens a fresh independent file handle, validates its version and constructs the
reader from the retained metadata. A mismatch refuses with
`Parquet file changed after scan planning` before decoding that row group.
Cache eviction cannot cause a footer reparse within this scan.

This deliberately avoids cloning a single seek cursor across concurrent readers:
Parquet 58.4.0's `ChunkReader for File` uses cloned handles and seeks, which share
cursor state. Each row-group reader still receives its own independent open.
The admitted decoder uses positional reads, but it remains unrouted. No file
handles are held merely to retain plan metadata. Files must remain immutable
while an opened reader consumes them. This is version validation, not a lock or
MVCC snapshot. Non-Unix live scans explicitly refuse because this implementation
has no sufficiently strong file identity there. IPC sidecar lifecycle/version
certification remains separate; its reader bypasses this raw-Parquet builder.
Query-owned footer metadata and parsing are not yet admitted to the query pool.

The new live regression replaces a two-row-group file with a one-row-group file
while preserving mtime and requires every declared partition to error before
output. Another test evicts the file's global cache entry, then verifies that
both ordinary and schema-override query snapshots reuse the original metadata
Arc and return exact duplicate values. Full library gate17707 exited zero:
965 passed, zero failed, 10 existing ignored, 29.97 seconds. The existing ignored
tests are not certified GPU/IPC coverage. Formatting and whitespace checks pass.
Integration gate89712 exited zero: partition_contract17, runtime_filter_domain_contract2,
shared_prescan_errors3 and systemic_numeric_tests12; 34 passed with zero skips.
The selected test sources contain no fixture-missing early-success branches.
Both gates used the 48 GiB safe wrapper, repository TMPDIR, Rayon4, one build
job, locked/offline lance,gpu features. Evidence: `benchmarks/2026-09-08-scan-metadata-ownership/`.
No new release candidate or query-performance result is claimed.

Local upstream check: DuckDB checkout `1c27c54f27bea397d18d86f7c0d6a9f1d4aa85f8`,
`extension/parquet/parquet_reader.cpp` around lines 825–850, retains parsed metadata
on its reader and validates a global-cache entry against an opened handle when
loading it. This supports separating reader lifetime from cache lifetime; it is
not proof that either implementation solves every mutation or allocation case.
