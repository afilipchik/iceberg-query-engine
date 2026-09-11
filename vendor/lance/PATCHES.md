# Local Lance patch

This is the exact Lance 10.0.0 crate, except for the patch listed below and
these provenance files. A copy of the Apache 2.0 license is included as LICENSE.txt. UPSTREAM.json records the original crate checksum and
every original file checksum. Upstream source headers and package license are
preserved. Dependency versions are unchanged.

- `src/io/exec/filtered_read.rs`: run refinement with its batch future in an
  owned `SpawnedTask`. The existing decode window limits concurrent batch
  futures; dropping one aborts its task. Preserve ordering, filter errors and
  NULL behavior. This addresses serialized CPU refinement, not general memory
  admission. A CPU operation already executing cannot be preempted by abort.

Regression: `tests/lance_filter_concurrency.rs` in the query-engine repository.
No source in the external Cargo registry is modified.
