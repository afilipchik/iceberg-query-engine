# Transactional partial-row restoration

`GroupRows::prepare_restore` now composes canonical key reading, fixed codecs
and owned selected-value decoding into one unpublished group transaction. It
restores the version-1 row payload written by `write_row`. This remains a storage
component outside live query ingestion/spilling; file identity/publication and
bounded controller work are still required.

The reader checks version and key-length bounds, validates the complete state
payload and canonical key, then admits index/key/state growth. `StateRows` stages
fixed fields and selected payloads in existing row scratch. The preparation token
owns both the fresh logical row and pending selected owners. Any error or token
drop removes the new state row and key; only commit publishes the index entry.
Previously admitted metadata capacity can remain for reuse, while all pending
selected payload leases are released on failure.

Whole-state validation precedes retained-value admission. Thus malformed later
slots cannot be reported as memory denial caused by an earlier selected value.
Fixed frames are padded into stack scratch for the existing strict decoder.
Selected slots use the allocation-free validator followed by their admitted
reader. Trailing state bytes are rejected. The file owner must still verify
layout identity and integrity: a version byte cannot distinguish equal-width
but semantically different bound functions or types.

Restoration requires an absent group and rejects duplicates explicitly. Duplicate
keys across spill runs must be restored into a staging store and merged with
`prepare_merge_update`, preserving internal aggregate algebra. Reapplying input
aggregation to finalized values is not used. Only fresh rows are restored, so
an absent selected payload correctly leaves that slot empty; restoration cannot
silently retain a preexisting selected value.

Final gates pass **69 morsel plus16 ownership tests**, zero failures/ignores.
Two new integration tests within the storage module cover:

- Byte-for-byte write/restore/write agreement after the source is dropped;
  exact large decimal SUM, COUNT/AVG partial states, all-NULL aggregates, NULL
  keys, discarded preparations, duplicate refusal and stable pending-owner cleanup.
- A second restored run merged under a full pool: four rows with sum100/count4
  plus one row with value10 produce COUNT5, AVG22 and exact decimal coefficient5a.
  The staging source can be dropped before merge commit.
- Every truncated prefix and a corrupt later fixed-state flag rejected under a
  full pool; warm metadata with only3000 bytes available admits the first decoded
  string but refuses the later8192-byte string. The whole group rolls back,
  pending owners release, and a subsequent unrestricted retry round-trips exactly.

The earlier69-test run is retained but not added to the final total. Formatting
passes. Commands used the required48G cgroup, one build job, four Rayon threads
and lance/gpu features. [Evidence](benchmarks/2026-09-07-row-restoration/) contains
13 SHA256-verified members,649 current source-input hashes, source snapshots,
isolated deltas, commands and logs. Manifest SHA256:
`3c9724a85e29f5d14e12a82c72172b933b0c4e66644ca47a0780345461ab6364`.

No dependency or live execution routing changed. No full-library/integration,
end-to-end spill, dedicated cap/GPU or performance gate ran. Both consuming-source
replay failures and the unchanged256KiB query completion case remain open and
were not rerun. Next implement versioned file identity/integrity, ownership and
publication; bounded flushing and partition merge/repartition; and retained-input
worker/cursor integration. Input and file buffers must also retain query-budget
ownership. Follow the existing
[implementation contract](../.claude/epics/realistic-benchmarks-duckdb-leadership/updates/2026-09-07-fused-spill-implementation.md).
