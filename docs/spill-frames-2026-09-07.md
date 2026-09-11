# Query-local spill frame identity and integrity

`physical/morsel_agg/spill_frames.rs` wraps grouped partial rows in ordered,
checksummed frames. Each successful `GroupLayout` binding now receives a UUID;
each run receives a separate UUID. Workers sharing the same bound layout can
share its identity. Independently bound layouts, even with identical schemas,
are intentionally different. This is query-local identity, not a durable schema
fingerprint or cross-process resume format.

A frame contains a64-byte header: magic/version, layout UUID, run UUID, ordinal,
payload length, header CRC32 and reserved-zero bytes. The grouped-row payload is
followed by its CRC32. Header integrity and identity/order are checked before
payload admission. CRC32 detects accidental corruption; it is not cryptographic
authentication. Frame payload parsing still performs the strict row validation.

`FrameCursor::write` prevalidates source layout and row size before touching the
writer. It streams the borrowed row through a checksum adapter without another
row buffer. Once output begins, any failure poisons the cursor; source groups
remain untouched and the file owner must discard incomplete output. The ordinal
advances only after the payload checksum is written successfully.

`FrameCursor::read` uses `FrameScratch`, whose buffer is admitted from the bound
layout's query pool and whose layout identity is checked. It returns a borrowed
payload only after the checksum verifies. Budget denial rewinds the seekable
input to the frame start and preserves the ordinal, allowing retry after memory
is released. A failed rewind or other IO/validation failure poisons the cursor.
The reader never exposes partially read or unchecked scratch through its API.

This layer does not own, publish, sync, rename or delete a file. Its future owner
must retain the expected run identity and completed frame count, reject missing
or extra frames, distinguish complete EOF from truncation, and keep source state
until publication succeeds. Framing alone cannot detect omission of a complete
suffix without that expected-count contract. File metadata/path/handle ownership,
cleanup, bounded partition processing and live worker integration remain open.

Final gates pass **72 morsel plus16 ownership tests**, zero failures/ignores;
three new tests are included. They cover two framed rows restored into grouped
storage; read admission denial and exact rewind/retry; every single-byte mutation
and truncated prefix of a frame; wrong run/layout identities; reordered/duplicated
ordinals; every short writer extent; poisoned-cursor refusal and source retention.
Checksums are tested with byte mutations, independently of semantic row parsing.
These tests use in-memory seekable IO, not published filesystem artifacts.

Formatting passes. Commands used the required48G cgroup, one build job, four
Rayon threads and lance/gpu features. Existing UUID/CRC32 dependencies are reused;
Cargo manifests/lockfile are unchanged. [Evidence](benchmarks/2026-09-07-spill-frames/)
contains nine SHA256-verified members,650 current source-input hashes, snapshots,
isolated deltas, commands and logs. Manifest SHA256:
`fee41068b9bc959b0d65cd575c25111395294851ee5c1e7d89bc9a95f8467136`.

No live execution routing changed. No full-library/integration, end-to-end spill,
dedicated cap/GPU or performance gate ran. Both consuming-source replay failures
and the unchanged256KiB query-completion case remain unresolved and were not
rerun. Next implement complete-file ownership/publication and expected-count
validation, then bounded flush/merge/repartition and retained-input worker/cursor
integration under the existing
[implementation contract](../.claude/epics/realistic-benchmarks-duckdb-leadership/updates/2026-09-07-fused-spill-implementation.md).
