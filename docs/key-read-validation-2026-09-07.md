# Canonical key validation and admitted framed reads

`physical/morsel_agg/key_rows.rs` can now restore canonical encoded keys into
query-owned scratch. It validates the full encoding before exposing a key for
hash lookup. This component remains outside live query ingestion/spilling;
complete state serialization and the bounded spill controller remain open.

`load_encoded` validates an existing byte slice without decoded allocations,
then admits and copies it. `read_encoded` admits the declared frame length before
consuming payload bytes, reads directly into owned scratch, then validates it.
Both invalidate the preceding key before starting. Budget denial consumes no
payload in the framed API; short reads, I/O errors and malformed encodings never
publish a partial key. The enclosing file owner must retain the frame length
and verify file version, integrity and layout identity before calling these APIs.
That file envelope is not implemented here. Equal-width type mismatches cannot
be detected from payload bytes alone.

The validator enforces exact field boundaries, validity bytes, Boolean values,
UTF-8, recursive list element nullability and the complete bound arity. It rejects
truncation and trailing bytes. Signed zero and NaN encodings must already be
canonical; accepting alternative encodings would make byte-based grouping wrong.
Fixed integer, timestamp and decimal payloads retain their exact bits. Decimal
coefficient validation here matches the existing encoder's domain; this is not
an additional finalized-value precision check.

String lengths and list counts are checked against remaining bytes before
traversal, and nesting is bounded at 64. Every list element consumes at least one
validity byte, preventing enormous declared counts from amplifying work beyond
the supplied payload. No String/List reconstruction occurs during validation.
The framed API separately checks addressable size before reservation. IO errors
retain their original QueryError::Io kind and OS error; they are not classified
as recoverable query-budget denial.

Final gates pass **63 morsel tests and 11 ownership tests**, with zero failures
or ignores. Four new tests cover all supported fixed types, exact large decimal
coefficients, canonical floating values, Unicode/embedded NUL strings, nested
lists and NULL/empty distinctions. They reject every truncated prefix of a
composite key, malformed validity/Boolean/UTF-8, alternate NaNs/negative zero,
hostile lengths, forbidden child NULLs and trailing bytes. Full-pool tests prove
capacity reuse, failed-read invalidation and preservation of stored keys. Framed
reads test adjacent frame boundaries, no payload consumption on denial, EOF,
injected EIO after a partial read, malformed payloads and successful reuse.

The first build encountered two test-only compilation problems (empty-slice type
inference and a temporary borrowed key); both were corrected. Earlier runs are
preserved but are not added to the final total. Commands used the required
48G cgroup, one build job, four Rayon threads and lance/gpu features. Formatting
passes. [Evidence](benchmarks/2026-09-07-key-read-validation/) preserves eight
SHA256-verified members and 647 current source-input hashes, including the exact
source delta, source snapshot, commands and logs. Manifest SHA256:
`d8c6ef938c511cedd5e4218f0c0a0e94c3948ef2e27adbbd990a864c3ec650f7`.

No full-library/integration, end-to-end spill, dedicated resource/GPU or optimized
benchmark gate was run. Both consuming-source replay failures and the unchanged
256KiB query-completion case remain unresolved and were not rerun. No dependency
or live execution routing changed. Next complete selected/fixed state-row
serialization and validated restoration, file identity/integrity, bounded
flush/merge and worker/cursor integration under the existing
[implementation contract](../.claude/epics/realistic-benchmarks-duckdb-leadership/updates/2026-09-07-fused-spill-implementation.md).
