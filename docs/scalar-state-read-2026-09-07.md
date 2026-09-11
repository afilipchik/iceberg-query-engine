# Admitted selected-state decoding

`execution/reserved_scalar/decode.rs` restores one typed scalar wire value with
its retained payload reservation. `BoundSelection::decode_payload` now uses this
reader to prepare a selected-state payload. Numeric values and NULL take a
separate allocation-free path; strings, lists and timestamps retain an admitted
owner. Complete transactional row restoration and live spill integration remain
unfinished.

The owned decoder performs three passes over immutable input: allocation-free
validation, complete payload/type admission, then construction. Malformed input
therefore cannot become a recoverable memory-denial error just because the pool
is occupied. Admission includes list element storage, strings, timestamp zones,
and retained child-type metadata, even for empty lists. String/List allocation
is fallible, actual capacity is checked, and partial construction drops before
the lease is released. Small metadata boxes follow the existing admitted scalar
copy contract. The caller owns and accounts for the encoded input buffer.

Validation enforces validity bytes, Boolean values, UTF-8, remaining-byte bounds,
list child nullability and nesting limits. Every list child consumes a validity
byte, bounding traversal by payload size. Fixed values preserve original bits,
including selected negative zero and NaN payloads. Decimal coefficients preserve
all128 bits and the bound signed scale; no finalized precision check is added.
Timestamp unit and timezone come from the bound layout. The enclosing file
reader must verify version, layout identity and integrity before decoding.

The reader returns exactly the consumed prefix length so subsequent slots remain
available to the row decoder. It does not itself reject trailing bytes, publish
a row, mutate an existing selected state or transfer unowned variable values.
`try_decode_inline` can return only NULL or allocation-free scalar variants.
Variable payloads require the owned path. The pending payload API does not yet
install decoded values into `StateRows`.

Final gates: **67 morsel plus16 ownership tests pass**, zero failures/ignores.
Six new tests cover nested retained values after source/schema destruction;
every truncated prefix; malformed later UTF-8 and huge lengths; forbidden child
NULLs; admission denial before construction; injected third-allocation failure
with cleanup; exact float/decimal/timestamp domains; retained large child-field
metadata; excessive nesting; and selected numeric decoding under a full pool.
The existing scalar writer byte-oracle test now decodes and re-encodes all bound
supported scalar domains, proving byte preservation. Interval remains outside
selected-state binding and is not claimed as decoded coverage.

Formatting passes. Tests used the required48G cgroup, one build job, four Rayon
threads and lance/gpu features. Earlier selected runs are retained but not added
to the final total. [Evidence](benchmarks/2026-09-07-scalar-state-read/) contains
13 SHA256-verified members and649 current source-input hashes, source snapshots,
isolated deltas, commands and logs. Manifest SHA256:
`d33d1b4db7f04fbed74ee8bce8aceadd19093c0e82c91abffc8111f54aa19957`.

No dependency or live query routing changed. No full-library/integration,
end-to-end spill, dedicated cap/GPU or performance gate ran. Both consuming-source
replay failures and the unchanged256KiB query completion case remain open and
were not rerun. Next compose fixed/selected decoding into whole-row transactional
restoration, then file identity/publication and bounded flush/merge with retained
input/cursor integration. Follow the existing
[implementation contract](../.claude/epics/realistic-benchmarks-duckdb-leadership/updates/2026-09-07-fused-spill-implementation.md).
