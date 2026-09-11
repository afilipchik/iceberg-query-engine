# Borrowed partial-state row serialization

Grouped storage now writes complete key/state row payloads without finalizing
aggregates, cloning selected values or constructing another row buffer. This is
a spill component, **not live spill integration**. Validated selected-state
read-back, file identity/publication and bounded merge/controller work remain.

`StateRows::encoded_size` validates all fixed slots and selected payload sizes
before output. `StateRows::write_to` emits slots in bound logical order using
existing packed fixed-state frames and borrowed selected scalar payloads.
`GroupRows::write_row` prevalidates state before emitting version 1, a little-endian
u64 canonical-key length, the key, and the partial-state payload. The enclosing
file format must identify bound functions, types, scales, timestamp domains and
slot order; the row version alone is not layout identity or an integrity check.

The new `scalar_state_codec.rs` uses a validity byte, fixed little-endian scalar
bits and u64 string/list lengths. Type metadata belongs to the bound file layout,
not each scalar. Selected floats preserve negative zero and original NaN bits,
unlike the canonical group-key representation. NULL and empty strings/lists
remain distinct. Size arithmetic and recursive traversal are checked before
writing; payloads are borrowed and only fixed words use stack scratch. The caller
owns and accounts for the output writer and buffering.

COUNT remains a count; AVG retains sum/count; decimal SUM keeps full i128 partial
coefficients, signed scale and seen/overflow flags. No finalized-value precision
check is introduced. Fixed frame width is now visible to sibling row-codec code.
No dependency or live query routing changes.

Prevalidation errors emit no bytes. I/O errors may leave partial output, retain
the original QueryError::Io identity, and leave source rows and reservations
intact. The future file owner must reject/remove incomplete files and retain
state until successful publication. These methods do not flush, fsync, publish,
release source groups or claim atomic file creation.

Final gates pass **66 morsel plus 11 ownership tests**, zero failures/ignores;
three new tests are included. The grouped byte oracle independently checks
canonical key zero, COUNT 4, AVG sum 100/count 4, a decimal coefficient above
64 bits, selected nested NULL/negative-zero/NaN values and Unicode strings.
A preallocated output slice and completely occupied query pool verify writing
without new query admission. Every shorter output length causes WriteZero while
preserving source state, lookup and later successful serialization. A separate
test injects a later invalid COUNT and proves zero output, then injects ENOSPC
and verifies original error/state. Scalar byte oracles cover every ScalarValue
variant, empty values, exact extremes and checked size/nesting limits.

Formatting passes. [Evidence](benchmarks/2026-09-07-state-row-write/) preserves
14 SHA256-verified members, 648 current source-input hashes, changed source,
isolated deltas, commands and logs. The commands used the required 48G cgroup,
one build job, four Rayon threads and lance/gpu features. The earlier 65-pass
run is preserved but not added to the final total. Manifest SHA256:
`59cb911a762de37d48627d520a07774855e3400ca1bf5f2e106359373dc3929e`.

No full-library/integration, end-to-end spill, dedicated cap/GPU or performance
gate ran. Both consuming-source replay failures and the unchanged 256KiB query
completion gate remain unresolved and were not rerun. Next implement admitted
selected-state decoding and transactional restoration of complete partial rows,
then versioned file identity/integrity and bounded flush/merge with worker/cursor
integration. Preserve the existing
[implementation contract](../.claude/epics/realistic-benchmarks-duckdb-leadership/updates/2026-09-07-fused-spill-implementation.md).
