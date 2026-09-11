# Partial aggregate state: fixed-size codec component

`physical/morsel_agg/state_codec.rs` now implements checked encoding/decoding
and query-owned working space for fixed-size partial accumulator states. It is
**not connected to operator routing or spilling**. MIN/MAX/ANY_VALUE/ARBITRARY
selected values, canonical group keys, full row-layout identity, file ownership
and bounded partition merge remain required parts of the implementation.

## Representation

The codec preserves internal accumulator values without finalizing SQL results.

| State | Preserved fields | Packed bytes per slot |
|---|---|---:|
| COUNT | Nonnegative i64 count |12|
| Floating SUM | f64 bits and seen flag |12|
| Integer SUM | Signed i64 sum and seen flag |12|
| Decimal SUM | Fulli128 coefficient or sticky overflow, signed scale, seen flag |20|
| AVG | f64 sum bits and nonnegative i64 count |20|
| BOOL_AND/BOOL_OR | None / false / true |4|
| Variance family | Nonnegative count, mean bits, M2 bits |28|

Each slot starts with a version byte, physical-state tag, flags and a decimal
scale byte (zero for other states). Numeric payloads use little-endian encoding.
The fixed stack representation has32 bytes; the row workspace writes only the
used prefix of each slot, concatenated into one admitted byte buffer. A
COUNT/decimal SUM/AVG/floating SUM fragment therefore writes64 bytes, not128.
That is a representation-size check, not a measured query-speed improvement.

Decoding checks version, kind, flags, signed scale and count validity. Standalone
frames require exact length and canonical unused bytes. Compact fragment length
comes from its bound codec list. Malformed/truncated input and writer failures
remain errors. This is not full file integrity protection: a valid numeric
payload bit change needs outer checksums/framing, and physical kind/scale checks
do not replace complete logical schema, expression/slot and query-layout identity.

Binding uses the operation/input type and explicitly rejects DISTINCT flags and
unsupported state families. It does not infer a codec from sample values or
estimates, and does not establish SQL type validity by itself. The complete row
layout must resolve unsupported components before consuming input. No fallback
or eligibility change has been added to the engine in this step.

## Ownership and publication

`FixedStateWorkspace` admits its codec vector, packed byte scratch and decoded
state vector before use. They use existing `ReservedVec` owners, including
allocation metadata allowances. There is no growth during encoding or reading.
Decoded states are borrowed from that owned workspace; destination state storage
must separately admit its capacity before copying/merging them.

The entire fragment validates and encodes before any byte reaches the writer.
A writer failure may still leave a partial file; a future spill-file owner must
remove or reject it. Read errors or later-slot validation failures return no
state reference. The workspace remains reusable after failure. This component
does not own spill files, selected strings/lists, group keys, queued input or
merge partitions, and it is not evidence of complete query-wide memory ownership.

## Verification

The final selected gate passes **39 tests**, with0 failures,0 ignores and777
filtered out. Seven are new codec/workspace tests;32 exercise the existing
morsel paths. Earlier selected runs are retained but are not additional unique
coverage. The tests verify:

- a decimal partial above38 digits that later cancels into a valid final result;
- exact fulli128 extremes, signed scale, seen state and sticky arithmetic overflow;
- weighted AVG merge (10/1 plus90/3 produces25, not20);
- floating bit patterns, including signed zero, infinities and a NaN payload;
- exact count, integer SUM, Boolean and variance fields;
- malformed/version/type/scale/truncated frames and DISTINCT refusal;
- complete workspace admission rollback and operation with zero available pool capacity;
- original EOF/ENOSPC error identity and reservation cleanup;
- no publication of an earlier valid slot when a later slot is invalid, followed
  by successful reuse after write/read/validation failure.

The IO tests use bounded in-memory cursors and an injected failing writer, not
real spill files. A workspace compile attempt used the wrong slice helper name;
that failed log is retained separately. The final compact implementation no
longer needs that helper.

All jobs are terminal. The final command was:

```bash
TMPDIR="$PWD/.scratch" RAYON_NUM_THREADS=4 SAFE_BUILD_MEM=48G SAFE_BUILD_JOBS=1 scripts/claude-safe-build.sh cargo test --locked --features lance,gpu --lib physical::morsel_agg::
```

Formatting passes. No existing production execution path or dependency changed;
the parent module only declares the new private codec module. The full library,
integration, GPU, cap and performance suites were not rerun for this unconnected
component. The last cursor-source gate remains799 library passes/10 ignores plus
39 integration passes, with two end-to-end spill-transition failures. Those are
historical results for that source, not fresh validation of every current path.

[The evidence archive](benchmarks/2026-09-07-aggregate-state-codec/) preserves
the module, parent patch, logs, status and640 source-input hashes. Its10 members
were verified against `SHA256.json`, hash
`865aab3e7dc03bb2472c369b00ff3f0bb833624c04a09115d4d7087778b4ae85`.

Next implement reservation-owned selected-value and group-key storage, with exact
type identity and no unreserved String/Vec cloning on merge. Compose those with
this fixed fragment into a complete bound state row, then implement bounded file
flush/merge and connect the existing ingestion cursor. Preserve the original
[implementation and acceptance contract](../.claude/epics/realistic-benchmarks-duckdb-leadership/updates/2026-09-07-fused-spill-implementation.md);
codec-only tests do not close the spill transition or the parent goal.
