# Admitted Arrow output from complete partial-state groups

`physical/morsel_agg/admitted_output.rs` adds `GroupRows::build_output_range`.
It validates output arity/types against bound keys and aggregate states before
construction, decodes canonical keys under admission, borrows selected values,
finalizes fixed states and builds a caller-selected row range. Numeric, Boolean,
UTF8, list, decimal, date, timestamp and NULL outputs use the bound normalized
types. Integer conversion and decimal final precision are checked; timestamp
unit/zone and list child types/nullability are preserved. Dictionary/view/large
representations remain outside this normalized component's binding contract.

Values and reference scratch vectors have reservations. Variable key decoding
uses ReservedScalar owners; selected strings/lists remain borrowed from states.
Output primitive data, validity, string bytes/offsets and nested list children
use ReservedBufferBuilder. Batch/schema/column metadata and copied data-type
metadata have separate owners retained with the output. Payload buffers retain
both buffer admission and shared array/type/batch owners. OwnedArray delegates
Arrow representation/downcasts and keeps those owners for bufferless NULL and
empty arrays; normal slices retain them, and extracted nonempty ArrayData keeps
buffer owners. The unsafe Array implementation delegates every representation
operation to the same valid inner Arrow array; it changes ownership only.

These reservations cover buffers and conservative construction-owner allowances,
not exact RSS or arbitrary allocations performed later by unrelated Arrow kernels.
Arrow-created clone/slice headers retain Arrow's existing allocation contract.
The caller must bound output ranges and admit any collection retaining batches.
The implementation currently materializes decoded key values per output range
and dispatches by type per row; it has no measured performance improvement.

Unsigned SUM exposed a new-state binding gap: UInt inputs selected floating SUM,
so an exact UInt64 output could not be produced. The preserved regression failed
before the correction. FixedStateCodec now binds unsigned SUM to a full-i128
scale-zero partial coefficient. Updates remain exact, spill/merge preserves the
coefficient, and final UInt64 conversion refuses overflow. The final state can
carry a value larger than UInt64 until output validation. This changes the new
partial-state path; it does not repair or certify the old live fused accumulator.

The controller now creates an untouched global group when input is empty, giving
COUNT0 and NULL AVG. Empty grouped input stays empty. This replaces the previous
controller checkpoint's deferred global-empty handling.

Five new tests cover output lifetime after group/layout drop, slices and ArrayData
extraction; exact large decimals, nullable strings/lists/timestamps; metadata and
buffer cleanup after denial/invalid schema/late unsigned overflow; NULL and selected
signed-zero bits; UInt64 sums above2^53, UInt64::MAX and overflow; and controller
spill/merge-to-Arrow with exact unsigned results plus empty global COUNT/AVG.

Final gate: **109 aggregate component tests passed**, zero failures or ignores;
formatting passes. The first compile failure, intermediate107-pass run and unsigned
before-fix failure are preserved, not counted as final coverage.
[Evidence](benchmarks/2026-09-07-admitted-group-output/) contains 19 verified members and
657 current source-input hashes. Manifest SHA256:
`2f97d994e9a318df471e7b8e0233bab73f8cf70a6b1ecc242e0c92e7e6c69c4c`.

Live planner/worker routing is unchanged. Next bind the controller and output into
the production operator, retain the single-evaluation input contract, admit result
collections and enforce worker working sets. Then rerun the unchanged consuming-source
replay and256KiB live decimal gates. Both remain open and were not rerun here.
No full integration/cap/GPU or benchmark gate ran; no speedup is claimed.
