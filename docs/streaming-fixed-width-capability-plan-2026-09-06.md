# Fixed-width streaming gather/copy capability

2026-09-06 implementation plan, not implemented or performance validated.
Reviewed docs/streaming-input-ownership-audit-2026-09-06.md and current source;
source-sha256.txt records the three principal source versions. This is a proposed
general fixed-width path, not a query-ID optimization or measured performance fix.

## Decision

Use a checked, zero-copy **exposed-buffer normalization** boundary on eligible
raw-Parquet outputs. This is narrower and more auditable than relying on every
decoder/filter kernel to produce compact extents. It needs no full payload copy
before the queue's existing admitted owned copy. No output row slicing/remainder
queue is needed: the raw reader already limits output rows to configured batch_size.
If it violates that contract, return a named contract error rather than retaining
an unreserved remainder or returning a larger batch under a false bound.

Initially admit Int32, Int64, UInt32, UInt64, Float32, Float64, Date32, Date64,
Decimal128 and Decimal256, Boolean and Null. These are flat fixed-width layouts;
NULL validity is permitted regardless of declared nullability when sizing. Other
primitive types may be added only after exercising their exact Arrow layout;
Utf8/Binary, dictionary, views, FixedSizeList, Struct, List and all other nested
or encoded arrays decline. A string used only in a scan predicate can remain
outside the emitted projection; its decoder/evaluator scratch is explicitly not
covered by an output guarantee. No string maximum is inferred from schema width.

## Stable eligibility and provider routing

At construction, after projection schema is resolved, compute an optional
FixedWidthOutputLayout { schema, max_rows=batch_size, widths }. It must have a
checked finite bound. Advertise it only when ipc_dirs is empty. That preserves
existing successful IPC routing: scans with an available sidecar simply decline,
rather than silently bypass it when a metadata getter is called. Existing
enforce_batch_size/custom batch size already disable IPC at construction; those
raw paths can qualify. No environment mutation or execution-time cache switch.

This permits raw-Parquet streaming without claiming row-group IPC batches obey
8192 rows or compact extents. An explicit planner-selected Parquet-only scan may
be considered separately if needed, recording that provider boundary change.
Never mutate ipc_dirs or active streams as a side effect of requesting a bound.

## Normalizer, before every eligible emitted raw batch

Add private normalize_fixed_width_output(batch, &layout) -> Result<RecordBatch>.
Run after wrap_batch, then reconstruct with the EXACT layout.schema even when
value-equal original schema metadata has different String capacities. Require
column count, exact physical type, no children, expected buffer count and rows
<=layout.max_rows. No dictionary cast or other physical coercion in this helper.

For each Arrow ArrayData:

1. Numeric/date/decimal values: checked start=data.offset()*width and
   len=rows*width. Check start+len<=values_buffer.len before calling
   slice_with_length(start,len). Build equivalent ArrayData with offset zero.
   Preserve the exact DataType (decimal precision/scale, date type). Standard
   Arrow primitive to_data typically already has offset zero and sliced values;
   handle nonzero legal ArrayData offsets too, rather than assume them away.
2. Boolean values: Arrow BooleanArray->ArrayData preserves values.bit_offset
   as data.offset. Use byte_start=offset/8, bit_offset=offset%8 and
   byte_len=ceil((bit_offset+rows)/8), checked before slicing. Rebuild with
   ArrayData offset=bit_offset. At most seven leading bits remain.
3. Validity: it has its OWN bit offset, independent of data.offset. Read
   NullBuffer.inner().offset(), trim its leading bytes the same way, preserve
   only residual bit offset and exactly rows bits with BooleanBuffer::new,
   then NullBuffer::new. Do not reuse the value offset to move the null bitmap.
4. Null arrays: no value/child buffers, reset irrelevant data offset. Reject
   unexpected physical buffers rather than infer a bound for malformed data.
5. Use safe ArrayDataBuilder::build validation and make_array, no unchecked
   ownership reconstruction. Buffer slicing preserves primitive alignment;
   Arrow build validation remains authoritative and errors unsupported input.

Only exposed buffer *lengths* shrink. Original parent/custom owners may remain
retained in the upstream returned batch until the queue copies it; this is NOT
source-residency admission. The existing queue copies all exposed buffers into
pre-admitted owned allocations, releasing opaque parent ownership at that boundary.
This does not inspect custom-owner capacity or pretend it proves ownership size.

Primary source checked locally: Arrow58.4.0 BooleanArray -> ArrayData at
arrow-array/src/array/boolean_array.rs:745 preserves the values offset; primitive
conversion in primitive_array.rs:1178 uses a ScalarBuffer; immutable.rs:298
implements O(1) Buffer::slice_with_length and panics on out-of-range, hence the
explicit checked range validation above. The earlier audit records Parquet
reader/filter output limits and sparse-mask intermediate allocation behavior.

## Checked metadata constructor

Add GatherCopyBound::from_compact_fixed_width(schema, max_rows) -> Option<Self>,
private or crate-visible, only callable with an enforced normalizer contract.
Start from actual empty-array structural/header/schema charges, then add:

- round16(max_rows*width) for numeric/date/decimal values;
- round16(ceil((max_rows+7)/8)) for Boolean values;
- round16(ceil((max_rows+7)/8)) validity allowance for every column;
- zero value bytes for Null. All additions/multiplications checked.

The extra 7 represents residual leading BIT offset, not seven bytes. Compute
ceil with a further checked +7: (max_rows+14)/8. Schema metadata uses the exact
same Arc passed by normalization. Empty-array structural charges are drawn from
the same owned_input_column_charge/owned_input_schema_charge helpers as queues.
The constructor records GatherColumn::Fixed/Boolean/Null and this compact identity
extent; no O(rows) value scan or large synthetic batch allocation. Existing
gather(n) adds repeated-output payload conservatively and preserves identity reuse.
Do not promote input_queue_fixed_width_row_limit's scheduling estimate to this
guarantee without enforcing the normalizer and unit-test equality against actual
owned_input_batch_charge.

## Pool dependency contract and plumbing

Do not call decoded inputs resident. Add a general
pool_independent_gather_copy_bound() defaulting to resident_gather_copy_bound(),
so existing resident implementations retain their stricter capability without
changes to source residency claims. Streaming overrides with the optional fixed
layout. HashJoin prepared-probe eligibility requests the general method;
no-subquery Filter and column-only Project propagate it through their existing
filtered/projected metadata operations and exact shared resolver.

In the SAME tranche add pool_independent_queue_copy_bound(), defaulting to
resident_queue_copy_bound(). Streaming returns the fixed layout's normalized
identity QueueCopyBound, without constructing a repeated-output gather. Filter
(no subqueries) and column-only Project must override the general queue method
and propagate it through their existing filtered/projected operations and exact
shared resolver. Change stream_merge_input_partitions to request the general
queue getter wherever it currently requests resident_queue_copy_bound. Keep the
prepared-input route and checked actual-copy validation unchanged. The resident
getters remain available for callers requiring the stronger residency condition.

Both general capabilities are required: prepared joins need probe gather metadata,
while direct aggregate and build-side queues need the ordinary copied-output
bound. Implementing only gather would leave those direct raw streaming pipelines
under the existing single-demand serial fallback. Unknown/encoded/IPC inputs still
decline and use the same safe serial fallback; no source or budget bypass.

StreamingParquetScan owns no SharedMemoryPool. Its raw open/read/filter/wrap code
and footer caches do not call query-pool allocation. Static pushed predicates
decline subqueries. Runtime filters clone immutable membership payloads under
short locks and produce masks; no same-pool budget waits. Thus future pulls can
be certified independent of nested query-pool reservations in this narrow path.
Decoder buffers, predicate scratch, cached footers and existing runtime-filter
payload ownership remain outside the queue contract. The normalizer adds metadata
allocation but no pool calls or asynchronous/background work. A later change
introducing pool reservations into these paths must revoke/review this capability.

## Required tests, then controlled measurement

1. Pure normalizer tests: construct actual sliced primitive arrays and independent
   sliced validity bitmaps with offsets 0..15, last-byte partial masks, empty/allNULL,
   Decimal128/256 exact values, dates, Boolean and several widths. Include a small
   slice retaining a huge parent, and equal schemas with different metadata String
   capacities. Assert exact typed values/NULLs, actual extents and
   owned_input_batch_charge <= fixed bound. Repeated take checks gather bounds.
2. Reject oversized emitted row count, physical type/column mismatch, bad checked
   range arithmetic and unsupported dictionary/string/view/nested schemas. Do not
   create malformed unsafe ArrayData merely to test a range guard; directly test
   the checked offset/length helper with overflow and impossible ranges.
3. Small Parquet fixture with multiple row groups, projected decimal/date/Int64,
   nullable columns, configured unusual batch quantum (e.g.17), all/none/partial
   static and runtime filters, sparse selection spans larger than the emitted
   quantum. Drive every declared partition; exact independent typed oracle.
4. IPC sidecar availability: same logical fixed schema must decline this raw
   capability when ipc_dirs is nonempty, with ordinary IPC route unchanged.
   Explicit enforced raw target qualifies and cannot emit an IPC-sized batch.
5. Direct raw streaming->aggregate and raw streaming build-input queue tests must
   request the general QUEUE capability, not the resident getter or prepared-join
   gather path. Verify Filter/Project propagation, unsupported fallback and actual
   multiple admitted pulls with a bounded root reservation peak. The resident-only
   default forwarding must preserve existing resident pipeline behavior.
6. Real aggregate->prepared Inner join->fixed streaming probe: exact duplicates,
   masks/encoding on build, multiple partitions, no pre-pull, root envelope peak,
   source failure/cancellation. Preserve the existing queue actual-charge check.
7. Only after correctness/resource gates, rebuild and rerun alternating original
   raw control/candidate queries plus IPC controls to quantify coverage, metadata
   normalization overhead and parallelism. No performance claim before evidence.

This plan deliberately supplies no variable-width guarantee and does not certify
decoder scratch, source allocations, whole query ownership or RSS. It eliminates
the exposed-extent proof gap for a useful fixed-width emitted domain while keeping
unsupported routes explicit.
