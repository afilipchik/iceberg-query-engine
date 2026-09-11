# Query-budget batching cliff in variable-width Parquet scans

The resource matrix reproduced a roughly120× slowdown that predates the bounded
parallel aggregate input change. The physical planner converts variable-width
Parquet scans to one-row reader batches when an estimated whole-table size
exceeds the query budget beneath a spill-capable consumer. This couples table
size to the decoding quantum even though the query streams its input. A one-row
batch is also not a byte bound: one string or a retained decoder page can exceed
the budget.

## Reproduction and isolation

Frozen candidate SHA256:
`b03f53d332c7d464070d9aac0ebb3cb060bf26a2a71ae0786c6ef3f5fe4fb999`.
Its282 Rust/Cargo source hashes remain unchanged. The control and candidate both
use features `lance,gpu`, CPU execution,16 threads, affinity0–15 and a32GiB cgroup
with swap disabled. Dataset and SQL hashes verify before/after each diagnostic.
These are diagnostic measurements, not accepted DuckDB timing samples.

The sequential six-query matrix covers1/4/16GiB query budgets and4/12/24GiB
process caps. Of72 requests across36 processes,70 complete with typed-correct
outputs. Both binaries finish the first1GiB Q12 request in about158.6s, then hit
the180s process watchdog during the second request. All other requests finish.
There are no cgroup OOM/max events or spontaneous process crashes. Each killed
Q12 process leaves four temporary files; all normally exited cases leave none.
SIGKILL does not exercise graceful cancellation cleanup. Timing-parent termination
also makes final RSS unavailable for the two timed-out processes.

The candidate's independent limit-isolation experiment completes both requests:

| Query pool | Process cap | Q12 elapsed | Build input rows | Build input batches | Typed oracle |
|---|---|---:|---:|---:|---|
|1GiB|12GiB|159,057.926ms|310,803|310,803|Pass|
|4GiB|4GiB|1,366.636ms|310,803|458|Pass|

These traced runs distinguish the query-budget trigger from the process hard
cap. Aggregate ingestion remains about45–48ms, with115 aggregate input batches
and zero aggregate spilled bytes in both cases. Inner probe output and final
answers match. Plan text is identical: it currently omits reader batch size.
The build initialization counter measures its own retained-input work, not all
upstream decoding; do not attribute the entire159s to its187.9ms state counter.

Commands are preserved in the diagnostic drivers, executed through
`scripts/claude-safe-build.sh` with repository TMPDIR. Resource matrix handle36181
and isolation handle6280 are both terminal0. A successful diagnostic driver exit
does not turn its recorded timeouts into query passes.

Evidence:

- [Resource driver](../.scratch/parallel-aggregate-input/run_resources.py),
  [36-process summary](../.scratch/parallel-aggregate-input/sf10-resource-matrix-02/resource-summary.json).
- [Independent limit driver](../.scratch/parallel-aggregate-input/isolate_q12_limits.py),
  [typed results and limits](../.scratch/parallel-aggregate-input/q12-limit-isolation/summary.json).
- [Candidate archive](benchmarks/2026-09-08-parallel-aggregate-input/manifest.json)
  preserves drivers, traces, configurations, hashes and outcomes.

## Source mechanism and upstream comparison

`src/physical/planner.rs::parquet_scan_batch_policy` asks the fixed-width layout
helper for a row limit. Variable-width output is unsupported. If the whole-table
estimate exceeds the query budget under a spill-covered scan, the fallback is
`1`; otherwise it retains the requested quantum. `lower_scan` passes this value
to `StreamingParquetScanExec`, which gives it to the Parquet reader. The existing
`unsupported_output_uses_one_row_only_under_covered_memory_pressure` unit test
explicitly preserves this policy. This is a general schema/budget problem,
not a query-specific optimizer omission.

The local DuckDB commit `1c27c54f27bea397d18d86f7c0d6a9f1d4aa85f8`
[Parquet reader](/media/afilipchik/nvme6tb/src/duckdb/duckdb/extension/parquet/parquet_reader.cpp:1396)
limits the scan count by `STANDARD_VECTOR_SIZE` and remaining row-group rows.
The local ClickHouse commit `a1b25f3f4beb3ba49aa3b73671cc244185331b86`
[adaptive chunk calculation](/media/afilipchik/nvme6tb/src/clickhouse/ClickHouse/src/Processors/Formats/Impl/ParquetBlockInputFormat.cpp:748)
uses projected columns' uncompressed metadata bytes to estimate rows for a
preferred block byte size, clamped between128 and its configured maximum.
These January source snapshots are distinct from the benchmark DuckDB wheel.
Neither snippet proves strict memory safety. In particular, an average row
size is a scheduling estimate, never a bound on one oversized string.

## Implementation sequence for the next model

1. Add diagnostic reader-quantum and scan-route reporting to physical plans or
   opt-in traces. Include projected schema, pressure reason and actual decoder
   batches/rows. Keep ordinary per-row timers off. A plan should reveal a
   resource-dependent execution change without reverse engineering source.
2. Separate three decisions: whether a whole table may be materialized, the
   preferred vector quantum, and whether the next decoder/output allocation is
   admitted. A large table should trigger streaming, not inherently scalar
   decoding. Preserve refusal for unsupported materializing consumers.
3. Establish a variable-width decoder contract before changing the fallback.
   Account for compressed input, decompression/page buffers, dictionaries,
   offsets/validity, retained row groups and copied output separately. Determine
   which Arrow/Parquet allocations can be admitted before construction. If the
   current decoder cannot enforce the required bound, isolate or replace that
   component rather than treating a row count as a byte guarantee.
4. Give output batches a byte target and a preferred vector row count. Retain
   unfinished input and output cursors across capacity boundaries without replay.
   An oversized value must obtain explicit admission or return a named refusal.
   Metadata and observed average lengths may tune the target; neither proves
   maximum decoded size. Keep unknown copied-output capability explicit until
   the resulting buffers and decoder dependencies satisfy it.
5. Replace the old one-row policy test with contract tests only when step3 is
   implemented. Cover long/skewed strings, NULLs, dictionary changes, sliced
   buffers, large pages, invalid extents, source errors after output, multiple
   partitions, consumer drop and genuine low-budget refusal/spill. Verify input
   is consumed once and owned reservations release. Include a multi-batch
   variable-width fixture whose full-table estimate crosses the budget while
   each admitted vector fits; demonstrate batched decoding and exact results.
6. Freeze the new implementation. Repeat the two independent limit cases and
   the36-process matrix, then the matched raw/native/Iceberg/Lance and separate
   IPC/GPU gates. Run balanced comparisons with tracing disabled. Add concurrent
   load before accepting the higher-memory parallel candidate as resource-safe.

Do not replace the fallback with an arbitrary128/8192-row constant or special-case
Q12. That would address the symptom while leaving variable-width allocation
unbounded. This checkpoint changes evidence and the next implementation priority;
the production scan policy is still unchanged and the bug remains open.

## First implementation checkpoint: observable scan policy

`PhysicalOperator::execution_details` is a default-empty diagnostic hook;
`display_plan` appends it without changing stable operator names. Streaming
Parquet reports its reader quantum, planner pressure flag, projected physical
types, partition count, Parquet/possible-IPC route and fixed/unknown copied-output
capability. The hook does not execute or initialize input. IPC is explicitly
qualified because its sidecar batches need not follow the Parquet reader quantum.

The new nullable-string regression checks the displayed policy against actual
execution: the same39 rows yield39 batches at quantum1 and3 batches at quantum17.
NULLs and duplicate values match independently constructed expected values.
The focused test passes; full library gate83759 completed with897 passed,
zero failures and10 previously documented ignores. Formatting and whitespace
checks pass. This diagnostic
change does not repair scalar decoding or certify byte admission. It also means
current source is newer than the frozen benchmark binary; earlier results remain
at their recorded source hashes.

The pinned `parquet-58.4.0` source exposes no general query-allocation callback in
`ReaderProperties` or `ArrowReaderOptions`. `with_max_predicate_cache_size` bounds
a particular cache, not all decoding. Concrete allocation boundaries inspected:

- `src/file/serialized_reader.rs` constructs a `Vec` from the page header's
  uncompressed size, decompresses into it, then wraps it in `Bytes`. Reserving
  only compressed input cannot cover this allocation or its retained lifetime.
- `src/arrow/array_reader/byte_array.rs` reserves output offsets and estimates
  value-buffer capacity before reading strings; dictionary decoding separately
  reserves expanded output bytes. A page allowance alone cannot cover decoded
  Arrow output, dictionary expansion or simultaneous old/new capacities.

The next implementation must either provide admission at these independent
allocation/ownership boundaries or use a decoder with that interface. Do not
advertise an outer `ChunkReader` wrapper as comprehensive decoding admission.
This source audit concerns the pinned dependency, not a claim about newer releases.

## Admitted PLAIN UTF8 output component

`storage/admitted_plain_utf8.rs` now implements a resumable decoder for flat,
already decompressed PLAIN BYTE_ARRAY values with optional decoded validity.
It validates all encoded lengths, exact trailing extent and UTF8 without output
allocation. Each output chunk measures its exact payload before allocating
admitted offsets, values and validity through `ReservedBufferBuilder`. The row
limit and preferred value-byte target control chunking; every actual buffer
still requires admission. One value may exceed the preferred target if its full
buffer charge fits; a value beyond Arrow's i32 UTF8 offset domain refuses.

The cursor advances only after successful Arrow construction. A failed
reservation releases provisional buffers and retains the original encoded/row
positions for retry. Output clones and slices retain reservation owners. The
input page and already decoded definition levels remain caller-owned; this
component does not admit those allocations, page decompression, dictionary ID
expansion or nested repetition. It is not yet connected to production scan
routing. The scalar fallback therefore remains unchanged, and no performance
or complete decoder-safety claim follows from these tests.

Four focused tests pass with no skips (handle6468, exit0): byte-target chunking
with sliced validity, NULLs/duplicates/empty strings/Unicode; refusal after a
consumed prefix followed by exact retry and output-slice ownership; malformed
lengths/UTF8/counts plus empty/all-NULL input; and actual writer-produced PLAIN
Parquet pages across several row groups. The latter obtains decompressed pages
through the ordinary library reader, outside the output component's admission
contract, then checks exact independently constructed strings. The initial
compile attempt used the wrong MemoryPool constructor in tests; the corrected
run and both logs are preserved. No dependency changed. Existing full-library
897/10 evidence predates this additional module; this checkpoint's new coverage
is the four focused tests, not an invented new full-suite run.

Next: give compressed/decompressed page storage admitted ownership before
connecting this component to scan routing; preserve encoded cursors and page
owners across output handoff. Decode definition levels and dictionary expansion
under their own measured/admitted extents. Do not infer coverage of these
remaining allocations from the admitted StringArray buffers.

## Admitted decoded page bodies

`storage/admitted_page_body.rs` now constructs decompressed page buffers through
`ReservedBufferBuilder`. It supports UNCOMPRESSED and SNAPPY, verifies actual
Snappy decoded length against the declared page extent before allocation, and
decodes into a fixed destination slice. The pinned `snap-1.1.1` slice decoder
uses stack state and borrowed source/destination slices; its allocating
`decompress_vec` convenience function is used only to prepare test inputs.
Other codecs return explicit NotImplemented errors in this component.

The API accepts a validated uncompressed prefix extent for V2 definition and
repetition bytes, preserving those bytes while decoding the value section. It
handles a level-only page with an absent value section, including all-NULL
payloads. The caller must resolve V2 is_compressed=false to UNCOMPRESSED.
Declared prefix/value extents are checked before admission; a malformed payload
after admission drops the provisional buffer and releases its charge. Returned
Buffer clones and slices retain the page reservation.

`snap = "=1.1.1"` is now a direct dependency, reusing the version already pinned
transitively by Parquet. The lockfile changes only the root dependency list;
no package version changed or network download was needed. The combined page
and PLAIN output gate passes9 tests, zero skips (75621 terminal0), using the
memory wrapper and `--locked --offline --features lance,gpu`. Tests cover exact
prefix decoding, invalid sizes before admission, bad compressed payload cleanup,
low-budget refusal, empty/level-only pages, unsupported codecs, slice ownership,
and simultaneous independent page/output charges. Formatting/whitespace pass.

This is still an unrouted component. Compressed input ownership and page-header
parsing are caller responsibilities, and definition/dictionary decoding remains
open. Before integration, add admitted reads for validated encoded extents and
a header parser whose own reads/allocations are bounded. Connecting ordinary
unbounded header/decompression readers would undo this contract. Neither these
component tests nor prior benchmark results certify the unfinished scan path.

## Admitted encoded reads

`storage/admitted_page_read.rs` adds positional encoded-range reads. It checks
overflow and the source extent, admits the complete destination before payload
I/O, and fills the fixed buffer with short-read and Interrupted handling. A
partial I/O failure or EOF releases all provisional storage. The source trait
exposes no shared seek cursor; the Unix File implementation uses `read_at`.
Other platform adapters have not been added or certified.

The integration test writes a Snappy PLAIN body to a file, then composes admitted
encoded reading, admitted decompression and admitted StringArray construction.
It checks exact strings and independent retained charges, with zero pool use
after final output release. This fixture is an encoded page body with a known
offset, not a full production Parquet reader. Other tests check denial before
any payload read, overflow/out-of-source ranges, interrupted/short reads,
slice-held ownership, partial I/O failure and truncation after the extent check.
The source can still change between header/body reads; immutable snapshot or
integrity validation remains the integrating reader's responsibility.

Bounded page-header parsing remains next. Merely capping a Thrift input stream
does not by itself prevent allocations from a forged binary/list length. The
header path must bound work, nesting and allocations before trusting page sizes,
validate the compressed extent against its column chunk, and retain the page
cursor on admission denial. It must distinguish malformed metadata from an
explicit unsupported capability. No one-row fallback or production routing
changes in this checkpoint.

Combined component gate62164 is terminal0:14 passed, zero skips, with the48GiB
wrapper, Rayon4, one build job and `--locked --offline --features lance,gpu`.
Formatting and whitespace checks pass. The earlier13-test gate also passed;
the final run adds the source-truncation case. Logs and source are preserved in
`docs/benchmarks/2026-09-08-admitted-page-read/`. No dependency changes in this
read component and no fresh SF10 performance claim.

## Bounded header envelopes and CRC

`storage/bounded_page_header.rs` now parses a Compact Protocol header envelope
without allocating nested metadata. It checks core i32 field types, duplicate
core/subheader fields, signed page sizes, varint/extent overflow and the combined
header/body extent against the column chunk. Unknown fields are skipped with
bounded byte work and nesting depth32; binary and collection lengths do not
allocate storage. Byte/nesting limits return explicit unsupported errors.
Truncated/malformed fields return storage errors. The deterministic malformed
corpus exercises4,352 short inputs for termination/panic detection; this is not
exhaustive fuzzing or a proof of full format conformance.

`AdmittedPageHeader` owns a pre-admitted header window and exposes its envelope
immutably. Subheader ranges borrow that window. Encoded bodies use the admitted
positional reader; a present CRC32 is verified over encoded bytes before returning
the body. Corruption releases the body charge while preserving the caller's
header owner. CRC absence does not prove snapshot integrity.

This layer validates the envelope, not all typed page semantics. Page-kind
requirements, dictionary/V1/V2 value counts, level lengths and encodings still
need typed validation before routing to a decoder. A real-file regression
compares actual writer-produced SNAPPY headers and decoded bytes with the
library page reader across multiple row groups, then checks exact PLAIN strings.
It obtains the expected value count from the oracle reader; this is deliberately
not presented as a complete independent typed page reader.

The next integration step is typed subheader validation and an owned page cursor:
validate page-kind fields, preserve encoded position on admission denial, and
advance only after a complete validated page handoff. Definition levels and
dictionary IDs must use admitted buffers before production scan integration.

Header gate3273 is terminal0:7 passed, zero skips, with the48GiB wrapper,
Rayon4/one build job and `--locked --offline --features lance,gpu`. Formatting
and whitespace checks pass. Earlier4/5/6-test development logs and the final
seven-test gate are preserved in `docs/benchmarks/2026-09-08-bounded-page-header/`.
No dependency or production scan routing changed in this checkpoint.

## Typed headers and owned column-page cursor

`AdmittedPageHeader::typed` now validates required V1/dictionary/V2 fields,
negative counts, duplicate/wrongly typed fields, conflicting page-kind
subheaders, V1 level encodings, V2 null/row counts and level extents. Missing V2
is_compressed means true; false requires equal encoded/decoded extents and
selects UNCOMPRESSED decoding regardless of the column codec. Unknown page or
encoding capabilities refuse explicitly. Validation does not yet decode levels
or dictionary IDs, prove nested-row structure, or certify encoding/type compatibility
for every column schema.

`storage/admitted_column_pages.rs` owns a positional source and column extent.
Each handoff requires a valid typed header, bounded extents/value counts, admitted
encoded input, valid CRC when present and admitted page decompression. Only then
does the cursor commit byte position and cumulative data-value count. Dictionary
pages may occur once before data. Final value count must match the declared
column count. Metadata counts are checked as format contracts, not statistical
proofs of SQL uniqueness or equivalence.

Admission refusal releases temporary buffers and preserves the cursor for retry;
I/O/corruption/unsupported-format errors poison it. Returned pages retain their
decoded-buffer owners after the cursor is dropped. Retrying an uncommitted page
may repeat positional reads; it never replays an already returned page. The source
must still supply snapshot consistency—CRC absence does not supply that guarantee.

Focused gates pass9 header tests and4 cursor tests without skips. Cases include
V2 compression/level/count boundaries, dictionary ordering, malformed headers,
refusal after a consumed prefix, refusal after encoded reads but before decoded
buffer allocation, CRC/count failures and owner release. The actual-file test now
uses our parsed value count to drive PLAIN decoding and also exercises the owned
cursor; the library page reader only supplies the oracle. An initial cursor-test
compile error required deriving Debug for test failure reporting; its corrected
run and original log are retained.

Production routing is unchanged. Next add admitted definition-level and dictionary
ID decoding, then assemble aligned column batches with retained per-column cursors.
Only after that path covers its actual allocations should the planner replace
the scalar fallback and the independent1/12GiB versus4/4GiB measurements rerun.

Full library gate80733 is terminal0:924 passed, zero failures,10 existing
documented ignores, in29.70s after compilation. Command uses the48GiB wrapper,
Rayon4/one build job, `--locked --offline --features lance,gpu --lib` and four
test threads. Formatting/whitespace pass. Evidence is preserved in
`docs/benchmarks/2026-09-08-typed-column-pages/`; no new benchmark binary or
SF10 performance result was produced by this checkpoint.

## Admitted hybrid level and ID chunks

`storage/admitted_hybrid.rs` now decodes RLE/bit-packed hybrid values into
pre-admitted UInt32 chunks. It supports widths0–32, checked run/count/byte extents,
repeated values and packed groups crossing output boundaries. Domain checks use
the caller's exact level maximum or validated dictionary size, not estimates.
Final packed padding is excluded from output. A truncated final group is accepted
only when the remaining bytes contain exactly all declared value bits; missing
actual value bits refuse. Legacy BIT_PACKED level encoding and non-spec trailing
padding are not implemented by this component.

Output admission precedes cursor work. Memory refusal preserves the consumed
prefix; malformed runs/domain violations poison the decoder while provisional
output is released. Successful chunks commit a copied working cursor, and output
slices retain their reservations. Page ownership remains external to this borrowed
decoder, as in the PLAIN output component.

Five focused tests pass, zero skips (58699 terminal0), using the48GiB wrapper,
Rayon4/one build job and `--locked --offline --features lance,gpu`. They cover
mixed/chunk-crossing runs, packed padding, width0/32, refusal after a consumed
prefix, slice ownership and malformed/domain errors. A real nullable dictionary
Parquet fixture reconstructs257 exact strings/NULLs across three row groups
using our page cursor, PLAIN dictionary decoder and hybrid level/ID chunks.
The fixture's expected values are constructed independently before writing.

The fixture assembles its oracle-comparison vectors with ordinary test allocations;
it is not production column assembly or complete scan-memory certification.
Admitted NullBuffer construction, dictionary-to-output expansion, retained current
page state and aligned column batches remain before routing. No dependency or
production scan policy changed, and no new SF10 gain is claimed. Evidence is in
`docs/benchmarks/2026-09-08-admitted-hybrid/`; formatting/whitespace pass.

## Retained decoder owners and admitted dictionary output

The next component checkpoint adds `admitted_dictionary_utf8.rs`, flat optional
NULL-buffer construction in the hybrid decoder, and owned input variants for
hybrid and PLAIN decoding. These are retained-state building blocks, not a
production scan-policy change.

Dictionary expansion checks the exact dictionary domain for every ID, rejects
NULL dictionary entries/dense IDs and mismatched non-NULL row counts, and retains
shallow clones of dictionary, ID and validity arrays. Each output admits its exact
UTF8 values and offset buffers before copying. NULL slices retain their admitted
mask owner. Chunk targets bound preferred payload; an oversized single value
still requires real buffer admission. Refusal leaves the committed row/ID prefix
unchanged. Duplicate values/IDs, Unicode, empty dictionaries with all-NULL rows,
sliced inputs and oversized values are covered.

Hybrid cursors can now retain a Buffer owner instead of borrowing a page. The
working cursor clone shares that owner; it never copies the page payload. The
PLAIN cursor also accepts an owned page and retains a validity handle. Tests drop
caller handles between output chunks and verify both exact results and eventual
pool release. Flat optional definitions produce pre-admitted packed NULL buffers
without an intermediate ordinary UInt32 allocation.

The real nullable dictionary Parquet fixture now uses admitted definitions, IDs
and expanded string chunks for257 independently expected rows across three row
groups. Only the final test-oracle collection uses ordinary vectors. This does
not cover nested columns or aligned production column batches.

Validation: `storage::admitted_` passes29 tests, zero failures/ignores
(session58315, `owned-decoder-tests-02.log`), using the48GiB containment wrapper,
Rayon4, one build job, `--locked --offline --features lance,gpu`. Earlier gates
in this checkpoint passed3 dictionary tests,27 composed admitted tests, and28
after hybrid ownership. Formatting and whitespace checks pass. Logs and exact
source provenance are archived separately under
`docs/benchmarks/2026-09-08-retained-decoders/`; the prior benchmark binary and
its source archive are unchanged.

Next: compose retained flat column state, add fixed-width decoding and aligned
batch assembly, then route supported scans through admitted decoding. Only after
that should the independent-limit Q12 reproducer and matched SF10 gates run.
The scalar fallback remains; no new SF10 performance or full memory-safety claim
is supported by this component checkpoint.

Full library gate17456 is terminal0:935 passed,0 failed,10 existing ignored,
29.53s, using the same contained feature/environment settings and four test
threads. Ignored coverage comprises eight dedicated CUDA tests, one dedicated
IPC-sidecar process test and one existing dependent-join test. These are not
successful hardware/provider coverage. Log: `owned-decoder-library.log`.


## Retained flat UTF8 column composition

`admitted_utf8_column.rs` now owns the page cursor, pending page, decoded dictionary
and current output decoder. It supports required/nullable PLAIN and dictionary
strings across V1/V2 pages. Flat V2 row counts, repetition extent and decoded NULL
counts are checked; legacy BIT_PACKED levels and delta string encodings refuse.
The caller must verify the flat UTF8 physical/logical schema before construction.

Page handoff and output handoff have separate commit points. If downstream mask,
ID or dictionary admission fails after page read, the pending page remains owned;
retry reconstructs only provisional decode state and does not reread that page.
If an output allocation fails after earlier chunks, its cursor remains at the
first unreturned row. Other errors poison the column. All input/output owners
release their pool charges when the reader and returned arrays are dropped.

The new real-file matrix covers12 combinations of V1/V2, dictionary enabled or
disabled, and required/mixed-NULL/all-NULL values. It writes257 independently
expected rows in three row groups with small page limits, then reads three-row
chunks through a source read counter. It asserts exact values/order, multiple
pages, denial both immediately after page handoff and after an output prefix,
no reread on these denials, terminal behavior and complete pool release.

Initial compile errors were corrected. The first executing fixture exposed that
V2 defaults to DELTA_BYTE_ARRAY without dictionaries; the reader returned its
explicit unsupported error. Supported-path fixtures now specify PLAIN. A separate
actual delta-file regression checks explicit refusal, subsequent poison behavior,
no source replay and owner release. Failed logs are preserved, not replaced.

This is column composition, not aligned multi-column batch assembly or production
routing. It still admits whole decoded pages, definitions and dense IDs and may
refuse oversized dictionaries/pages. It does not establish complete provider
metadata accounting or change the measured scalar fallback. Fixed-width decoding
and aligned batches remain next, followed by routing and the independent-limit
Q12 reproducer and matched SF10 validation.

Column checkpoint validation: full library81145 is terminal0,937 passed,0 failed,
10 existing ignored,29.68s. The ignored cases remain eight dedicated CUDA tests,
one dedicated IPC-sidecar test and one dependent-join test; no hardware coverage
is implied. All31 admitted tests pass separately (24387). Commands use
`TMPDIR="$PWD/.scratch" RAYON_NUM_THREADS=4 SAFE_BUILD_MEM=48G SAFE_BUILD_JOBS=1
scripts/claude-safe-build.sh cargo test --locked --offline --features lance,gpu`,
with `--lib storage::admitted_ -- --nocapture` or `--lib -- --test-threads=4`.
Formatting/whitespace pass. Source snapshots, hashes and all attempted test logs
are preserved in `docs/benchmarks/2026-09-08-admitted-utf8-column/`.


## Shared fixed-width and string column state

The retained column module is now `admitted_flat_column.rs`; strings and fixed
values share the page ownership, definition decoding, pending conversion and
poison/retry state machine. `admitted_plain_fixed.rs` provides PLAIN integer,
Date32, timestamp, FLOAT/DOUBLE, BOOLEAN and Decimal128 output. It validates exact
encoded extents, uses explicit little endian numeric reads and big endian signed
decimal extension, admits aligned typed buffers before fill, and checks each
emitted decimal coefficient against its declared precision. Float bit patterns
are preserved. It performs no logical-annotation inference or unit rescaling.

Numeric dictionary output retains the raw decoded dictionary page and dense ID
array, checks every ID against the actual dictionary size, and gathers directly
into admitted output. NULL rows consume no dense ID. Retry after a memory denial
preserves both row and dense-value positions. Hard value errors poison the
cursor; temporary output admission is released.

The actual-file matrix constructs eight independent nullable Arrow columns:
Int64, UInt32, Date32, wide Decimal128, narrow Decimal128, Float64, Boolean and
UTC microsecond timestamps. It writes V1/V2 with dictionaries on/off, small pages
and three row groups. Every three-row output chunk is compared as typed Arrow
data with the independent input slice. The previous12-case UTF8 matrix and
unsupported delta/no-replay test remain active in the shared module.

Focused tests also cover unaligned input bytes, i64 extrema, signed38-digit
coefficients, short signed decimal widths, precision overflow after a returned
prefix, negative decimal scale metadata, NaN payload/signed-zero/infinity bits,
BOOLEAN bit boundaries with NULLs, exact lengths, unsupported type pairs, empty
and all-NULL values, numeric dictionary duplicates/NULL IDs/domain rejection and
memory refusal/retry with retained output slices.

Initial compile logs include an ambiguous empty Buffer fixture; the first
executing fixed-value test found a BOOLEAN fixture inferred as Vec<i32> instead
of bytes. Both fixture issues were corrected, and failed logs remain preserved.
The combined admitted gate1035 then passed36 tests without skips. This does not
change production scan routing: aligned multi-column batch assembly, supported
schema/encoding selection and metadata/resource gates remain before replacing
the one-row fallback. No new SF10 result is claimed.

Fixed-column full library gate27567 is terminal0:943 passed,0 failed,10 existing
ignored,29.39s. It includes the additional numeric dictionary prefix/refusal test.
The10 ignores remain eight dedicated CUDA tests, one dedicated IPC-sidecar test
and one dependent-join test, not successful provider/hardware coverage. Commands:
`TMPDIR="$PWD/.scratch" RAYON_NUM_THREADS=4 SAFE_BUILD_MEM=48G SAFE_BUILD_JOBS=1
scripts/claude-safe-build.sh cargo test --locked --offline --features lance,gpu
--lib -- --test-threads=4`. Formatting and whitespace checks pass. Evidence and
source provenance: `docs/benchmarks/2026-09-08-admitted-fixed-column/`.


## Aligned admitted batch assembly

`admitted_batch.rs` now retains each column's chunk and row offset while driving
other columns. A source refusal preserves already-produced columns; a handoff
refusal preserves all pending rows. Successful batches contain a shared row range
across columns, limited by the shortest pending chunk. The assembler validates
schema types, required-field NULLs and declared row count and poisons hard errors
without retrying consumed sources. Zero-column projections preserve row count.

Source/pending vectors use ReservedVec. Handoff reserves the output column vector
and4096 bytes per flat column for Arrow headers, buffer-owner wrappers and small
buffer vectors before reconstruction. These metadata reservations are attached
to output buffers with shared owners, so typed clones and extracted ArrayData
retain admission even after the reader and batch are dropped. Actual decoded
payloads have separate leases. This conservative metadata allowance does not
certify exact RSS or caller-owned schema/footer memory.

New tests cover mismatched chunk boundaries, a second-column refusal after the
first was consumed, handoff refusal, exact NULL/value alignment, no repeated
source calls, early/excess row counts, terminal behavior, zero-column projection,
provider type and required-NULL errors, and reservation retention/release after
typed clone and ArrayData extraction. The real Parquet matrix now has nine typed
columns including variable UTF8. A five-byte string target forces different
column chunk boundaries; assembled V1/V2 and PLAIN/dictionary batches match the
independent typed input at every row across three row groups.

The initial compile caught a missing Array trait import in the owner test. The
corrected admitted suite23577 passed40 tests without skips before the additional
provider-type/NULL test. Logs remain preserved. No benchmark binary changed.

Routing inspection: the live entry point is physical
`StreamingParquetScanExec::execute`, whose unfold combines cached Parquet metadata,
static/runtime predicate pushdown, dictionary coercion and IPC sidecar routing.
It is distinct from `storage::StreamingParquetReader`, and currently has no
query-pool field. Integration must carry the query pool through the live planner/
scan path, retain provider metadata accounting, preserve predicate/projection
semantics and select supported flat schema/encoding cases before consumption.
Simply modifying the public storage reader would not fix the measured Q12 path.
The scalar fallback remains unchanged; production routing and measured Q12/SF10
gates are still required.

Batch checkpoint validation: full library92639 is terminal0:947 passed,0 failed,
10 existing ignored,29.56s. Ignored cases remain eight dedicated CUDA tests,
one dedicated IPC-sidecar test and one dependent-join test. Commands use
`TMPDIR="$PWD/.scratch" RAYON_NUM_THREADS=4 SAFE_BUILD_MEM=48G SAFE_BUILD_JOBS=1
scripts/claude-safe-build.sh cargo test --locked --offline --features lance,gpu`,
with `--lib storage::admitted_ -- --nocapture` for the40-test intermediate gate or
`--lib -- --test-threads=4` for the final library gate including the added type/
required-NULL test. Formatting and whitespace pass. Source/evidence archive:
`docs/benchmarks/2026-09-08-admitted-batch/`. No new SF10 performance claim.


## Validated row-group adapter

`admitted_row_group.rs` now constructs aligned readers from caller-owned parsed
metadata and an explicit output projection/schema. It checks row-group/physical
column counts, selected flat root structure, Arrow output types and Parquet
annotations before constructing page readers. Decimal precision/scale and timestamp
units/UTC-adjustment must match. Supported integer signedness, Date32 and UTF8
annotations are checked; unsupported conversion is an explicit capability error.
Only UNCOMPRESSED/SNAPPY and supported PLAIN/dictionary/level encoding metadata
pass preflight. A later malformed/unsupported page still poisons rather than
switching readers after consumed input.

The actual-file matrix now also reads through this adapter with all nine columns
and with reordered/repeated projection `[8,0,3,8]`. Every returned typed value and
schema matches the independent input. A separate annotation regression rejects
microsecond/millisecond, timezone-presence and decimal-scale/precision changes.
The delta fixture verifies capability refusal before page extent construction by
supplying an empty source after parsing real delta-file metadata; it gets the
capability error with zero pool charge, not a file-extent error.

The first compile corrected an API assumption: pinned Parquet58 returns an
encoding iterator directly. The admitted suite76733 then passed42 tests with no
skips. Footer parsing, metadata-cache ownership/snapshot consistency, live query-
pool wiring and admitted predicate/gather output remain outside this adapter.
The production path and scalar fallback are unchanged. No new SF10 gain is claimed.

Row-group checkpoint validation: library62226 is terminal0,948 passed,0 failed,
10 existing ignored,30.03s. The ignores remain eight dedicated CUDA tests, one
IPC-sidecar test and one dependent-join test, not successful hardware/provider
coverage. Focused suite76733 passed42 with no skips before the final preflight
assertion. Both use `TMPDIR="$PWD/.scratch" RAYON_NUM_THREADS=4 SAFE_BUILD_MEM=48G
SAFE_BUILD_JOBS=1 scripts/claude-safe-build.sh cargo test --locked --offline
--features lance,gpu`, selecting `--lib storage::admitted_ -- --nocapture` or
`--lib -- --test-threads=4`. Formatting/whitespace pass. Evidence is preserved in
`docs/benchmarks/2026-09-08-admitted-row-group/`. No new benchmark binary or SF10
result was produced.


## Admitted survivor gather

`admitted_gather.rs` now performs flat typed survivor copying with actual buffer
admission. The caller supplies a Boolean WHERE mask or dense UInt32 row IDs.
Masks must match row count and retain only valid true entries. IDs must be
non-NULL and within the exact input domain; duplicates/reordering are preserved.
The gather allocates exact typed values, validity bitmaps and UTF8 values/offsets
through ReservedBufferBuilder, preserving decimal coefficients, timestamp/type
metadata, float bits and NULL positions. It never emits one batch per survivor
run. Unsupported types refuse explicitly.

`admitted_batch::finish` reserves/retains flat handoff metadata on output buffers.
Provisional buffers are released if a later column or final metadata admission
fails; inputs remain immutable for retry. Typed array clones and ArrayData keep
the output leases after the input/batch is dropped. The helper does not admit
predicate expression evaluation or take ownership of external input schemas.

Three focused tests24354 pass with no skips, comparing duplicate/reordered typed
gather and SQL NULL masks against independent Arrow take/filter kernels. Coverage
includes sliced inputs, i64 extremes, signed wide decimals, dates, booleans,
Unicode/empty/long strings, NaN payload/signed-zero bits, empty/all-NULL masks,
invalid IDs/mask lengths and denial after earlier columns were constructed.

The actual V1/V2 dictionary/plain Parquet matrix now composes admitted gather
after nine-column aligned decoding. Survivor results are compared against row
selections from the independent original arrays, including NULL predicate masks
and empty outputs. Production routing is unchanged; admitted predicate masks,
live query-pool plumbing and retained provider metadata accounting remain before
Q12/SF10 measurement. No new performance result is claimed.

Gather checkpoint validation: library45937 is terminal0,951 passed,0 failed,
10 existing ignored,29.45s. The ignores remain eight dedicated CUDA tests,
one IPC-sidecar test and one dependent-join test. A subsequent test-only assertion
checks empty fixed-width typed-clone/ArrayData metadata retention; focused30367
passes3 tests with zero skips. Production source did not change after the library
gate. Commands use `TMPDIR="$PWD/.scratch" RAYON_NUM_THREADS=4 SAFE_BUILD_MEM=48G
SAFE_BUILD_JOBS=1 scripts/claude-safe-build.sh cargo test --locked --offline
--features lance,gpu`, with `--lib -- --test-threads=4` or
`--lib storage::admitted_gather -- --nocapture`. Formatting/whitespace pass.
Evidence: `docs/benchmarks/2026-09-08-admitted-gather/`. No new SF10 result.


## Admitted compiled numeric masks and SQL validity

`CompiledPredicate::evaluate_admitted` reuses the current chunked numeric register
program and comparison semantics. Column-reference storage, numeric/mask register
slabs and returned values/NULL bitmaps obtain query-pool admission before their
allocations. Denial returns a typed error, not a signal to run an unaccounted
fallback. Runtime schema/type incompatibility is separately `Ok(None)`.
Compilation and retained program/schema metadata are caller-owned contracts.

The admitted path adds per-register validity for nullable arithmetic, comparison,
AND/OR/NOT. false AND unknown is valid false; true OR unknown is valid true;
otherwise validity propagates according to SQL three-valued logic. Numeric values
reuse the existing evaluator, including its SQL float comparator. The legacy
public evaluator remains unchanged and still declines nullable Boolean programs.

The existing4000-row predicate matrix now checks admitted results against the
interpreter for nullable/nonnullable numeric, arithmetic and date predicates.
New tests cover admission refusal/cleanup, sliced3073-row input across register
and bitmap boundaries, output ownership through slices/ArrayData, empty input,
schema drift and explicit nine-case AND/OR/NOT truth tables with exact survivor
IDs. The real nine-column Parquet fixture now evaluates an admitted nullable
integer/date OR predicate, checks its mask against independent input values and
gathers survivors against the independent typed original arrays.

The first compile placed new tests outside the helper module; they were moved
into the existing module. Gates42269 and19408 each pass5 focused tests with no
skips, before and after adding per-register validity. Full library77670 passes
954 tests,0 failures,10 existing ignored,29.56s. Those ignores remain eight
dedicated CUDA tests, one dedicated IPC-sidecar test and one dependent-join test.
Post-gate changes only clarify source comments/docs. All runs use the48GiB capped
wrapper, Rayon4/one build job, repository TMPDIR and locked/offline lance,gpu
features; full library uses four test threads. Formatting/whitespace pass.
Evidence: `docs/benchmarks/2026-09-08-admitted-compiled-predicate/`.

This closes numeric mask evaluation, not general predicate support or production
routing. Strings/IN/LIKE and other unsupported compiler shapes remain explicit
capability gaps. Live query-pool wiring, metadata ownership/admission and supported
scan selection still precede the Q12/SF10 performance gate. No new gain is claimed.


## Admitted string predicate capability

The separate `compile_for_admitted_evaluation` entry point now enables UTF8
comparison operands, constant string IN/NOT IN and constant-pattern LIKE/NOT LIKE.
It leaves ordinary production compiler capability selection unchanged. The legacy
mask API declines extended instructions; the admitted API alone handles their
validity. Compiled program/literal allocations are still caller-owned metadata,
not covered by evaluator scratch admission.

String comparisons use borrowed UTF8 values. IN compares the actual literal list,
including duplicates; it does not infer semantic uniqueness from estimates.
A matching row is valid, an unmatched row with any NULL list member is unknown,
and a NULL input is unknown. NOT IN flips only the value, retaining validity.
NULL-list instructions trigger validity storage even when input arrays contain
no NULLs. Mixed numeric/string AND/OR/NOT reuse the admitted validity program.
LIKE reuses the existing allocation-free classifier/greedy Unicode matcher and
does not allocate strings per row. Unsupported mixed/dynamic IN and dynamic LIKE
patterns remain explicit compile declines.

Focused gates57723/1570 pass8/9 tests respectively, zero skips. Coverage includes
all six string comparisons (literal and column RHS), Unicode and decomposed
Unicode, nullable sliced2049-row inputs spanning chunks, duplicate literals,
IN/NOT IN with/without NULL members, NULL-free input plus NULL list, dominating
Boolean branches, memory refusal and exact survivor IDs. LIKE tests check prefix,
suffix, contains, wildcard `_` Unicode character width, empty/all patterns and
ordered substring matching with NULL/negation semantics. Masks are compared with
the interpreter and explicit independent membership/pattern expectations.

The real V1/V2 dictionary/plain Parquet fixture now composes integer/date logic,
string IN and LIKE through admitted decode/mask/gather, comparing every mask and
survivor with independent input values. Full library15375 passes957,0 failures,
10 existing ignored,29.79s. Ignores remain eight dedicated CUDA tests, one dedicated
IPC-sidecar test and one dependent-join test. All commands use repository TMPDIR,
Rayon4/one build job,48GiB safe-build scope and locked/offline lance,gpu features;
full library uses four test threads. Formatting/whitespace pass.
Evidence: `docs/benchmarks/2026-09-08-admitted-string-predicate/`.

Live scan selection and query-pool wiring, program/footer/cache admission and
snapshot ownership remain before the Q12/SF10 performance gate. Other unsupported
expression families remain capabilities to handle explicitly. No new performance
result is claimed.


## Compilation ownership and allocation-free qualified resolution

`compile_reserved` now performs a bounded borrowed preflight before recursive
compilation clones expressions, names or literals. It admits a construction
allowance derived from node/type storage, fourfold literal/name bytes, small-owner
allowances and bounded recursion scratch. Supported compilation is capped at
64 depth and4096 nodes; these are capability limits, not SQL validity claims.
Unsupported/oversized shapes decline before input execution. Memory denial returns
an error, never an instruction to retry through an unaccounted compiler.

On success, admission shrinks to actual retained Vec/String capacities and owner
allowances, then lives in CompiledPredicate until drop. Temporary construction
space is released rather than retained for the whole query. Returned mask buffers
keep their own reservations independently after the program is dropped. Input
AST/schema ownership remains external; this is not an exact RSS certification.
The mixed Parquet decode/mask/gather fixture now uses reserved compilation and a
1MiB component pool to include compilation construction space explicitly.

Preflight review found a shared allocation in `resolve_arrow_column`: matching
legacy `relation.name` formatted a String. Borrowed prefix/separator comparisons
now preserve its exact behavior without allocating. Annotated namespaces,
ambiguous names and dotted-component safeguards remain. This change applies to
the shared resolver, while live admitted scan selection is still not enabled.

Focused47358 passes11 compiler tests, zero skips. New coverage checks refusal
before construction, depth/node decline without admission, unsuccessful typed
compilation cleanup, retained program charge, construction-to-retained shrink,
evaluation denial without losing the program, and independent program/mask drop.
Qualified-identity integration98795 passes12 tests with no skips. Full library
71312 passes959,0 failures,10 existing ignored,29.96s. The ignores remain eight
dedicated CUDA tests, one IPC-sidecar test and one dependent-join test. Full gate
includes reserved compilation in the real Parquet fixture and final reservation
shrink behavior. All commands use repository TMPDIR, Rayon4/one build job,
48GiB safe-build scope, locked/offline lance,gpu features; full library uses four
test threads. Formatting/whitespace pass. Evidence:
`docs/benchmarks/2026-09-08-reserved-compilation/`.

Next: footer/cache ownership and live query-pool/scan routing, preserving predicate,
projection/runtime-filter and IPC contracts, followed by Q12/SF10 measurement.
The construction bound is conservative, and broader query-wide accounting remains
open. No new performance gain is claimed.


## Metadata identity prerequisite

A separate production cache bug was reproduced and fixed while preparing scan
integration: mtime-preserving replacement returned stale row counts (2 vs5), and
builders reopened the path after metadata acquisition. The cache now fingerprints
the opened file, retains schema-key owners, removes obsolete version variants and
builds from the same handle. Same-size replacement and pinned old readers are
covered. Full library91595 passes962 with10 existing ignores;32 selected
integrations35500 pass without skips. No new benchmark result. Cache budgeting,
footer admission and immutable-content policy remain distinct unresolved
contracts. See [the cache identity report](metadata-cache-identity-2026-09-08.md).
