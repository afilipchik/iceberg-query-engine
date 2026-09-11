# Streaming scan copied-output capability audit

Read-only source audit, 2026-09-06. No main edits, tests, builds or engines.
Measurements are pending; this establishes contract possibilities, not attribution
of the observed raw Q14 regression. Both streaming leaves are outside the current
resident-only capability. Supporting a fixed-width leaf alone does not establish
that every leaf of that plan qualifies (projected strings remain a blocker).

## Current paths and guarantees

`src/physical/operators/streaming_parquet_scan.rs:80` stores a schema, projection,
row-group work, batch size, optional static/runtime filters, optional dictionary
coercion and per-file IPC paths. It does not own/use a query MemoryPool. Execution
at :332 pulls synchronously inside an async unfold: open/decode/filter can run
until one output is ready. It may traverse many fully rejected row groups in one
poll. Decoder scratch, caches and this cancellation work interval are separate
from the copied queue-output budget.

- **Raw reader**, :527–647: the reader receives `with_batch_size(batch_size)`
  and one row group, optional RowFilter predicates, then projection. :404 wraps
  each output through `wrap_batch`. Parquet 58.4.0's
  `arrow/arrow_reader/mod.rs:1555` promises at most batch_size rows per output;
  :1373–1494 implements the row selection variants. This is a row bound, not a
  general retained-buffer byte bound.
- **Static predicates**, :228–248 and :561–590: subqueries are declined for scan
  pushdown. Evaluation calls the ordinary evaluator, not an execution context or
  same-query pool. Runtime filters at :597–626 read immutable bitmap/hash-set
  payloads and build Boolean masks; slot locks are briefly held when cloning
  the payload. These are not same-pool reservation waits. Arbitrary expression
  scratch and runtime-filter source ownership remain outside queue admission.
- **IPC**, :269 and :463–524: default 8192-row scans may take a sidecar path;
  enforced/custom row targets skip this route. `ipc_read_work` (:766–858) first
  reads a whole row group into a Vec, applies static/runtime filters, then calls
  `reslice_large(...,16384,8192)`. A 16,383-row survivor batch is not resliced;
  larger batches become zero-copy slices. Therefore neither an 8192-row promise
  nor a compact-buffer promise holds for this route.
- **IPC ownership**, `src/storage/ipc_cache.rs:433–466,524–539`: a mmap is wrapped
  as a custom Buffer with the full declared mapping length. Dictionary children,
  Utf8 value buffers and nullable slice bitmaps can retain larger exposed buffers
  than their logical slice. The current universal queue copy recursively copies
  those exposed extents, not only selected logical rows. Never infer custom-owner
  identity from capacity==0; the actual Arrow implementation returns declared
  custom size. Actual-array traversal could bound an already loaded immutable
  IPC row group, but doing that before each pull would be a different admission
  and retained-source lifecycle, not a schema-only streaming guarantee.
- **Output wrapping**, :865–899: dictionary columns declared Utf8 are decoded
  after filtering, so output bytes can grow with string length and repetition.
  Equal-schema and mismatched-column-count early returns currently preserve the
  original batch/schema metadata. A schema-only charge must not silently assume
  these metadata allocations equal the plan schema's retained capacities.

## What Arrow's source supports

Inspected installed primary implementation under
`~/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/` (Arrow/Parquet 58.4.0).

Top-level primitive output is a plausible small supported domain. Parquet
`arrow/array_reader/primitive_array.rs:155–211` consumes the value Vec and builds
ScalarBuffer at offset zero with the actual value length; Boolean uses a packed
buffer. `arrow/record_reader/mod.rs:180–213` takes the current record data and
null mask; `definition_levels.rs:94–108` finishes/reset the bitmap builder.
Byte-array decimal conversion in `array_reader/byte_array.rs:119–155` produces
fixed-width Decimal128/256 arrays. A nullable bitmap still needs its own rounded
byte bound; nonnullable schema alone is not a license to omit unexpected physical
validity metadata from the layout assertion.

Do not bound **decoder scratch** by batch_size: selection.rs:782–814 builds a
mask chunk until batch_size *selected* rows, potentially reading many more rows;
arrow_reader/mod.rs:1423–1434 then filters that chunk. Arrow-select
`filter.rs:424` returns a slice when all rows pass; partial primitive selection
uses `filter_native` (:642–685), and null masks use `filter_bits` (:570–621).
All-selected chunks contain at most the output row quantum; partial results are
compacted for primitive values/validity. This is useful output-layout evidence,
not a bound on pages, scratch or upstream transient arrays.

Utf8/Binary lengths cannot be bounded by row count. Parquet
`arrow/buffer/offset_buffer.rs:133–146` transfers the accumulated offsets/values;
one row can have a very large value. Dictionary readers preserve a dictionary
across data pages (`array_reader/byte_array_dictionary.rs:65–75,169ff`), so its
child may greatly exceed selected rows. Views, nested arrays, dictionaries and
variable-width types need a separate guaranteed byte protocol or exact pinned
content metadata, not compressed file size, average width, NDV or sampled lengths.

## Smallest defensible extension

1. Introduce an accurately named **pool-independent copied-output bound** trait
   capability, shared by resident and streaming inputs. Retain the resident API
   compatibility if useful, but do not advertise decoded streaming as resident.
   Its promise is exactly `owned_input_batch_charge(output) <= bound` for every
   successful output, plus no nested same-pool dependency during pulling. It
   explicitly excludes decoder/expression scratch, provider residency and RSS.
2. Initially enable only an audited raw-Parquet top-level fixed-width subset,
   no IPC shortcut and no encoded/nested/variable output. Use the established
   decoder row quantum, checked per-buffer 16-byte copy rounding, validity
   allowance and shared column/schema/header accounting. The existing
   `input_queue_fixed_width_row_limit` is documented as a scheduling model;
   do not silently promote it to a proof without the per-type reader audit and
   retained-extent tests. Start with Int32/Int64/Float32/Float64/Boolean and
   separately verified Decimal128/256 conversions; decline unaudited widths,
   coercions and wrappers until tested.
3. Normalize emitted schema metadata to the exact declared schema and explicitly
   reject type/column mismatches for this capability. Prove compact value and
   validity extents on every success path. If a decoder path cannot establish
   that shape, decline capability or implement a separately pre-admitted compact
   queue-copy layout; a row count check is insufficient. Keep actual queue charge
   validation before copy as a defensive provider-contract check.
4. Propagate only through the already audited no-subquery Filter and column-only
   Project contracts. Use the same exact column resolution and schema-capacity
   handling already required by the resident capability. Do not change SQL or
   specialize by query/table identity.
5. Reuse fixed pre-admitted K*bound envelopes and serial fallback for unsupported
   layouts or insufficient headroom. Keep no-byte-budget-wait behavior. Merely
   making primitive scans parallel does not solve string-bearing streaming leaves;
   measure coverage before extending the contract further.

## Required tests before enabling

- Actual queue-copy charges for multiple row groups/batches and empty output;
  nullable primitives, Decimal128/256, Boolean bit lengths not divisible by eight,
  all/none/partial static and runtime filters, sparse selection masks with source
  spans much larger than the output quantum, and final short batches.
- Projected schema aliases/capacities and physical mismatches; multiple partitions
  all consumed; actual output <= declared bound and queue peak <= envelope/root.
- IPC available/enabled must decline or take an explicitly selected raw path;
  cover 16,383 rows and larger resliced nullable/string/dictionary batches.
- Reject string/dictionary/view/nested capability even with a tiny logical batch;
  include one huge string and large dictionary with one surviving key.
- Deterministic overlapping upstream polls under pre-admitted slots, low-budget
  fallback, provider-bound violation before copying, and cancellation ownership.

This is a feasible restricted contract extension, not a present guarantee from
StreamingParquetScanExec and not full query-memory ownership. No performance
claim or implementation is made by this audit.
