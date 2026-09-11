# Native scan admission follow-up — 2026-09-11

On frozen candidate `1bba20b3`, the completed targeted native Q6 diagnostic reports
an aggregate-input queue with eight declared partitions and max_slots=8, but
prepared=false, admitted_buffers=false, no declared byte bound, and slots=1.
This is direct runtime evidence of serialized input admission for that workload.
It is not evidence that the engine declares only one partition or omits the others.
The trace is `admitted-coalesce-paired-01/b1-candidate-native/q06.stderr`, archived
with the current cycle after all timing finishes.

`NativeStreamingScanExec` exposes its segment-based partition count but implements
neither prepared-input factory nor the resident copied-output bound. The
`PhysicalOperator` defaults decline those capabilities. Its own source contract
states that footer metadata, dictionaries, deletion vectors, decode allocations and
retained outputs are not yet query-pool admitted. The intervening Project therefore
cannot establish the missing producer contract merely from a fixed-width logical
projection.

The current full SF10 native screen records Q1 and Q6 warmup timeouts; their
measured samples are not run. These failures remain separate from the successful
extended-deadline targeted outputs and from any noisy short-query ratio. No causal
claim that admission alone explains the entire timeout is justified without further
measurement.

A native follow-up must account for metadata, dictionary/deletion state, decoding
scratch and physical output representations, then provide an appropriate prepared
capability before enabling more concurrent owners. Verify the actual workload
boundary without output pre-pulls, then validate runtime slots, all partitions,
SQL outputs, retained-owner lifetime, memory refusal and cancellation. Do not force
queue concurrency or assert a resident bound while file decoding remains outside
its ownership contract. Keep native, raw Parquet and preloaded IPC modes distinct.

The current post-filter accumulator improves a separate admitted raw scan path.
Its batching repair does not certify native admission or native performance.

## Concrete native ownership and projection boundaries

Read-only source audit during the frozen7ddbf9ce screen identifies the following
implementation boundaries. No native implementation or admission claim changed.

| Boundary | Current behavior | Required follow-up |
|---|---|---|
| physical/planner.rs Scan routing | Uses NativeStreamingScanExec only for spill-covered scans whose materializing native scan exceeds its budget; otherwise uses the existing materializing/cache path | Keep both routes and measure which one each workload actually uses |
| native_scan.rs execute | Copies assigned segment IDs/projection; opens one segment per partition; spawn_blocking produces one output per pull | Admit reader/task/queue ownership before advertising prepared concurrency |
| NativeTable::open_segment_batches | Opens IPC reader and clones the segment deletion vector | Retain snapshot identity and account for deletion state before pulls |
| ipc_cache::open_row_group | Maps the file, converts footer schema, constructs FileDecoder, decodes every dictionary block, collects record-batch descriptors | Bound/admit metadata and dictionaries; projection needs a dictionary dependency closure |
| RowGroupReader::next | Validates block extents and invokes FileDecoder once per batch | Cover alignment/decompression allocation and retained mapped buffers, not only emitted Arrow payload |
| DeletionCursor and rewrap_batch | Deletion filtering allocates survivors; rewrap preserves actual dictionary types under logical names | Use admitted selection/output construction and keep physical representation in the ownership proof |

The pinned Arrow IPC58.4.0 implementation confirms that FileDecoder::with_projection
only records the projection. FileDecoder::read_dictionary calls read_dictionary_impl
without that projection, decodes dictionary values and updates its dictionary map.
The record-batch decoder applies projection later. Consequently, the project's loop
over every footer dictionary is not automatically pruned by configuring projection.
This is source evidence of potentially unnecessary dictionary work; its contribution
to the native Q1/Q6 timeouts remains unmeasured.

A dictionary-pruning repair must use actual IPC dictionary IDs referenced by selected
fields, including nested/shared-ID dependencies where supported. Do not assume field
ordinal equals dictionary ID: the writer assigns IDs automatically. Preserve needed
replacement/delta order and validate framing/extents. Test a selected numeric column
with large unused string dictionaries, selected/shared dictionaries, repeated and
reordered projection, nullable values, multiple record batches, and deletion vectors.
Prove which dictionary payloads were decoded in addition to checking exact outputs.

Arrow's default FileDecoder also allocates aligned copies when input buffers are
misaligned; it is not unconditionally zero-copy. A prepared native path must either
admit those copies or reject unsupported alignment before consumption through an
explicit capability rule. Do not disable validation or claim admission merely because
an ordinary well-aligned fixture happens to retain its mmap. All mapped and decoded
output lifetimes must remain covered after reader cancellation/drop.

Current7ddbf9ce native screen fails Q1 in warmup and Q6 on its first measured sample
after a successful warmup. Later dependent requests are not run. This differs from
1bba20b3's Q6 warmup failure and must retain the actual phase in the final report.
