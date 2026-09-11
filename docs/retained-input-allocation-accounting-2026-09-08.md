# Retained input allocation accounting — September 8, 2026

Native IPC columns share a single mmap allocation per file. Arrow58 Buffer
capacity retains that allocation's full extent after slicing. Summing
get_array_memory_size across primitive columns therefore charges the same backing
allocation repeatedly. The generic aggregate did this when admitting a received
batch. This differs from spill costing: logical slice bytes can estimate copy or
serialized size, but cannot account for a retained source mapping.

New execution::retained_batch computes batch-local retained bytes for recognized
primitive layouts (integer/float, decimal, temporal). It counts each exact
(base pointer, capacity) pair once, adds array metadata for every column, and
counts values and validity buffers together. Different custom extents at the same
base are conservatively separate. It retains full capacities even for a one-row
slice. Unknown layouts retain the previous conservative per-array accounting.
Bookkeeping uses ReservedVec under the query pool, including its owner allowance;
it is released before final input admission. No source buffers are copied or
reclassified as already admitted. The aggregate still retains its input lease
through evaluation and ingestion and can release/spill state before retrying an
admission refusal.

This is a local accounting correction, not a complete ownership registry or an
RSS estimate. Separate batches can still conservatively charge the same backing
allocation again. Source queues, decoder scratch, schemas and unrecognized
layouts are outside the new deduplication contract. Buffer construction by the
legacy native source remains outside query-wide admitted construction.

Four tests validate shared slices retaining the full backing capacity, independent
allocations, shared validity, ledger denial/release, conservative unknown strings,
and an actual mmap-backed IPC file with exact typed values. The real IPC test
expects file length plus two array metadata objects, even for a one-row slice.
The initial focused build used a nonexistent ReservedVec.push method; corrected
to its reservation-aware extension API before testing. That build error is
preserved and is not an engine regression result.

Validation (locked/offline lance,gpu, Rayon4, one build job,48GiB wrapper,
repository TMPDIR): four focused tests pass; full library992 pass/10 existing
ignores; selected spill/transition/input-error/binding integrations15 pass with
zero skips. Formatting and whitespace pass. No heavy job remains active.

The unchanged native256KiB suite still has six passes and four failures. Two
aggregate cases now advance beyond the prior326676-byte input request and fail
on an80512-byte request with200895/206015bytes already held. Source inspection
shows COUNT(*) constructs an admitted Int64 presence array for the full10000-row
batch; 10000*8+512 matches this request. This is the next bounded-expression
hypothesis, not proof that every remaining allocation has that source. The join
still requests383984bytes atused0; deletion still fails requesting4096 atused259398.
No native completion or performance gate is claimed to pass.

Next: bound evaluated-expression batches while keeping the complete retained
source charge and permit alive. Pre-admit slice/evaluation metadata, handle empty
inputs, duplicate/NULL groups and exact Decimal states, and preserve cancellation
and spill transitions. Do not replace retained capacities with logical slice
lengths. Measure any batching change on protected workloads: smaller expressions
can reduce peak memory while increasing dispatch overhead.

Evidence: `docs/benchmarks/2026-09-08-retained-input-accounting/`, including source
snapshot/hashes, all logs, native failures and this report. No new release was
built; all previous performance binaries predate this source.
