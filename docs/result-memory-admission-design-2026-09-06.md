# Projection/result admission and ownership: implementation design

Status: the optimized query-budget violation is reproduced. Five ownership
feasibility tests pass; production admission and collection remain unfixed.
This design preserves the full goal: refuse/spill before exceeding working
budgets, with charges retained as long as engine allocations remain alive.

## Proven lifetime mechanism

The existing bytes dependency provides safe `Bytes::from_owner`. An owner can
hold an immutable Arrow Buffer followed by an Arc<MemoryReservation>. Converting
these bytes to an Arrow Buffer preserves the pointer and transfers lifetime
ownership without application unsafe code or data copying. Recursive ArrayData
reconstruction must include value buffers, validity with its independent bit
offset, and children (including dictionary values). Every buffer shares the
same lease; escaping any one conservatively retains its full admitted charge.

`tests/result_buffer_ownership.rs` verifies typed slicing and extracted buffers,
nullable strings, nested child arrays, a thread-crossing drop, parent-pool
accounting, failed admission, and retained capacity. The initial slice-pointer
assertion failed because Arrow correctly advances the buffer pointer by one
Int64; the corrected test verifies that exact eight-byte offset. Both logs are
preserved. Five final tests pass with no skips.

This mechanism cannot establish admission by itself. It must receive an existing
reservation obtained before creating the allocation. Existing aliases made
before ownership transfer cannot escape untracked. Shared resident input and
fresh computed output require distinct ownership provenance.

## Constraints that prevent a superficial fix

- A custom owner can retain 8 KiB while the wrapped Arrow buffer reports only
  16 bytes of capacity. The verified test demonstrates this. Preserve the
  admitted retained extent explicitly; never reconstruct it from the view.
- ArrayData/RecordBatch/owner allocations, vectors and empty or bufferless arrays
  still need metadata ownership. A lease attached only to data buffers cannot
  account for metadata that has no buffer. Keep a separate batch/result metadata
  owner and define how API moves/clones transfer those obligations.
- Arrow conversion to mutable buffers can allocate a copy. Engine code doing so
  needs new admission; a reservation on the original immutable buffer cannot
  cover an unbounded new copy. Caller-created copies are a separate allocation
  domain after the API boundary, while retained engine buffers stay charged.
- Existing queue envelopes certify pool-independent pulls. Introducing nested
  reservations inside ProjectExec invalidates that proof for computed output.
  Such paths must use Unknown/serial admission until the queue contract supports
  shared leases; otherwise the same pool can deadlock or be charged twice.

## Ordered production work

1. Define owned output metadata containing retained charge and allocation
   provenance alongside the batch. Data-buffer leases survive Arrow escapes;
   metadata guards cover engine-owned batch/array headers and bufferless outputs.
   Keep the public API's lifetime semantics explicit. Do not silently claim that
   a guard on QueryResult follows arrays moved from public `batches`.
2. Build a budget-aware expression evaluator. Derive checked bounds for fixed
   output plus every live intermediate. For variable output use length discovery
   or a bounded builder with reserve-before-growth. Cover string casts/functions,
   nested values, dictionary expansion and validity; unsupported shapes must
   refuse by name, not silently call an unaccounted evaluator. Avoid a permissive
   fallback that would preserve the current violation.
3. Thread the shared query pool through projection selection and all evaluation
   entry points. Audit filter/CASE/compiled/subquery/aggregate expressions, not
   only the two SQL statements from the reproducer. Attach leases to freshly
   owned outputs before they leave their producing scope; preserve resident
   borrowing and existing reservations without releasing either early.
4. Replace unbounded collected-result retention with explicit ownership-aware
   collection. Account for concurrent partition outputs and dictionary decode
   scratch/output before expansion. Spill where the API supports it, otherwise
   refuse by name. Keep decoding inside metrics timing and memory snapshots.
5. Validate cancellation, errors, partial collection, nested output, zero rows,
   multiple partitions/batches, escaped slices and concurrent queries under one
   parent pool. The small 64 KiB reproducer must either complete inside the
   working contract or refuse cleanly; correct values alone are insufficient.
6. Run the SQL contracts, mandatory cap gates and then matched canonical provider
   screens against the preserved 612-file binary. Keep GPU admission separate:
   host leases do not impose a device-memory cap.

The prototype is deliberately confined to tests until allocation and metadata
contracts are integrated. No production path is newly certified by these tests.
See [the reproducible budget failure](result-memory-boundary-findings-2026-09-06.md)
and [verified artifacts](benchmarks/2026-09-06-result-memory-boundary/members-sha256.json).

## Implemented construction primitive

`execution::ReservedBufferBuilder<T>` now admits typed Vec payload capacity
before allocation, uses fallible allocation, and transfers the final allocation
to an Arrow Buffer with a safe Bytes owner retaining its reservation. It exposes
only slices, preventing callers from growing the underlying Vec without admission.
Geometric growth reserves old plus new storage before allocating a replacement;
the old allocation is dropped before its charge is released. Failed admission
leaves values, address, usage and high-water counters unchanged. Sibling query
pools share the parent limit.

A 512-byte per-buffer envelope covers the pinned implementations' owner/header
structures; it is not an allocator/RSS bound, nor accounting for ArrayData, schema,
collection vectors or expression intermediates. Opaque wrapped capacity can be
smaller than retained capacity; the original admitted lease stays live independently.
New kernels must preserve that lease and must not advertise pool-independent pulls.

The new builder tests plus prior ownership and memory hierarchy contracts pass
**20 tests, zero skips** under the 48 GiB wrapper. Production SQL kernels and result
collection do not yet call this primitive. The original 64 KiB query reproducer
is therefore still unfixed; no new performance claim is made. Next integration
must cover budget-aware expression construction and collection metadata/decoding,
not merely attach a charge after an existing Arrow kernel allocates.

Arrow 58.4's optional `MemoryPool::reserve` is infallible and may overfill a pool.
Its claim operation is post-allocation and replaces an existing claim. It was
reviewed locally and is not enabled as an admission substitute. No dependency
or feature changes are required for the new primitive.
