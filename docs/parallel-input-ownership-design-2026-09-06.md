# Bounded parallel input ownership proposal

Design only. No source edits, builds, tests or benchmarks. The measured queue
candidate is not accepted: raw Q9 regressed about26.9%; IPC Q6 about6.96× and Q14
about2.77× versus the immediate control. IPC plans remained MemoryTableScan, which
rules out its reader-cache bypass as an explanation for those specific IPC trials.

## Confirmed mechanism and evidence limits

`stream_merge_input_partitions` currently holds its single demand permit across
upstream try_next, owned copying and send. It serializes upstream Filter/Project
execution across partitions, not just queue insertion. This is source-established.
Its contribution to measured elapsed time still needs separate attribution.

`own_input_data` additionally copies every exposed buffer and all child data.
Arrow58 GenericByteArray::slice slices offsets/nulls but clones the whole value
buffer; its to_data preserves that value buffer. Dictionary children can likewise
be repeated across many batches. The queue can therefore recopy unused string
bytes and complete dictionaries repeatedly. PrimitiveArray::to_data uses its
ScalarBuffer's exposed extent, so the same amplification must not be assumed for
all primitive slices. ArrayDataBuilder::build also runs validate_data; measure
copy/validation costs rather than assigning all regressions to memcpy.

First follow-up targets lost upstream parallelism for auditable resident-leaf
pipelines. Logical compaction/rebased offsets is a separate experiment; do not
combine them and lose attribution.

## Exact contract, not statistics

Proposed PhysicalOperator capability (names illustrative):

```
fn queue_copy_bound(&self) -> Option<QueueCopyBound>;
struct QueueCopyBound {
    max_rows: usize,
    max_owned_copy_bytes: usize,
    layout: GuaranteedOutputLayout,
}
```

The bound guarantees, for EVERY output batch from EVERY declared partition,
that the queue's actual checked copy-charge function returns at most the bound.
It covers rounded copied buffer extents, copied validity buffers, all recursive
child/dictionary buffers, array/batch headers and schema metadata charged by the
current implementation. It describes the current copy representation, not just
logical row bytes. Arithmetic is checked; overflow/unknown returns None.

It does NOT bound decoder scratch, upstream allocation, retained source storage,
Tokio task/channel overhead, allocator RSS or final consumer ownership. Estimates,
NDV, min/max statistics and sampled average string widths cannot implement this
contract. Public/custom operators default to None. Before copying every returned
batch, verify actual charge against its assigned slot bound. A violation is an
explicit operator-contract QueryError, cancels the queue and fails correctness;
it is not an allowlisted memory-pool refusal, silent resize or truncation.

A byte bound alone does not prove that pre-reservation cannot starve upstream.
For this first tranche also require structurally verified eligibility: a resident
MemoryTableExec leaf followed only by the admitted Filter/column-only Project
wrappers, with no subquery, nested join/aggregate/sort, provider read or operator
that requests this pool during pull. Keep that eligibility separate from the
byte bound. Default everything else to the existing serial-demand path. Do not
reserve a full envelope above arbitrary reservation-dependent subtrees.

## Bounded capability implementations

### Resident MemoryTableExec

Cache the guarantee once on immutable actual batches, after effective projection
and logical-schema wrapping. Calculate maximum row count and actual queue copy
charge by metadata traversal of every batch, including buffer extents and nested
children. No payload sampling/hashing. Store the result on the operator/shared
immutable batch owner; avoid recomputing on every partition or query pull.

Use a thread-safe one-time cache if initialization is lazy. All zero partitions
and zero-row batches are included correctly. Unsupported layouts or arithmetic
failure decline the capability. A configured row maximum alone is insufficient
when a string slice pins a wider exposed value buffer.

This does not retroactively make eager Parquet-produced MemoryTable batches
provider-owned or budgeted. Existing query materialization ownership remains open.
The guarantee only bounds copies emitted from the already-existing resident leaf.

### Filter

Start with a deliberately narrow guarantee, declining subquery predicates and
unknown expression/operator behavior. Output rows cannot exceed child max_rows,
but rows alone do not establish a byte bound. For each admitted physical layout:

- Fixed-width primitives/Boolean: bound output values by child max_rows and
  physical width/bitmap rounding; allow validity even when input schema claims
  nonnullable. Include ArrayData/array/header changes and allocation rounding
  used by the queue's exact copy representation.
- Utf8/Binary: do not use output row count times an average width. Arrow filtering
  may compact values OR retain an input slice/fast-path buffer. Bound exposed
  value bytes by the child's guaranteed maximum exposed value extent and bound
  offsets by maximum output rows+1 (or the larger child exposed extent if any
  admitted fast path retains it). Include null bitmap allowance. If implementation
  audit cannot prove both cases, return None for this first tranche.
- Dictionary: key rows may shrink but dictionary values may remain complete.
  Preserve the FULL guaranteed dictionary-child copied charge; never scale it
  by selected-row fraction. Include logical NULLs in values as well as key NULLs.
  Decline unknown keys/nested types until a recursive guarantee is implemented.
- Selection indexes/masks are upstream temporary work, not queued output. Audit
  their lifecycle to ensure none is retained by output arrays without inclusion
  in the bound; explicitly do not count this contract as admission for scratch.

Use the maximum of preserved-input and compact-output representations plus the
schema/header allowance. A generic arrow filter kernel is not itself a proof;
validate the admitted type-specific branches and future encoding changes.

### Column-only Project

Admit only pure column selection/permutation, aliases and the established schema
rewrap. No CAST, arithmetic, functions, CASE or volatile expressions initially.
Derive bounds from selected child column bounds; duplicate selected columns count
repeatedly because universal copying duplicates them. Recompute the actual output
schema's field names/metadata/header charge, including alias growth. Do not assume
projection always reduces bytes. Return None if output physical encoding/schema
cannot be bounded from the child contract.

Fixed-width computed projections can be a later extension with separately proven
row count, physical result type, NULL bitmap and error semantics.

## Fixed pre-admitted slot envelope

For eligible resident pipelines, choose a small requested concurrency bounded by
declared partitions and existing configured thread count. Let B be the guaranteed
maximum queue-copy charge per batch and K the slot count; check K*B arithmetic.
Attempt to reserve the COMPLETE envelope through a named child of the existing
query pool BEFORE spawning/polling those producers. No independent root, cap raise
or per-batch estimates. Do not reserve every available byte opportunistically.

If the requested envelope cannot be admitted, reduce K deterministically within
the requested range, without pulling any batch. If even a useful envelope cannot
be admitted, use the existing serial-demand path with its immediate actual-byte
admission and named refusal behavior. This fallback preserves the current queue
ownership guarantee; it does not claim pre-reservation of upstream allocations.
Do not wait on byte-budget availability.

Own the envelope reservation in an Arc lifetime shared by queue state, active
producers and queued entries. A semaphore with K permits limits live slots. Acquire
a slot before upstream try_next. The fixed reservation covers every slot, even
while idle; no duplicate per-batch reservation. On Ready, check actual charge<=B,
copy and enqueue while retaining the slot. Release the slot only after that copy
leaves queue ownership. The envelope stays reserved until all queue/producer
references disappear. Error/drop drains queued entries, aborts producers, and
keeps reservation alive through asynchronous cancellation cleanup.

This provides parallel upstream pulls only inside a pre-admitted copied-output
envelope; it does not admit the upstream scratch itself. Eligibility restriction
avoids immediate nested-pool starvation in this tranche. Multiple independent
queues still compete through real parent reservations; failed envelope acquisition
falls back safely. A general nested pipeline needs transferable upstream/output
ownership credits, beyond this bounded experiment.

Keep current task panic/completion handling, zero-partition behavior, and direct
one-partition path. No custom-owner capacity detection: capacity does not identify
arbitrary custom owners, and the actual Arrow implementation differs from its
stale capacity-zero doc comment.

## Deterministic acceptance tests

1. Two eligible producers each notify when inside upstream poll and wait at a
   barrier. Prove both enter before release (K>=2), avoiding timing assertions.
   A bounded test timeout catches accidental serialization/deadlock.
2. Hold all K results without consumption. Assert one fixed K*B reservation,
   parent peak<=limit, no (K+1)th poll, and no allocation outside admitted slots.
   This assertion concerns copied queue payload, not upstream allocation/RSS.
3. Low-budget path where one batch fits but two do not: verify fallback completes
   all partitions/duplicates and existing exact spill regressions still spill.
   Preserve every existing test budget and expected result.
4. Lying bound: provider returns a larger batch. Verify refusal BEFORE owned copy,
   no silent growth, terminal contract error and cancellation of other producers.
5. Cancel while producers are blocked upstream and while output slots are occupied.
   Await deterministic drop acknowledgements; reservation remains live until
   cancellation actually destroys all retained copies.
6. Fixed-width/null/empty filter outputs, sliced Utf8 retaining a large parent,
   dictionary with unused/null values, and duplicate/renamed projection columns:
   compare every actual emitted charge against the guaranteed bound plus exact
   independently expected values. Unsupported wrappers decline the capability.
7. Hold a sibling query reservation to deny the envelope. Verify pre-poll fallback
   or genuine named refusal, with no byte-budget wait and no leaked reservation.

After correctness/cap gates, run the same alternating raw/IPC controls and full
public/provider gates. Capture per-queue copied bytes, emitted rows, upstream-poll
concurrency and copy/validation time in a diagnostic run first. Report this as a
general pipeline change; no query-ID thresholds or benchmark-specific branches.
