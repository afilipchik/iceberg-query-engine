# Native admitted reader: allocation contract before parallel input

The incremental native iterator removes whole-segment survivor queues. It must
not simply be wrapped in PreparedAdmittedInput: the underlying Arrow decoder and
selection path still allocate outside the query pool. This design records the
next source-level requirements while optimized incremental-reader measurement runs.
It is not an implemented or certified capability.

## Verified dependency behavior

Reviewed the locally installed, locked arrow-ipc58.4.0 `src/reader.rs`, SHA256
`d27d205e98d2158515f6d2ceab7059b38142441fba3059e0b750e27009890ff7`.
The dependency source is under the Cargo registry, not vendored into this project.
Cargo.lock pins the package; no dependency modification is proposed.

- FileDecoder defaults require_alignment=false. Its documented behavior copies
  misaligned input into new aligned allocations. A selected admitted route must
  validate alignment or use with_require_alignment(true), preserving ordinary
  fallback only before source selection. Never disable Arrow validation.
- RecordBatchDecoder::try_new reads optional IPC compression and creates a
  decompression context. Mmap does not make compressed payloads zero-copy. An
  admitted capability must reserve decompression before allocation, or decline
  unsupported compressed layouts during metadata preflight before output.
- update_dictionaries concatenates old/new arrays for delta dictionaries. The
  source ownership claim must cover that allocation and its lifetime, or decline
  deltas during preflight. Ordinary dictionary decoding also owns a HashMap and
  array metadata; zero-copy dictionary values do not eliminate that metadata.
- read_record_batch collects variadic buffer counts, builds arrays, and under a
  projection constructs another schema and column vector. Flat supported types
  should use a closed validated layout; nested/variadic layouts must not slip
  through based only on their final logical field name.

The current native reader additionally copies its selected segment's deletion
vector as an immutable snapshot and collects footer block descriptors. Both are
explicit metadata costs; one-batch output does not bound them by output size.

## Implementation requirements

1. Preflight a closed physical IPC capability across every selected segment:
   field types/encodings, projection indexes, checked message extents, dictionary
   forms, compression and alignment. Preserve logical field metadata and actual
   array representation. Do not use statistics to prove SQL semantics.
2. Reserve snapshot/descriptor/schema/dictionary and decoder metadata before
   creating their owned structures. Use bounded reserved containers where possible.
   Any conservative decoder envelope must be derived from concrete supported
   structures and verified allocation paths, not a guessed fraction of file size.
   Raw file mapping and process RSS remain separately measured resource dimensions.
3. Select deletion survivors in bounded reserved windows; reuse the monotonic
   exact row cursor without constructing a whole-segment mask or survivor list.
   Use existing admitted_selection and admitted_batch ownership machinery for
   published output, with actual buffer reservations retained by detached arrays.
   Do not call the ordinary Arrow deletion filter then relabel its result.
4. Preserve the original immutable snapshot while every declared partition runs.
   Budget concurrent readers and output windows against one shared parent pool.
   Retained outputs must lead to a named refusal, never wait indefinitely for the
   consumer to release them. Optional preparation refusal may decline only before
   consuming source/output; selected-stream errors are terminal without replay.
5. Exercise supported/unsupported encodings, compression, alignment and dictionary
   deltas; empty/all-deleted batches, NULL/dictionary values, multiple segments and
   partitions; metadata exhaustion, output pressure, detached lifetimes and early
   cancellation. Compare actual values with independent typed oracles, and assert
   the intended admission capability plus runtime concurrency rather than only
   declared partition count.

## Measurement sequence

Completed `run_native_incremental_paired.py` ran Q1/Q9/Q17 over raw, native and
32GiB decoded residency in two reversed-order blocks,36fresh-process outputs.
The candidate is frozen c20b0648, control703b8564. Both use
16threads CPU0–15, default disjoint ownership, GPU0, matching provider budgets and
independent typed oracles;180seconds is a diagnostic watchdog, not acceptance.
Resident preparation is separate. These small instrumented comparisons cannot
establish a protected regression bound or DuckDB leadership.

After this control, repeat the native admitted candidate with actual frontier
traces and run full provider/residency/resource gates. Native Q1 also spends most
of its time in aggregate ingestion: do not promise that scan parallelism alone
solves it. General aggregate ownership costing remains a separate measured task,
including the known4thread partial-ownership Q18 regression.

[Incremental implementation](native-incremental-ipc-2026-09-10.md),
[CPU attribution](build-schema-bottleneck-attribution-2026-09-10.md),
[architecture](architecture.md).

Paired11044 is now terminal0 with36typed-correct outputs; all400archive files
verify. Native timings are not consistently improved. Resident Q1's16admitted
slots still spend~9s in aggregate ingestion, making shared aggregate costing the
next performance investigation alongside this resource contract.
[Full result](native-incremental-measurement-2026-09-10.md).

## Current boundary check — frozen0c867c5c

The completed LEFT COUNT comparison preserves another important control: Q1
uses `MorselAggregate` over raw Parquet, but `SpillableHashAggregate -> Project ->
Filter` over native and decoded MemoryTable input. Candidate block1 times are
613.491ms raw,8552.282ms native and8271.699ms resident16. These provider timings
are not a same-source causal attribution. In particular, admission alone cannot
explain the already-admitted resident input's large remaining gap.

Current `PhysicalPlanner::lower_aggregate_cpu` selects the general morsel operator
from extracted Parquet files. Native routing is limited to its dense-direct
shape; `MorselAggregateExec` has no generic non-Parquet fallback. The existing
`QE_MORSEL=0` switch allows a same-binary, same-raw-input control before porting
kernels. Run it only after the active full provider screen, preserving typed
oracles, budgets, thread counts and order reversal. Treat any timing delta as
that controlled route's observation, not a bound on what another provider can gain.
The result should choose between shared batch-kernel work and reader admission;
neither copying the legacy decoder into a prepared wrapper nor provider-specific
SQL shortcuts would satisfy the resource and semantic contracts above.
