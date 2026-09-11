# Incremental native IPC decoding and deletion selection

NativeStreamingScanExec formerly decoded every IPC record block in a segment,
filtered all deletion survivors into a Vec, and queued them before first output.
The current source decodes and selects one batch per pull. This removes eager
whole-segment survivor retention; it does not advertise query-pool admission or
claim a measured speedup. Native admitted input and aggregate costing remain open.

## Shared contracts

`ipc_cache::RowGroupReader` owns the mmap buffer, FileDecoder dictionaries and
footer block descriptors. `open_row_group` validates the footer and loads required
dictionaries; each iterator pull checks the selected block extent/framing and
uses Arrow validation before yielding. A late error is terminal. Detached arrays
retain their mmap owner independently of the iterator. The collecting
`read_row_group` API consumes this iterator and preserves its existing optional
reslicing behavior. There is no self-referential footer or unsafe validation bypass.

`NativeSegmentReader` snapshots the segment deletion vector and owns a shared
DeletionCursor. It decodes then selects only the next batch. Empty batches do not
advance offsets; the cursor uses checked u64 row arithmetic. Unaffected batches
pass through without copying. Arrow filter_record_batch preserves explicit row
counts even for zero-column batches. The collecting deletion path shares the same
cursor. Metadata, dictionaries, deletion masks and output allocations still lack
query-wide reservations; no admitted factory is exposed.

The physical scan moves the reader and exact segment cursor through one blocking
job per requested output, keeping filesystem/decode work off reactor threads.
It drains each segment before opening the next and preserves round-robin partition
assignment, pruning, logical field metadata and physical dictionary types. Error
ends the stream without replay. Dropping an outstanding blocking task can leave
one pull running to completion, not an unbounded segment producer; its resulting
state drops when the abandoned task finishes. This is not cancellation preemption.

## Evidence

New regression6687 is terminal101 on the prior reader: a malformed second IPC
block prevents the first valid batch from being delivered. With incremental
reading,44913 terminal0 delivers the first50exact values, then the named error,
then EOF. An array retained after dropping the batch/reader/plan remains valid.
Both focused runs use default features and the48GiB capped wrapper.

Two additional library regressions cover a12,288-row segment with a fully deleted
first batch, an empty physical batch, deletions across the remaining batches,
NULL/string values, immutable deletion snapshot, exact independently constructed
survivors and release of filtered output references. A separate cursor test covers
zero-column row counts and row positions crossing u32::MAX without overflow.

Library6970 is terminal0 with features lance,gpu:1,080passes/11ignored. Commands
use locked/offline dependencies, one build job, TMPDIR in repository scratch and
48GiB containment. Integration/resource gate31859 is terminal1 in both disjoint and partial ownership.
It stops early at two dictionary anti-join plan assertions; two semi-join tests
pass. Separate no-fail-fast runs complete the remaining targets: default56passes;
partial51passes/5failures (four known256KiB native refusals and one exact formatted
floating-point aggregate comparison differing in low bits). Full spill remains
8passes/6failed names in both modes. These failures are not accepted coverage.

Control89731 runs the dictionary tests against all518verified frozen703b8564
inputs and reproduces both plan assertions (2pass/2fail). Control84090 on the same
verified source with partial ownership reproduces the floating-point assertion
(11pass/1fail). Both control drivers preserve and restore the three candidate files
and verify their hashes. The four partial native budget failures and six spill
failure names match preceding gates; their allocation boundaries remain open.
 Source changes versus frozen703b8564 are exactly native_scan.rs,
ipc_cache.rs and native_table.rs; there is no dependency or ownership default change.

The next step is to freeze an optimized candidate and measure the affected native and raw/IPC paths before
extending native admission. A one-batch iterator alone does not justify relabeling
legacy allocations as reserved memory.

[CPU attribution](build-schema-bottleneck-attribution-2026-09-10.md),
[prior frozen provider screen](build-schema-admission-provider-screen-2026-09-09.md).

[Correctness evidence archive](benchmarks/2026-09-10-native-incremental-ipc/manifest.json).

Release32424 completed in8m52s, frozen c20b0648/518verified inputs. Paired11044
validates36outputs and1236complete traces; no consistent native speedup is established.
[Matched measurement and next bottleneck](native-incremental-measurement-2026-09-10.md)
preserves all samples;400-file archive verified. All jobs are terminal.
