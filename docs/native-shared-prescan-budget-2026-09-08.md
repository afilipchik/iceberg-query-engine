# Native shared prescan budget — September 8, 2026

The optional shared-table cache ran before physical scan routing and attempted
materialization even when NativeTable::scan_budget_exceeded already required
refusal. A self-join over a 979452-byte native fixture therefore failed during
planning against a 209715-byte scan budget, despite both occurrences being under
spill-capable consumers. The separate source control in the previous checkpoint
reproduced this failure before the projection-identity change.

The cache now consults the same native preflight condition before invoking the
provider. Declining an optional optimization before consumption lets normal
per-occurrence routing choose streaming scans when covered. Uncovered/materializing
queries still receive the provider's existing named refusal. A scan that actually
starts and fails still propagates its original error; no retry was introduced.
This is not proof that shared caching generally has query-wide reservations;
other providers and multiple retained caches remain separate accounting work.

A new real-native initialization regression confirms both self-join occurrences
plan as NativeStreamingScanExec without invoking the refused materializing path.
The red test fails during planning. The first post-fix test used the wrong display
name for the streaming operator; corrected to the actual name, it passes. This
assertion correction is preserved separately from the original engine failure.

Final validation uses locked/offline lance,gpu, Rayon4, one build job, repository
TMPDIR and the 48GiB safe-build wrapper:

- Library plus shared_prescan_errors and aggregate_binding_transparency: 988
  library passes, 10 existing ignores; six integration passes, zero skips.
- Final native_streaming_scan_tests: six passes, four failures, zero skips. The
  new planning regression passes. The previous join no longer dies during
  prescan; it reaches execution and refuses a 383984-byte request against the
  262144-byte query pool. The three aggregate/deletion completion failures remain.
- Formatting and whitespace checks pass. No heavy job remains active.

Do not call the native completion gate fixed. The source reads one segment into
a Vec<RecordBatch>, retains it in NativeStreamingScanExec's pending queue, and
passes batches to consumers. Segment-at-a-time scheduling is not a byte-bound
admission contract. The next resource task must bound/admit decoder and deletion
filter scratch, queued buffers and consumer construction together. Slicing a
batch without accounting for retained backing buffers is insufficient; raising
the test budget or removing refusal would hide the problem.

Evidence: `docs/benchmarks/2026-09-08-native-prescan-budget/`. Logs retain the red
reproducer, intermediate assertion mistake, corrected focused test, library/error
propagation gates and final native failures. Source hashes and a snapshot identify
the current dirty tree. No new release or performance measurement applies to this
source, and the previously failed wrapped-SQL benchmarks remain open.
