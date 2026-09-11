# Aggregate startup: reserve input before optional workers

Current source fixes the reproduced startup starvation without changing query
budgets, SQL, provider batch sizes or ownership defaults. All 10 partial-native
streaming tests now pass, including the four previously failing cases. Five new
integration tests cover preparation, first-pull allocation, exact NULL/duplicate
results, source replay prevention, late errors and real budget exhaustion. Full both-mode validation is complete, with the remaining historical failures
reported below. No optimized performance claim is made yet.

## Three distinct admission boundaries

The earlier [native allocation trace](partial-native-input-attribution-2026-09-10.md)
locates a 163,434-byte retained-input reservation after optional partial workers
already consume most of a 256 KiB query budget. Generic red83869 reproduces that
unused workers cannot release headroom. The initial repair clears three native
failures, but the join case still fails. Preserve that incomplete intermediate
result: initial partial-native62108 is 9 passes/1 failure.

The owned join trace87315 locates its 161,343-byte request inside the asynchronous
`stream_merge_input_partitions` producer at spillable.rs:829. A child may allocate
while preparing a nested build or while pulling the first batch. Neither boundary
is covered by successfully allocating aggregate worker state first.

Generic preparation-order red86662 needs 128 KiB of temporary build scratch before
opening its prepared streams. Preparing the input before workers fixes that test,
but partial-native54277 remains 9/1. Generic first-pull red61483 then reproduces a
163,840-byte producer reservation at 179,014 used under the same 262,144-byte cap.
This distinguishes successful preparation from affordable first-pull execution.

## Current execution contract

1. Bind semantic eligibility, aggregate layout and output filtering before input
   preparation. Unsupported capabilities still decline before consuming input.
2. Prepare InputFrontier once. Preparation may execute nested builds; errors are
   terminal and must never cause a second factory invocation.
3. Pull the first input batch or EOF before optional worker allocations. Retain
   that exact batch and its existing admitted owners, or reserve its full backing
   footprint once. A nested Option distinguishes saved EOF from an unprimed input.
4. Construct the worker/reducer set against the remaining query budget. Existing
   constructor fallback can select one owner without reopening the child.
5. Process the saved batch through the ordinary evaluation/ingestion loop, then
   continue the same frontier. Empty batches are preserved and evaluated normally.
   The saved input lease remains live through ingestion and is not double-charged.
6. Before any nonempty ingestion attempt, an input-retention denial may reclaim
   unused workers and optional reducers while preserving the first worker, same
   incoming batch and prepared frontier. Restore the original global group-count
   scheduling limit; the initial writer is not sized by that limit. No replacement
   worker allocation or source/expression replay occurs.
7. Once nonempty ingestion has started, ownership never contracts through this
   startup path, even after spilling empties resident group tables. Ordinary spill
   handling remains in place; poisoned states and remaining denials stay terminal.

`ParallelControllers` owns the irreversible ingestion flag and global scheduling
limit. Profile counters cannot prove this state: they update only when profiling
and bypass the single-owner path. `ReservedVec::truncate` drops unused owners but
keeps its own capacity charged. `IngestionController::restore_unused_group_limit`
rejects used/poisoned workers; published runs and populated state cannot be erased.

This does not prove that every unknown later producer allocation will fit. Full
provider, memory/concurrency and performance acceptance remain separate gates.

## Regression coverage and current validation

- The controller regression uses a 256 KiB pool, 16 partial or four disjoint owners,
  shared COUNT/SUM buffers, duplicate/NULL keys and values, and an independent
  integer oracle. Both empty-prefix and immediate-input cases complete after
  reclaiming unused state, with final accounting and spill-directory cleanup to zero.
- A populated four-owner partial aggregate spills actual state, attempts reclamation
  again, then ingests more duplicates. It retains all owners/reducers, records
  nonzero spill bytes and produces independently computed exact results.
- The prepared unknown-bound integration source has two partitions and explicit
  factory/open counters. Empty and all-NULL outputs, repeated batches, duplicates,
  build scratch, first-pull scratch, preparation/late errors and impossible input
  budgets all verify one-time consumption and owner cleanup.

Initial focused97143 passes seven controller tests. Library71724 passes 1,090 with
11 ignored before the subsequent preparation/first-pull ordering changes.
Final focused90096 passes five integration tests and all 10 unchanged native
streaming tests in partial mode with 16 Rayon threads. Both-mode full validation99343 is terminal1 with all520source inputs verified:
each mode passes1,090library tests/11ignored and82contract integrations. Both
native streaming suites pass10/10. The wider native/IPC family has58passes/2known
dictionary-plan failures in default mode,57passes/3known failures in partial mode
(the two plan assertions and one formatted floating-result comparison). Four
partial-native memory failures are removed; no new failure names appear.
Each mode retains7focused decimal/parallel-spill passes, legacy spill8passes/6failures
and systemic numeric11passes/1failure. These unresolved gates remain failures.
Formatting and whitespace checks pass.

All builds/tests run through scripts/claude-safe-build.sh with repository TMPDIR,
48 GiB scope, swap disabled, one build job, locked/offline lance,gpu. The full
validation driver freezes all 520 source inputs and verifies them after completion.
No dependency or benchmark-harness changes are part of this repair. The latest
frozen measured binary is still 38966ae6, before these startup changes; its
[provider results](retained-input-credit-provider-screen-2026-09-10.md) are historical
baseline evidence for this new source candidate.


[Correctness archive](benchmarks/2026-09-10-aggregate-startup-headroom/manifest.json)
retains final520-input source, baseline/source-change hashes, all red/intermediate/
green logs, exact both-mode commands and failure comparisons. The intermediate
join trace includes its original debug-binary/source hashes and unchanged
allocation-site files; it is not a new optimized measurement. The preceding38966ae6
source archive supplies the baseline for reproducing the new regression tests.

Release78261 subsequently completed in8m51s, freezing008d92f7 with520 verified
source inputs. The [matched performance diagnostic](startup-headroom-measurement-2026-09-10.md)
completed as session81205:64typed-correct outputs/1820complete traces. The572-file
archive verifies520inputs. Native2–4%slowdowns and rawQ9 3.97%slowdown remain
visible; this two-block screen does not certify performance neutrality.
