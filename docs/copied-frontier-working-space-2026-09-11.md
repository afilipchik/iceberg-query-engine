# Copied input concurrency and downstream working space

The decimal spill trace on frozen008d92f7 identifies nine copied slots reserving
228060 bytes of a262144-byte query budget. GroupRows subsequently refuses its
first index reservation after spilling. See the [source and trace audit](resource-gate-follow-up-2026-09-10.md).

Generic regression63124 fails with241041bytes already reserved before a consumer
can acquire128KiB of working state. The fixture uses16 declared partitions,
3000 nullable integer values with duplicates, an empty prefix and two repeated
batches per partition. Every partition must execute once; all48 batches and exact
nonnull counts/sums are checked, with cleanup to zero.

Current InputFrontier caps optional copied prefetch at half of the pool available
when choosing queue slots. It retains the largest fitting parallel slot count,
or the existing serial route when two slots do not fit. This is a costing policy,
not a certified minimum consumer reservation: all allocations still obey the hard
shared pool, and an intrinsically oversized input or consumer may refuse. No data
or expression replay, semantic rewrite, budget increase or ownership-default change.
Admitted buffers retain their existing producer-owned progress-credit contract.

Focused16432 passes all9 frontier tests, including cancellation, late errors,
prepared-source no-replay and a real scan/project/aggregate spill oracle. The
original exact-decimal test4757 still fails at unchanged256KiB/default disjoint,
16 Rayon threads:1536bytes requested at261469used. This is an incomplete repair.
Owned trace27619 is terminal0 (debugger protocol); decimal test still fails.
It confirms4copied slots/101360-byte envelope. Remaining1536-byte refusals occur
in FrameScratch::new during RunMerge creation, then repartition preparation,
inside bounded run compaction. Other aggregate owners can still retain state
while one owner needs compaction scratch. The trace identifies that boundary;
it does not yet account for every remaining reservation.
Broader validation and performance measurement remain pending.
The first new-test build94952 failed due to a test-helper call; corrected63124
is the behavioral red, not that compile error. All execution uses48GiB capped
wrapper,swap0,one build job,locked/offline lance,gpu and repository TMPDIR.

[11-file intermediate archive](benchmarks/2026-09-11-copied-frontier-progress/manifest.json)
verifies520source inputs and retains the exact current source, all red/green logs
and compaction refusal stacks. This is an incomplete resource repair.

Subsequent full validation exposed a fitting-producer regression in the initial
half-pool policy. Current scheduling caps the downstream window at1MiB; all6
input-concurrency tests pass again. See [coordinated spill follow-up](coordinated-spill-progress-2026-09-11.md)
for the later disjoint decimal repair and remaining partial failure.
