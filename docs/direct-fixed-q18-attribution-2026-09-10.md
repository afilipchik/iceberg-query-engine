# Remaining compact-state Q18 ingestion cost

The completed ea1e9019/c20b0648 comparison retains a14.59%resident16threadQ18
regression. This attribution does not change that result or certify a cause.

Owned-child GDB control99009 and candidate43017 are terminal0. Each produces a
correct independently typed Q18 result and100all-thread snapshots. A temporary
breakpoint on the first StateRows::prepare_arrays_indexed entry excludes resident
preload from the sampling window. Sampling requests run every15ms, depth8, maximum
100snapshots,300second watchdog. GPU0/default disjoint,16threads/CPU0–15,
32GiBquery/48GiBprocess inside a48GiB capped wrapper. No attach to other services
and no host profiling-policy changes. Both debugger children exited.

## Evidence and limits

Candidate leaf frames include28StateRows::prepare_arrays_indexed and14
PreparedGroup::commit; control includes1each. Candidate13snapshots stop at
prepare_arrays_indexed offset0x261, runtimePC0x55555b5227e1, corresponding to
filePC0x5fce7e1. Disassembly shows a16byte unaligned load during copying of the
returned Result/Option scalar payload, followed by more temporary copies.
Candidate12snapshots stop at PreparedGroup::commit filePC0x5fa0a2f, another16byte
load while moving the prepared-row token out of its Option. These repeated PCs
suggest unnecessary value movement is a concrete next target. They do not prove
hardware store-forwarding stalls: no hardware counter measured that mechanism.

Most sampled threads are sleeping. Control and candidate have different sampling
phase distributions; fixed-period stops, debugger perturbation and inlining mean
these counts are neither CPU percentages nor an estimate of time saved. Only
2candidate memcpy leaf snapshots have StateRows::begin as caller and3have group
commit as caller; the strongest observed sites are the surrounding value copies,
not proof that changing memcpy itself would fix the regression.

## Next candidate

Avoid returning a wide owning ScalarValue inside Result/Option for every fixed
Arrow input. Consume the decoded scalar by reference inside the existing checked
input adapter, returning only the update result. Preserve exact decoding, dictionary
codes/NULLs, float bits, decimal scale, terminal errors and the staged row.

Publish a PreparedRow in place inside its existing PreparedGroup Option, retaining
the externally consuming commit operation. Drop the contained state in place before
key rollback. This can remove the token move without weakening the single-use
publication or selected-payload rollback contract. Neither change is query-specific
or changes ownership defaults. Validate first, then freeze and compare against c20.

Source and assembly are frozen ea1e9019; all519current source inputs and both binary
hashes verify. Reproduction: profile_direct_fixed_q18.py control/candidate and
summarize_direct_fixed_q18_stacks.py under `.scratch/parallel-aggregate-input`.
