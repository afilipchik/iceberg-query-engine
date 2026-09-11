# Partial native startup: reproduced retained-input admission failure

The owned debug test trace confirms the163434-byte reservation is made by
`live_spill::execute` while retaining an incoming unadmitted batch, before that
batch's expression evaluation. The exact aggregate test then fails with the same
named256KiBquery-budget error. This is allocation-site evidence, not a repair or
a performance result.

Session2503 completed the debugger protocol with exit0 and one breakpoint hit.
The test itself correctly reproduces failure:0passes/1failure/9filtered, inferior
exit101. Default-feature selection is not inferred from cargo defaults: the traced
existing test binary is the lance,gpu target recorded in the preceding both-mode
validation log. Its exact SHA256 and source hashes are retained in the manifest.
The source differs from that compilation only by a comment and the separate
memory-reservation unwind-test correction; the engine behavior under test is the
progress-credit candidate. No engine or test source was edited for this trace.

The debugger launches only its own local test child, with partial ownership,
RAYON_NUM_THREADS16, repository TMPDIR and a180sowned watchdog. It runs after the
paired benchmark terminated and before the provider screen starts. The actual
48GiBcgroup/swap0 is checked. Peak2,482,515,968bytes; zero max/OOM events.

The stack is `MemoryPool::allocate` → `live_spill.rs` retained-batch admission
closure at157 → admission match at159 → fused spillable aggregate execution.
The test reports requested163434,used183961,limit262144 in query1.
There is only one matching allocation attempt. With the current control flow,
this is consistent with `release_for_input` returning false rather than successfully
releasing state and retrying the same batch. Source inspection explains that
empty workers do not release startup writers/workspaces/merge reservations.

This does not license rebuilding or replaying the child. A repair must retain the
same incoming batch and prepared frontier, positively track never-ingested state,
and release only unused worker/merge owners. An empty group table after a spill is
not that proof. Remaining single-owner metadata, writer bounds and finalization
must stay admitted. See the [implementation sequence](resource-gate-follow-up-2026-09-10.md).

[Trace archive](benchmarks/2026-09-10-partial-native-input-attribution/manifest.json)
retains the exact debugger commands, complete stack and expected failure, driver,
resource counters, binary identity and current519input source provenance.
