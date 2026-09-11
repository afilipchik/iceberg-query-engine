# Shared-provider error lifecycle repair

Reproduced a hidden provider error: shared prescan discarded an error, retried
through ordinary planning and returned six rows successfully. The planner now
propagates the original Result and publishes newly collected cache entries only
after successful collection. This preserves storage and typed memory errors.
No replayability capability, decoder bound or performance improvement is claimed.

Validation:889 library passes with10 documented ignores;20 selected integration
passes without skips. Before-fix failure and successful cache control are archived.
Benchmark supervisor validation passes109 tests including opt-in Lance and actual
comparator spill; real SF10 setup records one launch and three explicitly
unexecuted dependent requests retaining the original refusal.

No parent task is complete. Release/provider/resource/concurrency acceptance and
the remaining measured aggregate/join latency gaps remain open. Next performance
work needs a bounded typed-batch or parallel-state experiment with the existing
partial-state spill/ownership contracts, not another query-specific rewrite.

[Production repair](../../../../docs/shared-prescan-error-propagation-2026-09-08.md).
[Preparation/supervisor evidence](../../../../docs/ipc-preload-admission-2026-09-08.md).
