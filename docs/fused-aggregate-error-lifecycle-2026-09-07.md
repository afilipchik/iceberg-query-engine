# Fused aggregation: terminal failures and producer cancellation

Fused aggregation could hide an input error or panic by re-executing the input
and returning a successful aggregate. It could also hang after an input or worker
error when another partition remained pending. These failures were reproduced
independently of benchmark SQL and repaired in `physical/operators/spillable.rs`.

The initial four regressions all failed: an execute error, a stream error after
a prefix, an execute panic, and a stream panic after a prefix were each replayed
into successful output. Two additional bounded regressions reproduced hangs
with a pending sibling/input. The source intentionally succeeds on replay, so a
result-only assertion would have accepted the wrong outcome.

The final seven-fixture control confirms the complete contrast: the pre-fix
operator passes the successful-input control and fails all six failure cases;
the repaired operator passes all seven. The control run then restores and
hash-verifies all633 candidate inputs. Its orchestrator exits0 while the
preserved pre-fix test command exits101.

## Changed behavior

Producer tasks now belong to a `JoinSet` and are observed as they complete.
Input errors keep their original `QueryError` classification; task panics become
named execution errors. On failure the coordinator cancels pending sibling tasks,
waits for their cancellation, and joins aggregation workers before returning.
Worker processing failures notify the coordinator, so it cannot remain blocked
on an unrelated input. A worker panic guard also notifies the coordinator during
unwinding. Ordinary worker channel sends and coalesced flushes now propagate
task/channel failures instead of silently stopping or ignoring the result.

Already-running blocking sends are not forcibly aborted. Workers keep draining
after the abort flag so those senders can release their owned batches/channels;
state reservations are released before the error escapes. Failed input is never
retried by these error branches. When multiple failures compete, a captured
worker processing error is retained first, then worker panic, then producer error.

## Validation

All **25 selected tests pass with zero ignores** under the memory-capped wrapper,
locked Lance/GPU features, Rayon4, one compiler job and repository TMPDIR:

| Target | Passed |
|---|---:|
| `fused_aggregate_input_errors` |7|
| `aggregate_arena_admission` |2|
| `aggregate_encoding_contract` |8|
| `decimal_root_reuse_aggregate_contract` |3|
| `dense_aggregate_null_domain` |1|
| library `agg_sort_directory_ownership_tests` |4|

The seven new tests cover both shared and disjoint routing, original error
classification, input-task panics, worker processing errors, pending-input
cancellation, single execution of each input, zero retained query reservations,
and successful duplicates/NULLs/empty batches against an independent integer
oracle. Existing tests additionally exercise real spill, exact decimal values
and spill-file cleanup. They do not directly inject an OS-worker panic; that
notification branch is source-reviewed, not claimed as separately reproduced.

Reproduction logs, exact source snapshots, the isolated operator patch, final
fixtures and the633-input hash manifest are preserved under
`.scratch/fused-aggregate-error-propagation/` and indexed in the
[evidence checkpoint](benchmarks/2026-09-07-fused-aggregate-error-lifecycle/).
All recorded test commands configure a48GiB wrapper scope. This is not a new
RSS/cap benchmark or optimized performance claim.

## Remaining work

The [256KiB fused spill-completion failure](fused-aggregate-budget-overshoot-2026-09-07.md)
and the group-count-triggered replay path remain open. This repair deliberately
does not reinterpret admission errors as authorization to replay consumed input.
Work granularity, real state-growth admission and spill transition still need a
coordinated design. Group-budget cancellation is also distinct from the error
notification added here.

The working tree also contains the earlier decimal-bound lookup experiment.
The operator patch here is isolated against that exact input manifest; its
current validation must not be attributed to the older frozen662 benchmark.
No optimized binary or latency comparison has yet measured this lifecycle change.
Current and historical protected follow-ups and the broader goal remain open.
