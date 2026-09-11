# Prepared input ownership: initialization prerequisite and current status

The query engine's direct hash join could detach initialization work when its
caller was cancelled. It also awaited partition tasks in declaration order,
allowing a stalled earlier partition to hide another partition's error or panic.
The all-partition collectors forced partition zero even when an input declared no
partitions. Five new deterministic tests reproduced all these failures.

## Implemented and validated

`collect_join_partitions` owns producers in a JoinSet, handles failures as tasks
complete, aborts and drains siblings on error, and returns results in declared
partition order. Dropping initialization aborts owned tasks; eventual cancellation
cleanup is verified by drop acknowledgments. Semi/Anti prefetch owns its outer
task and nested partition tasks. A cancelled OnceCell initialization can retry
without detached work. Collected payload admission remains a separate gap.

The five tests fail on the exact preserved pre-fix file and pass with the repair.
The selected Lance/GPU gate passes 644 tests with one pre-existing ignored test.
Separating `ensure_build_cache` from probe execution then passes 25 focused
initialization, streaming-join and spill tests. That extraction changes structure,
not join routing, SQL semantics or resource limits.

`PreparedQueueInput` is an explicit optional handoff in the physical operator
contract. Preparation precedes outer output reservation, returns one unpulled
stream per declared partition and guarantees pool-independent future pulls.
Queues validate the descriptor count, reserve parallel output slots when possible,
and consume the prepared streams exactly once even under serial fallback.
Unknown operators retain the existing path; ordinary execute completion is not
silently treated as preparation. No production join advertises this capability yet.

`GatherCopyBound` separately analyzes actual immutable take inputs when requested.
It bounds repeated values using real string/binary lengths, retained dictionary
children, identity extents and checked output arithmetic. Ordinary queue bounds
remain metadata-only. This covers audited Arrow take; it does not automatically
cover new dictionary encoding, multi-batch concat or join schema transformations.
Four gather tests and three prepared-handoff tests pass in the 614-test selected
contract gate (one pre-existing ignored test).

## Remaining work before performance acceptance

The accepted scalar binary remains the control; the latest measured parallel
candidate still regresses on raw Q9/Q14 and IPC Q14. This initialization repair
has not been rebuilt for release or benchmarked. The prepared protocol is not yet
a Q14 fix.

Actual gather metadata now propagates through MemoryTable, audited Filter and
column-only Project. Its separate lazy cache keeps ordinary queue metadata
requests from scanning value spans. Incremental merges are atomic on mismatch;
duplicated projection columns preserve their full copied charge. The selected
Lance/GPU gate passes 617 tests with one pre-existing ignored test, including
three new propagation/cache tests. The initial run exposed a nonnullable fixture
error in a NULL-producing take test; the corrected fixture passes without an
engine workaround. Logs are archived under the resident-gather evidence.

Next, implement a prepared in-memory Inner join using its completed cache and
certified probe streams. Bound every admitted physical gather/schema alternative;
unknown, spill and non-Inner paths must decline safely. Preparation must not start
spill producers or pre-pull output. Verify cancellation and retry throughout both
build-decision and hash-cache phases before enabling the guarantee.

Preserve the existing failed timing gates and memory caps. Once the complete
contract is enabled, repeat exact, cap and alternating performance gates. Streaming
byte contracts, full query memory ownership, GPU hard-budget admission and broader
provider/public leadership remain open.

Evidence: `docs/benchmarks/2026-09-06-join-initialization/` contains before/after
logs, the exact files, source hashes and the current source snapshot.

## Spill initialization lifetime repair

The real spill build helper also leaked its directory on upstream error or
cancellation after opening build files. Two deterministic tests reproduce those
failures with the old implementation. A guard now owns only a newly created
directory immediately and transfers ownership to SpillState after successful
partitioning. Local writers drop before guard cleanup. Existing child directories
are refused without adoption; successful state retains files until its last Arc
drops. All four new tests pass in the 621-test selected Lance/GPU gate (one
pre-existing ignored test), and formatting passes.

See [spill initialization evidence](benchmarks/2026-09-06-spill-initialization/README.md).
Filesystem cleanup errors remain best-effort. Similar aggregate/sort trailing
cleanup paths require their own lifetime fixes; they are not covered here.
Prepared join output is still not enabled or performance accepted.

## Prepared Inner integration — latest source checkpoint

Production HashJoin/SpillableHashJoin now prepare in-memory Inner output for a
single columnar cached build batch and a certified resident probe. Both build
phases complete before output-slot admission; initialized streams are unpulled
and owned across failure/cancellation. The ordinary and prepared paths share
one Inner stream factory.

Distinct plain/dictionary physical schemas contribute a checked maximum copied
byte charge. Actual queue-byte validation remains active. Masks, orientation,
dictionary children and identity extents are covered. Multi-batch, row-store,
unknown probes, non-Inner and spill paths decline safely. Full source/scratch
and query-wide memory admission remain separate work.

Selected validation: 663 tests passed, one pre-existing ignored. Six public
regressions cover exact results, encodings, no pre-pull, execute-once, cancellation,
spill decline without probe output and actual aggregate-to-join initialization
before shared-pool envelope admission. The initial compile failed only a new test
module import; corrected before passing gates. Formatting passes.

Release build is running. No new latency/cap result or performance acceptance.
The accepted scalar binary remains the control; previous failed screens remain
preserved.

Earlier not-enabled statements describe preceding checkpoints. See
[current evidence](benchmarks/2026-09-06-prepared-inner/README.md).

## Measured prepared Inner result

The release passes six actual-spill development cap scenarios. All 110 IPC
screen attempts validate; Q14 is 82.6 versus 85.0 ms control, recovering the
previous serialization regression. Raw still fails: Q9 is 25.8% slower and Q14
warmup times out. All 66 completed raw attempts validate; Q18 is not reached.
This candidate therefore remains unaccepted overall. Detailed ratios, source,
binaries, plans and verified archives are in the prepared-inner evidence.

Next: implement the reviewed raw fixed-width normalization plus both general
queue/gather capabilities. Also tighten build-only take bounds: the current
conservative identity allowance reserves about 787 MiB for IPC Q14 despite no
build identity reuse. Any tighter proof must retain full dictionary children
and account for index validity extents; it must not use average widths.
