# Next bounded memory increment: shared input-queue admission

Read-only source audit after the aggregate/regex fixes. No engine build, test or benchmark was run for this proposal.

## Recommendation

Add owned byte admission to `stream_merge_input_partitions` in `src/physical/operators/spillable.rs:246`, together with producer cancellation tied to the returned stream. This is a small shared boundary used by join build (:824), join probe (:1035), aggregate fallback (:3224) and sort (:3827). Today its channel holds `4 * partition_count` batches, irrespective of byte size, and each detached producer can additionally hold a batch while waiting to send. Its comments currently overstate what a batch-count bound establishes.

This increment accounts **only for batches retained by this queue, including pending sends**. It must not be described as complete join/aggregate/result ownership or admission before the upstream allocation.

## Implementation

1. Pass the existing query `SharedMemoryPool` and an operator label into the helper at its four callers. Create a named child such as `join build input queue`; keep the existing batch-count backpressure as an additional bound.
2. Introduce a private `QueuedBatch { batch: RecordBatch, reservation: MemoryReservation }`, with the reservation declared last. Before a producer retains/sends a batch, reserve a conservative retained-buffer charge through the shared pool. Include checked metadata/header arithmetic. Use Arrow retained array/buffer memory size, **not `estimate_batch_size` at :4843**: that helper intentionally charges logical slice size and may undercount buffers pinned by a small slice. Shared/sliced buffers may be charged more than once across queue entries; explicitly document conservative queue occupancy accounting, not unique physical bytes or RSS. Do not add unsafe buffer ownership plumbing or global pointer deduplication.
3. Carry the guard inside `mpsc::Sender<Result<QueuedBatch>>`, including across `send().await`. Admission denial sends the named resource error and stops that producer. No retry/sleep waiting for budget: another operator may hold its budget until this stream finishes, so waiting could deadlock. No unreserved alternative queue.
4. A private stream wrapper owns its receiver **and its producers** (e.g. `JoinSet<()>`, whose drop aborts tasks). Polling a received entry transfers the `RecordBatch` to the existing public `RecordBatchStream` consumer and releases the queue charge. This is an explicit end of queue retention, not end of Arrow buffer lifetime. Consumer retained-state admission is the following increment. The one-partition bypass creates no staging queue and remains direct.
5. Dropping/erroring the stream must abort producers even if one is awaiting upstream input forever, drop pending/queued entries, and release every queue guard. Existing detached `tokio::spawn` loops notice a closed receiver only after another upstream batch arrives; that is a cancellation/lifetime hole. Do not infer that dropping bare `JoinHandle`s cancels their tasks.

## Focused acceptance tests (new unit tests in the owning module)

Run only after the parent build/latency window closes:

`TMPDIR="$PWD/.scratch" SAFE_BUILD_MEM=32G SAFE_BUILD_JOBS=1 scripts/claude-safe-build.sh cargo test --locked --lib input_queue_memory_tests`

Use small deterministic mock partitioned operators and barriers/notifications, not timing-sensitive sleeps or real giant allocations:

- A batch whose retained buffer exceeds a tiny queue/query budget is refused with the named resource error before successful enqueue; reservations return to zero. A 1-row slice of a larger Arrow allocation must be charged for the retained buffer, not one row.
- Two independent queues under one parent each fit alone, but their concurrently retained batches exceed the shared parent. Synchronize production to prove the second admission fails, parent peak never exceeds its limit, and cleanup releases the first charge.
- Fill a capacity-limited channel so a producer is pending send with an owned guard; drop the receiving stream. Assert producer cancellation and zero reserved bytes. Separately park a producer forever in upstream `try_next` and prove stream drop cancels it.
- Drain all declared partitions and verify exact IDs, including duplicate values, empty partitions, and upstream errors. Ensure byte admission does not recreate omitted-partition errors.
- Holding/dropping a dense reservation under the same parent changes queue admission availability, proving this boundary composes with the existing actual owner rather than using independent local thresholds.

## Existing owners versus remaining gaps

- **Already real:** `DenseAccumulators` at `operators/morsel_agg.rs:387` reserves vector layout before allocation (:430), keeps its guard through accumulation/output construction, and drops buffers before the guard. Copied output arrays remain outside this owner. Process/context/query pools provide shared admission only to callers that reserve.
- **Perfect/hash aggregate:** `AggregationState` at `physical/morsel_agg.rs:1645` is `Clone`; perfect vectors expand/remap at :1825–1878, merge grows/clones at :2621–2753 and :2857–2905. It also owns string keys, multiple key maps, raw maps and accumulator vectors. A lone guard on `perfect_accs` would miss clone/remap overlap and fallback. Fused aggregation additionally has crossbeam queues/coalescing buffers at `spillable.rs:2798–2960` and group-count estimates instead of owned reservations. These queues do not use the recommended helper and remain explicit follow-up work.
- **Joins:** `BuildDecision` at `spillable.rs:355`, `SpillState` at :412 and `hash_join.rs:214` (`BuildSideCache`) retain input batches, several hash representations, matched bitmaps and row-store copies. Local threshold comparisons/`observe` at :834–858 do not reserve. A guard attached only to `BuildDecision` is insufficient because output producer streams and cached child operators can outlive it. Spill-state `Arc` ownership already provides a useful future guard location; in-memory cache must own its own guard through final use. Reserving only flat input payload would still miss hash-table expansion and copies.
- **Collected results:** `context.rs:1444–1473` uses `try_collect`/`join_all`; public `QueryResult.batches` at :33 exposes cloneable Arrow arrays. A private guard on `QueryResult` does not follow arrays cloned or moved out, so it cannot certify retained-result ownership. This needs an explicit API/lifetime design, not a local field bolted onto the current public struct.

After queue ownership is proven, implement owned working state for a single spillable aggregate representation with reserve-before-growth and merge-scratch admission. Keep perfect-to-hash fallback, cloning, retained strings and fused bypasses in that design; do not claim the queue increment solves them.


## Additional source audit — 2026-09-06, scalar latency window

Two contract details must be included in the queue increment, beyond byte guards:

- The current helper computes `input.output_partitions().max(1)`. The physical
  contract declares the valid range as `0..output_partitions()`. For a provider
  declaring zero partitions, invoking partition zero invents work outside that
  range. The replacement should return an empty stream without calling execute;
  keep the direct bypass only for exactly one declared partition. Add a mock
  whose zero-partition execute method fails if called. This is a source finding,
  not a runtime reproducer yet.
- Current detached `tokio::spawn` handles are discarded. If a producer panics,
  dropping its sender can look like normal partition completion, allowing the
  receiver to return partial results. Merely storing a JoinSet for cancellation
  does not fix this: the returned stream must also drive task completions and
  translate JoinError into query failure. Add a deterministic producer-panic test
  alongside the ordinary upstream-error test. On receiver close, await/check all
  producer completions before declaring successful end-of-stream. On error/drop,
  close the receiver, cancel remaining producers, and drop retained entries.

A stream state machine should poll the receiver and task-completion set fairly.
Record the first terminal error and stop returning successful batches afterward;
consumers may already have seen earlier batches, so their query must still fail.
Do not turn a producer panic into an empty partition. Keep task panic/cancellation
errors distinct from named memory refusal. Cancellation of these async producers
cannot promise preemption of arbitrary upstream spawn_blocking work; retain that
limitation explicitly and verify the owned async queue cleanup separately.

No runtime test or source modification was performed for these findings while
canonical latency measurement was active.


## Scratch implementation review: external owners

The first review incorrectly relied on Arrow's stale `Buffer::capacity()` doc
comment. In the pinned arrow-buffer 58.4.0 implementation, `bytes.rs:106–112`
returns the declared size for `Deallocation::Custom(_, size)`, and
`immutable.rs:169–174` stores the length passed to from_custom_allocation.
Current native mmap input therefore exposes the full mapping length through
capacity; existing `dictionary_column_estimate_is_content_aware_not_mmap_capacity`
coverage confirms this behavior. The capacity-zero detection draft was rejected
without application or execution and preserved under the scratch directory.

A generic custom owner can nevertheless retain more than its declared extent,
and no public buffer API exposes a custom-owner classifier. The current scratch
candidate therefore uses a universal owned-copy boundary for this staging queue:
reserve a checked aligned copy layout, fallibly copy every array/null/child/
dictionary buffer, and rebuild equivalent ArrayData with safe validation. Queued
arrays no longer retain arbitrary source buffer owners. This is a deliberate
copy-cost tradeoff requiring performance gates, not a zero-copy claim. The direct
single-partition path creates no queue and retains its existing behavior.

Tests must prove external-owner release while output remains queued, correct
sliced-null/dictionary/Decimal128/Decimal256 values, and refusal before copying
when admission is insufficient. Allocator rounding and metadata-allocation
limitations remain explicit; this does not solve upstream/cache or downstream
state ownership. Consider slice compaction only from measurement and with the
same value/lifetime contract, rather than weakening the budget.

The scratch implementation and eight tests live in `.scratch/queue-ownership/`.
They are not applied, compiled or validated while the scalar candidate's latency
matrix is active. MemoryPool errors propagate unchanged for the cap harness's
strict refusal grammar. Capacity overflow/runtime bounds refuse before creating
tasks. All four callers use their existing shared pool hierarchy.

The final scratch review also removes an unconditional self-wake while the
channel is closed but producer completion remains pending. JoinSet already
registers the completion waker; only exhausting the bounded completion poll
budget needs an explicit wake. A ninth prepared test holds task completion
after closing its sender, checks absence of busy waking, then checks EOF.
Rustfmt parses this scratch revision; compilation and execution remain pending.

## Applied candidate: broader gate exposes prefetch admission conflict

The initial owned-copy/JoinSet candidate compiles and passes all nine focused
regressions. Its broader library gate fails: 547 pass, 19 fail, one pre-existing
ignored; integration targets were not reached after the library failure. All
19 failures are named admission refusals from low-budget spill fixtures. Every
logged requested batch fits its pool alone, but prior batches from that same
queue consume the remaining budget (for example 197,884 + 197,884 bytes against
262,144, or 30,142 + 60,204 against 65,536). These are avoidable prefetch conflicts,
not demonstrated minimum viable query-budget failures. The failed source and
logs are preserved in `docs/benchmarks/2026-09-06-queue-ownership/`.

Do not raise fixture budgets or remove exact spill expectations. Do not wait
for byte admission while retaining an unreserved pulled batch. The next bounded
correction acquires a per-queue demand permit before polling another upstream
batch and carries it through copying, pending send and consumer handoff. The
reservation must release before that permit. A one-slot channel alone would
still allow one pending batch per producer and does not solve this problem.
Byte admission remains immediate, so contention with other genuine owners still
refuses by its original pool name.

This demand limit serializes prefetch and introduces head-of-line blocking;
performance acceptance is explicitly unproven. A wider safe window needs an
upstream batch-size/ownership contract, not a heuristic treated as a bound.
All existing spill tests and budgets remain unchanged for the next rerun.

The demand correction resolves all 19 library failures at their original
budgets: 568 library tests pass, including all 11 queue tests, with one existing
ignored test. Join-stream 7, materialization 6, memory-reservation 10 and partition
17 integration tests also pass. Spill integration passes 12 and fails one:
`anti_join_not_exists_spill_matches_in_memory` refuses a single 9,928-byte
probe-queue copy with zero queue usage against an 8,192-byte budget. This differs
from the now-fixed multi-batch overlap. The gate exits 101; performance/cap
acceptance remains blocked on resolving or correctly specifying this boundary.
The batch-size/retained-buffer source contract is under investigation; no test
budget or spill expectation has been changed.

## Single-batch layout reproduced

A contained targeted rerun at the same 8,192-byte budget records a probe batch
with 1,171 rows and one Int64 column, no NULLs, offset zero, buffer length9,368
bytes/capacity9,408, and admitted copy-layout charge9,928 bytes. Other partitions
show1,170 rows/9,360-byte payload/9,912-byte charge. Thus this particular refusal
is caused by actual logical payload exceeding the budget, not irrelevant sliced
string/dictionary buffers. The targeted gate exits101 as expected. Diagnostic
source/logs are preserved; temporary instrumentation was removed immediately
after the process exited.

Source inspection also shows that ExecutionConfig.batch_size is not consistently
propagated through main scans: Parquet paths hardcode8,192 rows and MemoryTableExec
forwards provider batches. A general upstream batch-output contract is the next
avenue; it must distinguish immutable provider-owned buffers from query-owned
materializations. Merely slicing inside the queue and retaining an unreserved
remainder is not an acceptable fix. The original test remains unchanged.

## Upstream output sizing correction validated

The unchanged8KiB NOT EXISTS spill test now completes with actual spill and
matching answers. The complete selected gate exits zero:626 passed, one existing
ignored. This includes573 library tests plus7 join-stream,6 materialization,10
reservation,17 partition and13 spill integrations. The new actual-reader test
checks all nullable values across multiple Parquet row groups with<=17 output
rows per batch. Compact fixed-width layout tests check actual nullable decimal
and integer copies against the target allowance and reject invented variable-
width bounds. Source is frozen in queue-scan-source.tar.gz/hashes; only three
source files differ from the accepted scalar candidate.

The release build is active before cap and matched performance gates. The
queue's universal copies and single-demand prefetch may regress throughput;
626 passing tests do not establish performance or full memory ownership.

Cap-coverage review: the current LazyGeneratorExec in oom_cap_harness.rs
declares one input partition. Its aggregate/sort/join runs therefore bypass
the new multi-partition queue. They remain useful existing operator checks,
but cannot certify this queue increment. A scratch-only enhancement is being
prepared with explicitly configured multiple input partitions and exact total
id coverage, unchanged query/process caps, and stronger typed aggregate/sort
result validation. It must be applied/tested after the active release and
latency windows; no new cap result is claimed yet.


## Queue candidate performance rejection (2026-09-06)

The frozen queue/scan release built successfully; benchmark binary SHA256
`eea512ac0c31b25ecfa5cde608cb09aa898cb0522a5a80f2a550971a2e64d603`.
All 526 source identities matched the archived source before measurement.
The alternating screen compared it with the accepted scalar binary `0d5cf9…`
under unchanged 16-thread affinity, query/process caps and typed oracle.

Raw Parquet completed ten pairs each for Q1/Q6/Q9 (66 validated executions
including warmup), then the candidate Q14 warmup exceeded its fresh
1401.316 ms query ceiling. Q18 was not reached. Raw after/before median ratios:
Q1 0.9715, Q6 1.0134, Q9 1.2685. This is an incomplete failed screen.
Decoded IPC completed 110 validated executions: Q1 0.9852, Q6 6.9558,
Q9 1.2071, Q14 2.7652, Q18 1.0577. Passing the absolute ten-times-reference
gate does not excuse these large regressions against the previous candidate.
All completed paired physical plan strings match. Q6/Q14 still use
MemoryTableScan; cache bypass does not explain their IPC regression.

The source now holds one demand permit across upstream try_next, serializing
Filter/Project work as well as queue delivery. This is a concrete architectural
loss of parallelism; its share of elapsed regression still needs controlled
attribution. Copying full exposed slice/dictionary buffers is another possible
cost. The candidate is **not accepted for performance**. It remains in the
uncommitted worktree for investigation; the accepted scalar binary and all
previous evidence are preserved. Do not attribute its prior six-mode scores
to this subsequent candidate.

Evidence: [screen summary](benchmarks/2026-09-06-queue-ownership/screen-summary.json),
paired attempts/plans/manifests and release identities in the same directory;
complete screen archives (including Arrow outputs) are preserved and verified
byte-for-byte against the scratch originals.

The cap harness now has opt-in positive QE_HARNESS_PARTITIONS, exact
contiguous input ranges, complete declared-output consumption, independent
aggregate count and sort ID/value oracles, and actual spill reporting.
Five example tests pass with no skips; formatting passes. Its release rebuild
is active (session 97772). Cap execution is still pending. The source change
is benchmark-only and does not alter the refusal classifier or memory caps.


## Four-partition cap evidence and next contract

The enhanced cap release built and all six scenarios completed with actual spill:
aggregate, sort and equality-filtered join, each under cgroup1GiB and
RLIMIT_DATA2GiB (the existing1GiB cap plus1GiB virtual-startup allowance,
inside8GiB containment). Inputs use4partitions and2M rows (join2Mbuild/1Mprobe),
with32MiB query budget. These are development-sized cap cases, not the default
250M-row aggregate/sort certification. Peak RSS ranged102–247MiB in the driver.
Aggregate independently validates1,000,003 groups and every distinct-ID count;
sort validates all2M IDs, values and order. Join retains its fixture-specific
500,000-row count check, which is narrower correctness evidence. All scenarios
reported spilled=true; no cap, query budget or refusal classifier was loosened.
Full logs, source/binary identities and settings are in the queue evidence folder.

The candidate still fails performance acceptance.
[The bounded parallel ownership design](parallel-input-ownership-design-2026-09-06.md)
specifies guaranteed copied-output bounds for audited resident scan/filter/project
pipelines, pre-admitted slots, conservative fallback, explicit contract violations,
and deterministic overlap/cancellation tests. It is a proposal, not implemented
capability. General nested pipelines and full upstream scratch ownership remain
open; pre-reserving all available memory can itself starve nested operators.


## Implemented resident-pipeline parallel slots (2026-09-06)

The follow-up is now implemented, with default-feature validation passing609
tests and one pre-existing ignored test. Nine new tests cover actual copied-layout
bounds, equal-valued schemas with differing retained metadata capacities, runtime
column resolution, duplicate aliases, and deterministic parallel queue ownership.
A first compile failed on a private-module resolver import; the shared crate-private
reexport fixes it. The Lance/GPU feature gate is running; performance is unproven.

`PhysicalOperator::resident_queue_copy_bound` defaults to None. The new
`physical/queue_layout.rs` derives conservative guarantees from every actual
resident batch, with incremental traversal and a MemoryTableExec OnceLock cache.
Audited Filter and column-only Project wrappers propagate bounds, including
complete dictionary children, exposed string buffers, output validity and schema
metadata. Runtime and bound derivation share column resolution. Unknown views,
nested layouts, computed projections and subquery filters decline the capability.

Eligible queues reserve K×B copied-output bytes under their existing parent pool
before spawning producers, with K limited by partitions and Rayon worker count.
If no parallel envelope fits they retain single-demand actual-byte admission.
They never wait for byte-budget availability. Each producer takes a slot before
upstream polling, checks actual charge against B before copying, and retains the
slot through delivery. A contract violation fails explicitly. Shared reservation
guards survive asynchronous producer cancellation and release at completion.
The deterministic barrier test proves two upstream polls overlap and both copies
can remain queued without a third pull or extra reservation.

This accounts for copied queue outputs. It does not account for retained source
residency, upstream expression scratch, allocator RSS or downstream results.
The resident-pipeline restriction avoids reserving a full envelope above nested
operators that need the same pool. General transferable pipeline ownership remains
open. No dependency, cap, SQL or query-specific routing changes.

The corresponding Lance/GPU selected gate now passes639tests (582library plus
57integration), with one pre-existing ignored test. Formatting passes. Source
snapshot527files is frozen and the release build is active. Previous cap cases
use LazyGenerator inputs and therefore exercise the serial fallback; a resident
fixture cap extension is being prepared to exercise the new parallel capability
explicitly. No claim of parallel cap or performance acceptance yet.


## Parallel candidate screen: resident recovery, remaining rejection

Release binary `2eb501c53d6ecd435e0840c5f21f92b9b036511479b15a23919feae535e4f542`
built successfully in10m19s. All527frozen source identities matched before runs.
Both screens use the accepted scalar binary as control and unchanged canonical
SF10 SQL/data,16threads/affinity0–15,40GiB query/48GiB process/96GiB outer caps.

IPC completed110exactly validated executions. Median after/before ratios:
Q1 0.9870, Q6 1.0447 (80.675vs77.226ms), Q9 1.0408,
Q14 2.6320 (228.860vs86.953ms), Q18 1.0625.
The earlier serial candidate's Q6 ratio was6.96; this resident implementation
recovers that major regression. These are separate alternating screens, not a
three-binary causal experiment. The remaining Q14 regression is unacceptable.

Raw completed66validated executions before candidate Q14's warmup timed out
at a1373.726ms fresh query ceiling; Q18 was not reached. Completed median
ratios: Q1 0.9887, Q6 0.9780, Q9 1.2601. All completed paired physical
plan strings match. Raw Q14 has streaming leaves, outside the new resident
capability. Neither raw failure nor IPC Q14 regression is closed.

The candidate remains uncommitted and not performance accepted.
Complete screens, attempts, plans, binary/source identities and verified archives
are in `docs/benchmarks/2026-09-06-parallel-queue/`.
A source audit of streaming bounds is linked in
[streaming ownership](streaming-input-ownership-audit-2026-09-06.md). It establishes
a possible restricted fixed-width extension, not an implemented guarantee.

The explicit resident cap fixture passes7example tests, with no skips.
Its payload is bounded development-fixture overhead under the process/cgroup cap,
not newly admitted query materialization. It preserves exact rows/partitions and
logs actual copy bounds; capability availability is not direct envelope-selection
telemetry. Release build64917 is active; cap runs and Q14 CPU diagnostic follow
without overlapping latency measurements.


## Resident cap completion and Q14 diagnostic

The resident fixture completed all six development cap cases with actual spilling
under unchanged limits. Peak RSS ranged 127–305 MiB, including fixture overhead.
The measured queue reservation peaks match four times the actual batch-copy bounds;
this is consistent with the source path, while direct selection remains unobserved
in cap telemetry. Seven harness tests pass. This is not complete query-wide memory
ownership certification.

All 16 Q14 diagnostic attempts passed typed validation and fresh time gates.
Steady medians (elapsed / CPU milliseconds): before one thread 183.20 / 180;
after one thread 182.47 / 180; before 16 threads 83.12 / 320; after 16 threads
219.06 / 260. The lost speedup with lower CPU work supports remaining serialization
of nested join output. Source confirms the outer aggregate queue applies its
single-demand fallback to the join. A safe prepared-stream bound after join build
initialization is the next investigation; declaring a nested operator independent
of the pool before its build completes would be unsound.

[The candidate evidence index](benchmarks/2026-09-06-parallel-queue/README.md)
contains the readable results, limitations, source/binary identities and complete
verified artifacts. No heavy jobs are active. The candidate is still not performance
accepted; full provider/public/resource gates and the overall objective remain open.
