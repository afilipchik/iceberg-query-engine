# Serial CPU work behind a multi-partition aggregate input

The active join measurements exposed a broader execution bottleneck than hash
lookup or output gathering. The five-query follow-up completed10/10 independent
typed comparisons and dataset checks with the frozen profiling executable.
It adds existing queue/aggregate traces and process CPU accounting, without
changing the16 execution threads, CPUs0–15 or4/12/32GiB budgets.

| Query | Process CPU utilization | User + system seconds | Process wall seconds |
|---|---:|---:|---:|
| Q5 |120%|7.04|5.85|
| Q9 |125%|15.69|12.52|
| Q10 |134%|8.63|6.42|
| Q12 |101%|3.01|2.95|
| Q13 |300%|29.84|9.92|

Each process executes two instrumented queries, and these process figures also
include startup/serialization.100% represents one CPU core. This is evidence
of low realized CPU parallelism for the first four queries, not a latency score
or a measurement of exclusive operator CPU.

## Source mechanism and reproducer

`physical/morsel_agg/live_spill.rs::execute` opens partitions through
`FuturesUnordered` and merges their streams with `SelectAll`. Both collections
are polled by one aggregate future. They permit overlapping asynchronous waits,
but synchronous work in a child poll executes on that same task. In particular,
`StreamingParquetScanExec` calls Arrow's synchronous `reader.next()` inside its
stream future. The aggregate's subsequent expression evaluation and state
ingestion also execute in the consumer's control flow. Adding more declared
partitions therefore does not itself parallelize these source polls.

This differs from the inspected DuckDB pipeline scheduler, which creates multiple
`PipelineTask` instances when source/operator capabilities permit parallelism:
`/media/afilipchik/nvme6tb/src/duckdb/duckdb/src/parallel/pipeline.cpp`,
`Pipeline::ScheduleParallel`, in the previously recorded January checkout.

The new `tests/fused_input_cpu_parallelism.rs` isolates the mechanism with four
partitions, four batches each, an explicit pool-independent copied-output bound,
four Tokio workers and four Rayon threads. Each synchronous source poll records
overlap while briefly sleeping. The test asserts actual overlap, not a speed
threshold. Exact duplicate/NULL SUM results and exactly one execution per
partition pass; the final overlap assertion fails because peak overlap is1.

Command: `TMPDIR="$PWD/.scratch" RAYON_NUM_THREADS=4 SAFE_BUILD_MEM=48G
SAFE_BUILD_JOBS=1 scripts/claude-safe-build.sh cargo test --locked
--features lance,gpu --test fused_input_cpu_parallelism`.
Result:0 passed,1 failed,0 ignored; exit101. Log:
`.scratch/join-stream-profile/serial-input-reproducer.log`. The failure is
intentional evidence of the unresolved contract, not a green regression gate.

## What the extra traces rule in and out

The304 queue records show that join-build copying is small for Q5/Q12 and costs
about332ms cumulatively across both Q9 requests. Q10/Q12 also have existing
single-slot input queues where copied-output bounds are unknown. The live
aggregate does not use those queue tasks for its own input frontier.

Aggregate ingestion per request is approximately17ms for Q5,544–560ms for Q9,
598–602ms for Q10 and47ms for Q12. Q13's inner aggregate retains its parallel
state updates at1.672–1.675s; its outer aggregate is247–253ms. The inner-join
profiler does not cover Q13's collected LEFT probe. These nested wall observations
do not quantify the exact speedup available from a new input scheduler.

## Required repair and acceptance

Implement an owned, bounded input frontier that can schedule independently
pollable partition streams on separate executor tasks. Reuse the existing
prepared-output and query-pool contracts; do not simply spawn one unbounded
producer per partition or remove conservative unknown-layout limits.

1. Establish output capabilities before pulls. A prepared source may already
   own initialized streams and build-state reservations; retain those exact
   streams, including when its output bound is unknown. Never call ordinary
   `execute` again after preparation to reconstruct an already-consuming input.
2. Admit the complete active frontier before concurrent pulls: task metadata,
   evaluated/copied pending batches, handoff scratch and retained outputs. The
   number of workers follows the available query budget. Keep the existing
   serial path when only one slot fits or capability is unknown. A copied-output
   bound does not bound arbitrary provider decoder scratch or retained sources.
3. Keep one owner for each stream and pending batch. Move a stream back to the
   frontier after its batch is consumed; preserve backpressure before the next
   pull. Carry reservations across task completion and consumer handoff. Do not
   discard a charge before the receiving owner takes over.
4. Propagate the original source error. Stop and join owned tasks before an
   execution error returns; cancellation must leave no detached producer or
   retained spill/state owner. Preserve existing all-partition and no-replay
   regressions, including pending peers and panic-after-prefix cases.
5. Make the new overlap regression pass, then extend it for budget-limited
   concurrency, unknown capabilities, prepared streams, empty/multiple batches
   and cleanup. Run existing real-spill, exact decimal/AVG, dictionary and input
   failure gates before release measurements.
6. Freeze and compare the five unresolved queries plus protected Q20 and the
   provider/resource suites. Require actual concurrent input work in traces and
   independent typed equality. Keep all baseline timeouts. Do not infer success
   from synthetic overlap alone or relax the existing memory/time gates.

This report preserves the failing reproducer. The subsequent
[bounded input candidate](parallel-aggregate-input-2026-09-08.md) makes it green
and passes896 library plus39 integration tests. Optimized performance and
resource/provider validation remain pending; preserve this earlier red evidence.
