# Next shared CPU boundary profile — source-only, 2026-09-06

No engine/build/profile jobs or main edits. Historical paired evidence belongs to 65becb409a176c3358bdf9fd0d15dd00817c6fedb5fc19b0ce73143820b9db66, before current recursive Inner preparation/decimal/binder repairs. Source inspected during the frozen585 release build. New screens must establish which regressions survive; this note does not attribute old timings to the new source.

## Recorded evidence

Source document: docs/prepared-pipeline-regressions-2026-09-06.md. Samples: .scratch/public-bench/prepared-pipeline-cpu-screens-01/{raw_parquet,decoded_ipc}/attempts.jsonl, response.physical_plan/optimized_plan/metrics, three steady requests (iterations1–3).

| Historical track/query | Scalar steady wall ms | Pipeline steady wall ms | Pipeline execute ms | Reserved peak bytes |
|---|---|---|---|---|
| raw Q12 |179.659,190.677,188.167|905.544,898.561,900.723|894.380,895.639,891.959|13,480 each|
| raw Q21 |858.367,813.236,868.372|2672.096,2732.324,2706.513|2629.131,2655.204,2662.862|1,073,552 each|
| IPC Q21 |387.528,319.353,392.140|1169.830,1219.019,1201.196|1160.772,1198.414,1192.200|8,528,272 each|

The slowdown is predominantly execute time in these samples, not optimizer time. Reserved peaks show accounted buffers existed; they do NOT identify which queue, selected slot count, spill state or CPU concurrency.

Q12 physical shape: Sort→Project→SpillableHashAggregate→Inner SpillableHashJoin→(Project→StreamingParquetScan,StreamingParquetScan). Required surviving columns include l_shipmode and o_orderpriority. Q21 in both modes: Sort→Project→Aggregate→Anti→Semi→nested Inner joins. Raw leaves mix eager MemoryTable and streaming; IPC leaves MemoryTable. The Inner association differs between modes, so do not infer identical build orientations.

## What current source proves

1. stream_merge_input_partitions (spillable.rs553–638) bypasses zero and one declared partition BEFORE preparation. For multiple partitions it uses PreparedOutputBound.max_bytes, else pool_independent_queue_copy_bound; only an admitted2+slot envelope permits concurrent pulls. Unknown/declined/insufficient shared budget gives one demand permit held through pull/copy/send. This is a genuine serial upstream-poll boundary, but only for queues with multiple declared partitions.
2. StreamingParquetScan fixed_output exists only for IPC-ineligible scans and supported fixed-width projected layouts (streaming_parquet_scan.rs306,338–348). Q12's required strings on both sides prevent that static capability irrespective of build orientation. Project cannot derive a gather proof if its child lacks one (project.rs121). Current Inner preparation falls back to probe.pool_independent_gather_copy_bound when recursive preparation returns None (hash_join.rs1556). Thus the recorded Q12 shape still statically declines join copied-output preparation; with multiple declared output partitions its aggregate input queue takes serial demand. Row count/shipmode filter values do not establish a bound. This repeats the known variable-width boundary only to classify it; see existing runtime-variable-output design for broader scope.
3. Recursive preparation now exists for Inner (hash_join.rs1486–1597), replacing the outdated static-only limitation in the historical document. It completes its build first, consumes child prepared streams once, composes every physical layout/gather variant, and retains Some Unknown streams on failed metadata composition. Filter and column/alias Project propagate this; subquery predicates/computed projections remain explicit preflight barriers.
4. Current static Inner declines: non-Inner, subqueries, partition mismatch, cached multi-batch build, row_store, missing VHT, empty build, unavailable gather metadata or unsupported composition. Spillable wrapper additionally declines BuildDecision::Spill (spillable.rs1163). Runtime build cache/layout and shared-budget availability are NOT recorded by the historical plain physical plan.
5. Q21's Anti and Semi explicitly declare ONE output partition (spillable.rs1151), and reject preparation. Therefore the aggregate→Anti and Anti/Semi one-output boundaries cannot be blamed on the multi-partition queue demand semaphore: the queue bypasses it. Non-Inner decline is proven, serialization caused by that decline is NOT. HashJoin's Semi/Anti route collects ALL probe partitions via an owned JoinSet (hash_join.rs24,1638–1708), optionally overlaps collection with build; runtime-filter linkage deliberately defers probe until publication. Its filtered Semi/Anti kernel can run parallel (probe_semi_anti_parallel, call~4100). Whole-probe materialization/finalization still exists; one output partition is not proof of single-thread execution.
6. For Q21 the relevant remaining queue candidates are deeper build-decision queues (spillable.rs1366), and spill probe queues if spilling actually occurs (1579). Current recursive Inner may restore their parallelism on IPC. Fixed-width raw probes may also qualify; retained supplier strings may prevent a raw leaf proof when they are on the probe side. The plan alone does not show build_right/retained cache schema. A large null-free unswapped fixed-width build can select row_store (hash_join.rs1287), which explicitly declines preparation even on resident input. Record it rather than guessing it occurred.

## Minimal diagnostic once fresh screens identify survivors

Keep screen binary/SQL/data/typed oracle/caps unchanged. First one-thread versus16-thread isolated requests with own-process CPU time, wall time and all valid failures retained; do not run alongside other latency work. Existing RS_DEBUG build-drain output can expose build rows, batch count, VHT, row_store, key types and swapped orientation, but diagnostic env timings belong to a separate run.

The smallest useful diagnostic telemetry must attach a stable operator path/id, not only the repeated labels "join build input queue":

- Per queue: declared partitions; preparation None/Layouts/Unknown + decline reason; preparation duration; bound bytes; selected slots; failed envelope admission versus unsupported bound; copied bytes/batches; aggregate permit-wait and upstream-poll durations; maximum overlapping pulls. This separates proof decline from oversized proof/shared-budget contention.
- Per join: build orientation/type; build-decision/cache duration; in-memory/spill; rows/batches/actual physical retained schema; row_store/VHT; runtime filter linked/published; probe collect duration/rows per partition; probe-kernel CPU/wall and matched candidates/output rows. Separate await/build duration from actual gather/hash/filter CPU.
- Query: process CPU/wall and existing execute/plan/optimizer intervals. Do not sum nested wall intervals as exclusive time; process CPU and concurrency counters distinguish waiting from work.

Interpretation: slots1 + maxpull1 + weak16thread scaling on a multi-partition Inner build queue supports remaining admission serialization; slots>1 but large copy/gather CPU supports ownership-copy overhead; Q21 long deferred probe decode with low admitted row reduction supports runtime-filter effectiveness investigation; parallel probe CPU dominating after collection points to shared filtered Semi/Anti kernel, not a queue fix. row_store decline is a distinct bounded capability experiment. None of these signals licenses relaxing byte ownership or extending Semi/Anti preparation without tracker/finalization proofs.

No new variable-output quantum design, new GPU claims, performance acceptance or full memory certification is made here.
