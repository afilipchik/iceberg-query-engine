# Aggregate state processing does not scale with input threads

The frozen borrowed-output binary completes canonical SF10 Q13 in10.693 seconds
at1 thread and7.340 seconds at16 threads, only1.46× faster. Its inner aggregate
ingestion remains approximately4.4 seconds at every thread count. All18 outputs
pass the independent typed DuckDB oracle. This isolates a concrete limitation:
the current live grouped aggregate has concurrent input streams but one
`IngestionController` executing all state updates synchronously.

| Threads | Query median ms | Inner ingestion median ms | Inner finish ms | Inner output ms |
|---|---:|---:|---:|---:|
|1|10693.402|4420.877|113.579|109.458|
|4|7897.232|4511.679|113.446|109.241|
|16|7340.390|4404.443|111.120|106.987|

Each configuration has two fresh processes, one warmup and two subsequent
requests per process: four steady samples and two warmups per thread count.
Orders are1/4/16 then16/4/1. CPU affinity remains0–15; only configured execution
threads change. Query/process/cgroup budgets are4/12/32 GiB. Input SQL, data,
oracle and binary remain identical. `QE_AGG_PROF=1` is enabled; finer sampling
and input-queue tracing are disabled. These are diagnostic wall intervals, not
exclusive CPU samples or acceptance latency. Output is included in finish.

The inner aggregate consumes15,345,388 rows in1,832 batches and emits1,500,000
groups. It reports zero ingestion spill bytes. Its outer aggregate consumes
those groups in23,438 batches and returns45 groups. The large inner state
processing time is not explained by output construction or spill I/O in this run.
The provider pre-scan error repair and IPC admission guard are absent from this
frozen binary; do not attribute this diagnostic to an unbuilt working-tree release.

## Rejected small optimization: changing the byte hash

The current key implementation hashes canonical bytes using `DefaultHasher`.
An isolated optimized Rust probe compares that exact slice-hash operation with
the already-available xxHash64 dependency, using262,144 distinct byte strings,
32 repeated passes and six alternating-order blocks on CPU0.

| Encoded-width bytes | DefaultHasher ns/hash | xxHash64 ns/hash |
|---|---:|---:|
|9|5.378|2.800|
|27|7.428|4.035|
|41|9.241|5.560|

These are synthetic width-matched inputs and hash-only timings; they do not
exercise encoding, lookup, collisions, state updates or full queries. At9 bytes,
the arithmetic saving over15,345,388 hashes is about40ms. Cache and integration
effects could differ, so this is not a guaranteed bound or a query speedup.
It nevertheless does not justify a production hash change as the next response
to4.4 seconds of state processing. Production hashing remains unchanged.

## Source interpretation and next implementation

DuckDB's inspected `GroupedAggregateHashTable::AddChunk` hashes groups in a
batch and `UpdateAggregates` dispatches aggregate updates over resolved state
addresses. Its radix aggregate sink coordinates local states and reservations.
ClickHouse's `executeAggregateInstructions` dispatches batch operations over
state places. These sources separate input parallelism from state processing;
merely opening more sources does not parallelize this engine's controller.
The exact local snapshots remain DuckDB1c27c54f and ClickHousea1b25f3f,
both January2026 snapshots, not the benchmark's DuckDB1.4.4 wheel.
[Local source comparison and links](local-engine-source-comparison-2026-09-07.md).

The next experiment should partition evaluated rows by complete canonical group
key into bounded worker controllers. Equal keys must always reach one worker;
hash collisions still require exact equality within that worker. This avoids
workers multiplying high-cardinality state and avoids cross-worker final-value
merges that would corrupt AVG and other partial aggregates. Existing per-worker
spill/compaction/partition merge remains mandatory. Routing itself adds serial
work and must be measured; no speedup is promised before an actual comparison.

Implementation contract and gates live in
[the existing epic update](../.claude/epics/realistic-benchmarks-duckdb-leadership/updates/2026-09-08-parallel-state-experiment.md).
No engine code or dependency changed during these diagnostics. Broader workload,
provider, resource/concurrency and DuckDB leadership acceptance remain open.

## Reproduction and evidence

Both probes run with repository `.scratch` as TMPDIR through
`scripts/claude-safe-build.sh`; the thread probe uses `taskset -c 0-15`, the
hash probe `taskset -c 0`. The thread driver is
`.scratch/live-schema-boundary/aggregate_thread_probe.py`; the hash source is
`key_hash_probe.rs` alongside it. The latter compiles with rustc1.93.0,
`--edition=2021 -C opt-level=3`, linked to the existing release xxhash-rust rlib.
[Archived inputs, commands, samples and checksums](benchmarks/2026-09-08-aggregate-scaling/manifest.json).
