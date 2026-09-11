# Aggregate state layout: measured next step

No new engine state representation is implemented in this checkpoint. Frozen653
remains the validated production source. Component profiling and a standalone
layout model support a dense accumulator arena as the next bounded experiment.
They do not establish a production speedup or close existing performance gates.

## Cross-provider evidence

Session56436 completes120typed/time-correct requests across15cases: Q18/Q13/Q1,
each on native, raw Parquet, Iceberg, Lance and extra IPC. Paired frozen649/651
workers preserve one warmup and three steady samples per side. The following
medians describe the dominant Q18 finalization interval, in milliseconds:

| Required track |649|651|Ratio|
|---|---:|---:|---:|
| Native: largest-group fused finalization |375.494|312.498|.832232|
| Raw Parquet: morsel merge/output/HAVING |605.603|469.851|.775841|
| Iceberg: morsel merge/output/HAVING |581.560|483.765|.831839|
| Lance: largest-group fused finalization |373.992|308.432|.824701|

Thus the previous direct decimal output change reduces this component in all
four required CPU tracks. Remaining finalization is still substantial. These
intervals include construction, filtering and destruction; nested intervals and
worker durations must not be summed as exclusive CPU. Process CPU/RSS samples
have tick quantization and delivery limitations. IPC is excluded from required-
track weighting. This instrumented run is not latency acceptance: for example,
raw Q1's three-sample query ratio is1.137045, while Lance Q18's is1.020145 despite
lower finalization cost. Preserve these observations rather than presenting only
favorable query timings. The earlier balanced screen and future current-source
full screen remain distinct evidence.

Per-request log byte ranges, all phases, CPU/fault samples and warmups are in
`.scratch/public-bench/decimal-output-component-profile-01/` and
`.scratch/decimal-output-repair/component-analysis-15.json`. The analyzer checks
sample membership and recorded typed/time outcomes. Every process is terminal.

## Representation experiment

Current raw grouped state stores `HashMap<u64, Vec<AccumulatorState>>`, creating
one separate accumulator-vector allocation per group. Existing phase profiling
also identifies substantial state destruction after output. A standalone model
compared this layout with inline hash values and a key-to-index map plus dense
state vector. All use the same deterministic hasher,15million keys, duplicate
updates and independent checksum; a64-byte tagged state includes a drop-bearing
alternative. This models layout, not the engine's full enum/SQL semantics.
Three fresh-process repetitions rotate layout order. Each child has a4GiB data
limit under8GiB containment onCPU0. All9runs complete and checksums pass.

| Layout | Build ms | Update ms | Destroy ms | RSS KiB |
|---|---:|---:|---:|---:|
| Boxed vector per group |1439.624|633.032|553.429|2255244|
| State inline in hash entry |1071.064|496.232|203.690|2656292|
| Dense state arena, key-to-index map |700.739|586.065|94.285|1496640|

At this hash-table capacity, inlining increases RSS even though destruction is
faster. The arena avoids both per-group allocations and oversized empty hash
buckets. Do not copy these model timings into engine performance claims. The
model uses std HashMap and a synthetic state; the engine uses hashbrown and
additional semantics. Source, binary digest, commands, all raw samples and
summary are preserved in `.scratch/aggregate-state-layout-probe/`.

## Required implementation boundary

1. Introduce a key-to-group-index raw state table with a contiguous, fixed-arity
   accumulator arena. Keep group identity and aggregate input types explicit.
   Preserve zero aggregates, one/multiple aggregates, all-NULL groups and the
   existing specialized float path; no SQL text or query-ID dispatch.
2. Admit actual hash metadata and arena capacity through the owning query pool
   before allocation/growth. Growth must cover old and new storage together;
   checked offsets and fallible allocation must refuse by name. Do not rely on
   MemoryPool::observe, group-count estimates, or a process-only pool as proof.
   Thread the query owner through state creation as well as finalization.
3. Make ingestion failure propagate through batch/range processing and worker
   completion. Do not hide admission errors by switching to an unreserved path
   or replay partially consumed input without a replayability contract.
4. Audit every consumer: perfect-hash overflow, raw/general conversion, null
   groups, float-state demotion, disjoint and shared merge, dense/raw shards,
   output iteration, state clone, error/cancellation cleanup and disk fallback.
   Materializing per-group vectors during merge would reintroduce the target cost.
5. Validate duplicates, NULLs, dictionary encodings, empty global/grouped output,
   decimals beyond f64 precision, signed scale, precision/overflow and false
   HAVING. Assert real spill, shared-query refusal, ancestor charges and cleanup.
   Use independent typed oracles. Preserve existing semantic identity fixes.
6. Freeze separately from653. Measure the same component and protected workloads
   with balanced startup/execution order, then full current-source provider and
   resource gates. The repeated647/649 Lance Q17 regression remains open and
   cannot be cleared by an unrelated Q18 layout gain. If the bounded experiment
   fails, preserve it and follow the existing epic's escalation criteria.

No dependency, allocator policy, THP setting or production engine code changed
for either experiment. This is a measured implementation boundary, not completed
query-wide memory accounting or DuckDB leadership.


## Storage prerequisite implemented

Current source introduces `execution/reserved_vec.rs::ReservedVec<T>` and routes
`ReservedBufferBuilder` through it. Unlike the Arrow-only builder, its storage
accepts drop-bearing values. Growth admits old plus new payloads, moves values
without cloning, then frees the old allocation before releasing its charge.
Capacity/length arithmetic is checked. Refusal preserves existing contents and
capacity. Fallible initialization rolls back partial appends and destroys their
values. Zero-sized elements require no payload reservation. Existing Arrow
bulk-copy/direct-fill paths and zero-copy lease transfer are preserved.

The outer vector allocation and512-byte owner envelope are covered; heap storage
inside individual elements is not. No aggregate table, shard representation or
query-pool construction path has switched to an arena yet. This refactor does not
establish an engine performance gain, and frozen653 is still the last release
candidate with optimized validation. Hash-table capacity admission and fallible
ownership across ingestion, conversion, merge and sharding are the next work.

Final default-feature validation passes719library tests and36integration tests
with no failures. The library retains two existing ignores: `test_flatten_exists`
and the isolated-IPC test requiring `QE_IPC_CACHE=auto`; neither is counted as a
pass. Five new library tests cover drop-bearing relocation, peak/ancestor charges,
transactional refusal, partial initialization cleanup, lazy initialization bounds,
overflow and zero-sized values. Existing integration tests cover buffer ownership,
query reservations, decimal aggregate fallback/spill, coercion and projection
admission, and escaping result buffers. `cargo fmt --all -- --check` and
`git diff --check` pass. Tests run through the48GiB/jobs1 safe-build wrapper with
repository scratch asTMPDIR. Final source copies, before/after patch, command and
revision metadata and complete test logs are in
`docs/benchmarks/2026-09-07-reserved-state-storage/`; no release performance result
is attributed to this refactor.


## Arena candidate now in production paths

The subsequent source candidate integrates flat raw state through ingestion,
sharding, dense/hash merge and output, with query-pool ownership and fallible
error propagation. See [implementation and current validation](aggregate-state-arena-2026-09-07.md).
The storage-prerequisite evidence above remains historical; no optimized arena
performance acceptance is implied by its tests or by the standalone model.
