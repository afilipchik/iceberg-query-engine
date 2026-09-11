# Bounded outer joins and retained-input progress — 2026-09-09

## Scope and status

This is an uncommitted implementation following the frozen `b61ce3d3` diagnostic
in [collected join attribution](collected-join-profile-2026-09-09.md). It is not a
performance result or provider certification. HEAD remains `88849c4f` plus the
preserved working tree. No dependency changes belong to this implementation.

The preceding diagnostic found a 16-partition outer join feeding a one-slot
aggregate input frontier because it could not describe admitted output ownership.
The repair addresses that shared execution boundary, without recognizing query
numbers or using data estimates as semantic proofs.

## Implementation contracts

- `physical/operators/hash_join/outer_probe.rs` implements incremental LEFT,
  RIGHT and FULL equijoins for supported primitive column keys and flat output.
  It retains a probe batch and resumable hash-chain cursor, emits bounded chunks,
  and records matches only after the ON predicate succeeds. It does not collect
  the complete probe side or the complete joined result.
- A per-execution round tracks claimed/completed partitions and matched build
  rows. The last successful partition emits unmatched build rows. Cancellation
  invalidates the round; repeated complete executions use fresh match state.
  Unsupported computed-key/output combinations retain the guarded existing path.
- `storage/admitted_selection.rs` builds nullable selected output with owned
  reservations, checked dictionary resolution and exact primitive representation.
  Buffer clones and slices retain admission until their last owner drops.
- Prepared outer output requires a transitive admitted child descriptor. A copied
  output-size estimate alone is insufficient. The spill wrapper delegates only
  after its existing in-memory/spill decision; default spill policy is preserved.
- `scan/admitted_memory.rs` shares immutable registered batch headers through an
  Arc and copies selected output into admitted buffers. `filter/admitted_input.rs`
  propagates the capability through supported compiled predicates. Unknown
  layouts/predicates decline before consuming source output.
- `MemoryPool::prepaid_child` reserves input working capacity before downstream
  aggregation grows. Descendant allocations use that capacity without charging
  the parent twice; descendant buffers retain its reservation. This is bounded
  accounting, not proof that all query allocations or resident source storage
  are reserved.

## Regression discovered during integration

The first source adapter attached a semaphore credit to each output frame's
buffer lifetime. A collecting consumer can retain those buffers until EOF.
After consuming every credit, the source waited for the consumer, while that
consumer waited for EOF: output ownership had incorrectly become a scheduling
prerequisite. Each tiny batch also retained a full maximum-sized frame.

Broad run 90646 made no further progress in 13 library tests for more than eight
minutes and was terminated with SIGTERM. Its debugger attach was unavailable;
there is no captured stack proof. The dependency follows directly from the source
and consumer contracts. Logs are in `.scratch/parallel-aggregate-input/`:
`outer-pipeline-headroom.log` and `outer-pipeline-headroom-stacks.log`.

The source now admits actual live buffers directly against its reserved domain.
It never waits for retained outputs to be dropped. Exhaustion returns a named
memory refusal. Queue backpressure remains the consumer queue's responsibility.
Run 39423 (`outer-pipeline-no-credit.log`) completed in 16.60 seconds:
**1,016 library passes, zero failures, 11 ignored**. This includes the formerly
stalled spill joins and high-cardinality aggregates. The preceding adapter also
exposed 16 regressions (source header duplication and unprotected input capacity);
those failure logs remain preserved, not reclassified as historical spill failures.

## Validation and next gates

Before the resident/filter adapters, run 45946 passed 1,012 library tests
(11 ignored) and 67 selected integrations. The new outer tests cover independent
nested-loop oracles for LEFT/RIGHT/FULL, duplicate and NULL keys, empty sides,
multiple partitions, repeat execution, cancellation, late successful ON matches,
large fanout, slow consumption and buffer lifetime. A pre-change control failed
its high-fanout guard before producing output; the bounded candidate passed.

The two new source regressions pass: retaining 128 batches through EOF and prompt
named refusal when the reserved input domain is exhausted. Gate 99893 passed
1,018 library tests but exposed two native integration regressions: optional
compiled-filter preparation attempted a 277KB program under a 256KiB query cap.
Because no child had been touched, declining that optional capability on memory
refusal correctly preserves ordinary execution without replay or reduced safety.

Final selected gate **60127 exits 0: 1,018 library passes, 11 ignored, and 67
integration passes**, including all ten native streaming tests. Gate **16583 exits
101: six spill integrations pass and the same seven historical names fail**.
The denial sizes include whole-page 120,512-byte allocations and 8KiB-query
capacity; identical names do not establish identical allocation boundaries.
Formatting and whitespace checks pass. Every test ran through the memory-capped
wrapper with `--locked --offline --features lance,gpu`, repository TMPDIR,
RAYON_NUM_THREADS=4, SAFE_BUILD_MEM=48G and SAFE_BUILD_JOBS=1.

[Immutable implementation evidence](benchmarks/2026-09-09-bounded-outer-pipeline/manifest.json)
contains 13 verified files and a snapshot of all 500 source inputs, including the
failed and interrupted gates. Release79400 completed in8m48s at2026-09-09T18:05:37.522220Z. Binary SHA256
`7b5b7e93f893269df4df151c767c0982406252b8c59caf2cdd352eccc60fc7a3`; all500
inputs verified. Diagnostics57985 and49264 completed with exit0. See measurements below;
this is not performance certification.

Next: finish the ownership gates, inspect per-row gather overhead, freeze a new
release, and repeat the matched Q12/Q13 attribution with typed output validation.
Only then run paired performance and protected-query gates, followed by separate
raw Parquet, native, Iceberg, Lance, decoded IPC and GPU acceptance. No DuckDB
leadership claim follows from removing a materialization boundary alone.

## Reproduction commands

Run sequentially from the repository root:

```sh
TMPDIR="$PWD/.scratch" RAYON_NUM_THREADS=4 SAFE_BUILD_MEM=48G SAFE_BUILD_JOBS=1 scripts/claude-safe-build.sh cargo test --locked --offline --features lance,gpu --lib --test outer_stream_contract --test outer_on_pushdown --test hash_join_streaming_tests --test hash_join_initialization_ownership --test prepared_join_contract --test prepared_row_store_contract --test streaming_prepared_join_contract --test partition_contract --test native_streaming_scan_tests
TMPDIR="$PWD/.scratch" RAYON_NUM_THREADS=4 SAFE_BUILD_MEM=48G SAFE_BUILD_JOBS=1 scripts/claude-safe-build.sh cargo test --locked --offline --features lance,gpu --test spill_tests
cargo fmt --all -- --check
git diff --check
```

The eleven library ignores are existing ignored tests, not successful coverage.
The selected integrations have no ignored tests. GPU-feature compilation does not
establish GPU execution; the next residency gate must retain device evidence.

## Next CPU hypothesis from local source review

Local DuckDB checkout `1c27c54f27bea397d18d86f7c0d6a9f1d4aa85f8`,
`src/common/types/row/tuple_data_scatter_gather.cpp:1812`, selects a templated
primitive gather function by physical type; its implementation starts at line
1373. This is checkout-specific evidence, not the pinned benchmark wheel's
source identity. The candidate's primitive selection currently resolves each
selected cell again after its separate validity pass and downcasts per value.
If phase measurements attribute meaningful time here, bind typed source views
once per column/batch and combine validity/value traversal. Retain checked
dictionary addressing, exact float/decimal bits, NULL extension and admission.
No such optimization has been included in the frozen build or measured yet.

## Frozen diagnostic outcome

Primary57985 and supplemental49264 both exit0. All **18 completed outputs are
typed-correct** against the saved complete DuckDB oracles. Both runs verify the
canonical SF10 dataset, native/IPC provider inventories, binary and all500 source
inputs before and after execution. CPUs0–15 and16 threads match the preceding
instrumented diagnostic. Raw/native use4GiB query/12GiB process; preloaded CPU
uses32GiB query/48GiB process. The180s watchdog remains diagnostic, not the10×
DuckDB acceptance ceiling. Preparation and result serialization are excluded.

| Mode | Candidate Q12 ms (two requests) | Candidate Q13 ms (two requests) | Preceding Q13 ms |
|---|---:|---:|---:|
| Raw Parquet |1067.6 /1046.8|2648.4 /2653.5|4280.9 /4295.0|
| Native |496.4 /503.9|2416.1 /2423.7|4480.9 /4675.4|
| Preloaded CPU,32GiB |1063.7 /595.3|2741.3 /2250.1|4850.2 /4420.3|

These are two instrumented requests in sequential diagnostic runs, not randomized
paired confidence intervals. Q13 is encouraging, but **preloaded Q12 regresses**:
the preceding values were583.7 /185.9ms. Do not accept or advertise this candidate
as a general improvement, or infer DuckDB leadership.

All six supplemental Q13 requests show the outer `SpillableHashJoin` frontier
with16 partitions,16 slots and admitted_buffers=true. The preceding frozen
candidate showed16 partitions,1 slot and admitted_buffers=false. Supplement Q13
values are raw2759.1 /2666.8ms, native2441.8 /2392.0ms and resident2725.6 /2345.6ms.
The outer aggregate above the grouped result still has its separate single
partition; the trace is not evidence that every stage now runs16 ways.

Preloaded Q12 build initialization receives the same310,803 rows but7,323 batches
instead of916. Its probe rows and batches remain unchanged. The new resident
source copies before filtering and imposes an8,192-row quantum; extra copying
and eightfold filtered-batch fragmentation are concrete next attribution targets,
not yet exclusive measured CPU causes. Preserve these failing timings.

Primary scope peak19,523,506,176bytes; supplemental15,066,664,960bytes. Both have
zero max/OOM/kill events and swap disabled. This does not clear query-wide
reservation gaps or lower-budget capacity tests. The
[diagnostic archive](benchmarks/2026-09-09-outer-pipeline-diagnostics/manifest.json)
contains243 verified files and all500 frozen source inputs, full output/oracles,
traces, setup, drivers and harness. All jobs are terminal.

## Next implementation sequence

1. Attribute resident source copying and filtered-batch fragmentation with the
   same frozen Q12/Q13 setup. Preserve the current candidate and oracle outputs
   as the control; do not hide the Q12 regression behind a suite average.
2. Remove per-cell type dispatch from shared admitted selection by binding checked
   typed source views once per column/batch. Fuse primitive validity/value loops;
   preserve dictionary/NULL/slice/decimal/float representation and buffer ownership.
3. Replace the fixed resident8,192-row ceiling with a byte-admitted quantum that
   can preserve useful source batches and shrinks transactionally under pressure.
   Do not attach output-lifetime credits or increase query memory limits. Evaluate
   predicate-before-copy only with an explicit immutable-source ownership contract.
4. Repeat the library/native/outer/ownership/spill gates, then freeze and measure.
   Require restored resident Q12 behavior as well as preserved Q13 output and
   parallelism. Follow with randomized paired protected-query and full provider,
   residency, resource and concurrency gates from the existing epic.

These are shared operator/source changes, not query-specific dispatch or SQL rewrites.
