# Collected join phase attribution

The current `HashJoinExec::execute` collects non-inner probe input, performs a
candidate admission preflight for outer/cross variants, computes a complete
result vector, and appends unmatched build rows before returning its stream.
The earlier `HJ_TIMING` interval began after the preflight; the existing opt-in
inner-stream profiler did not cover this work. This was a measurement gap,
not evidence that the preflight was the dominant bottleneck.

## Instrumentation contract

`QE_JOIN_STREAM_PROF=1` now emits `collected_left`, `collected_right`,
`collected_full`, `collected_cross`, `collected_semi`, `collected_anti`,
`collected_single` and `collected_mark` route records through the existing
`[join-stream]` JSON channel. The added wall intervals are `build_cache_wait_ms`,
`preflight_ms`, `collected_probe_ms` and `unmatched_build_ms`. `input_wait_ms`
covers probe collection or the remaining prefetch wait. Cache wait includes
initialization or waiting for another initializer and overlaps the existing
`build_initialization` record. Semi/anti prefetch can overlap cache waiting.

Collected probe time includes key evaluation, traversal, filtering, match
tracking, gathering and result-vector construction. Those subphases are not
separately measured by this change. `candidates` is null for collected routes,
not a misleading zero. `completed=true` means execution produced its collected
result successfully, not that its caller consumed every returned batch.
Errors and cancellation retain `completed=false`; the flag does not distinguish
their reasons. Concurrent/nested wall intervals cannot be summed as exclusive
CPU time or as an additive query critical path.

The candidate guard, key equality, filter ordering, partition completion,
allocations, admission and default execution policy remain unchanged. No new
module or dependency. The only engine source changed versus frozen `11d16e73`
is `src/physical/operators/hash_join.rs`.

## Validation

All invocations use repository `.scratch` as TMPDIR and
`scripts/claude-safe-build.sh`, with48GiB cap, one build job and Rayon4.
Commands use `--locked --offline --features lance,gpu`.

- Initial compilation21277 exposed missing Single/Mark diagnostic match arms;
  both were added before the successful gates below. This was an instrumentation
  compile error, not a reproduced engine semantic failure.
- Integration62066 with tracing enabled: `cargo test --test outer_on_pushdown
  --test hash_join_streaming_tests --test hash_join_initialization_ownership
  --test partition_contract -- --nocapture`:32 passed, no failures/ignores.
- Library79661 with tracing disabled: `cargo test --lib hash_join`:32 passed,
  no failures/ignores,987 filtered out. This is a focused gate, not a new full
  library/provider/resource certification.
- Separate stderr parses57 JSON records:22 completed build initializations,
  15 completed inner probes,6 completed left,3 completed right and1 completed
  semi;5 incomplete builds,2 incomplete inner probes and one each incomplete
  left/semi/anti. Existing candidate-refusal and cancellation regressions pass.
- Formatting and whitespace checks pass.

## Architectural follow-up

Local DuckDB checkout `1c27c54f27bea397d18d86f7c0d6a9f1d4aa85f8`,
`src/execution/join_hashtable.cpp`: `ScanStructure` owns vector-sized selection
and match state (842 onward); `NextInnerJoin` saves a pending match selection
when output capacity is exhausted (993 onward); `NextLeftJoin` emits unmatched
probe rows after exhausting matched output (1284 onward). This is local source
comparison, not a claim about the pinned benchmark wheel's exact source.

The corresponding engine repair requires a retained outer-probe cursor,
post-filter match tracking, bounded admitted output, and a correct all-partition
unmatched-build completion phase. Existing match-count guards must remain until
that replacement passes high-fanout, NULL/duplicate/filter, empty-input,
partition, cancellation and memory tests. A timing result alone cannot justify
removing admission or changing SQL semantics.

## Frozen optimized diagnostics

Release12129 completed with exit0 in8m47s, frozen2026-09-09T16:44:05.216251Z.
Binary SHA256 `b61ce3d3e73f50d35bd097b080032115fe96db79bac47a0e6bfbdbd744f0ba35`;
all495 source inputs verified before/after build and measurements. The paired
source manifest differs from `11d16e73` only in `hash_join.rs`.

Driver40828 completed with exit0. It uses16 threads, CPUs0–15, two requests per
query in fresh per-query processes, canonical SF10 data and full typed saved
DuckDB oracles. Raw/native budgets are4GiB query/12GiB process; preloaded CPU is
the separately labelled32GiB query/48GiB process capacity experiment. Its180s
watchdog is diagnostic, not a replacement for failed10×DuckDB acceptance gates.
All12 completed outputs validate. Dataset and native/IPC provider inventories
verify before/after. Setup and serialization remain outside response query time.
The64GiB scope peaks at20,875,730,944 bytes; all max/OOM events are zero.

| Mode | Q12 query wall ms, two requests | Q13 query wall ms, two requests |
|---|---|---|
| Raw Parquet |1039.2 /1057.0|4280.9 /4295.0|
| Native |495.8 /509.7|4480.9 /4675.4|
| Preloaded CPU,32GiB |583.7 /185.9|4850.2 /4420.3|

The following Q13 phase values sum across16 partition records per request,
averaged across two requests. These are overlapping wall intervals, **not
additive query-wall fractions or exclusive CPU**.

| Mode | Probe collection ms | Preflight ms | Combined probe ms | Unmatched build ms | Actual build-state ms |
|---|---:|---:|---:|---:|---:|
| Raw Parquet |1877.3|256.2|69.5|5.9|7.3|
| Native |2026.7|245.0|475.0|5.3|8.4|
| Preloaded CPU,32GiB |2033.9|247.4|464.0|5.0|8.4|

Every Q13 request consumes14,845,369 probe rows and constructs15,345,388 joined
rows. Its preflight is measurable but cannot by itself explain a4.3–4.9s query.
Cache-wait intervals reach2092ms raw and2598ms native despite sub9ms measured
cache construction. This is consistent with delayed resumption of waiting
futures; it is not evidence of a slow cache lookup or lock implementation.
Q12 uses the inner route. Its native/resident probe sees15M rows versus291,909
raw rows, a provider/filter-routing difference that must be kept separate from
per-row join efficiency.

## Confirmed frontier limitation and next repair

Supplemental22219 completed with exit0 using the same binary, data, affinity and
budgets, with existing `QE_AGG_PROF=1` and `QE_INPUT_QUEUE_TRACE=1`. All6 Q13
outputs validate; full dataset/provider/source verification passes. Scope peak
15,127,830,528 bytes, zero max/OOM events. Neither scope peak proves query-wide
admission or exact process RSS. No canonical GPU workload was executed here.

All six supplemental executions report the join's aggregate frontier as:
`operator=SpillableHashJoin, partitions=16, slots=1, copy_bound=null,
admitted_buffers=false, envelope_bytes=null`.

This is a measured instance of the structural limitation in
`morsel_agg/input_frontier.rs`: without admitted buffers or a finite copied-output
bound, it selects one slot and polls opening futures locally. Collected outer
joins finish the partition inside `execute()` before returning a stream.
`HashJoinExec::prepare_queue_input` supports only Inner, so the outer route
cannot use the already-implemented bounded parallel input machinery. Future
waiting can therefore include other synchronous work in that polling task.
This explains why16 declared partitions do not imply16 parallel outer producers.
It does not quantify a causal speedup from changing the route.

The first aggregate ingests15,345,388 rows in1258–1371ms and emits1.5M groups in
23,440 batches; the second ingests those groups in221–225ms. These measured
aggregate intervals are another substantial cost and are not eliminated by
removing join preflight. Raw uses1832 joined input batches versus230 native/
resident batches. No spilling occurred in these supplemental aggregate records.

Implement the next task as a shared outer-output contract, not a Q13 special case:

1. Add an exact outer-probe cursor that retains one input batch, evaluated keys,
   chain position and pending match selection. Emit bounded match chunks without
   collecting an entire probe partition. Match bits become true only after ON
   filtering; emit NULL-extended probe rows only after all their candidates finish.
2. Admit output/index/match state before construction. Extend the prepared queue
   contract through SpillableHashJoin only for proven output representations,
   including nullable/dictionary fields and retained schema metadata. Preserve a
   clean explicit fallback for unsupported representations. Do not force more
   frontier slots while output ownership remains unbounded.
3. Implement an all-partition unmatched-build completion phase that handles
   cancellation/error and repeated execution deliberately. Do not use an early
   dropped partition as evidence of successful completion. Preserve default
   spilling and existing guards until the bounded replacement is proven.
4. Add independent typed cases for duplicate/hot keys, NULLs, empty sides,
   residual ON predicates, both build orientations, multiple batches/partitions,
   dictionary encodings, cancellation, low budgets and spill. Assert first output
   before full probe consumption and bounded retained memory under a slow consumer.
   Existing high-fanout refusal cases should succeed only on the proven bounded
   route; unsupported legacy cases must still refuse cleanly.
5. Freeze and compare against `11d16e73` with the matched harness, including full
   typed outputs, canonical raw/native/IPC/Iceberg/Lance and protected inner-join
   controls. Confirm the frontier actually selects bounded parallel slots before
   claiming concurrency gains. Keep GPU residency and upload boundaries separate.

This completes attribution of the collected outer phases and identifies the
frontier capability gap. Fine-grained collected filter/gather cost and aggregate
kernel optimization remain separate follow-ups. No performance gain, full
resource gate or DuckDB leadership is claimed.

[Immutable evidence archive](benchmarks/2026-09-09-collected-join-profile/manifest.json):
246 files hash-verified,495 frozen source inputs, both diagnostic drivers and
outputs, typed oracles, tests, harness and full source tarball. All jobs terminal.

