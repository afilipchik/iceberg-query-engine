# Aggregate input retention and Q9 attribution — 2026-09-09

## Reproduced Q18 regression

Corrected matched diagnostic47364 terminal0 compares frozen b61ce3d3 (before the
bounded outer/resident changes) and acdb8c51. Two fresh blocks reverse execution
order. Both control requests complete with typed-correct Q18 outputs; both
candidate requests return the same named input-domain memory refusal. Setup is
Lance canonical SF10,16 threads/CPUs0–15,4GiB query and12GiB process. The48GiB
scope peaks at7,383,310,336 bytes with no max/OOM/kill events and no swap. Binary,
503 current source inputs, dataset and Lance versioned provider hashes verify
before/after. This is refusal attribution, not performance acceptance.

The initial52985 control output also validates, but the driver then incorrectly
rejected normal ready/progress messages as extra query responses. The corrected
driver recognizes a single terminal response. Both attempts are preserved; no
engine startup failure is inferred from that driver assertion.

Both control traces complete `aggregate_drain` over MemoryTableScan:7,323 batches,
1,454,661,762 estimated bytes, without crossing its query-level spill threshold.
The candidate's corresponding drain terminates with completed=false after about
22ms, while retaining admitted source outputs. The error's source domain is
approximately512MiB. This locates the failure before the aggregate delegate,
not in the surrounding join probe or final sort.

`morsel_agg/live_spill.rs` currently returns None immediately whenever post_filter
(HAVING) is present. SpillableHashAggregate then collects raw input until a
query-level threshold, through `stream_merge_input_partitions`. The latter now
prefers admitted memory-source streams with a prepaid input domain. That domain
fills before the collector reaches its much larger threshold. The same Q18
refusal appears in the32GiB IPC/CPU screen at an approximately687MiB input cap.

The next implementation should connect admitted filtering to completed aggregate
output, before retaining result batches. Compile/bind supported predicates before
input consumption. Apply them only after all partial/spilled state for a group
is merged; SQL NULL means discard. Reuse admitted mask/selection construction and
preserve exact aggregate result types and aliases. Unsupported expressions must
decline before input is consumed. Test duplicate keys across batches/partitions,
NULL aggregate values, all-true/all-false/partial output, output lifetimes, and
real spills against independent expected values. A generic HAVING adapter removes
this unnecessary full-input collection; other materializing consumers and their
input-domain/retention contracts still need review. Do not loosen memory safety
or special-case Q18.

## Frozen Q9 diagnostic

Diagnostic72893 terminal0 adds join/aggregate traces with an explicitly separate
180-second watchdog. All6 outputs are typed-correct; all source/binary/dataset/
provider hashes verify. This does not clear the10×DuckDB gate.

| Mode | Query milliseconds, two requests | Aggregate input frontier |
|---|---|---|
| Raw Parquet |5876.655 /5750.551|Project,16 partitions,1 slot,not admitted|
| Native |2697.231 /2589.617|Project,8 partitions,1 slot,not admitted|
| Preloaded CPU32GiB |2868.474 /2281.387|See preserved trace|

Every request delivers3,261,613 rows in916 batches to the aggregate and returns
175 rows. Raw/native aggregate ingestion is approximately251–259ms; it is only a
fraction of query execution. The first part-key join sees3,261,613 probe rows raw
versus59,986,052 native/resident. Raw's runtime filter reduces rows, yet the raw
query remains slower. Its summed first-join input wait is about4,015ms, versus47ms
native (averaged over the two requests). Wait counters across nested joins and
partitions overlap and are not additive exclusive CPU measurements.

The computed Project has no admitted-buffer or copied-output bound, so the
aggregate input frontier chooses one producer slot. This is a confirmed capability
boundary; a speedup from changing it is not yet measured. A general admitted
computed-projection adapter is a candidate after the HAVING resource regression
is repaired. Preserve input/output ownership, expression evaluation count and
variable-value admission. Do not substitute estimated string lengths for bounds.

No engine source changed during these diagnostics. Sources and scripts are
frozen in the linked archives; actual repair and acceptance are still pending.
