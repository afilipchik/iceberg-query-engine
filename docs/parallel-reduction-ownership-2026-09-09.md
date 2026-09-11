# Ownership choice after parallel reduction — September 9, 2026

## Result and decision

In frozen `bd689bf6`, partial ownership improves native canonical SF10 Q1 by an
observed 48.48% and Q18 by 12.84% at sixteen query threads compared with default
disjoint ownership. At four threads, Q1 improves 30.13% but Q18 regresses 16.17%.
All sixteen outputs and execution traces validate. Keep default ownership
unchanged: one ownership strategy is not universally better.

| Query threads | Query | Disjoint, block ms | Partial, block ms | Geometric mean paired ratio |
|---:|---|---|---|---:|
| 4 | Q1 | 11582.149 / 11783.919 | 8230.584 / 8095.068 | 0.698693 |
| 4 | Q18 | 5097.798 / 5099.305 | 5884.947 / 5961.470 | 1.161719 |
| 16 | Q1 | 11808.517 / 11848.561 | 6105.298 / 6083.599 | 0.515232 |
| 16 | Q18 | 5195.695 / 5269.141 | 4568.949 / 4551.816 | 0.871583 |

This is a two-block diagnostic without confidence intervals. It is neither a
DuckDB comparison nor full-suite performance acceptance. At sixteen threads,
disjoint ingestion still caps owners at four while partial ingestion uses sixteen;
this measures the existing whole-path choices, not an equal-owner-count algorithm
comparison. The preceding [paired reduction experiment](parallel-reduction-native-2026-09-09.md)
isolates the serial-to-parallel final merge change at matched ownership counts.

## Reproduction and evidence

Job81286 exits0. Both sides run the same frozen binary, canonical native SF10
provider and original Q1/Q18 SQL. Query budgets are 4 GiB, process caps 12 GiB,
CPU affinity 0–15, query threads 4/16, GPU disabled. One fresh process runs at a
time and is reaped before the next. Block two reverses query, thread and mode
order. Completed outputs match existing independent typed oracles. Partial Q18
requires nonzero parallel merge windows, and serial fallback invalidates the
requested algorithm check.

All sixteen outputs are typed-correct and all sixteen algorithms verify. Source
(510 inputs), binary, provider, dataset, driver and harness hashes verify after
the run. The 48 GiB, zero-swap scope peaks at 3,795,841,024 bytes with no OOM/max
events. Query spill counters are zero. The 180-second watchdog remains a separate
diagnostic allowance, not the mandatory 10× DuckDB acceptance ceiling.

Candidate SHA256:
`bd689bf675ea1977e8573d6a63587cf5a8fb882164161b26439c8793b202ab23`.
[Immutable archive](benchmarks/2026-09-09-parallel-reduction-ownership/manifest.json).
[Implementation and correctness](parallel-aggregate-reduction-2026-09-09.md).

## Path forward

1. Preserve these two distinct experiments. Parallel final reduction removes a
   measured bottleneck; whole-path ownership still has a workload/thread tradeoff.
2. Extend the frozen candidate's evaluation to the protected query set and each
   provider separately (raw Parquet, native, Iceberg, Lance), with decoded IPC and
   GPU residency separately labelled. Validate results, completion, timing and
   admission independently. Do not treat the native diagnostic as certification
   of a provider whose execution takes another route.
3. Build a general ownership cost model from observed input rows, estimated group
   cardinality, key skew, available workers and memory. Compare ingestion imbalance
   against partial-state duplication and reduction cost. Estimates choose an
   algorithm only; exact canonical keys still establish semantic ownership.
   Exercise low/high cardinality and uniform/skewed synthetic workloads before
   selecting thresholds. Do not encode query IDs or a universal sixteen-thread rule.
4. Keep input-key preparation and output-before-HAVING construction as separate
   measured optimization candidates. Any such source change requires a new frozen
   binary and its own protected comparison.
5. Complete cap/concurrency and full suite gates before promoting a new default.
   Six existing spill failures, canonical Q9/Q12 completion, and broader provider/
   residency acceptance remain open. These improvements do not establish DuckDB
   leadership.
