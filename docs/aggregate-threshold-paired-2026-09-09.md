# Frozen aggregate-threshold protected comparison — 2026-09-09

Frozen1efb2554 removes the unrelated quarter-root aggregate group cap from
control322c8042. Two source inputs differ,507 are frozen. The same query limits,
checked reservations and spill retry contracts remain. See the
[repair and matched Lance diagnostics](aggregate-spill-threshold-2026-09-09.md).

Job22140 is terminal1: custom-memory child0, canonical child1. All44 blocks
finished with six measured pairs plus warmups, balanced startup/execution order
and fresh typed DuckDB calibration per block. Canonical raw Parquet uses4GiB
query/12GiB process,16 threads and CPU0–15. Custom memory is a separately labelled
600,000-row float workload with4 threads. Query ceilings remain10× the fresh
matched DuckDB reference. All reference blocks are valid.

All527 completed engine outputs pass typed comparison;521 pass timing and6 are
late. Eight requests time out and81 dependent slots are not run. Q9/Q12 remain
incomplete. Every source and binary hash verifies after execution. The48GiB
scope peaks at4,527,124,480 bytes with zero max/OOM/kill events.

| Workload/query | Candidate/control ratio | 95% block-bootstrap interval |
| --- | ---: | --- |
| Canonical Q1 | 0.95533 | 0.92124–0.99069 |
| Canonical Q2 | 0.99809 | 0.94103–1.06920 |
| Canonical Q5 | 1.00776 | 0.99125–1.02454 |
| Canonical Q9 | Incomplete | No complete comparison |
| Canonical Q10 | 0.99410 | 0.98822–1.00280 |
| Canonical Q12 | Incomplete | No complete comparison |
| Canonical Q13 | 1.00047 | 0.99574–1.00870 |
| Canonical Q19 | 0.99174 | 0.96411–1.02099 |
| Canonical Q20 | 0.98780 | 0.96785–1.00926 |
| Custom memory Q1 | 0.98592 | 0.97075–1.00731 |
| Custom memory Q6 | 1.01190 | 0.93584–1.10794 |

All complete canonical upper confidence bounds are below1.070. Q1's observed
interval is below1; the data show an improvement in this study, without
establishing a cause for an unchanged raw provider route. Other complete
canonical intervals include1. Custom Q6's original upper bound1.10794 does not
exclude a10% slowdown. Preserve that interval. A single fixed eight-block,
twelve-pair follow-up was specified before execution, with20,000 bootstrap
samples and seed20260909; it requires all outputs typed-correct/gated and an
upper95 ratio no larger than1.10. Job35348 terminal1: all208 completed outputs are typed-correct and timing-gated,
but the ratio1.06521 has95% interval0.96007–1.21064 and fails that uncertainty
gate. Scope peak201,625,600 bytes with zero max/OOM/kill. All507 source and both
binary hashes verify. The failed follow-up is preserved; no repeated run is
used to replace it. The original and follow-up intervals both remain reported.

The Q6 physical route is a global SpillableHashAggregate over Project/Filter/
MemoryTableScan. It has one group, so changing a multimillion-group advisory cap
is not a demonstrated mechanism for its variability. This does not waive the
failed gate: distinguish instruction-layout effects and measurement variability
with a prespecified identical-binary control before attributing or optimizing.
Individual request times span roughly1.5–7ms in the follow-up. A null comparison
must use the same binary/setup and preserve all outcomes; it can diagnose study
precision, but cannot retroactively pass this candidate comparison.

This component screen does not certify a complete canonical suite, broader
providers, residency, resources or concurrency. The [paired archive](benchmarks/2026-09-09-aggregate-threshold-pairs/manifest.json)
and [fixed follow-up archive](benchmarks/2026-09-09-aggregate-threshold-q6-followup/manifest.json)
verify1,202 and460 files respectively, including507 unchanged source inputs.
Identical-binary null control33603 terminal1:208correct/gated outputs, ratio
1.09628 with95%1.03061–1.17850 despite identical hashes. Its460-file archive
verifies. See the [precision diagnosis and next protocol](short-query-null-control-2026-09-09.md).
It cannot replace either candidate interval. Provider43384 is running; broader
acceptance remains open.

## Subsequent isolated-window comparison

The [isolated-window report](isolated-window-benchmark-2026-09-09.md) preserves
a failed long-window null under the old affinity, a passing physical-core null,
and the fresh candidate comparison97043. The latter validates32,784 outputs
and passes the upper-bound gate with primary mean-time ratio0.99105
(95%0.94622–1.04071). Affinity and the comparison protocol changed; this is a
separate result, not a reclassification of this document's original failures.
