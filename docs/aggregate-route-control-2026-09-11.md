# Same-source aggregate route control — 2026-09-11

Sequence82829's route stage completed successfully. All16 outputs match independent typed oracles, and the actual physical plans select the requested generic or morsel route. Binary68912c23 and521 source inputs, dataset, driver and harness hashes verify after execution. This is a same-binary comparison of QE_MORSEL0 and1 on identical raw Parquet input, default disjoint ownership, GPUoff,4/12GiB query/process, CPU0–15 or0–3, two reversed blocks. It is a diagnostic with a180-second watchdog, not DuckDB acceptance or a confidence bound.

| Threads/query | Generic samples ms | Morsel samples ms | Morsel/generic means | Block ratios |
|---|---|---|---:|---|
| 16/q01 | 7979.264, 7955.312 | 609.402, 640.563 | 0.078444 | 0.076373, 0.080520 |
| 16/q06 | 197.236, 188.388 | 174.574, 205.671 | 0.986051 | 0.885101, 1.091741 |
| 4/q01 | 11930.107, 11970.849 | 2151.761, 2145.984 | 0.179815 | 0.180364, 0.179267 |
| 4/q06 | 548.902, 549.256 | 551.764, 562.263 | 1.014450 | 1.005213, 1.023680 |

The grouped Q1 gap is large and consistent across both orderings: morsel query time is92.16% lower at16threads and82.02% lower at4threads. Q6 has opposing16thread block ratios and is slightly slower on morsel at4threads. This rules out treating morsel routing as a universal speedup. It does not isolate one function or prove that native input can inherit the same gain.

Generic raw Q1 block1 already has16 admitted input slots. It processes59,142,609 rows in7,323 batches, reports4 disjoint owners,2,295.238ms routing,4,071.553ms processing wall time,1,534.711ms evaluation and6,374.419ms ingestion. No spill occurred. These nested/overlapping phase values must not be summed as independent exclusive CPU costs. They place a large remaining cost in generic grouped ingestion despite admitted parallel input. The four-state output is deliberately a low-cardinality diagnostic; high-cardinality and skew controls are required before changing general scheduling or kernels.

Next investigate batch-bound fixed input views and grouped update dispatch using current profiling; preserve staged row publication, reservations and exact retry cursor. Current source already has compact fixed cells and borrowed scalar updates. A new reader admission alone cannot explain this same-raw-input gap. Keep default ownership unchanged and do not add a Q1-specific rule. See [bounded investigation](aggregate-batch-binding-investigation-2026-09-11.md).

The archive retains all samples, plans, typed results, independent oracles, traces and resource snapshots. ZeroOOM/max events were recorded. The48GiB cgroup is shared with the preceding diagnostic, so its peak is cumulative across stages, not a separate route-only RSS measurement. Full provider screen is running; residency/resource/concurrency acceptance remains open.

[Archive](benchmarks/2026-09-11-right-semi-route-control/manifest.json). Reproduce with `run_right_semi_route_control.py` under the repository capped wrapper, Python environment and CPU affinity recorded in the driver; output directories must be new.
