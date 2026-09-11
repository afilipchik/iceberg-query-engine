# Retained input credit: shared-path regression comparison

Frozen38966ae6 completes all64independently typed-correct outputs against
fde271b1, with1820complete join traces and no unwind. This two-block diagnostic
supports retaining the resource fix for further screening, but does not prove
performance neutrality or DuckDB leadership. RawQ1, nativeQ17/Q18 and resident
16-threadQ18 retain small slowdowns that must remain visible.

## Conditions and evidence

Session88178 is terminal0. CanonicalSF10 Q1/Q9/Q17/Q18; raw Parquet, native,
decoded CPU resident16threads and resident4threads. Default disjoint ownership,
GPUoff. Raw/native use4GiBquery/12GiBprocess; resident uses explicitly larger
32GiBquery/48GiBprocess budgets, with preload excluded. CPU affinity0–15 or0–3.
Each query starts a fresh process, with two reversed control/candidate blocks.
The180sdiagnostic watchdog is not a fresh10×DuckDB acceptance ceiling.

The48GiBcgroup recorded peak40,446,885,888bytes,swap0,zero max/OOM events.
This scope peak includes setup, engine and comparator work; it is not a per-query
RSS comparison. All519source inputs, both binaries, data/provider provenance,
driver and harness guards verify after measurement. Per-query process resource
logs, reservation peaks, cumulative profiles and every sample are retained.

## Ratios: candidate / preceding fde271b1

| Mode/query | Ratio of means | Block 1 | Block 2 |
|---|---:|---:|---:|
| raw_parquet/q01 | 1.031488 | 1.055461 | 1.006534 |
| raw_parquet/q09 | 0.970680 | 0.966182 | 0.975370 |
| raw_parquet/q17 | 0.995477 | 0.998527 | 0.992484 |
| raw_parquet/q18 | 0.996498 | 1.003352 | 0.990087 |
| native/q01 | 0.946260 | 0.896121 | 1.002324 |
| native/q09 | 0.876180 | 0.771512 | 1.007203 |
| native/q17 | 1.021594 | 1.021539 | 1.021649 |
| native/q18 | 1.031349 | 1.013740 | 1.049489 |
| cpu_resident_32g/q01 | 0.996012 | 0.989615 | 1.002507 |
| cpu_resident_32g/q09 | 0.988547 | 0.969905 | 1.007744 |
| cpu_resident_32g/q17 | 0.994336 | 1.000420 | 0.988302 |
| cpu_resident_32g/q18 | 1.029584 | 1.016387 | 1.042938 |
| cpu_resident_4t_32g/q01 | 1.012056 | 1.005609 | 1.018562 |
| cpu_resident_4t_32g/q09 | 1.007881 | 1.037278 | 0.979342 |
| cpu_resident_4t_32g/q17 | 0.993165 | 0.993900 | 0.992430 |
| cpu_resident_4t_32g/q18 | 0.989270 | 0.986936 | 0.991580 |

RawQ1 is3.15%slower on average; nativeQ17 2.16%, nativeQ18 3.13%, resident16Q18
2.96%. These are limited observations, not confidence-certified regressions.
NativeQ1/Q9 apparent mean improvements have inconsistent blocks; no speedup claim
is warranted. Do not hide these samples by comparing only with an older slower
state-layout prototype or by repeating until favorable.

The separately reproduced [LanceQ7 resource fix](retained-input-credit-q07-2026-09-10.md)
remains useful: old fde refuses twice and38966ae6 returns two typed-correct outputs
at unchanged budget. Shared-path performance certification, full provider
completion, resource/concurrency gates and multi-session acceptance remain open.
Full provider session4926 subsequently completed; see the
[full screen](retained-input-credit-provider-screen-2026-09-10.md) for333validated
outputs and remaining deadline/reference failures.

[Immutable archive](benchmarks/2026-09-10-retained-input-credit-paired/manifest.json)
contains all samples, independent oracles, requests, plans/profiles, build records,
source provenance and reproducible scripts. No source or harness changes occurred
during this measurement.
