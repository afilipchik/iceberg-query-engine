# Startup admission: shared-path regression measurement

Frozen008d92f7 completes64 independently typed-correct outputs against38966ae6,
with1820 complete join traces and no unwind. Session81205 is terminal0. The
startup resource repair remains useful, but this two-block diagnostic does not
establish performance neutrality: native queries show2–4% mean slowdowns and
raw Q9 is3.97% slower. No DuckDB leadership claim follows.

## Conditions and verification

Canonical SF10 Q1/Q9/Q17/Q18 across raw Parquet, native storage, decoded resident
16threads and decoded resident4threads. Two blocks reverse control/candidate
order. Default disjoint ownership, GPUoff, affinity0–15 or0–3. Raw/native use
4GiB query/12GiB process budgets; residency uses32/48GiB with preload excluded.
The180-second diagnostic watchdog is not the10×DuckDB acceptance rule.

Release78261 completed in8m51s with520 frozen source inputs. Full binary SHA256:
008d92f70ea2a4143430a23db221369b806bf132615e28d026b983961fcb8499.
After-run checks verify both binaries, all source inputs, data/provider provenance,
driver and harness. The48GiB cgroup recorded28,254,543,872 peak bytes, swap0 and
zero max/OOM events. This scope includes setup and comparator work; it is not a
per-query RSS comparison. All samples, reservation peaks and profiles are retained.

## Candidate / control ratios

| Mode/query | Ratio of means | Block1 | Block2 |
|---|---:|---:|---:|
| raw_parquet/q01 | 0.951767 | 1.047343 | 0.870012 |
| raw_parquet/q09 | 1.039681 | 1.051312 | 1.028175 |
| raw_parquet/q17 | 0.997321 | 0.993499 | 1.001090 |
| raw_parquet/q18 | 0.989999 | 0.917925 | 1.070097 |
| native/q01 | 1.023330 | 1.014981 | 1.031886 |
| native/q09 | 1.039536 | 1.022707 | 1.055928 |
| native/q17 | 1.022573 | 1.025834 | 1.019211 |
| native/q18 | 1.041795 | 1.054782 | 1.028663 |
| cpu_resident_32g/q01 | 1.011594 | 1.022438 | 1.000930 |
| cpu_resident_32g/q09 | 1.005508 | 0.997884 | 1.013329 |
| cpu_resident_32g/q17 | 1.018083 | 1.012401 | 1.023773 |
| cpu_resident_32g/q18 | 1.013648 | 1.018647 | 1.008489 |
| cpu_resident_4t_32g/q01 | 1.002954 | 0.994631 | 1.011373 |
| cpu_resident_4t_32g/q09 | 0.997679 | 1.012340 | 0.983148 |
| cpu_resident_4t_32g/q17 | 1.015605 | 1.019599 | 1.011606 |
| cpu_resident_4t_32g/q18 | 1.001477 | 1.004333 | 0.998600 |

Raw Q1 and Q18 have opposing blocks; their mean improvements are not reliable
speedup evidence. Native Q1/Q9/Q17/Q18 are slower in both blocks, by2.33%,3.95%,
2.26% and4.18% on average. Resident differences are smaller. These limited
observations must remain visible, but do not certify confidence bounds or a
protected regression. Do not rerun until favorable or compare only to an older,
slower prototype. Full provider, resource/concurrency and multi-session gates remain.

See the [startup contract and completed correctness validation](aggregate-startup-headroom-2026-09-10.md).
The [immutable archive](benchmarks/2026-09-10-startup-headroom-paired/manifest.json)
retains outputs, independent oracles, commands, profiles and reproducibility inputs.
