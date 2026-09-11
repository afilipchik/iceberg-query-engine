# Coordinated spill: matched performance diagnostic

Frozen a4103dfa completes64 independently typed-correct outputs against008d92f7.
Session69904 is terminal0; all1820 join traces complete without unwind. Timing
changes are small and mixed. Native Q1/Q17 are faster in both blocks; resident
16-thread Q1 is2.23%slower. These two blocks do not certify performance neutrality,
protected regression bounds or DuckDB leadership.

## Conditions and verification

Canonical SF10 Q1/Q9/Q17/Q18, raw Parquet/native/decoded resident16threads and
resident4threads, two reversed binary-order blocks. Default disjoint ownership,
GPUoff. Raw/native4GiB query/12GiB process; residency32/48GiB with preload excluded.
Affinity0–15 or0–3, enclosing48GiB scope, swap0. The180s diagnostic watchdog is
separate from the fresh10×DuckDB acceptance ceiling used by the provider screen.
Release56527 completed in8m51s and freezes520 source inputs.

All source, binary, data/provider, driver and harness after-run checks pass.
The scope records27,161,505,792 peak bytes and zero max/OOM events. Scope peak
includes setup and comparator work; it is not a per-query RSS comparison.
Every sample, reservation peak, profile and process resource record is retained.

## Candidate / control ratios

| Mode/query | Ratio of means | Block1 | Block2 |
|---|---:|---:|---:|
| raw_parquet/q01 | 1.031488 | 1.067841 | 0.995860 |
| raw_parquet/q09 | 1.005644 | 1.041492 | 0.970380 |
| raw_parquet/q17 | 0.979636 | 0.984241 | 0.975086 |
| raw_parquet/q18 | 1.032423 | 0.993516 | 1.071699 |
| native/q01 | 0.958631 | 0.940016 | 0.977690 |
| native/q09 | 0.993295 | 0.953182 | 1.035376 |
| native/q17 | 0.961119 | 0.950588 | 0.971728 |
| native/q18 | 0.977544 | 0.953633 | 1.002020 |
| cpu_resident_32g/q01 | 1.022320 | 1.014333 | 1.030482 |
| cpu_resident_32g/q09 | 0.991525 | 0.984321 | 0.998806 |
| cpu_resident_32g/q17 | 0.980057 | 0.983250 | 0.976862 |
| cpu_resident_32g/q18 | 0.986688 | 0.963515 | 1.010204 |
| cpu_resident_4t_32g/q01 | 0.973791 | 0.977562 | 0.970031 |
| cpu_resident_4t_32g/q09 | 1.010295 | 1.004569 | 1.016073 |
| cpu_resident_4t_32g/q17 | 0.989480 | 0.983701 | 0.995297 |
| cpu_resident_4t_32g/q18 | 1.012223 | 0.991525 | 1.033390 |

Native Q1/Q17 mean ratios are0.958631/0.961119. Raw Q1/Q18 show mean slowdowns
of3.15%/3.24%, but each has opposing blocks. Resident16 Q1 is consistently slower;
resident4 Q9 is1.03%slower. Do not hide these differences or repeat until favorable.
No confidence-certified speedup follows from the apparent gains.

The [resource repair and correctness validation](coordinated-spill-progress-2026-09-11.md)
remain useful at unchanged query budgets. This diagnostic supports proceeding to
full provider screening, not closing resource/concurrency/residency acceptance.
The [immutable archive](benchmarks/2026-09-11-peer-spill-paired/manifest.json)
contains independent oracles, outputs, commands, profiles and reproducibility inputs.
