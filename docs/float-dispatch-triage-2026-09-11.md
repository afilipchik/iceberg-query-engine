# Float comparison dispatch: three-binary diagnostic — 2026-09-11

Candidate `ece6a4d63da0fd32b00defacbc5616c802a71b304306e119b37942f3b1791ca5` produces42independently typed-correct outputs and792join traces. Source526,all three binaries,dataset/providers,harness and driver verify after execution. Of14three-way logical/physical plan groups,0differ.

Baseline13210a20 predates mixed numeric compilation; control97e53169 is the regressing mixed-coercion candidate. Two reverse-order blocks,default disjoint ownership,GPUoff. Raw/native16threads and4/12GiB;resident4threads at32/48GiB,preload excluded. Generic rawQ6 disables morsel routing,with actual plan assertions. All instrumentation matches. The180second diagnostic watchdog is not a fresh DuckDB10x gate. No confidence interval,leadership or full provider/resource/concurrency acceptance is implied.

| Mode/query | Candidate/baseline | Block ratios | Candidate/regressing control | Block ratios |
|---|---:|---|---:|---|
| cpu_resident_4t_32g/q06 | 1.263582 | 1.180966, 1.347427 | 0.918953 | 0.857063, 0.982030 |
| cpu_resident_4t_32g/q09 | 1.099195 | 0.998762, 1.199148 | 1.116252 | 1.021328, 1.209426 |
| native/q06 | 1.233593 | 1.676860, 0.835911 | 0.804085 | 1.188255, 0.508316 |
| native/q09 | 0.899585 | 0.971033, 0.839685 | 0.985696 | 0.966516, 1.005032 |
| raw_parquet/q06 | 1.086430 | 1.106985, 1.066464 | 0.869593 | 0.904202, 0.837280 |
| raw_parquet/q09 | 1.030660 | 1.000262, 1.059891 | 1.038475 | 0.999139, 1.076952 |
| raw_parquet_generic/q06 | 2.074367 | 2.078289, 2.070386 | 0.928567 | 0.934378, 0.922719 |

Cumulative scope peak22803419136bytes;swap maximum0;events `low 0; high 0; max 0; oom 0; oom_kill 0; oom_group_kill 0`. This scope includes additional contract compilation and release build; its peak is not query-only RSS.

[Attribution and tests](compiled-coercion-attribution-2026-09-11.md). [Complete evidence](benchmarks/2026-09-11-float-dispatch-triage/manifest.json). All samples,failures,oracles and plans are preserved.
