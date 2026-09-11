# Coordinated reader output: three-binary diagnostic — 2026-09-11

Candidate `7ddbf9cee70d3355e1866bed91103f36ad280e99ed127bf55f056d31db3e439f` produces42independently typed-correct outputs and792join traces. Source531,all three binaries,dataset/providers,harness and driver verify after execution. Of14three-way logical/physical plan groups,0differ.

Baseline13210a20 predates mixed numeric compilation; control 1bba20b3 precedes coordinated page preparation and reversible output trials. Two reverse-order blocks,default disjoint ownership,GPUoff. Raw/native16threads and4/12GiB;resident4threads at32/48GiB,preload excluded. Generic rawQ6 disables morsel routing,with actual plan assertions. All instrumentation matches. The180second diagnostic watchdog is not a fresh DuckDB10x gate. No confidence interval,leadership or full provider/resource/concurrency acceptance is implied.

| Mode/query | Candidate/baseline | Block ratios | Candidate/previous checkpoint | Block ratios |
|---|---:|---|---:|---|
| cpu_resident_4t_32g/q06 | 1.217995 | 1.234959, 1.200803 | 1.011703 | 1.026946, 0.996290 |
| cpu_resident_4t_32g/q09 | 0.972744 | 0.961645, 0.983977 | 1.020718 | 1.024135, 1.017361 |
| native/q06 | 1.059554 | 1.116820, 1.000340 | 0.875128 | 0.865448, 0.886575 |
| native/q09 | 1.014268 | 1.048037, 0.982141 | 1.018964 | 1.029718, 1.008275 |
| raw_parquet/q06 | 1.116760 | 1.116609, 1.116901 | 1.005111 | 1.014053, 0.996925 |
| raw_parquet/q09 | 0.997278 | 0.924445, 1.069233 | 1.012272 | 0.918843, 1.108550 |
| raw_parquet_generic/q06 | 1.779016 | 1.818307, 1.740719 | 1.030694 | 1.048305, 1.013361 |

Cumulative scope peak21759344640bytes;swap maximum0;events `low 0; high 0; max 0; oom 0; oom_kill 0; oom_group_kill 0`. This scope includes only the diagnostic requests; its peak is not query-only RSS.

[Attribution and tests](compiled-coercion-attribution-2026-09-11.md). [Complete evidence](benchmarks/2026-09-11-coordinated-output-triage/manifest.json). All samples,failures,oracles and plans are preserved.
