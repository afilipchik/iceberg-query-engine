# Planned scan quantum: three-binary diagnostic — 2026-09-11

Candidate `beb0cdfd180507aa4ab00e7aa2ec9ea2c38f787b803aaa62de0b26bb239b75c5` produces42independently typed-correct outputs and792join traces. Source527,all three binaries,dataset/providers,harness and driver verify after execution. Of14three-way logical/physical plan groups,0differ.

Baseline13210a20 predates mixed numeric compilation; control ece6a4d6 includes the per-chunk float dispatch repair. Two reverse-order blocks,default disjoint ownership,GPUoff. Raw/native16threads and4/12GiB;resident4threads at32/48GiB,preload excluded. Generic rawQ6 disables morsel routing,with actual plan assertions. All instrumentation matches. The180second diagnostic watchdog is not a fresh DuckDB10x gate. No confidence interval,leadership or full provider/resource/concurrency acceptance is implied.

| Mode/query | Candidate/baseline | Block ratios | Candidate/dispatch control | Block ratios |
|---|---:|---|---:|---|
| cpu_resident_4t_32g/q06 | 1.209431 | 1.222906, 1.195870 | 1.018302 | 1.035771, 1.000928 |
| cpu_resident_4t_32g/q09 | 0.992881 | 0.966461, 1.020482 | 1.020605 | 1.026513, 1.014825 |
| native/q06 | 0.844779 | 0.808705, 0.881195 | 1.093413 | 0.920720, 1.323359 |
| native/q09 | 0.997220 | 0.980843, 1.013831 | 0.987476 | 0.987552, 0.987401 |
| raw_parquet/q06 | 1.076038 | 1.075656, 1.076415 | 0.940179 | 0.974781, 0.908403 |
| raw_parquet/q09 | 1.010335 | 1.005349, 1.015191 | 1.001579 | 1.013522, 0.990327 |
| raw_parquet_generic/q06 | 2.016039 | 2.059330, 1.974194 | 0.996619 | 0.996529, 0.996711 |

Cumulative scope peak19570667520bytes;swap maximum0;events `low 0; high 0; max 0; oom 0; oom_kill 0; oom_group_kill 0`. This scope includes only the diagnostic requests; its peak is not query-only RSS.

[Attribution and tests](compiled-coercion-attribution-2026-09-11.md). [Complete evidence](benchmarks/2026-09-11-admitted-quantum-triage/manifest.json). All samples,failures,oracles and plans are preserved.
