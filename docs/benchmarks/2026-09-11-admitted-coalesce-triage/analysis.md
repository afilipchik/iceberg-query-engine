# Bounded admitted output: three-binary diagnostic — 2026-09-11

Candidate `1bba20b347f3cad6b6afc53047848dacbb61f8a76393aed16b486058733fae70` produces42independently typed-correct outputs and792join traces. Source530,all three binaries,dataset/providers,harness and driver verify after execution. Of14three-way logical/physical plan groups,0differ.

Baseline13210a20 predates mixed numeric compilation; control beb0cdfd precedes projected survivor construction and bounded output accumulation. Two reverse-order blocks,default disjoint ownership,GPUoff. Raw/native16threads and4/12GiB;resident4threads at32/48GiB,preload excluded. Generic rawQ6 disables morsel routing,with actual plan assertions. All instrumentation matches. The180second diagnostic watchdog is not a fresh DuckDB10x gate. No confidence interval,leadership or full provider/resource/concurrency acceptance is implied.

| Mode/query | Candidate/baseline | Block ratios | Candidate/dispatch control | Block ratios |
|---|---:|---|---:|---|
| cpu_resident_4t_32g/q06 | 1.199195 | 1.205780, 1.192640 | 0.992687 | 1.009576, 0.976253 |
| cpu_resident_4t_32g/q09 | 1.009331 | 1.008664, 1.009993 | 1.035154 | 1.031692, 1.038614 |
| native/q06 | 1.195510 | 1.124461, 1.272169 | 0.898392 | 1.172872, 0.734471 |
| native/q09 | 1.020662 | 1.032563, 1.008892 | 1.031920 | 1.032324, 1.031512 |
| raw_parquet/q06 | 1.084807 | 1.072607, 1.097082 | 0.939554 | 0.957738, 0.922326 |
| raw_parquet/q09 | 0.985737 | 0.936369, 1.035617 | 0.978023 | 0.945332, 1.009928 |
| raw_parquet_generic/q06 | 1.828793 | 1.831359, 1.826228 | 0.897777 | 0.907985, 0.887773 |

Cumulative scope peak19520114688bytes;swap maximum0;events `low 0; high 0; max 0; oom 0; oom_kill 0; oom_group_kill 0`. This scope includes only the diagnostic requests; its peak is not query-only RSS.

[Attribution and tests](compiled-coercion-attribution-2026-09-11.md). [Complete evidence](benchmarks/2026-09-11-admitted-coalesce-triage/manifest.json). All samples,failures,oracles and plans are preserved.
