# Aggregate batch views: matched diagnostic — 2026-09-11

Sequence35732 completed successfully. Candidate `480109f6bbfdcde081967eace22220e8148be9c690c2849121eb30f4bdeeb78c` versus control240cd5e2 produced120independently typed-correct outputs and2476join traces. All523 source inputs, both binaries, dataset/providers, driver and harness verify after execution. Of60matched logical/physical plan pairs, 0 differ. Complete comparison details are retained in plan-comparison.json.

Two reversed-order blocks, default disjoint ownership,GPUoff. Raw/native16threads,4/12GiB query/process; decoded resident16/4threads,32/48GiB with preload excluded. Generic rawQ1/Q6 disables morsel routing and actual physical routes are asserted. Instrumentation is matched. The180-second watchdog is diagnostic, not a fresh DuckDB10× gate. No confidence-certified regression bound or provider/resource/concurrency acceptance is implied.

| Mode/query | Candidate/control mean | Block ratios |
|---|---:|---|
| raw_parquet_generic/q01 | 0.819005 | 0.783858, 0.853867 |
| raw_parquet_generic/q06 | 1.063923 | 0.974944, 1.157082 |
| raw_parquet/q01 | 1.025499 | 0.994352, 1.057184 |
| raw_parquet/q06 | 1.036121 | 0.926285, 1.155606 |
| raw_parquet/q09 | 0.998234 | 0.989350, 1.007572 |
| raw_parquet/q10 | 1.028254 | 1.044971, 1.011199 |
| raw_parquet/q13 | 0.987106 | 0.989390, 0.984823 |
| raw_parquet/q17 | 0.992853 | 0.997662, 0.988066 |
| raw_parquet/q18 | 0.988247 | 0.966982, 1.009631 |
| native/q01 | 0.789009 | 0.796721, 0.781382 |
| native/q06 | 0.967679 | 0.642290, 1.599987 |
| native/q09 | 1.002991 | 0.995203, 1.010657 |
| native/q10 | 0.950205 | 0.939782, 0.960905 |
| native/q13 | 0.990349 | 0.971816, 1.008931 |
| native/q17 | 0.997864 | 1.010288, 0.985257 |
| native/q18 | 0.959084 | 0.952835, 0.965541 |
| cpu_resident_32g/q01 | 0.792398 | 0.796129, 0.788653 |
| cpu_resident_32g/q06 | 0.999096 | 0.999278, 0.998913 |
| cpu_resident_32g/q09 | 0.983399 | 0.968201, 0.998960 |
| cpu_resident_32g/q10 | 1.011322 | 1.008576, 1.014071 |
| cpu_resident_32g/q13 | 0.956365 | 0.959282, 0.953500 |
| cpu_resident_32g/q17 | 1.001705 | 0.988348, 1.014563 |
| cpu_resident_32g/q18 | 0.937011 | 0.945772, 0.928335 |
| cpu_resident_4t_32g/q01 | 0.813728 | 0.808635, 0.818908 |
| cpu_resident_4t_32g/q06 | 0.990985 | 0.998614, 0.983453 |
| cpu_resident_4t_32g/q09 | 0.987802 | 0.959863, 1.016136 |
| cpu_resident_4t_32g/q10 | 1.031138 | 1.054255, 1.008554 |
| cpu_resident_4t_32g/q13 | 0.961166 | 0.984507, 0.939037 |
| cpu_resident_4t_32g/q17 | 0.992423 | 0.985574, 0.999329 |
| cpu_resident_4t_32g/q18 | 0.973418 | 0.984259, 0.962761 |

Lower ratios mean lower candidate query time. Faster in both blocks: raw_parquet_generic/q01, raw_parquet/q13, raw_parquet/q17, native/q01, native/q10, native/q18, cpu_resident_32g/q01, cpu_resident_32g/q06, cpu_resident_32g/q09, cpu_resident_32g/q13, cpu_resident_32g/q18, cpu_resident_4t_32g/q01, cpu_resident_4t_32g/q06, cpu_resident_4t_32g/q13, cpu_resident_4t_32g/q17, cpu_resident_4t_32g/q18.

Slower in both blocks: raw_parquet/q10, cpu_resident_32g/q10, cpu_resident_4t_32g/q10.

These directional observations preserve all regressions and controls; two blocks do not establish causal attribution, confidence-certified gains, neutrality or DuckDB leadership. Review this complete result before choosing the next source change.

Scope peak23082176512bytes, swap maximum0. Build and measurement share the48GiB scope; peak is cumulative, not query-only RSS. Exact cgroup events: `low 0; high 0; max 0; oom 0; oom_kill 0; oom_group_kill 0`.

[Source and tests](aggregate-batch-views-2026-09-11.md), [feature/resource gates](batch-views-validation-2026-09-11.md), [complete archive](benchmarks/2026-09-11-batch-views-paired/manifest.json). The archive preserves samples,plans,results,independent oracles,traces and provenance.

## Complete-result assessment

Retain480109f6 provisionally for the observed shared grouped-ingestion improvement. Q1 mean-time reductions are18.10% on generic raw,21.10% native,20.76% resident16 and18.63% resident4; each improves in both blocks. NativeQ18 and both residentQ18 cases also improve in both blocks. Q10 is slower in both blocks on raw (2.83%),resident16 (1.13%) and resident4 (3.11%). NativeQ6 has strongly opposing block ratios0.642290/1.599987; do not infer precision or neutrality from its mean.

The914-file archive verifies523inputs and preserves all regressions. These two-block results support a shared improvement candidate, not confidence-certified performance or leadership. Full provider/residency screens31964 now measure this exact frozen binary, with default ownership and matched conditions. Source remains frozen through their terminal verification and archival.
