# Packed-word decoding: matched diagnostic — 2026-09-11

Sequence56279 completed successfully. Candidate `63cabb3acf305b7f69cfa2a75390a732828d7417ae4b54e364038365761441f8` versus control240cd5e2 produced120independently typed-correct outputs and2476join traces. All521 source inputs, both binaries, dataset/providers, driver and harness verify after execution. Of60matched logical/physical plan pairs, 0 differ. Complete comparison details are retained in plan-comparison.json.

Two reversed-order blocks, default disjoint ownership,GPUoff. Raw/native16threads,4/12GiB query/process; decoded resident16/4threads,32/48GiB with preload excluded. Generic rawQ1/Q6 disables morsel routing and actual physical routes are asserted. Instrumentation is matched. The180-second watchdog is diagnostic, not a fresh DuckDB10× gate. No confidence-certified regression bound or provider/resource/concurrency acceptance is implied.

| Mode/query | Candidate/control mean | Block ratios |
|---|---:|---|
| raw_parquet_generic/q01 | 1.035281 | 0.988685, 1.082248 |
| raw_parquet_generic/q06 | 1.066437 | 1.006817, 1.124783 |
| raw_parquet/q01 | 1.003390 | 1.008464, 0.998368 |
| raw_parquet/q06 | 1.020992 | 1.044380, 0.997772 |
| raw_parquet/q09 | 0.998218 | 0.999943, 0.996532 |
| raw_parquet/q10 | 1.005995 | 0.970231, 1.042324 |
| raw_parquet/q13 | 1.001847 | 0.986969, 1.017025 |
| raw_parquet/q17 | 1.007431 | 1.013706, 1.001188 |
| raw_parquet/q18 | 0.970651 | 0.967739, 0.973602 |
| native/q01 | 0.986295 | 0.987305, 0.985304 |
| native/q06 | 1.136598 | 1.167019, 1.089013 |
| native/q09 | 1.035107 | 1.025852, 1.044344 |
| native/q10 | 0.991553 | 0.974316, 1.008964 |
| native/q13 | 0.981288 | 0.987284, 0.975308 |
| native/q17 | 0.993536 | 1.023957, 0.964569 |
| native/q18 | 0.994528 | 1.016744, 0.972877 |
| cpu_resident_32g/q01 | 0.984030 | 0.986956, 0.981150 |
| cpu_resident_32g/q06 | 0.993836 | 0.989665, 0.998049 |
| cpu_resident_32g/q09 | 0.993000 | 0.988695, 0.997279 |
| cpu_resident_32g/q10 | 1.000453 | 0.990199, 1.010849 |
| cpu_resident_32g/q13 | 0.993886 | 0.998409, 0.989408 |
| cpu_resident_32g/q17 | 0.996796 | 0.993742, 0.999871 |
| cpu_resident_32g/q18 | 1.047894 | 1.055585, 1.040067 |
| cpu_resident_4t_32g/q01 | 1.006354 | 1.008360, 1.004344 |
| cpu_resident_4t_32g/q06 | 0.993022 | 0.997894, 0.988176 |
| cpu_resident_4t_32g/q09 | 1.000490 | 0.993398, 1.007520 |
| cpu_resident_4t_32g/q10 | 1.003086 | 1.012893, 0.993221 |
| cpu_resident_4t_32g/q13 | 0.996122 | 1.008114, 0.984376 |
| cpu_resident_4t_32g/q17 | 0.995415 | 0.997730, 0.993114 |
| cpu_resident_4t_32g/q18 | 1.007606 | 0.996855, 1.018720 |

Lower ratios mean lower candidate query time. Faster in both blocks: raw_parquet/q09, raw_parquet/q18, native/q01, native/q13, cpu_resident_32g/q01, cpu_resident_32g/q06, cpu_resident_32g/q09, cpu_resident_32g/q13, cpu_resident_32g/q17, cpu_resident_4t_32g/q06, cpu_resident_4t_32g/q17.

Slower in both blocks: raw_parquet_generic/q06, raw_parquet/q17, native/q06, native/q09, cpu_resident_32g/q18, cpu_resident_4t_32g/q01.

These directional observations preserve all regressions and controls; two blocks do not establish causal attribution, confidence-certified gains, neutrality or DuckDB leadership. Review this complete result before choosing the next source change.

Scope peak23109767168bytes, swap maximum0. Build and measurement share the48GiB scope; peak is cumulative, not query-only RSS. Exact cgroup events: `low 0; high 0; max 0; oom 0; oom_kill 0; oom_group_kill 0`.

[Source and tests](packed-word-decoding-2026-09-11.md), [feature/resource gates](packed-word-validation-2026-09-11.md), [complete archive](benchmarks/2026-09-11-packed-word-paired/manifest.json). The archive preserves samples,plans,results,independent oracles,traces and provenance.

## Decision after complete evidence review

Reject the packed-word production optimization. Generic rawQ1 mean time is3.53%higher with opposing blocks; nativeQ6 is13.66%higher, nativeQ9 3.51%higher and resident16Q18 4.79%higher, with both blocks slower for those three controls. RawQ18 improves2.93%, but the shared decoder/grouped-ingestion gain is not established. Unaffected-provider movement cannot be assumed to be caused by decoder instructions, nor dismissed as harmless noise. The evidence does not support retaining this change for throughput.

After native contract tests82790 completed and archived, restore only the production packed extraction to240cd5e2; retain independent width/domain tests and the native membership/operator test repair. Current production hybrid code matches240cd5e2 exactly. Follow-up validation79575 is active. [Rollback evidence](decoder-rollback-2026-09-11.md). The original915-file measurement archive remains immutable.
