# Compiled numeric coercion: matched diagnostic — 2026-09-11

Sequence43258 completed successfully. Candidate `97e53169faead2f401b929842341077dcd0b84e122644a0893579c95d2624f4e` versus control13210a20 produced120independently typed-correct outputs and2476join traces. All526 source inputs, both binaries, dataset/providers, driver and harness verify after execution. Of60matched logical/physical plan pairs, 0 differ. Complete comparison details are retained in plan-comparison.json.

Two reversed-order blocks, default disjoint ownership,GPUoff. Raw/native16threads,4/12GiB query/process; decoded resident16/4threads,32/48GiB with preload excluded. Generic rawQ1/Q6 disables morsel routing and actual physical routes are asserted. Instrumentation is matched. The180-second watchdog is diagnostic, not a fresh DuckDB10× gate. No confidence-certified regression bound or provider/resource/concurrency acceptance is implied.

| Mode/query | Candidate/control mean | Block ratios |
|---|---:|---|
| raw_parquet_generic/q01 | 1.044120 | 1.104538, 0.985162 |
| raw_parquet_generic/q06 | 2.224442 | 2.264318, 2.184428 |
| raw_parquet/q01 | 1.005349 | 1.003021, 1.007709 |
| raw_parquet/q06 | 1.203546 | 1.190474, 1.216752 |
| raw_parquet/q09 | 1.013074 | 1.028330, 0.998374 |
| raw_parquet/q10 | 1.013298 | 1.015017, 1.011650 |
| raw_parquet/q13 | 1.010024 | 1.021036, 0.999195 |
| raw_parquet/q17 | 1.000311 | 0.998667, 1.001949 |
| raw_parquet/q18 | 1.025425 | 1.003787, 1.047532 |
| native/q01 | 1.004208 | 1.007026, 1.001413 |
| native/q06 | 1.421561 | 1.505894, 1.358322 |
| native/q09 | 1.017546 | 1.034935, 1.000708 |
| native/q10 | 1.000240 | 1.005551, 0.994835 |
| native/q13 | 1.015745 | 1.003546, 1.027820 |
| native/q17 | 1.013249 | 1.020674, 1.005866 |
| native/q18 | 0.983183 | 0.979627, 0.986810 |
| cpu_resident_32g/q01 | 0.996901 | 1.000677, 0.993119 |
| cpu_resident_32g/q06 | 1.166881 | 1.152848, 1.180968 |
| cpu_resident_32g/q09 | 0.973830 | 0.945669, 1.003206 |
| cpu_resident_32g/q10 | 1.037115 | 1.069517, 1.004737 |
| cpu_resident_32g/q13 | 1.044026 | 1.001109, 1.087418 |
| cpu_resident_32g/q17 | 0.984333 | 0.989341, 0.979440 |
| cpu_resident_32g/q18 | 0.997435 | 0.996518, 0.998353 |
| cpu_resident_4t_32g/q01 | 0.996751 | 0.997874, 0.995630 |
| cpu_resident_4t_32g/q06 | 1.376582 | 1.384785, 1.368396 |
| cpu_resident_4t_32g/q09 | 0.995661 | 0.984882, 1.006686 |
| cpu_resident_4t_32g/q10 | 0.991094 | 0.981221, 1.001094 |
| cpu_resident_4t_32g/q13 | 1.029473 | 1.018613, 1.040521 |
| cpu_resident_4t_32g/q17 | 1.001079 | 0.999508, 1.002650 |
| cpu_resident_4t_32g/q18 | 0.993352 | 0.981137, 1.005889 |

Lower ratios mean lower candidate query time. Faster in both blocks: native/q18, cpu_resident_32g/q17, cpu_resident_32g/q18, cpu_resident_4t_32g/q01.

Slower in both blocks: raw_parquet_generic/q06, raw_parquet/q01, raw_parquet/q06, raw_parquet/q10, raw_parquet/q18, native/q01, native/q06, native/q09, native/q13, native/q17, cpu_resident_32g/q06, cpu_resident_32g/q10, cpu_resident_32g/q13, cpu_resident_4t_32g/q06, cpu_resident_4t_32g/q13.

These directional observations preserve all regressions and controls; two blocks do not establish causal attribution, confidence-certified gains, neutrality or DuckDB leadership. Review this complete result before choosing the next source change.

Scope peak22504140800bytes, swap maximum0. Build and measurement share the48GiB scope; peak is cumulative, not query-only RSS. Exact cgroup events: `low 0; high 0; max 0; oom 0; oom_kill 0; oom_group_kill 0`.

[Source, tests and native boundary proof](compiled-numeric-coercion-plan-2026-09-11.md), [feature/resource gates](benchmarks/2026-09-11-compiled-coercion-validation/manifest.json), [complete archive](benchmarks/2026-09-11-compiled-coercion-paired/manifest.json). The archive preserves samples,plans,results,independent oracles,traces and provenance.
