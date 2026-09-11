# Incremental Parquet headers: matched diagnostic — 2026-09-11

Sequence57421 completed successfully. Candidate `13210a204fab3675cd8200d9361ec55e0a2e6f38a64d12c7a4d74d9cdbe2a1ca` versus control480109f6 produced120independently typed-correct outputs and2476join traces. All524 source inputs, both binaries, dataset/providers, driver and harness verify after execution. Of60matched logical/physical plan pairs, 0 differ. Complete comparison details are retained in plan-comparison.json.

Two reversed-order blocks, default disjoint ownership,GPUoff. Raw/native16threads,4/12GiB query/process; decoded resident16/4threads,32/48GiB with preload excluded. Generic rawQ1/Q6 disables morsel routing and actual physical routes are asserted. Instrumentation is matched. The180-second watchdog is diagnostic, not a fresh DuckDB10× gate. No confidence-certified regression bound or provider/resource/concurrency acceptance is implied.

| Mode/query | Candidate/control mean | Block ratios |
|---|---:|---|
| raw_parquet_generic/q01 | 1.003091 | 1.015882, 0.990462 |
| raw_parquet_generic/q06 | 1.034254 | 1.087360, 0.981897 |
| raw_parquet/q01 | 0.974282 | 0.939697, 1.010666 |
| raw_parquet/q06 | 0.983020 | 0.961397, 1.005473 |
| raw_parquet/q09 | 0.963374 | 0.957647, 0.969336 |
| raw_parquet/q10 | 0.975682 | 1.003353, 0.949213 |
| raw_parquet/q13 | 1.007620 | 1.001402, 1.013942 |
| raw_parquet/q17 | 1.001257 | 0.995093, 1.007437 |
| raw_parquet/q18 | 1.016844 | 0.987812, 1.048379 |
| native/q01 | 0.998183 | 0.993811, 1.002666 |
| native/q06 | 1.182007 | 1.238962, 1.110117 |
| native/q09 | 0.981249 | 0.996347, 0.966603 |
| native/q10 | 0.991758 | 1.010284, 0.973861 |
| native/q13 | 1.003743 | 1.004268, 1.003224 |
| native/q17 | 1.001682 | 0.985481, 1.018218 |
| native/q18 | 0.987114 | 1.023941, 0.952024 |
| cpu_resident_32g/q01 | 1.009384 | 1.010292, 1.008463 |
| cpu_resident_32g/q06 | 1.006984 | 0.993600, 1.020611 |
| cpu_resident_32g/q09 | 1.000769 | 0.983066, 1.018622 |
| cpu_resident_32g/q10 | 1.004591 | 0.997573, 1.011771 |
| cpu_resident_32g/q13 | 1.009470 | 0.981419, 1.037871 |
| cpu_resident_32g/q17 | 0.998123 | 0.982512, 1.013800 |
| cpu_resident_32g/q18 | 1.001246 | 1.008617, 0.993936 |
| cpu_resident_4t_32g/q01 | 0.984349 | 0.989744, 0.978949 |
| cpu_resident_4t_32g/q06 | 1.005229 | 1.008342, 1.002092 |
| cpu_resident_4t_32g/q09 | 0.999802 | 1.033657, 0.966159 |
| cpu_resident_4t_32g/q10 | 0.993542 | 1.007051, 0.980328 |
| cpu_resident_4t_32g/q13 | 0.992579 | 0.998387, 0.986815 |
| cpu_resident_4t_32g/q17 | 1.001321 | 1.007590, 0.995075 |
| cpu_resident_4t_32g/q18 | 0.995827 | 1.003080, 0.988604 |

Lower ratios mean lower candidate query time. Faster in both blocks: raw_parquet/q09, native/q09, cpu_resident_4t_32g/q01, cpu_resident_4t_32g/q13.

Slower in both blocks: raw_parquet/q13, native/q06, native/q13, cpu_resident_32g/q01, cpu_resident_4t_32g/q06.

These directional observations preserve all regressions and controls; two blocks do not establish causal attribution, confidence-certified gains, neutrality or DuckDB leadership. Review this complete result before choosing the next source change.

Scope peak22650191872bytes, swap maximum0. Build and measurement share the48GiB scope; peak is cumulative, not query-only RSS. Exact cgroup events: `low 0; high 0; max 0; oom 0; oom_kill 0; oom_group_kill 0`.

[Source and tests](incremental-header-admission-plan-2026-09-11.md), [feature/resource gates](incremental-header-validation-2026-09-11.md), [complete archive](benchmarks/2026-09-11-incremental-header-paired/manifest.json). The archive preserves samples,plans,results,independent oracles,traces and provenance.

## Review and follow-up

The candidate has a reproduced header-admission improvement, but is not accepted as performance-neutral. RawQ9 is lower in both blocks (mean0.963374), while nativeQ6 is slower in both (1.182007); raw/nativeQ13 and resident16Q1 also regress in both blocks. NativeQ6 precision is unresolved. Preserve this result rather than dismissing it as noise. Identical480109f6 nativeQ6/Q9 control12690 runs8balanced blocks at the same4/12GiB and instrumentation boundary. A predeclared bootstrap interval within0.95–1.05 for both queries is required before a dependent13210a20 comparison runs. This is a precision diagnostic, not broad confidence certification. Source stays frozen through that check.
