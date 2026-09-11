# Repeated validity decoding: matched diagnostic — 2026-09-11

Sequence66120 completed successfully. Candidate240cd5e2987324f3d950642fe218c63856ce7742dcfe9470e6b2e98429e0b532 versus control68912c231c1442f79d3f5433bb5c41795cde37b18a2ca777976fb8cb3799b203:120independently typed-correct outputs and2476join traces. All521 source inputs, both binaries, dataset/providers, driver and harness verify after execution. Every one of60matched logical and physical plan pairs is equal.

Two reversed-order blocks, default disjoint ownership,GPUoff. Raw/native16threads,4/12GiB query/process; decoded resident16/4threads,32/48GiB with preload excluded. Generic rawQ1/Q6 explicitly disables morsel routing; actual physical routes are asserted. Instrumentation is matched. The180-second watchdog is diagnostic, not a fresh DuckDB10× gate. No confidence-certified regression bound or provider/resource/concurrency acceptance is implied.

| Mode/query | Candidate/control mean | Block ratios |
|---|---:|---|
| raw_parquet_generic/q01 | 1.011340 | 1.022329, 1.000165 |
| raw_parquet_generic/q06 | 1.074764 | 0.986766, 1.166644 |
| raw_parquet/q01 | 1.019667 | 1.013386, 1.025956 |
| raw_parquet/q06 | 0.994095 | 0.994927, 0.993232 |
| raw_parquet/q09 | 0.959229 | 0.958297, 0.960161 |
| raw_parquet/q10 | 0.953205 | 0.961157, 0.945551 |
| raw_parquet/q13 | 0.999512 | 1.016713, 0.982888 |
| raw_parquet/q17 | 0.986539 | 0.997030, 0.976246 |
| raw_parquet/q18 | 0.927114 | 0.972878, 0.885965 |
| native/q01 | 0.991283 | 0.995951, 0.986645 |
| native/q06 | 1.137260 | 0.900959, 1.518787 |
| native/q09 | 1.012739 | 0.984288, 1.042079 |
| native/q10 | 0.978182 | 1.018514, 0.940882 |
| native/q13 | 0.976195 | 0.967223, 0.985275 |
| native/q17 | 1.019884 | 1.011079, 1.028683 |
| native/q18 | 1.000625 | 0.994847, 1.006360 |
| cpu_resident_32g/q01 | 0.981648 | 0.980411, 0.982891 |
| cpu_resident_32g/q06 | 1.007468 | 1.006421, 1.008510 |
| cpu_resident_32g/q09 | 0.989242 | 0.986044, 0.992482 |
| cpu_resident_32g/q10 | 0.991703 | 1.005577, 0.977975 |
| cpu_resident_32g/q13 | 1.012873 | 1.017373, 1.008441 |
| cpu_resident_32g/q17 | 0.997629 | 0.993298, 1.001996 |
| cpu_resident_32g/q18 | 0.986290 | 0.973410, 0.999572 |
| cpu_resident_4t_32g/q01 | 1.009499 | 1.018783, 1.000282 |
| cpu_resident_4t_32g/q06 | 0.992881 | 0.982934, 1.003060 |
| cpu_resident_4t_32g/q09 | 1.013020 | 0.982718, 1.044091 |
| cpu_resident_4t_32g/q10 | 0.985171 | 0.979204, 0.991057 |
| cpu_resident_4t_32g/q13 | 0.977131 | 0.991687, 0.962902 |
| cpu_resident_4t_32g/q17 | 0.997268 | 0.992674, 1.001872 |
| cpu_resident_4t_32g/q18 | 1.004674 | 1.010146, 0.999193 |

RawQ9 andQ10 improve in both blocks, by4.08% and4.68% on ratios of means. RawQ18 averages7.29%lower time but has asymmetric blocks. Generic rawQ1 does not improve (1.13%higher mean); ordinary rawQ1 is1.97%slower in both blocks. NativeQ17 is1.99%slower in both. NativeQ6 and generic rawQ6 have large opposing block differences, reinforcing the unresolved short-query precision gate. Preserve these negative results; unaffected-provider variations cannot be attributed to validity decoding from these samples.

This candidate supplies modest observed improvements on some admitted raw scan workloads, but does not solve the large generic grouped-ingestion gap. Retain it provisionally as an experimentally measured decoder change, with full provider/resource/concurrency and precision gates still open. The current binary is not certified faster than DuckDB. Do not infer a universal gain or performance neutrality.

Scope peak23,280,218,112bytes,swap0,zeroOOM/max events. Build and measurement share the scope, so peak is cumulative rather than query-only RSS. Footer-only inspection confirms optional Q1 columns; the first row group has no NULLs and five of six inspected columns are dictionary-encoded. It does not prove encoded run distribution or runtime frequency. Packed dictionary-ID decoding still extracts each bit individually in HybridDecoder::value; that is a bounded next source investigation alongside batch-bound aggregate inputs, requiring independent domain/error/ownership tests and fresh measurements.

[Source and tests](repeated-validity-decoding-2026-09-11.md), [feature/resource gates](validity-repeated-validation-2026-09-11.md), [complete archive](benchmarks/2026-09-11-validity-repeated-paired/manifest.json). The archive preserves every sample,plan,result,oracle,trace and provenance record; all raw numbers are in paired-ratios.json.
