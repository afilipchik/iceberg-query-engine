# SF10 baseline — 2026-09-05

Measured from production source `88849c4f8e47b6fc9149ffd15de0429cd9d3ad41` versus DuckDB **1.4.4**. This report covers **six engine modes plus a GPU-binary CPU control**: raw Parquet, decoded IPC cache, native tables, Iceberg, Lance and GPU-assisted execution. Each track runs all 22 queries ten measured times: **1,540 measured engine executions, all validated and within the 10× time gate**.

This is the repository’s **custom TPC-H-derived SF10 workload**, not an official TPC-H result. GPU measurements use a benchmark-only adapter and additional device memory; they are not CPU-only comparisons. Production engine source and dependencies were not modified.

## Results

Totals are **sums of per-query medians**, not one suite’s wall time. Ratios are engine ÷ DuckDB; lower is better. Wins count median comparisons without a statistical-significance claim.

| Engine / DuckDB storage | Engine total (s) | DuckDB total (s) | Total ratio | Geometric mean ratio | Wins / 22 | Validated samples |
|---|---:|---:|---:|---:|---:|---:|
| Raw Parquet / Parquet | 7.446 | 4.547 | 1.638× | 1.626× | 1 | 220/220 |
| Decoded IPC / Parquet | 5.207 | 4.527 | 1.150× | 1.078× | 8 | 220/220 |
| Native / native | 5.844 | 3.438 | 1.700× | 2.325× | 1 | 220/220 |
| Iceberg / Iceberg | 9.933 | 7.712 | 1.288× | 1.187× | 6 | 220/220 |
| Lance / direct Lance extension | 7.013 | 6.900 | 1.016× | 0.894× | 12 | 220/220 |
| GPU-assisted IPC / Parquet (CPU) | 4.914 | 4.510 | 1.089× | 0.812× | 10 | 220/220 |
| GPU binary, CPU control / Parquet | 5.207 | 4.524 | 1.151× | 1.069× | 8 | 220/220 |

**No track establishes a suite-level performance lead over its DuckDB reference.** Raw Parquet and native remain substantially behind. Direct Lance is approximately level by suite total (1.016×), with query variability making a 1.6% difference insufficient to claim a robust lead for either engine. Its geometric mean is 0.894× and it wins 12 queries, but larger losses offset those wins.

GPU-assisted execution lowers the measured engine suite total by **5.63%** versus its same-binary CPU control. Actual device work occurred only on **Q1 and Q6**, ten measured samples each; the other twenty queries ran on CPU. The assisted suite still takes 1.089× DuckDB Parquet. These CPU/GPU controls were sequential blocks, so their aggregate difference should not be attributed to acceleration on every query.

The native geometric mean is 2.325× despite a 1.700× suite ratio. Q9 is the only native win: 1,158.8 versus 1,613.7 ms. Q13 (751.6 versus 135.8 ms) and Q1 (461.8 versus 113.1 ms) contribute about 40% of the net native suite excess.

Decoded IPC lowers the engine’s measured total by about 30% relative to raw Parquet, but it uses a different storage premise from DuckDB Parquet. Direct Lance results use the installed community extension and must not be confused with the older Arrow-materialized reference.

See [all-mode query summaries](benchmarks/2026-09-05-sf10/all-modes-summary.json), [all 1,540 measured pairs](benchmarks/2026-09-05-sf10/all-modes-samples.csv), and the per-query tables below.

## Shared conditions and core timing contract

- Core run window: **2026-09-05 18:44:16–18:50:50 UTC** (11:44:16–11:50:50 PDT). Tracks ran sequentially; no overlapping engine/DuckDB queries. Every engine query record reports zero other active queries.
- Host: Intel Core i9-13900KF, approximately 125 GiB RAM. Both processes were pinned to **CPUs 0–15: eight P cores, sixteen hardware threads**. Affinity limits CPU placement; it does not reserve those cores from other host activity. Per-sample host load is retained.
- DuckDB `threads=16`; engine `RAYON_NUM_THREADS=16` and `TOKIO_WORKER_THREADS=16`. The engine has multiple worker pools, so these settings are not a claim that it has only sixteen OS threads. Both share the same sixteen-CPU allocation.
- Both use a **40 GiB configured execution-memory setting**. Engine `QE_MEM_CAP=48G` adds an anonymous-memory rlimit. The benchmark and its child engine share one independently verified **64 GiB cgroup**, with swap disabled. These are distinct controls; they are not equivalent per-engine hard RSS budgets.
- Core build: default features, Rust 1.93.0, release opt-level 3, LTO, one codegen unit; no custom RUSTFLAGS. DuckDB 1.4.4, Python 3.12.3, PyArrow 25.0.1. THP was disabled in the observed processes.
- Every query receives one excluded DuckDB warm-up, three excluded DuckDB calibration samples, one excluded engine warm-up, then ten measured pairs. Engine/DuckDB order within each pair is randomized with seed 20260905. Query order is Q1–Q22. Loading, file hashing, preflight checks, validation and plan retrieval are outside timings.
- Warm storage/OS caches; no global cache drops. Raw mode forces `QE_IPC_CACHE=0`. Cached mode uses Auto and requires all eight freshness stamps plus every expected row-group sidecar. Native tables use existing snapshot version 1; DuckDB native tables are loaded from the same raw Parquet inputs before timing.
- **Primary engine time:** HTTP `x-qe-elapsed-ms`, through query result collection, excluding Arrow HTTP response encoding. **DuckDB time:** `execute(sql).fetch_arrow_table()`, including Arrow materialization. Engine client wall time, including HTTP and Arrow decoding, is also retained. This remaining boundary difference slightly favors the engine.

Using engine client wall times instead gives suite ratios **1.642× raw**, **1.154× cached**, and **1.705× native**. The timing-boundary difference does not change the conclusion.

The measured-sample ceiling is ten times each query’s fresh three-sample DuckDB calibration median. Transport has a separate two-second allowance; the server-time check still enforces the actual ceiling. A lost request stops the owned engine process so abandoned work cannot contaminate later samples. No request failed or required a restart. The largest measured engine/calibration ratio was 5.638×.

## Data and answer validation

Eight raw Parquet files total **3,331,971,396 bytes**; lineitem has 60 million rows and orders 15 million. The SQL is extracted from `src/tpch/queries.rs`; Q11’s single threshold is changed from 0.0001 to **0.00001**, matching `get_query_for_sf(11, 10)`. Neither data nor engine source was changed.

The comparator checks typed **row multisets**, preserving duplicates. Integers, decimals, strings, dates and NULL values compare exactly; integer/decimal widths and dictionary encodings may differ without losing value precision. Floating values use explicit relative tolerance **1e-10** and absolute tolerance **1e-8**. It does not round numbers or parse numeric-looking strings. NaN matches only NaN; infinities retain sign.

In the original three-track block, all 660 measured comparisons, 66 warm-up comparisons and 24 per-table preflight comparisons passed. The additional block contributes 880 measured pairs and also compares both engines’ answers with the preserved raw-Parquet oracle. The preflight checks count, non-NULL key count, minimum, maximum and exact key sum. They are a sanity check, not proof of full native/Parquet equivalence. Raw files, IPC sidecars, native manifests and native file contents have retained SHA-256 fingerprints; native segment hashing happened after timing.

Schema differences occurred on Q7, Q8, Q9 and Q12 in every track; full schemas are preserved. Projection positions and logical type families were compared, while names, nullability and numeric-width differences were recorded separately. This is **value/multiplicity validation, not identical-schema or ORDER BY certification**. In the original core block, maximum observed relative floating error was 9.48e-15; maximum absolute error was 0.004089 on large aggregate values, within the relative tolerance.

## A concrete architectural signal

Q1’s optimized logical plan is identical across engine storage modes, but its physical route changes:

```text
Raw Parquet / IPC: ExternalSort → Project → MorselAggregate
Native:            ExternalSort → Project → SpillableHashAggregate
                     → Project → Filter → MemoryTableScan
```

| Q1 track | Server median (ms) | Planning median (ms) | Execution metric median (ms) |
|---|---:|---:|---:|
| Raw Parquet | 352.106 | 0.062 | 348.466 |
| Decoded IPC | 290.665 | 0.062 | 287.473 |
| Native | 461.775 | 79.230 | 295.722 |

The [physical planner](../src/physical/planner.rs) exposes the fused scan/aggregate route for Parquet, while native Q1 falls through to a provider scan and separate filter/project stages. See the Parquet aggregate routing near line 1275, native shape restrictions near line 256, and fallback provider scan near line 1571. [Native scanning](../src/storage/native_table.rs) collects mapped batches; the [filter](../src/physical/operators/filter.rs) then compacts columns.

Native planning time is material and its execution metric alone understates query latency. Around 86 ms also lies outside the recorded parse/optimize/plan/execute components; identifying that residual requires further instrumentation. It should not be attributed to a kernel from wall time alone. DuckDB’s saved native Q1 plan uses a perfect-hash aggregate with compact string encoding, while its Parquet plan uses hash grouping. The engine already contains specialized aggregation kernels: the issue is how provider routing and materialization select and feed them, not proof that such kernels are absent.

All Q1 query records report only 454 bytes of pool peak while process high-water marks are orders of magnitude larger. Pool observations are not whole-query memory accounting. Process VmHWM is also a lifetime high-water mark, not incremental per-query peak; DuckDB’s process includes the Python driver, setup and earlier tracks. This run establishes an in-memory latency baseline, not larger-than-memory safety.

Use native Q13 and Q1 as the first profiling targets, then Q21/Q18. Keep raw Parquet and native parity gates separate. The [comprehensive audit](project-audit-2026-09-05.md) remains the broader architecture and correctness plan.

## Per-query measurements

Entries are **median [25th–75th percentile] milliseconds**. All rows have 10/10 completed, validated samples and pass the time gate. Percentiles are interpolated from the ten retained samples.

### Raw Parquet / Parquet

| Query | Engine ms [IQR] | DuckDB ms [IQR] | Ratio | Rows |
|---|---:|---:|---:|---:|
| Q01 | 352.106 [350.065–370.179] | 170.962 [169.269–171.308] | 2.060× | 6 |
| Q02 | 58.785 [54.479–63.349] | 62.336 [61.636–65.124] | 0.943× | 100 |
| Q03 | 419.024 [393.104–450.019] | 178.329 [176.397–182.202] | 2.350× | 10 |
| Q04 | 218.976 [216.007–228.883] | 115.956 [114.013–119.128] | 1.888× | 5 |
| Q05 | 288.430 [283.547–297.670] | 152.295 [150.800–154.378] | 1.894× | 5 |
| Q06 | 103.370 [103.006–104.528] | 74.034 [71.462–75.219] | 1.396× | 1 |
| Q07 | 338.199 [327.735–346.263] | 154.238 [152.684–156.595] | 2.193× | 4 |
| Q08 | 331.320 [325.104–363.906] | 232.863 [226.779–235.743] | 1.423× | 2 |
| Q09 | 1349.197 [1330.284–1381.583] | 957.227 [953.831–962.092] | 1.409× | 175 |
| Q10 | 464.433 [438.509–538.683] | 226.343 [220.078–228.333] | 2.052× | 20 |
| Q11 | 51.820 [51.250–58.199] | 35.901 [34.252–36.880] | 1.443× | 100 |
| Q12 | 226.903 [218.899–241.210] | 126.211 [122.221–132.291] | 1.798× | 2 |
| Q13 | 299.966 [292.349–308.713] | 148.444 [143.886–149.279] | 2.021× | 24 |
| Q14 | 169.296 [151.771–172.582] | 124.368 [121.941–128.672] | 1.361× | 1 |
| Q15 | 122.864 [120.817–127.468] | 83.790 [81.684–87.996] | 1.466× | 1 |
| Q16 | 150.752 [144.444–152.578] | 77.452 [75.605–82.817] | 1.946× | 320 |
| Q17 | 436.776 [415.774–443.838] | 208.210 [203.274–211.501] | 2.098× | 1 |
| Q18 | 584.718 [553.153–588.675] | 366.031 [356.580–369.847] | 1.597× | 100 |
| Q19 | 209.109 [206.073–220.283] | 160.300 [158.296–163.646] | 1.304× | 1 |
| Q20 | 465.923 [451.690–506.005] | 314.740 [309.066–320.686] | 1.480× | 3953 |
| Q21 | 657.796 [654.084–673.039] | 444.539 [436.987–449.109] | 1.480× | 100 |
| Q22 | 146.142 [140.431–149.894] | 132.453 [128.330–136.518] | 1.103× | 7 |

### Decoded IPC / Parquet

| Query | Engine ms [IQR] | DuckDB ms [IQR] | Ratio | Rows |
|---|---:|---:|---:|---:|
| Q01 | 290.665 [286.656–297.933] | 170.921 [169.074–173.690] | 1.701× | 6 |
| Q02 | 26.952 [26.038–27.927] | 61.262 [58.681–62.576] | 0.440× | 100 |
| Q03 | 241.394 [232.267–263.135] | 178.063 [176.749–185.935] | 1.356× | 10 |
| Q04 | 136.174 [131.502–142.846] | 117.091 [115.859–118.248] | 1.163× | 5 |
| Q05 | 160.214 [152.651–167.243] | 153.641 [146.946–156.485] | 1.043× | 5 |
| Q06 | 90.774 [87.882–95.241] | 70.811 [68.851–73.836] | 1.282× | 1 |
| Q07 | 208.969 [205.361–221.051] | 152.705 [151.911–156.136] | 1.368× | 4 |
| Q08 | 197.344 [193.798–225.812] | 229.471 [227.357–233.903] | 0.860× | 2 |
| Q09 | 1207.510 [1157.682–1228.668] | 964.721 [954.821–972.508] | 1.252× | 175 |
| Q10 | 195.245 [174.993–209.503] | 225.476 [221.930–226.753] | 0.866× | 20 |
| Q11 | 22.151 [20.601–22.679] | 36.672 [36.322–37.377] | 0.604× | 100 |
| Q12 | 169.597 [164.223–172.102] | 129.809 [122.971–130.780] | 1.307× | 2 |
| Q13 | 255.488 [251.325–273.093] | 147.316 [142.636–150.995] | 1.734× | 24 |
| Q14 | 116.569 [112.975–119.797] | 121.741 [119.762–124.465] | 0.958× | 1 |
| Q15 | 92.490 [91.155–95.083] | 83.533 [82.063–90.526] | 1.107× | 1 |
| Q16 | 127.657 [119.035–134.466] | 78.284 [76.481–81.914] | 1.631× | 320 |
| Q17 | 154.252 [149.794–157.906] | 205.224 [201.816–206.285] | 0.752× | 1 |
| Q18 | 431.986 [388.640–456.804] | 361.252 [358.052–362.408] | 1.196× | 100 |
| Q19 | 228.511 [220.817–232.472] | 159.354 [155.127–162.316] | 1.434× | 1 |
| Q20 | 284.331 [276.135–314.437] | 305.888 [304.377–311.028] | 0.930× | 3953 |
| Q21 | 452.836 [443.988–485.025] | 443.655 [439.376–459.960] | 1.021× | 100 |
| Q22 | 115.681 [113.610–127.968] | 129.720 [127.023–138.628] | 0.892× | 7 |

### Native / native

| Query | Engine ms [IQR] | DuckDB ms [IQR] | Ratio | Rows |
|---|---:|---:|---:|---:|
| Q01 | 461.775 [456.413–463.350] | 113.139 [112.622–113.730] | 4.081× | 6 |
| Q02 | 30.635 [29.651–31.604] | 20.753 [19.891–21.317] | 1.476× | 100 |
| Q03 | 247.846 [242.564–253.900] | 83.166 [81.599–85.104] | 2.980× | 10 |
| Q04 | 138.062 [136.777–139.629] | 60.514 [57.843–61.371] | 2.281× | 5 |
| Q05 | 160.955 [155.426–165.089] | 51.953 [51.001–53.582] | 3.098× | 5 |
| Q06 | 110.054 [109.487–110.762] | 25.356 [25.112–27.733] | 4.340× | 1 |
| Q07 | 213.392 [212.389–214.042] | 77.422 [76.229–88.045] | 2.756× | 4 |
| Q08 | 164.458 [159.107–169.235] | 77.252 [75.421–90.153] | 2.129× | 2 |
| Q09 | 1158.774 [1128.703–1184.274] | 1613.738 [1588.619–1622.234] | 0.718× | 175 |
| Q10 | 243.139 [233.164–246.478] | 89.456 [85.372–97.288] | 2.718× | 20 |
| Q11 | 20.640 [20.446–20.909] | 11.573 [11.318–11.818] | 1.784× | 100 |
| Q12 | 195.758 [190.568–197.173] | 87.060 [86.583–88.781] | 2.249× | 2 |
| Q13 | 751.584 [741.105–758.282] | 135.769 [135.369–136.464] | 5.536× | 24 |
| Q14 | 91.934 [91.233–94.537] | 37.259 [36.953–39.371] | 2.467× | 1 |
| Q15 | 99.882 [98.536–106.064] | 34.137 [33.811–34.323] | 2.926× | 1 |
| Q16 | 107.834 [93.198–120.637] | 44.757 [43.251–46.171] | 2.409× | 320 |
| Q17 | 144.260 [135.532–147.729] | 97.615 [91.195–103.544] | 1.478× | 1 |
| Q18 | 436.706 [427.273–450.856] | 258.813 [257.219–261.719] | 1.687× | 100 |
| Q19 | 254.707 [248.632–260.534] | 91.043 [90.296–95.158] | 2.798× | 1 |
| Q20 | 311.320 [306.467–319.579] | 175.742 [172.274–183.205] | 1.771× | 3953 |
| Q21 | 438.493 [414.992–485.492] | 218.485 [215.723–225.021] | 2.007× | 100 |
| Q22 | 62.201 [61.071–65.656] | 32.713 [32.408–32.910] | 1.901× | 7 |

## Iceberg, Lance and GPU execution details

All additional tracks retain the same sixteen-CPU affinity, worker settings, 40 GiB configured execution budget, 48G engine rlimit, 64 GiB shared cgroup, warm timing protocol, exact SQL, and typed comparator. Builds and tracks ran sequentially. The extension’s 880 measured pairs passed both paired validation and independent comparison of **each side** against the preserved source-Parquet answers. No schema casts or numeric-tolerance relaxations were introduced.

| Block | UTC start | UTC end | Engine features / reference |
|---|---|---|---|
| iceberg | 20:38:52 | 20:42:40 | Default build / DuckDB Iceberg extension |
| lance | 20:42:40 | 20:45:49 | Lance build / direct DuckDB Lance extension |
| gpu | 20:45:50 | 20:50:04 | GPU adapter: CPU control, then GPU-assisted / DuckDB Parquet |

### Iceberg

Both engines read the same selected metadata files and current snapshots. The driver follows the engine’s version-hint / last-updated selection rules and retains selected paths, snapshot IDs and file hashes. DuckDB loads the existing Avro **93da8a1** and Iceberg **1095c1fa** extensions; no download was needed.

These eight format-v2 tables are local, unpartitioned, single-snapshot and delete-free. They contain 24 ZSTD Parquet files totaling **2,926,294,256 bytes**. Their schemas and query answers match the source dataset, but compression and file layout differ from the raw-Parquet track. Consequently, an Iceberg/raw timing difference cannot be attributed solely to metadata overhead.

The engine resolves Iceberg metadata/manifests during registration and exposes Parquet providers to steady queries. DuckDB uses views over pinned `iceberg_scan(metadata_path)` and may perform different binding work per query. Startup/readiness timings are retained; this comparison is steady SQL latency, not an isolated test of equal metadata-processing overhead.

### Lance

The engine was rebuilt with **Lance 10**. DuckDB uses its installed community Lance extension **892b224**, scanning the same eight version-1 datasets directly inside each timed query. The legacy Python-LanceDataset bridge and Arrow-materialized performance path are not used. PyLance **0.23.2** is used only to inspect dataset versions/schemas. Extension binaries and all Lance input files are fingerprinted.

The direct views preserve DATE values; Q18 and the full typed oracle checks pass without converting dates to strings. DuckDB extension queries run in an owned, killable worker process; primary time is measured inside that worker through Arrow materialization, excluding IPC back to the driver.

Observed Lance suite totals are close, while individual queries differ substantially. Q6 favors the engine (147.039 versus 657.784 ms), whereas Q18 favors DuckDB (1,133.755 versus 690.992 ms). Engine Q1 samples range from 472.385 to 596.715 ms. Preserve the dispersion and use broader workloads before claiming parity or superiority.

### GPU-assisted execution and CPU control

The normal `serve` entry point explicitly disables GPU offload, even in a GPU build. The new [benchmark adapter](../examples/sf10_gpu_serve.rs) registers the ordinary Parquet providers, calls the existing GPU opt-in, and serves through the production local HTTP/Arrow path. It preserves the library allocator, process cap, THP setting and topology initialization. GPU initialization must succeed; an unavailable device cannot silently become a CPU result.

Both control and assisted tracks use this same freshly built executable. `QE_GPU=0` is the CPU control; `QE_GPU=1` enables the hybrid path. Both use fresh existing IPC sidecars. Hardware is an **RTX 5090**, driver **580.173.02**, with 32,607 MiB reported device memory. `QE_GPU_CACHE_MB=24576` supplies a **24 GiB device-cache target in addition to host memory**. The cache target is soft; it is not an equivalent CPU-only memory envelope.

| Query | CPU control median | GPU-assisted median | Control / assisted | Measured device executions |
|---|---:|---:|---:|---:|
| Q01 | 295.485 ms | 28.075 ms | 10.52× | 10/10 |
| Q06 | 92.780 ms | 1.708 ms | 54.32× | 10/10 |

Every measured device use above has a completed `[gpu-trace] run OK` with no pending upload or failed kernel. The other 200 assisted samples ran on CPU. Counting a GPU wrapper in a plan is insufficient evidence; the request-scoped traces are archived.

**Residency preparation is excluded from the measured GPU timings.** Q1 needed 15 warm-up executions: fourteen CPU attempts followed by device execution. Summed request wall time plus explicit waits is at least **5.83 seconds**, excluding startup and preflight. Preflight key aggregates had already uploaded some columns. Q6 then reused residency established by Q1. These are warmed device-resident Q1/Q6 measurements, not cold end-to-end speedups.

**Q15 is a documented CPU fallback.** Its attempted wrapper occurs inside CTE materialization and is absent from the final displayed plan. All ten measured requests report `not ready` followed by `codes SKIP (not codeable)` for the integer supplier key; the implementation rejects that encoding before scanning. The initial warm-up plus ten samples produce **11 rejected group-code attempts**, recorded in the GPU cache’s `upload_failures` counter. These are unsupported-encoding refusals, not incorrect results or failed CUDA kernels. The suite must not be described as GPU-resident or GPU-executed on all queries.

The final cache holds **3,152,800,240 bytes (2.94 GiB)** across thirteen resident columns plus group-code state, excluding CUDA overhead. There were zero evictions and zero device-run fallbacks. Maximum measured GPU-block relative numerical difference is **1.52e-14**, within the unchanged 1e-10 relative / 1e-8 absolute tolerances; maximum absolute difference is 0.005981 on large aggregates.

The assisted/control suite decrease is 5.63%, but the median of per-query assisted/control ratios is about 1.007. The accelerator currently speeds up a narrow part of the workload. It does not resolve the broad join/planning/provider-routing gaps identified by the [architecture audit](project-audit-2026-09-05.md). Early rejection or caching of unsupported GPU encodings would also avoid Q15’s repeated unsuccessful requests.

## Additional per-query measurements

Median [25th–75th percentile] milliseconds; ten validated samples per row. In the GPU-assisted table, the device column counts measured samples with successful device work. A zero denotes CPU execution, including Q15’s unsupported-encoding fallback.

### Iceberg / Iceberg

| Q | Engine ms [IQR] | DuckDB ms [IQR] | Ratio | Device samples |
|---|---:|---:|---:|---:|
| Q01 | 452.743 [446.296–506.422] | 296.939 [289.998–297.390] | 1.525× | 0/10 |
| Q02 | 83.889 [81.267–91.243] | 384.238 [380.687–387.958] | 0.218× | 0/10 |
| Q03 | 589.714 [579.158–617.827] | 265.386 [258.626–268.655] | 2.222× | 0/10 |
| Q04 | 285.784 [283.142–293.896] | 143.849 [141.495–145.253] | 1.987× | 0/10 |
| Q05 | 490.798 [473.070–529.453] | 254.085 [246.995–257.720] | 1.932× | 0/10 |
| Q06 | 231.657 [230.257–233.475] | 152.147 [148.467–152.920] | 1.523× | 0/10 |
| Q07 | 495.426 [487.010–508.390] | 236.971 [233.735–245.243] | 2.091× | 0/10 |
| Q08 | 607.030 [580.428–637.129] | 345.950 [344.590–347.903] | 1.755× | 0/10 |
| Q09 | 1586.012 [1507.730–1601.841] | 1439.653 [1433.128–1442.581] | 1.102× | 0/10 |
| Q10 | 725.434 [691.277–740.385] | 377.116 [372.234–382.807] | 1.924× | 0/10 |
| Q11 | 73.495 [70.713–79.799] | 279.633 [276.121–286.107] | 0.263× | 0/10 |
| Q12 | 261.230 [254.678–274.647] | 159.901 [157.500–167.848] | 1.634× | 0/10 |
| Q13 | 325.609 [323.402–328.792] | 175.360 [170.807–178.234] | 1.857× | 0/10 |
| Q14 | 237.690 [235.477–255.587] | 233.395 [230.561–242.421] | 1.018× | 0/10 |
| Q15 | 183.675 [181.155–186.219] | 150.400 [147.682–151.615] | 1.221× | 0/10 |
| Q16 | 161.343 [158.618–196.369] | 341.515 [340.067–348.262] | 0.472× | 0/10 |
| Q17 | 641.699 [630.281–655.566] | 336.791 [331.108–339.435] | 1.905× | 0/10 |
| Q18 | 738.724 [732.228–758.770] | 498.709 [491.655–502.691] | 1.481× | 0/10 |
| Q19 | 236.973 [231.027–257.076] | 291.792 [287.513–301.234] | 0.812× | 0/10 |
| Q20 | 552.705 [529.383–589.753] | 624.927 [616.727–627.750] | 0.884× | 0/10 |
| Q21 | 794.959 [780.745–807.897] | 528.841 [524.503–536.889] | 1.503× | 0/10 |
| Q22 | 176.649 [174.743–189.088] | 193.914 [189.616–195.227] | 0.911× | 0/10 |

### Lance / direct Lance extension

| Q | Engine ms [IQR] | DuckDB ms [IQR] | Ratio | Device samples |
|---|---:|---:|---:|---:|
| Q01 | 585.927 [515.230–589.818] | 634.597 [601.952–643.483] | 0.923× | 0/10 |
| Q02 | 37.025 [34.319–38.421] | 51.068 [50.700–53.472] | 0.725× | 0/10 |
| Q03 | 326.844 [274.552–391.224] | 233.358 [228.307–236.099] | 1.401× | 0/10 |
| Q04 | 137.716 [136.115–143.227] | 151.945 [150.441–152.697] | 0.906× | 0/10 |
| Q05 | 188.968 [185.190–197.334] | 184.095 [182.832–188.435] | 1.026× | 0/10 |
| Q06 | 147.039 [114.231–181.404] | 657.784 [437.400–1044.885] | 0.224× | 0/10 |
| Q07 | 238.254 [233.321–310.314] | 217.883 [215.997–221.044] | 1.093× | 0/10 |
| Q08 | 214.471 [208.085–224.412] | 257.321 [255.832–259.120] | 0.833× | 0/10 |
| Q09 | 1210.832 [1065.890–1243.350] | 995.172 [989.374–1010.515] | 1.217× | 0/10 |
| Q10 | 259.147 [256.873–396.418] | 300.463 [299.597–306.231] | 0.862× | 0/10 |
| Q11 | 28.387 [27.662–29.657] | 40.681 [39.429–41.649] | 0.698× | 0/10 |
| Q12 | 255.031 [246.447–276.967] | 273.145 [269.931–277.134] | 0.934× | 0/10 |
| Q13 | 264.807 [263.825–274.593] | 196.015 [192.729–202.673] | 1.351× | 0/10 |
| Q14 | 111.760 [109.493–122.613] | 147.630 [145.306–150.885] | 0.757× | 0/10 |
| Q15 | 113.803 [110.352–117.218] | 259.193 [257.952–259.930] | 0.439× | 0/10 |
| Q16 | 88.430 [86.654–93.680] | 81.271 [80.159–83.909] | 1.088× | 0/10 |
| Q17 | 206.262 [196.509–282.644] | 232.780 [230.676–236.140] | 0.886× | 0/10 |
| Q18 | 1133.755 [1067.519–1154.275] | 690.992 [675.384–705.023] | 1.641× | 0/10 |
| Q19 | 388.142 [383.817–396.854] | 265.946 [264.329–268.024] | 1.459× | 0/10 |
| Q20 | 330.729 [303.589–421.751] | 327.295 [320.664–331.462] | 1.010× | 0/10 |
| Q21 | 687.934 [496.078–692.205] | 603.013 [596.063–607.307] | 1.141× | 0/10 |
| Q22 | 57.646 [56.203–59.171] | 98.264 [96.713–99.359] | 0.587× | 0/10 |

### GPU binary, CPU control / Parquet

| Q | Engine ms [IQR] | DuckDB ms [IQR] | Ratio | Device samples |
|---|---:|---:|---:|---:|
| Q01 | 295.485 [291.258–303.559] | 171.293 [169.582–173.773] | 1.725× | 0/10 |
| Q02 | 27.541 [25.215–28.237] | 62.057 [61.607–63.169] | 0.444× | 0/10 |
| Q03 | 236.892 [231.703–245.518] | 180.359 [174.931–181.587] | 1.313× | 0/10 |
| Q04 | 132.067 [130.621–139.317] | 117.182 [116.410–118.813] | 1.127× | 0/10 |
| Q05 | 156.229 [148.968–160.326] | 149.111 [146.416–156.065] | 1.048× | 0/10 |
| Q06 | 92.780 [89.602–98.336] | 71.013 [69.476–72.685] | 1.307× | 0/10 |
| Q07 | 209.030 [201.482–211.978] | 157.182 [149.732–161.653] | 1.330× | 0/10 |
| Q08 | 191.942 [190.442–196.734] | 234.078 [229.960–236.235] | 0.820× | 0/10 |
| Q09 | 1219.362 [1166.357–1264.621] | 948.107 [943.099–960.508] | 1.286× | 0/10 |
| Q10 | 182.971 [170.639–188.988] | 216.178 [212.453–219.158] | 0.846× | 0/10 |
| Q11 | 20.575 [20.062–20.834] | 35.317 [33.393–36.443] | 0.583× | 0/10 |
| Q12 | 164.101 [153.761–173.381] | 128.251 [123.102–129.541] | 1.280× | 0/10 |
| Q13 | 256.139 [251.507–261.184] | 143.204 [142.616–145.060] | 1.789× | 0/10 |
| Q14 | 112.304 [109.873–114.481] | 124.641 [122.423–125.482] | 0.901× | 0/10 |
| Q15 | 94.097 [91.295–98.362] | 83.341 [82.087–87.384] | 1.129× | 0/10 |
| Q16 | 133.125 [118.070–148.962] | 80.106 [77.822–80.667] | 1.662× | 0/10 |
| Q17 | 153.846 [150.430–156.586] | 206.590 [204.010–208.760] | 0.745× | 0/10 |
| Q18 | 433.006 [397.248–467.663] | 361.459 [355.223–363.062] | 1.198× | 0/10 |
| Q19 | 222.116 [217.053–234.310] | 159.754 [156.119–163.768] | 1.390× | 0/10 |
| Q20 | 289.038 [273.178–298.588] | 319.562 [305.583–322.372] | 0.904× | 0/10 |
| Q21 | 468.878 [459.173–487.720] | 443.028 [435.255–457.857] | 1.058× | 0/10 |
| Q22 | 115.230 [109.794–124.491] | 131.752 [128.635–136.440] | 0.875× | 0/10 |

### GPU-assisted IPC / Parquet (CPU)

| Q | Engine ms [IQR] | DuckDB ms [IQR] | Ratio | Device samples |
|---|---:|---:|---:|---:|
| Q01 | 28.075 [28.004–28.143] | 168.963 [167.726–169.764] | 0.166× | 10/10 |
| Q02 | 26.606 [26.134–27.927] | 60.912 [60.082–61.337] | 0.437× | 0/10 |
| Q03 | 249.519 [236.914–261.031] | 181.508 [179.654–183.780] | 1.375× | 0/10 |
| Q04 | 131.709 [128.007–133.431] | 120.342 [113.449–122.307] | 1.094× | 0/10 |
| Q05 | 155.195 [150.895–159.006] | 150.747 [148.420–154.020] | 1.030× | 0/10 |
| Q06 | 1.708 [1.692–1.798] | 71.890 [69.302–74.229] | 0.024× | 10/10 |
| Q07 | 206.044 [201.989–209.361] | 149.587 [149.093–150.931] | 1.377× | 0/10 |
| Q08 | 198.691 [187.425–204.423] | 232.477 [229.735–237.600] | 0.855× | 0/10 |
| Q09 | 1262.294 [1197.481–1302.668] | 948.270 [944.877–959.065] | 1.331× | 0/10 |
| Q10 | 173.587 [169.786–194.498] | 218.169 [214.911–223.146] | 0.796× | 0/10 |
| Q11 | 21.106 [20.845–22.228] | 34.361 [33.643–36.494] | 0.614× | 0/10 |
| Q12 | 169.887 [160.628–181.787] | 128.838 [122.863–131.439] | 1.319× | 0/10 |
| Q13 | 256.947 [252.918–289.633] | 144.903 [143.370–147.506] | 1.773× | 0/10 |
| Q14 | 117.811 [112.743–122.062] | 120.586 [120.203–123.037] | 0.977× | 0/10 |
| Q15 | 91.261 [88.112–94.238] | 83.013 [82.275–86.163] | 1.099× | 0/10 |
| Q16 | 142.193 [133.844–149.195] | 81.991 [77.596–84.035] | 1.734× | 0/10 |
| Q17 | 156.186 [148.099–159.339] | 207.201 [203.370–209.959] | 0.754× | 0/10 |
| Q18 | 414.857 [403.881–460.865] | 358.838 [352.175–362.195] | 1.156× | 0/10 |
| Q19 | 227.507 [221.691–234.878] | 161.227 [156.466–164.460] | 1.411× | 0/10 |
| Q20 | 292.331 [274.786–318.280] | 311.797 [308.445–315.782] | 0.938× | 0/10 |
| Q21 | 468.408 [456.303–503.460] | 445.046 [440.913–452.647] | 1.052× | 0/10 |
| Q22 | 121.671 [112.821–125.910] | 129.670 [128.679–130.897] | 0.938× | 0/10 |

## Additional reproducibility and evidence

The [driver](../scripts/sf10_baseline.py) accepts `--tracks iceberg`, `--tracks lance`, and `--tracks gpu_cpu_control,gpu_assisted`. Run each block sequentially; select the default, Lance, and GPU-adapter binaries respectively. The same oracle directory can be supplied to compare against a previous source-Parquet run. Source SQL equality is checked before using it.

```bash
export TMPDIR="$PWD/.scratch"
SAFE_BUILD_MEM=48G SAFE_BUILD_JOBS=8 scripts/claude-safe-build.sh cargo build --offline --locked --release --features lance --bin query_engine
SAFE_BUILD_MEM=48G SAFE_BUILD_JOBS=8 scripts/claude-safe-build.sh cargo build --offline --locked --release --features gpu --example sf10_gpu_serve

# GPU control + assisted example; use a new output directory.
SAFE_BUILD_MEM=64G RAYON_NUM_THREADS=16 TOKIO_WORKER_THREADS=16 \
  scripts/claude-safe-build.sh taskset -c 0-15 \
  .scratch/venv-lance/bin/python scripts/sf10_baseline.py \
  --binary target/release/examples/sf10_gpu_serve \
  --tracks gpu_cpu_control,gpu_assisted --samples 10 \
  --out .scratch/sf10-gpu-new \
  --oracle-dir .scratch/sf10-baseline-20260905/raw_parquet
```

The adapter’s NVRTC library directory is configured by the driver from the existing repository virtual environment. Exact commands, binary hashes, source-input fingerprints, timestamps and configurations are retained in each manifest.

- [All modes: summary](benchmarks/2026-09-05-sf10/all-modes-summary.json), [query CSV](benchmarks/2026-09-05-sf10/all-modes-queries.csv), [all-sample CSV](benchmarks/2026-09-05-sf10/all-modes-samples.csv)
- Iceberg: [manifest](benchmarks/2026-09-05-sf10/iceberg/manifest.json), [summary](benchmarks/2026-09-05-sf10/iceberg/summary.json), [full evidence](benchmarks/2026-09-05-sf10/iceberg/evidence.tar.gz)
- Lance: [manifest](benchmarks/2026-09-05-sf10/lance/manifest.json), [summary](benchmarks/2026-09-05-sf10/lance/summary.json), [full evidence](benchmarks/2026-09-05-sf10/lance/evidence.tar.gz)
- GPU/control: [manifest](benchmarks/2026-09-05-sf10/gpu/manifest.json), [summary](benchmarks/2026-09-05-sf10/gpu/summary.json), [full evidence with device traces](benchmarks/2026-09-05-sf10/gpu/evidence.tar.gz)
- [Extension/build provenance](benchmarks/2026-09-05-sf10/extension-provenance.json) and [complete artifact checksums](benchmarks/2026-09-05-sf10/checksums-all-modes.json)

## Reproduce and inspect the original core block

The reusable [driver](../scripts/sf10_baseline.py) refuses to run outside the safe-build cgroup and refuses stale or incomplete cached inputs. It includes twelve comparator self-tests. Existing SF10 inputs and the Python dependencies are prerequisites; the driver never regenerates the dataset. Use a new output directory each run.

```bash
mkdir -p .scratch
export TMPDIR="$PWD/.scratch"
SAFE_BUILD_MEM=48G SAFE_BUILD_JOBS=8 scripts/claude-safe-build.sh \
  cargo build --offline --locked --release --bin query_engine \
  --target-dir .scratch/sf10-build

SAFE_BUILD_MEM=64G RAYON_NUM_THREADS=16 TOKIO_WORKER_THREADS=16 \
  scripts/claude-safe-build.sh taskset -c 0-15 \
  .scratch/venv-lance/bin/python scripts/sf10_baseline.py \
  --binary .scratch/sf10-build/release/query_engine \
  --out .scratch/sf10-baseline-new --samples 10
```

- [Query summaries, JSON](benchmarks/2026-09-05-sf10/summary.json) and [CSV](benchmarks/2026-09-05-sf10/queries.csv)
- [All 660 measured pairs, CSV](benchmarks/2026-09-05-sf10/samples.csv)
- [Run manifest](benchmarks/2026-09-05-sf10/manifest.json), [build/native-content provenance](benchmarks/2026-09-05-sf10/supplement.json), and [artifact checksums](benchmarks/2026-09-05-sf10/checksums.json)
- [Full evidence archive](benchmarks/2026-09-05-sf10/evidence.tar.gz): JSONL traces, all query records and plans, exact SQL, warm-ups/calibrations, Arrow answers, setup logs, preflight checks and the exact driver used.

Uncompressed working evidence remains at `.scratch/sf10-baseline-20260905/`; the freshly built binary remains at `.scratch/sf10-bin/query_engine-88849c4`. Binary SHA-256: `2c6bca94f8b8555899df293739d57e95907f69a56e0d4a50ea474099aaeb659f`.

## Relationship to earlier reports

This is the current baseline for the six engine modes and the GPU CPU control shown above. The earlier eight-way report used different thread/affinity settings, best-of-three selection, mixed timing paths, an Arrow-materialized Lance reference and the older comparator. Do not call a difference from that report a source-code regression or improvement. Distributed execution, cold starts, spill stress and workload-general optimizer performance remain outside this run.
