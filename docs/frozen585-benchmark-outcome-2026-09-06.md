# Frozen585 benchmark outcome, 2026-09-06

The new release passes750 selected correctness tests (one pre-existing ignored) and six actual-spill cap cases. All880 paired CPU screen requests across five modes pass typed results and exact time gates. Performance acceptance remains rejected because every mode has protected per-query regressions. These are one-session development screens, not full leadership certification.

| Mode | Suite / scalar control | Geomean / scalar control | Suite / fresh matched DuckDB |
|---|---:|---:|---:|
| raw_parquet | 1.074 | 1.074 | 2.428 |
| decoded_ipc | 0.962 | 1.021 | 0.489 |
| native | 1.003 | 1.022 | 3.685 |
| iceberg | 1.086 | 1.076 | 0.324 |
| lance | 0.983 | 1.001 | 1.257 |

Lower is better. IPC decoded residency and Iceberg provider integration are distinct comparison tracks; their favorable DuckDB ratios do not demonstrate raw/native CPU leadership. Raw and native remain2.43× and3.69×DuckDB respectively.

Q1 improves about9%raw,21%IPC and20%native against the control, consistent with the shared decimal input reuse change; the combined binary comparison is not an isolated causal ablation. RawQ21 no longer shows the older3×control regression, but Q10/Q12/Q16 remain1.59/4.86/1.57×control. The same trio regresses on Iceberg. Q16 also regresses on IPC/native/Lance, motivating a shared operator/runtime investigation.

Canonical GPU routing and fresh same-binary CPU control each pass66samples, with0actual device runs and0uploads. Supported custom float smoke passes40device-executed samples and40CPUcontrol samples. This proves routing/fallback and limited supported execution, not canonical GPU acceleration or hard VRAM admission.

Full CPU evidence:2,891files verified before/after archiving,518,206,132uncompressed bytes, archiveSHAf84f7849e83e8a01542e26ff7e46e9566697222aea82919ab33a121eb2d8efe3. GPU evidence:850verifiedfiles,204,982,658bytes, archiveSHAc9c0301554b1961435ef5fccce1ab1628c68377fcf0d3962ba1e00b12c1ab201. Both frozen CPU/cap binaries were decompressed and hash-verified. [Evidence and full summaries](benchmarks/2026-09-06-expression-substitution/README.md).

Both frozen-binary process-counter diagnostics are complete: raw64requests and IPC48requests pass typed/time and CPU-observation gates. The measured preparation barriers are documented in [shared CPU diagnosis](shared-cpu-regression-diagnosis-2026-09-06.md). It preserves typed/time gates and requires complete process CPU boundaries. Nested subqueries use an independently sized runtime, so this is not strict single-thread execution. Diagnose CPU work versus lost overlap before implementing the next shared change; do not remove ownership or NULL/partition contracts to recover speed. Full query-memory/VRAM ownership, concurrency, broader public workloads and multi-session leadership remain unfinished.
