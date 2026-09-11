# Resident GPU correctness and validation — 2026-09-06

The required-residency development run now completes correctly on both unchanged floating-point fixture queries: **40/40 measured GPU requests, exactly one completed device run each, no CPU fallback, no typed-comparison or 10× time-gate failures**. The matched CPU control passes40/40 comparisons. Two deliberately insufficient-cache preparations refuse explicitly and retain their requested samples as not_run. This closes the current residency-protocol validation step, not the overall DuckDB leadership or GPU memory-safety goals.

## Shared fixes, with reproduced failures

| Contract | Evidence before repair | Current behavior |
|---|---|---|
| Provider/cache identity | Real MemoryTable preparation failed; removing the identity guard alone would alias identity-less providers | Typed keys retain exact immutable provider identity or full version bytes/files; grouping keys preserve ordered column vectors; owners release with cache metadata |
| Scalar aggregate cardinality | All-filtered SUM/COUNT/MIN/MAX/AVG returned0rows; DuckDB returned [NULL,0,NULL,NULL,NULL] | Scalar output retains one row and typed NULLs; grouped empty output remains empty |
| Floating extrema domain | All-NaN MIN/MAX returned [+infinity,-infinity] | Actual nonfinite source arrays decline; MIN/MAX requires direct columns because finite fused expressions can generate NaN |
| Exact SUM domain | GPU SUM([2^52,1,2^52]) returned9007199254740992 instead of9007199254740993 | Device SUM requires Float64 output; ordinary exact integer SUM uses CPU, required mode refuses explicitly |
| Delayed duplicate cache jobs | Replayed column/code jobs inflated resident bytes36→72 | Worker-time cache-hit checks avoid duplicate work; replacement accounting uses checked net byte changes |
| COUNT(*) expression proof | First full resident Q1 preparation refused; all20 sample slots were preserved | Wildcard admitted only as the sole nondistinct COUNT argument; other wildcard contexts still decline |

These changes apply to shared planning, preparation, execution and accounting boundaries. No benchmark SQL or data was modified. Numeric-domain checks are also enforced at worker entry, so manually constructed plans cannot bypass planner eligibility. Unsupported required execution never silently falls back to CPU.

## Validation

Gate77913:699 GPU-feature library tests pass; eight explicitly invoked real-CUDA tests and one explicit IPC test pass (**708 selected Rust tests**). Only the pre-existing ignored flatten-dependent-join test remains excluded. Default adapter build check passes. The unchanged benchmark harness passes100 tests, including explicit spill/Lance cases, with no skips. Formatting and diff checks pass.

Both complete fixture queries additionally pass a debug-adapter real-GPU/independent typed DuckDB check before the final release build. The first debug attempt had a missing comparator scratch directory, retained separately as a diagnostic-script error. Release validation independently confirms the original empty/NaN/integer reproducers and the exact CPU fallback for integer SUM. Small unit tests are insufficient to establish full fixture routing; retain this full-SQL debug gate before future GPU release measurements.

## Matched development measurement

Unchanged custom floating fixture:600,000 lineitem rows, Q1/Q6 derived from historical custom SQL. **Not canonical TPC-H, not SF10, and not a complete workload.** Independently validated IPC is fully preloaded into Arrow on both engine and DuckDB sides. Each query has a fresh worker; preparation is acknowledged once before warmup and measurement. One session,20 samples/query, three fresh DuckDB calibrations, original typed oracle and exact10× query ceiling. Timing includes parse/optimize/plan through fully consumed Arrow output; preparation, setup and serialization are separate.

Four threads, CPU affinity0–3,4GiB query budget,8GiB process cap,16GiB cgroup with swap disabled;256MiB GPU cache target. RTX5090, driver580.173.02; Rust1.93.0; DuckDB1.4.4/PyArrow25.0.1. Build: locked release, features lance,gpu,64GiB build cap and one compile job.

| Query | Resident GPU median ms | Matched DuckDB median ms | Same-binary CPU median ms | Separate preparation ms |
|---|---:|---:|---:|---:|
| q01 | 1.513 | 15.372 | 13.775 | 40.520 |
| q06 | 0.657 | 6.510 | 1.730 | 14.635 |

GPU/DuckDB: suite sum-of-medians ratio0.09917; geometric mean0.09966. CPU control/DuckDB: suite ratio0.73159, geometric mean0.51988. Each track uses its own fresh matched references; complete per-query samples and calibration differences are retained. GPU/CPU-control sum-of-medians is0.13996 (descriptive, separate runs). These narrow resident timings do not imply cold first-query speedups or general engine leadership. Preparation alone adds about40.5ms/14.6ms, and process/catalog/host preload costs are also outside resident query timing.

At1MiB cache target, both queries return explicit Capacity preparation errors; the engine sample slots remain not_run, never CPU results. **This is refusal/protocol evidence, not a hard VRAM-bound proof:** the existing upload cache may transiently exceed its soft target before final residency verification. Host staging, provider ownership, kernel scratch and query-wide hard admission remain open.

## Evidence and implementation order

[Passing source, binary, complete runs and validation evidence](benchmarks/2026-09-06-gpu-resident-count-star/README.md). Earlier incorrect/refused candidates are preserved separately: [scalar/NaN](benchmarks/2026-09-06-gpu-resident-identity-red/README.md), [integer SUM](benchmarks/2026-09-06-gpu-integer-sum-red/README.md), [COUNT(*) preparation](benchmarks/2026-09-06-gpu-count-star-preflight-red/README.md). Their measurements must not be attributed to the corrected source.

1. Review/test the generic scalar-subquery attribution draft in `.scratch/scalar-subquery-attribution/`. The [source-backed CPU investigation](ipc-scalar-subquery-attribution-2026-09-06.md) shows most observed Q22 regression lies in plan_ms, which includes scalar RHS execution. Separate RHS construction, bridge/runtime and actual operator execution before choosing a shared optimization; preserve per-query cache/error/cardinality semantics.
2. Implement actual GPU host/device allocation admission and bounded queue ownership from the [existing design](gpu-hard-admission-design-2026-09-06.md). Include protected resident dependencies plus kernel/readback scratch; refusal after allocation is insufficient.
3. Continue [raw variable-output ownership work](variable-output-ownership-integration-review-2026-09-06.md). Post-decode output sizing alone cannot prove decoder/tail ownership or authorize parallel execution.
4. Resume the existing epic's full provider/public/resource/concurrency/holdout gates. Canonical decimal GPU execution remains unsupported; frozen593 CPU/provider results still contain protected regressions. No parent epic task or overall leadership goal is closed by this two-query development result.
