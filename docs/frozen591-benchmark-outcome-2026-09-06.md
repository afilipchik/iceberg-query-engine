# Frozen591: initialized membership outcome — 2026-09-06

The systemic immutable membership filter improves the measured IPC pipeline, but the engine still fails overall performance acceptance. Raw Parquet remains 2.4397× matched DuckDB by suite sum of medians. The change contains no query names, SQL recognition or query-specific limits.

## What changed

A positively proven root IN/NOT IN filter pins an actual immutable MemoryTable RHS, streams every partition into one budget-owned exact Int64 membership state, and shares it across prepared input streams. Every actual LHS layout must qualify before parallel output admission. NULL/empty/duplicate semantics, cancellation, error ordering and unsupported physical domains retain their contracts. Retained identifier accounting has a reproduced negative control and checked correction.

## Reproducible results

Each screen covers all 22 canonical SF10 queries with three paired steady samples plus warmup, 16 threads/affinity 0–15,40 GiB query/48 GiB process budgets and 96 GiB containment. Both controls and candidate use the same inputs, residency/timing boundary, typed oracle and fresh DuckDB 10× gate. All 528 engine requests pass. These are one-session development screens, not multi-session certification.

| Comparison | Suite candidate/control | Geomean candidate/control | Protected regressions >10% |
|---|---:|---:|---|
| IPC versus immediately preceding frozen587 | 0.9457 | 0.9619 | None |
| IPC versus accepted scalar control | 0.9636 | 1.0037 | Q10:1.2586×; Q22:1.1989× |
| Raw Parquet versus scalar control | 1.0681 | 1.0704 | Q10:1.5645×; Q12:5.0002×; Q16:1.5294× |

IPC Q16 improved 359.970→302.805ms against frozen587, a 15.9% reduction. In the separate paired scalar screen it measured 283.961ms versus 282.178ms, a 1.0063× ratio. This recovers the observed Q16 regression; separate sessions must not be subtracted as an isolated causal ablation. Suite IPC/DuckDB ratios 0.4784 and 0.4853 apply to decoded residency, not raw CPU leadership. See [all ratios](benchmarks/2026-09-06-initialized-membership/cpu-screen-summary.json).

The actual public-plan probe changed IPC Q16 Filter from None to 16 bounded streams, max 1340030 copied bytes per stream. Held reservation 11567040 bytes drops to zero after plan/context cleanup. Raw correctly remains None because its RHS provider is Parquet, outside this immutable MemoryTable proof. This probe pulls no output and does not itself certify latency, actual envelope admission or complete query memory.

## Validation and identity

719 unique selected tests pass, including dedicated IPC and exact resource-refusal classification. One pre-existing ignored test remains. Six cap checks complete with actual spill, RSS 129–294MiB, cgroup 1GiB and RLIMIT_DATA 2GiB; aggregate/sort have independent exact oracles, joincap checks row count only. Default-feature tests and Lance/GPU release compilation are distinct coverage.

Source591 archive SHA256 `736b136bffdabdef2832a29f98861b51dd80e42aac2086a4b024760d6f919749`; benchmark binary `6a2c065f52475ec5e867c8af53482993a59e61a68f8e90255ee684db57150f59`. Source hashes were verified at freeze and publication. [Evidence](benchmarks/2026-09-06-initialized-membership/README.md) preserves exact commands, source, binaries, logs, plans, typed outputs and archive verification.

## Remaining work

Q10/Q22 IPC and Q10/Q12/Q16 raw still violate protected gates. Inspect actual remaining preparation barriers before changing execution policy. [Variable-width output sizing](variable-output-prefix-component-2026-09-06.md) is scratch-only and must be connected to allocation and source/tail ownership; a byte estimate alone cannot license more parallelism.

Native, Iceberg, Lance and GPU measurements in the [full previous checkpoint](frozen587-benchmark-outcome-2026-09-06.md) belong to frozen587, not this binary. New-source full provider, GPU residency/hard-admission, public workload and concurrency certification remain pending. [The explicit GPU residency gate design](gpu-residency-benchmark-gate-2026-09-06.md) addresses a reproduced warmup readiness race without discarding fallback samples.

The [post-measurement preparation probes](benchmarks/2026-09-06-post-membership-plan-probe/README.md) verify that Q10’s nested build-input joins return None while its outer join exposes16 bounded streams. The exact decline reason is still unverified; Q22 bypasses that multi-partition queue entirely.
