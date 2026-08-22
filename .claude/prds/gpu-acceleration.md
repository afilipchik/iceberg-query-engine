---
name: gpu-acceleration
description: Feature-gated CUDA offload for the aggregate shapes where a resident RTX 5090 provably beats the CPU - priced, validated, kept only on wins
status: backlog
created: 2026-08-22T06:16:43Z
---

# PRD: gpu-acceleration

## Executive Summary

GPU databases succeed in exactly one regime and disappoint outside it. The
successes (Spark RAPIDS, HeavyDB, cuDF/Velox-cuDF, the Crystal research line)
win when data is GPU-RESIDENT and the operation is bandwidth- or
compute-bound; the failures (BlazingSQL et al.) died on PCIe: shipping
host-resident data to the GPU per query costs more than computing on the CPU,
because CPU RAM (~80GB/s here) outruns PCIe. The hardware on this box moves
the boundary: an RTX 5090 with 32GB VRAM (~1.8TB/s, ~22x host bandwidth) on
PCIe Gen5 x16 (~55GB/s practical) means (a) the entire hot column set of
SF=10 fits resident with room to spare, and (b) even a cold upload costs
about one CPU memory pass. This epic prices that opportunity with a
standalone benchmark, then implements the narrowest defensible offload —
flat and low-cardinality aggregates with numeric filters over a GPU column
cache — behind `--features gpu`, keeping it only if engine-level wins are
real.

## Problem Statement / Research

Where the engine spends time (established ledger): parquet decode, hash
probes (MLP-bound, 3.8ns/row), and bandwidth-bound scan+aggregate passes.
The GPU's leverage on each:

- **Scan + filter + aggregate (Q6/Q1 shapes)**: bandwidth-bound on CPU.
  GPU-resident columns turn 80GB/s passes into 1.8TB/s passes — the Crystal
  result. REQUIRES the column cache; streaming over PCIe every query caps at
  ~55GB/s ≈ CPU parity minus transfer latency.
- **Hash probes**: GPUs hide memory latency with massive parallelism; wins
  are real but need build+probe tables resident and a GPU hash table —
  large engineering surface. OUT OF SCOPE v1; noted as the follow-up with
  the probe-side numbers to justify it.
- **Parquet decode on GPU** (RAPIDS' actual ace): biggest architectural
  change; out of scope.
- **Sorts / windows / strings**: result sets here are small post-aggregate;
  no leverage.

Toolchain: `cudarc` (pure-Rust CUDA driver + NVRTC bindings) with kernels
compiled AT RUNTIME from CUDA C source via libnvrtc — no nvcc, no build.rs,
no bindgen. libcuda/libnvrtc confirmed present (driver 580). The dependency
is feature-gated `gpu` (off by default, like `lance`), so the default build
and every existing benchmark are untouched; Cargo.lock gains add-only
entries.

## User Stories

1. **Analyst on this box**: repeated aggregate-heavy queries (dashboards,
   iterative exploration) over cached columns return several times faster;
   first touch pays one PCIe upload comparable to a CPU pass.
2. **Correctness owner**: GPU answers are cell-exact vs the CPU path for
   integers/counts and within the established 1e-6 relative tolerance for
   f64 sums (parallel reduction reorders float addition — same class of
   difference as the distributed two-phase path, documented the same way).
3. **Everyone else**: default build has zero GPU surface; `--features gpu`
   plus `QE_GPU=0` gives a kill switch even when compiled in.

## Functional Requirements

- Pricing bench first (`examples/gpu_price_bench.rs`, feature-gated):
  measured H2D bandwidth; fused filter+SUM kernel over resident columns vs
  the CPU path on Q6/Q1 shapes; cold (upload included) vs warm (resident).
  GO/NO-GO gate: warm GPU >= 3x CPU on the shape, cold >= 0.8x (not a
  regression). If the bench refuses, the epic ends as a documented
  refutation.
- If GO: `GpuAggExec` routed when ALL of: feature on, QE_GPU!=0, plan shape
  is Aggregate(Filter?(Scan(single parquet table))) with decomposable
  aggregates (COUNT/SUM/MIN/MAX/AVG), agg inputs and filter columns numeric
  (F64/I64/I32/Date32) non-null, group by ABSENT (v1) or all-dictionary
  low-cardinality (stretch); anything else takes the normal path.
- GPU column cache: per (table, column) device buffers uploaded on first
  use from the scanned batches; capped (default 24GB, `QE_GPU_CACHE_MB`);
  cache misses evict LRU; upload happens once per process.
- Filters supported on GPU: same subset as compiled_expr predicates
  (numeric comparisons, AND/OR/NOT, BETWEEN) — reusing its compile-check to
  decide routability.
- Fallback correctness: any condition not met at plan OR execution time
  (missing columns, nulls discovered, VRAM exhausted) falls back to the CPU
  operator transparently.

## Non-Functional Requirements

- Default build: no new compile units, no runtime probing.
- All existing suites green in both builds; DuckDB-validated comparisons
  pass with the GPU path active (tolerance note above).
- No unsafe beyond cudarc's own API surface.

## Success Criteria (keep-or-kill, per the user's directive)

1. Pricing bench GO gate met and published.
2. Engine-level: Q6-shape SF=10 warm >= 2x faster than CPU path; suite-level
   no regression with feature on; results cell-exact/tolerance-exact.
3. If engine-level wins do NOT materialize, the code is NOT merged to main;
   the refutation is published like radix/JIT before it.

## Constraints & Assumptions

- CUDA driver API only (no toolkit installed); kernels as CUDA C strings
  through NVRTC; f64 atomics need sm_60+; RTX 5090 is sm_120 — nvrtc arch
  flag derived from the device at runtime.
- Single GPU; no multi-GPU, no distributed-worker GPU scheduling (workers
  on one box would contend — distributed mode keeps the CPU path in v1).

## Out of Scope

- GPU hash joins, GPU parquet decode, GPU strings/sorts/windows, multi-GPU,
  UVM/pinned-memory streaming pipelines, Lance/Iceberg providers (parquet
  provider only in v1).

## Dependencies

- `cudarc` 0.19 (feature `gpu`, default-off), driver libs present at
  runtime only.
