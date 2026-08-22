---
name: gpu-acceleration
status: completed
created: 2026-08-22T06:16:43Z
updated: 2026-08-22T07:06:08Z
progress: 100%
prd: .claude/prds/gpu-acceleration.md
github: (will be set on sync)
---

# Epic: gpu-acceleration

## Overview

Feature-gated CUDA offload for filtered aggregates over a GPU-resident
column cache, priced before built, kept only on engine-level wins. cudarc +
NVRTC runtime kernel compilation; no toolchain requirements beyond the
driver already on the box.

## Architecture Decisions

1. **Price first, GO/NO-GO gate** — examples/gpu_price_bench.rs measures
   H2D bandwidth and fused filter+agg kernels vs the CPU on Q6/Q1 shapes,
   cold and warm. Gate: warm >= 3x, cold >= 0.8x. NO-GO ends the epic as a
   published refutation (radix/JIT tradition).
2. **Narrow routing**: Aggregate(Filter?(Scan(parquet))) with decomposable
   aggs, numeric non-null columns, no/low-card groups. Reuses
   compiled_expr's subset test for filter routability. Everything else:
   untouched CPU path. QE_GPU=0 kill switch.
3. **GPU column cache** keyed (table, column): upload once from scan
   batches, LRU-capped (QE_GPU_CACHE_MB, default 24576). Cold query = CPU
   decode + one PCIe pass; warm query = pure VRAM bandwidth.
4. **Kernels**: grid-stride fused predicate+reduction producing per-block
   partials (f64 sum / i64 count / min / max), final merge on CPU — avoids
   atomics precision surprises and keeps kernels trivial to audit. Float
   sums differ from CPU order — same 1e-6 tolerance class as distributed
   two-phase, validated the same way.
5. **Keep-or-kill**: the branch merges only with measured engine-level wins
   (Q6-shape SF=10 warm >= 2x); otherwise the refutation is published and
   the branch stays unmerged.

## Task Breakdown Preview

1. cudarc dependency (feature gpu), device init, NVRTC hello-kernel proof,
   lock add-only check.
2. Pricing bench: H2D, fused filter+SUM/count kernels, cold/warm vs CPU;
   GO/NO-GO published in the task file.
3. (GO only) GpuAggExec + column cache + planner routing + fallbacks.
4. (GO only) Validation: cell-exact/tolerance suites, engine A/B at SF=1
   and SF=10 warm; keep-or-kill decision + publish.

## Success Criteria (Technical)

Per PRD; the kill switch and the unmerged-branch discipline are part of
the deliverable.
