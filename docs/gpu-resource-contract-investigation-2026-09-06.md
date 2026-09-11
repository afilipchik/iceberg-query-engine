# GPU resource-contract investigation — 2026-09-06

Source-confirmed gap; no new GPU memory experiment or production change has
been performed for this investigation. The live canonical scalar-candidate
matrix must finish before another engine invocation.

`GpuCache::reserve` in [gpu.rs](../src/physical/gpu.rs:1121) returns no admission
result. If `total_bytes + need` exceeds `QE_GPU_CACHE_MB`, it evicts entries until
the cache is empty, then exits even if the requested buffer alone exceeds the
budget. The documentation explicitly calls this a soft target. Insertion then
adds the allocation's bytes. This contradicts treating the configured value as
a hard resource budget. The addition also lacks checked arithmetic. These are
source facts, not a measured VRAM-overrun result.

CPU cgroup containment does not establish VRAM admission. Canonical decimal
queries that perform no device work cannot validate this boundary. The current
scalar candidate's final canonical GPU track is still running; device execution
must be reported from its actual counters separately from answer completion.

The existing supported-GPU development fixture remains available at
`.scratch/public-bench/custom-gpu-float-smoke/dataset.json`: 600,000 custom
floating-point lineitem rows, unchanged Q1/Q6 SQL. The prepared
`.scratch/run_scalar_gpu_smoke.py` reruns its 40 CPU-control and 40 GPU pairs
against the frozen scalar binary, with four threads, affinity 0–3, 4 GiB query,
8 GiB process, 16 GiB outer cgroup and 256 MiB configured GPU cache target.
Run it sequentially after the current matrix with the existing NVRTC path.
It is a supported-operator regression check, not realistic-workload leadership,
canonical decimal acceleration, cold-transfer or hard-memory certification.

The systemic repair needs explicit fallible GPU admission before allocation,
checked size arithmetic, accounting for resident and transient device buffers,
and independently bounded host staging/upload queues. An oversized request must
produce a recorded, always-correct CPU fallback or a named resource refusal;
it must not silently override the configured budget. Eviction, cancellation,
failed uploads and mutation identities must release their ownership and clear
queued/resident metadata consistently. Do not add an unlimited opt-out.

Validation must cover insufficient budget before allocation, an individual
oversized column, combined column/group/kernel scratch pressure, upload failure,
eviction/re-upload and query cancellation. Compare exact supported integer/count
results and independently validated floating results; record actual device
execution and allocation peaks. Retain at least one fitting workload that really
runs on device, so universal fallback cannot masquerade as a repaired GPU path.
None of these resource gates is claimed complete by the existing latency suite.

## Later checkpoint

The frozen scalar candidate subsequently completed the canonical six-mode matrix: its GPU track performed zero device work. The supported float smoke completed40 GPU and40 CPU control pairs, with40 device samples. Those results are preserved in `benchmarks/2026-09-06-scalar-comparison/`; they do not repair the hard-budget defects above. Newer streaming/ownership source is being validated separately. No GPU production admission change has been made.

The [staged hard-admission design](gpu-hard-admission-design-2026-09-06.md) inventories upload, kernel scratch, asynchronous retirement, host queue and fallback contracts. Additional source findings include multi-partition runtime fallback, group-NULL handling, and integer-minimum panic risks. These are source findings requiring reproductions; no GPU implementation or validation claim is made.
