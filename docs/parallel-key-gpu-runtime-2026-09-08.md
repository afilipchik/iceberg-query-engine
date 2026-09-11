# GPU runtime correction and measured residency — September 8, 2026

The missing NVRTC library search path caused the preceding GPU startup failure.
The existing repository Python environment already contains NVRTC; a process-local
LD_LIBRARY_PATH correction resolves initialization without installing packages,
changing engine code, or changing host configuration. The failed run remains
immutable in the parallel-key residency archive.

A new prespecified matched pair uses frozen e6a60347 and the same custom float
Q1/Q6 dataset,4 threads,4GiB query/8GiB process and20 samples/query in one session.
Both CPU control and required-GPU runs exit0; driver81085 exits0. All80 measured
engine outputs and four completed warmups pass independent typed comparisons.
Every one of40 measured GPU requests records one completed device run, zero
failures and a matching prepared session. Warmups also execute on device. No
measured request uploads or falls back. This establishes actual execution, not
just a GPU wrapper in a plan.

|Custom query|CPU control median ms|Required resident GPU median ms|GPU run DuckDB median ms|Preparation ms|
|---|---:|---:|---:|---:|
|Q1|95.519933|0.979805|14.890433|43.407158|
|Q6|7.282898|1.274319|11.802430|27.765183|

The CPU control's own DuckDB medians are13.653690ms and12.590164ms. These are
sequential single-session screens, not balanced GPU/CPU statistical estimates.
Preparation is a separate one-time operation, excluded from resident query
latency. Do not call resident medians cold or upload-inclusive performance. Q1
retains24,000,000 column bytes plus600,000 group-code bytes; Q6 retains19,200,000
column bytes. Each query's preparation covers600,000 rows. Resident byte counters
are telemetry, not proof of complete host/device hard admission.

The shared16GiB scope peaks720,711,680bytes; max/oom/oom_kill all zero. This is
host cgroup evidence, not a VRAM cap test. No engine/harness edits occurred during
measurement; all493 frozen source hashes and harness hashes are verified before
archiving. Runtime path and hashes of the installed NVRTC libraries are preserved.

This custom float workload is not canonical SF10. Canonical decoded IPC/CPU
control at32GiB still fail Q1/Q13, and the dependent canonical GPU suite remains
blocked. Multi-session, cold/upload/eviction, provider and shared-budget acceptance
remain open. No full-suite DuckDB leadership follows from this result.

Evidence: `benchmarks/2026-09-08-parallel-key-gpu-runtime/`. Driver:
`.scratch/parallel-aggregate-input/run_parallel_key_gpu_runtime.py`.
