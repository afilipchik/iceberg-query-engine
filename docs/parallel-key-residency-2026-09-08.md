# Parallel key preparation: residency screen — September 8, 2026

Frozen e6a60347 completed the planned residency screen29847 with exit1. Canonical
preload now succeeds at32GiB query/48GiB process inside64GiB containment. This is
a separately labelled capacity experiment; the previous16GiB refusal remains.
The decoded IPC invocation now correctly omits the GPU-only explicit preload flag.

|Case|Valid measured pairs|Outcome|
|---|---:|---|
|Canonical decoded IPC|60/66|Q1/Q13 engine warmup timeouts|
|Canonical preloaded CPU control|60/66|Q1/Q13 engine warmup timeouts|
|Canonical mixed GPU|No execution|Rejected incomplete CPU control|
|Custom float CPU control|40/40|Both queries complete20 samples|
|Custom required GPU|0/40|Two worker startup failures; no queries executed|

All160 completed measured engine outputs validate. Supplemental independent typed
comparison validates all42 completed warmups:202 correct completed engine outputs
in total. Four canonical warmups time out. Canonical comparisons use all22 queries,
16 threads and3 samples. The custom float Q1/Q6 workload uses4 threads,4GiB query,
8GiB process and20 samples; it is not SF10. Custom Q1 CPU warmup118.992360ms and
Q6 warmup14.755092ms both pass their fresh gates. All timing boundaries remain
embedded parse-to-Arrow-consumed with matched DuckDB calibration and typed oracles.

Required GPU workers exit1 during CUDA initialization. Stderr records a caught
GPU-thread panic from cudarc: the dynamic loader cannot locate libnvrtc. Worker
records classify startup as crash/exit1. The runner subsequently overwrites the
more useful startup reason with missing required-residency acknowledgement, then
reports resident preparation failed. The preserved stderr establishes a runtime
library search-path failure, not SQL, GPU kernel correctness or measured latency.
No device execution is established by this screen. The repeated startup attempts
belong to two different requested queries, not query retries.

The existing NVRTC library is in repository
`.venv/lib/python3.12/site-packages/nvidia/cuda_nvrtc/lib/libnvrtc.so.12`.
A separate prespecified custom control/required-GPU pair will prepend that directory
to process-local LD_LIBRARY_PATH and record library hashes. No host installation
or global loader change is needed. Preserve this failed run unchanged.

The shared scope peaks at29,976,264,704bytes; max/oom/oom_kill remain zero. This
includes file cache and prior cases, not query RSS. No engine/harness edits occurred
during measurement. Source snapshot remains the parallel-key paired archive.
Evidence: `benchmarks/2026-09-08-parallel-key-residency/`; driver and state are under
`.scratch/parallel-aggregate-input/` and
`.scratch/public-bench/parallel-key-residency-32g-01/` respectively.
Full provider/resource/concurrency acceptance and DuckDB leadership remain open.

The planned runtime correction has completed successfully: see
[matched CPU/GPU evidence](parallel-key-gpu-runtime-2026-09-08.md). Both custom
cases pass40/40 measured pairs; canonical failures remain unchanged.
