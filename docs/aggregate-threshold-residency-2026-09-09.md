# Aggregate threshold residency screen — 2026-09-09

Frozen engine `1efb25547486dc98a297d8824e8738b5fea2dcd15255cdded0cdaad6b74687dc` completes 252 typed-correct outputs, including 46 warmups. This does not certify canonical residency or DuckDB leadership.

| Track | Valid measured pairs | Completed warmups | Outcome |
|---|---:|---:|---|
| Canonical decoded IPC | 63/66 | 21 | Q1 warmup timeout; three dependent samples not run |
| Canonical resident CPU control | 63/66 | 21 | Q1 warmup timeout; three dependent samples not run |
| Canonical mixed GPU | Not executed | 0 | Incomplete CPU control rejected |
| Custom float CPU control | 40/40 | 2 | Complete |
| Custom float required GPU | 40/40 | 2 | Complete; request-scoped device evidence verified |

Canonical runs use 32 GiB query memory, 48 GiB process cap, 16 threads and CPU affinity 0–15. These are capacity experiments, not clearance of the 16 GiB preload refusal. Custom runs use 4/8 GiB and four threads, twenty samples per query on 600,000 rows. All runs use one session, matched DuckDB references, typed result comparisons and the 10× reference query ceiling. A sequential 64 GiB memory-capped wrapper contains the driver. Existing NVRTC libraries are selected through a process-local loader path.

Every measured custom GPU request reports one attempted and completed device run, zero failures, matching preparation/session identity, stable residency and fallback counters, and zero upload requests. Resident medians are Q1 1.030573 ms and Q6 0.715981 ms. Separate preparation response times are 51.146292 ms and 24.567474 ms. These are custom float resident measurements, not canonical SF10 or upload-inclusive latency.

Driver session 92429 exited 1 because canonical acceptance is incomplete. Independent audit 34442 exited 0. All 507 source inputs and the frozen binary were verified after execution. Raw failures, outputs, comparisons, manifests, GPU evidence, harness and scripts are preserved in [the archive](benchmarks/2026-09-09-aggregate-threshold-residency/manifest.json).

Reproduction: run `.scratch/parallel-aggregate-input/run_aggregate_threshold_residency.py` with `TMPDIR="$PWD/.scratch"`, `PYTHONPATH="$PWD/scripts"`, `SAFE_BUILD_MEM=64G`, `SAFE_BUILD_JOBS=1`, through `scripts/claude-safe-build.sh taskset -c 0-15 .scratch/venv-lance/bin/python`. Output roots are exclusive; choose a fresh root for another experiment. Audit uses the archived `summarize_aggregate_threshold_residency.py` in an 8 GiB scope. This report preserves the separate failure and correctness outcomes.
