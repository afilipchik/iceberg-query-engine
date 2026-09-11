# Bound key arrays: residency screen — 2026-09-09

Frozen0f30c946 completes252 typed-correct outputs, including46 warmups.
Canonical IPC/CPU residency remains incomplete, and canonical GPU is not executed.

| Track | Valid measured pairs | Correct warmups | Outcome |
|---|---:|---:|---|
| Canonical decoded IPC | 63/66 | 21 | Q1 warmup timeout; dependent samples not run |
| Canonical resident CPU control | 63/66 | 21 | Q1 warmup timeout; dependent samples not run |
| Canonical mixed GPU | Not executed | 0 | Incomplete CPU control rejected |
| Custom float CPU control | 40/40 | 2 | Complete |
| Custom float required GPU | 40/40 | 2 | Complete; request-scoped device evidence passes |

The custom GPU median resident times are Q1 1.050424 ms and Q6 0.721129 ms.
Separate preparation response times are51.275143 ms and23.188547 ms. Every
measured GPU request reports one attempted/completed device run, zero failures,
a matching preparation session, stable residency/fallback counters and zero
upload requests. These are600,000-row custom float measurements, not canonical
SF10 or upload-inclusive latency.

Canonical runs use32 GiB query/48 GiB process memory and16 threads on CPUs0–15.
This is a capacity experiment, not clearance of the16 GiB preload refusal.
Custom runs use4/8 GiB and four threads, twenty samples per query. All use one
session, matched DuckDB reference calibration, typed validation and the10× query
ceiling. Existing NVRTC libraries are selected by a process-local loader path.
The64 GiB sequential scope peaks at19,855,237,120 bytes with zero OOM/max events.

Run69745 exits1 for incomplete canonical acceptance; independent audit95178
exits0. All508 source inputs and the frozen binary verify after execution.
Reproduce using `run_bound_keys_residency.py` through the repository capped
wrapper with `TMPDIR="$PWD/.scratch"`, `PYTHONPATH="$PWD/scripts"`,
`SAFE_BUILD_MEM=64G`, `SAFE_BUILD_JOBS=1` and `taskset -c 0-15`.
Use fresh output roots. The [archive](benchmarks/2026-09-09-bound-keys-residency/manifest.json)
preserves all outputs, failures, manifests, GPU evidence, harness and scripts.
Full resource/concurrency and leadership acceptance remain open.
