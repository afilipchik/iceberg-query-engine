# Frozen spill-I/O residency screen — 2026-09-09

Residency20505 is terminal1; supplemental audit37204 is terminal0. All252 completed
outputs are typed-correct:206 measured and46 warmups. Binary322c8042 and all507
source inputs verify after execution. No source/dependency change occurred during
this screen.

| Mode | Valid measured pairs | Correct completed warmups | Remaining failure |
| --- | ---: | ---: | --- |
| Canonical decoded IPC | 63/66 | 21 | Q1 warmup timing failure |
| Canonical preloaded CPU control | 63/66 | 21 | Q1 warmup timing failure |
| Canonical mixed GPU | Not executed | 0 | Incomplete CPU-control manifest |
| Custom CPU control | 40/40 | 2 | None in this screen |
| Custom required GPU | 40/40 | 2 | None in this screen |

Canonical cases use32GiB query/48GiB process budgets and16 threads, a separately
labelled capacity experiment. They do not clear the16GiB preload refusal or the
4GiB native/Lance gate. Q18 now completes under these capacity-screen ceilings;
this is not evidence that the4GiB spill policy is fixed.

The custom GPU workload is600,000 rows of float-based SQL, not canonical SF10.
All40 measured required-GPU requests have successful request-scoped device evidence;
the42 warmup/measured outputs are correct. No per-request fallback/upload occurs.
Resident medians are Q1 1.098ms and Q6 0.965ms. Separate preparation acknowledgements
are48.004ms and25.638ms. Do not present these resident timings as cold or upload-
inclusive execution, or extrapolate them to canonical decimal SQL.

The64GiB scope records zero max/OOM/kill events and a cumulative21,196,767,232-byte
peak. Existing repository NVRTC libraries are selected with a process-local loader
path; no host installation/configuration changes are made. Full resource/concurrency
and complete canonical performance acceptance remain open. The next source gate
is the generic same-budget premature-spill regression, with this binary and its
completed archives preserved as controls.

The [immutable archive](benchmarks/2026-09-09-spill-io-residency/manifest.json)
contains1,092 verified files, including request-scoped device checks.
