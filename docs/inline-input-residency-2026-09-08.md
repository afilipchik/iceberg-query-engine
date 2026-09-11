# Direct-input candidate: residency screen — September 8, 2026

Frozen binary `a023079ff45b8159614b4a313a7874a4599b355b85ef1cfa449996465d6df0e2`
was built from493 verified inputs. The [paired study](inline-aggregate-input-2026-09-08.md)
records its single changed source file and matched CPU results. Source and harness
were unchanged throughout this screen.

Residency3109 is terminal1; independent audit77556 is terminal0. All244 completed
engine outputs pass typed full-oracle comparison:200 measured and44 warmups.

| Track | Valid measured pairs | Completed warmups | Remaining failure |
|---|---:|---:|---|
| Canonical decoded IPC | 60/66 | 20 | Q1/Q13 warmup timeouts |
| Canonical preloaded CPU control | 60/66 | 20 | Q1/Q13 warmup timeouts |
| Canonical mixed GPU | No execution | 0 | Incomplete CPU control rejected |
| Custom float CPU control | 40/40 | 2 | None |
| Custom float required GPU | 40/40 | 2 | None |

The four canonical warmup timeouts leave12 measured engine slots not run. The
canonical mixed-GPU rejection is a separate prerequisite failure, not66 successful
or skipped executions. No reference failure or late completed output is relabelled
as a passing pair. All original timing gates remain in force.

Canonical runs use16threads,32GiB query/48GiB process, three samples in one session.
This is a labelled capacity experiment; it does not clear the16GiB preload refusal
or the lower-memory resource gates. The custom600000-row float workload uses four
threads,4GiB query/8GiB process and20 samples per query in one session. It is not
canonical SF10. The outer scope is64GiB with no swap, repository TMPDIR and
sequential cases. Scope peak21804122112bytes and zero max/OOM/kill events are host
containment evidence, not exact RSS, query-pool admission or a VRAM cap certificate.

## Actual GPU evidence and timing boundary

Existing repository NVRTC libraries are supplied by process-local LD_LIBRARY_PATH;
all four library hashes are recorded in `runs/runtime.json`. No installation or
global loader change was needed. Each of the40 measured required-GPU requests has
one attempted and one completed device run, zero failures, and a session ID that
matches its prepared query. Both warmups also run on device. The independent audit
checks these records, unchanged residency/fallback counters and zero measured
upload requests. Device use is established for this custom workload, not inferred
from the final plan or extended to the unexecuted canonical GPU suite.

| Custom query | CPU-control median ms | Resident-GPU median ms | CPU-run DuckDB median ms | GPU-run DuckDB median ms |
|---|---:|---:|---:|---:|
| Q1 | 65.023479 | 0.979679 | 13.893921 | 15.704530 |
| Q6 | 4.058038 | 0.794912 | 8.069383 | 7.950967 |

Preparation and upload are recorded separately and excluded from resident query
latency. These medians are not cold or upload-inclusive latency and are not a
balanced multi-session CPU/GPU effect estimate. The paired CPU study provides its
own confidence intervals; this screen preserves the residency boundary separately.

Evidence: `benchmarks/2026-09-08-inline-input-residency/`, including every command,
result, reference calibration, failure, plan, runtime hash, preparation record,
request-scoped device record and supplemental typed comparison. The four-provider
screen on this candidate is the next validation; older476ec119 provider results
remain a distinct control. Full resource/concurrency/held-out acceptance is open.
