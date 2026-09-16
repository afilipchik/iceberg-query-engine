# UTF-8 binding residency follow-up — 2026-09-11

After intermediary commit `8c89899` was pushed and remote-verified, job 55937
completed five residency cases on the unchanged `59619bde` binary. The screen and
independent audit both exited successfully: **348 typed-correct outputs and
278/278 valid measured pairs**. All 800 source inputs, the binary and harness
hashes verify after measurement. This cycle adds evidence, with no architecture change.

## Conditions and results

Canonical SF10 uses 32 GiB query and 48 GiB process budgets, 16 threads and three
samples per query. This is a capacity experiment and does **not** clear the 16 GiB
preload refusal. The two custom float cases use 4 GiB query and 8 GiB process
budgets, four threads and 20 samples per query. They are not canonical SF10.
All cases use one session and default disjoint ownership. One 64 GiB, no-swap
scope runs them sequentially with CPU affinity 0–15, after preceding heavy jobs
finished. The existing NVRTC runtime is selected through a recorded process-local
loader path.

Ratios compare engine query time with DuckDB; lower is better:

| Track | Typed outputs | Valid pairs | Geometric mean | Suite ratio | Query wins |
|---|---:|---:|---:|---:|---:|
| Canonical decoded IPC | 88 | 66 | 0.753197 | 1.175861 | 14/22 |
| Canonical GPU control | 88 | 66 | 0.771589 | 1.194102 | 14/22 |
| Canonical mixed GPU | 88 | 66 | 0.795073 | 1.254514 | 14/22 |
| Custom float GPU control | 42 | 40 | 1.369644 | 1.702059 | 1/2 |
| Custom float required GPU | 42 | 40 | 0.081845 | 0.080844 | 2/2 |

All 88 canonical mixed outputs record zero successful device runs. These results
provide no certification of canonical GPU acceleration. The custom required-device
workload has valid request-scoped evidence for all 40 measured device executions,
matching preparation sessions, with no per-run uploads or new fallback/allocation
failures. Its speedup applies to that custom resident-float workload.

The scope peaks at 20,442,001,408 bytes, with swap disabled and zero OOM or
memory-limit events. This is cumulative cgroup charge, not exact process RSS or
proof of complete query-wide memory accounting. Resource, concurrency and
multi-session acceptance remain open. No DuckDB leadership is certified.

## Where suite time remains worse

Per-query medians in decoded IPC leave 2,588.735 ms of net excess time. Q1 accounts
for 3,432.245 ms (5,165.574 versus 1,733.329 ms); Q18 accounts for 3,175.857 ms
(3,990.909 versus 815.052 ms). Other query wins partially offset those deficits.
The archived `bound-utf8-decoded-excess.json` contains all per-query values and the
source sample/report hashes.

The earlier frozen `99e1c0dd` phase diagnostic places Q18's inner aggregate at
about 1.10 s in key preparation/routing, 1.58 s in state processing and 0.97 s in
finish/output, with zero spill and only 624 output rows after HAVING. Q1 has a
different balance: paired `59619bde` runs halve key preparation, while expression
evaluation and state updates remain significant. These wall intervals are not
additive exclusive CPU samples. They support further shared aggregate attribution,
not a query-specific rule or an unmeasured switch to partial ownership.

## Reproducible evidence and next step

The [verified archive](benchmarks/2026-09-11-bound-utf8-residency/manifest.json)
contains 1,317 files plus its top-level manifest. It excludes transient scratch and
cache files. Commands, source/binary/harness hashes, request-scoped device evidence,
typed comparisons, all outcomes and cgroup counters are retained.

Live artifact locations:

- Controller: `.scratch/parallel-aggregate-input/bound-utf8-residency-cycle.json`
- Run: `.scratch/public-bench/bound-utf8-residency-32g-01`

Evidence checkpoint `d37e836` is committed, pushed and remote-verified. Job42323
completed six independently typed-correct diagnostic outputs for Q1/Q18. The
[decimal metadata investigation](decimal-scale-cache-2026-09-11.md) records its
sampled phase costs and the next candidate. These intervals are potentially
biased diagnostics. Native timeouts, reference stability and broader
resource/concurrency gates remain unresolved.
