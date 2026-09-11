# Release validation and live aggregate attribution

The repaired release build completed successfully in 8m40s. Its immutable copy
is `.scratch/live-schema-boundary/benchmark_embedded`; binary hash and all659
source-input hashes are [archived](benchmarks/2026-09-07-live-aggregate-profile/manifest.json).

The complete uninstrumented canonical SF1 run now validates **21/22 queries**:
63 typed measured pairs pass, including all six previously failing dictionary
queries. Q13 exceeds the 349.630ms matched DuckDB query ceiling. Its worker is
terminated by the watchdog, causing the next two samples to report worker
unavailable. There is no valid full-suite performance score. Full raw artifacts
remain in `.scratch/public-bench/live-dictionary-sf1-01/`; metadata, samples,
execution events and the report are archived beside the hashes.

## Q13 phase evidence

Four separate diagnostic requests use unchanged canonical SQL, raw Parquet,
16 threads, CPUs0–15, 4GiB query memory, 12GiB process cap and a32GiB cgroup.
All four outputs pass the typed ordered DuckDB oracle. Diagnostics deliberately
use a60s process watchdog to collect evidence beyond the normal query ceiling;
they are not latency acceptance samples.

| Interval | Observed range across four requests |
|---|---:|
| Entire query |579.8–630.5ms|
| Inner aggregate ingestion |180.4–223.8ms|
| Inner aggregate finish, including output |10.0–12.6ms|
| Inner aggregate output construction |9.6–12.1ms|
| Outer aggregate ingestion |11.9–12.3ms|

The inner aggregate consumes1,534,302 rows in185 batches, produces150,000 groups
in2,344 batches, and spills zero bytes. The outer aggregate produces42 groups.
These are wall intervals, not exclusive CPU samples; finish includes output.
The small output batches are a possible secondary cost, but output construction
alone is too small to explain the deadline failure. Ingestion deserves narrower
attribution before changing output policy. Upstream join and other query work
also remain material and have not been isolated by these timers.

`perf record` was attempted under containment and failed before executing the
worker: host `perf_event_paranoid=4` disallows sampling. The error is preserved;
no host security settings were changed. A follow-up `QE_AGG_DETAIL_PROF` diagnostic
samples one row per1024 within each completed ingestion call, separating key
encoding, hash lookup/new-group setup, state preparation and commit. All110
aggregate component tests pass with diagnostics both disabled and enabled
(separate runs; not220 distinct tests). Optimized measurement is pending.
Clock overhead and periodic-sampling
bias must be acknowledged; do not extrapolate these samples into exact CPU totals.
Pressure-truncated calls do not emit completed-call records.

No production output-size, parallelism or memory policy was changed by either
diagnostic. Fresh SF10, provider/residency validation, resource gates and measured
CPU improvement remain open.
