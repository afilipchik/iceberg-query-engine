# Infallible numeric cast construction

Status: candidate; optimized performance is unproven. The preceding goal turn
made progress by completing and archiving source629 provider, protected, GPU and
aggregate-cap validation. Its integer-to-double component remained 13.1% slower
than source612, so this increment targets that shared conversion path.

`planner/reserved_cast.rs` now classifies supported integer-to-Float32/Float64
and Float32-to-Float64 pairs from their complete type domains. These conversions
cannot fail, although integer-to-float can round. The path fills its pre-admitted
buffer through the infallible iterator API and shares input validity. It avoids
per-row Result handling, manual fallible Vec push and TRY validity construction.
All other pairs retain the fallible converter. No admission, ownership or SQL
failure rules have been relaxed. This is a source-based bottleneck hypothesis;
only paired optimized measurements can establish a performance improvement.

The first existing Arrow oracle passes. Expanded coverage adds integer rounding
boundaries and nonzero-offset nullable slices (1,600 combinations across types,
metadata, modes and shapes). Expanded oracle passes (two test functions), and all26 selected integration
tests pass without skips, including ownership/refusal checks. Formatting and
whitespace checks pass. Frozen630 source SHA256:
`b27789e2be7043b117f0aa12461f653d91e6341f53d0e427671e12568d0d934a`.
Optimized build session22707 is running in64GiB/jobs1 with lance+gpu.
Next: finish build, verify and freeze binaries, run the component
comparison against629 and612, then applicable semantic/provider/resource gates.
Do not substitute this component result for the epic's workload leadership gates.


## Benchmark telemetry correction

While the optimized build runs, a separate harness correction removes conflicting
GPU labels. Resident execution has request-scoped evidence but does not emit the
legacy run-OK trace marker. Worker telemetry now preserves the trace count while
using valid request-scoped evidence for device execution; trace silence becomes
unobserved. Malformed resident evidence remains invalid even if a legacy marker
exists. This does not replace preparation/session certification or alter query
correctness/timing gates. Historical artifacts remain unchanged.

Benchmark tests:103 run,101 pass,2 skips. The opt-in Lance provider integration
and exact-bag spill integration were not enabled in this test invocation. Three
new telemetry tests cover real resident evidence without trace, legacy-only
observations, and malformed resident evidence. Live GPU validation on the frozen
candidate remains pending. The engine source is unchanged since the630 freeze;
harness changes will be included separately in final evidence provenance.


Full library gate completed:685 passed,2 existing ignores; combined with26
selected integration passes,711 unique Rust tests pass. The expanded two cast
unit functions are included in the library count. Logs are in
`.scratch/infallible-cast-repair/`. The release build remains live; no latency
measurements have started. The prior source629 results are preserved controls,
not validation of the new candidate.

The opt-in harness integrations were subsequently enabled using the existing
Lance venv: all103 tests pass, zero skips (7.263s). This supersedes the earlier
101-pass/two-skip invocation; both logs are retained.


## Optimized630 validation

Release build22707 completed successfully in10m40s. Binary SHA256:
`c527d6d0f4349fde524dd748b33991abf4bb2b876b051abe51159ce7b9314f29`.
All production source hashes were checked before freezing the binaries.
Validation session81844 completed:89 float/date,10 dense-float and56 coercion
queries match DuckDB; all58 primitive cases match the preserved engine control
(50 match DuckDB; eight pre-existing integer-division differences remain).
Twenty literal cases validate, retaining the documented separate bare-NULL
schema/value policy. Borrowed input succeeds at64KiB while expanded literal,
DOUBLE and DECIMAL output buffers refuse by memory-budget name.
Decimal metadata rejection remains fixed; three timestamp metadata mismatches
remain explicit failures in the diagnostic probe, not claimed as passing coverage.

Paired component session30894 is running in24GiB, affinity0–3, against frozen629
and612. No other heavy jobs remain active. Performance acceptance is pending.

## First paired component results

All352 requests passed typed validation and timing gates. Eight components,
ten measured pairs plus warmup against each control,262144 rows in65536-row
batches,4threads,24GiB scope and affinity0–3. Lower ratios are faster.

| Component | 630/629 | 630/612 |
|---|---:|---:|
| `SELECT i AS v FROM t` | 0.9382 | 1.0749 |
| `SELECT i + 1 AS v FROM t` | 0.9492 | 1.0125 |
| `SELECT f * 1.25 AS v FROM t` | 1.0031 | 0.9818 |
| `SELECT CAST(i AS VARCHAR) AS v FROM t` | 0.9768 | 1.0529 |
| `SELECT i + 1 AS v, CAST(i AS VARCHAR) AS s FROM t` | 0.9876 | 1.0189 |
| `SELECT CAST(i AS DOUBLE) AS v FROM t` | 0.7635 | 0.8239 |
| `SELECT CAST(i AS DECIMAL(38,2)) AS v FROM t` | 0.9215 | 0.6965 |
| `SELECT TRY_CAST(i AS TINYINT) AS v FROM t` | 1.0379 | 1.0390 |

The targeted integer-to-double component improves23.6% against629 and17.6%
against612. This supports the infallible-construction hypothesis, but it is one
paired session per control and does not establish whole-query leadership.
Full five-provider SF10 validation is running next; protected repeats, supported
GPU residency and aggregate caps remain pending for630.


## Full provider screen progress

Decoded IPC completed all176 requests with typed correctness/time gates passing.
Suite630/612 ratio0.96522149; no query exceeded1.10. Raw Parquet is now running
in session12582, followed by native, Iceberg and Lance. No production source
changed during timing. Post-screen cast scaling (8,192/65,536/262,144 rows,
borrowed/cast/fallible-control components against629 and612), protected repeats,
GPU, cap and archive drivers are prepared; none has run for this candidate yet.

## Completed five-provider SF10 screen

All880 requests pass typed correctness and calibrated time gates (22 queries,
three measured pairs plus one warmup pair per track). Ratios compare630 to612;
this remains a development A/B screen, not DuckDB leadership certification.

| Track | Suite630/612 | Geomean630/612 | Queries above1.10 |
|---|---:|---:|---|
| decoded_ipc | 0.9652 | 0.9781 | None |
| raw_parquet | 0.9938 | 0.9942 | None |
| native | 0.9728 | 0.9889 | None |
| iceberg | 0.9825 | 0.9830 | None |
| lance | 0.9286 | 0.9464 | q08 (1.3545) |

Lance Q8 requires two fresh ten-pair protected repeats; those are now running.


Protected Lance Q8 repeats completed44 valid requests (two fresh ten-pair
sessions plus warmups):630/612 ratios0.96335336 and0.99583301. The initial
35.5% slowdown did not recur. Optimized and physical plan text hashes match
across controls/candidates/repeats. Execution-phase medians are similar; the
initial screen has higher planning/optimization medians. Phase medians must not
be summed as though they came from one request, and these observations do not
establish a root cause. Raw phase observations are retained in the evidence.
There is no confirmed>10% protected regression in this full screen.


## Size scaling and live GPU validation

All396 size-scaling requests passed (8,192/65,536/262,144 rows; borrowed column,
integer-to-double and overflow-heavy TRY_CAST; ten pairs plus warmups per control).
Integer-to-double ratios630/629 are0.9282/0.8486/0.7591 and630/612 are
0.7461/0.9030/0.9328 respectively. The target improves at every measured size.
The262,144-row TRY_CAST control is1.1482 of612 in this session, despite0.9757
of629; two fresh protected component sessions are required and running.

Current GPU run completed40 correct CPU controls and40 required-device samples.
Every device sample confirms one successful request-scoped device run, the
corrected telemetry says device_executed, and the cache target is256MiB.
This uses the custom600k-row float fixture,16GiB host cgroup, affinity0–3 and
NVRTC from `.venv/lib/python3.12/site-packages/nvidia/cuda_nvrtc/lib`.
It is supported-path validation, not canonicalSF10 GPU coverage or a hard VRAM cap.


## Performance acceptance failure

Two fresh protected component sessions completed132 valid requests. Ratios630/612:

| Component | Repeat1 | Repeat2 |
|---|---:|---:|
| Borrowed column |0.87043|0.98484|
| Integer-to-double |0.85824|0.90555|
| Overflow-heavy TRY_CAST to TINYINT |1.10739|1.16128|

The TRY_CAST control regression exceeds10% in both protected repeats. The current
candidate is **not performance-accepted**, despite correct results and the
integer-to-double gain. Full SF10 has no confirmed protected regression, but that
cannot erase this independent component failure. Do not relabel it as noise.

Next hypothesis: the fallible TRY path always iterates source.iter(), constructing
Option input values and checking validity even for non-null arrays. Unlike its
strict path, it has no non-null values-only branch. Dispatch on actual array
null_count outside the loop, retain the same conversion semantics and pre-admitted
output/bitmap, and compare the next frozen binary against630 and612. This is a
shared input-validity/construction fix; no query-specific dispatch is proposed.
Source630 aggregate cap checks are running before its rejected-candidate archive.


## Aggregate caps

Both current630 aggregate scenarios pass:1,000,003 groups, exact_counts=true,
spilled=true and3,855,541,894 accounted spill bytes. Peak RSS is408MiB under
1GiB cgroup and411MiB under2048MiB RLIMIT_DATA within the8GiB outer scope.
Both exit0. These passing safety checks do not override the protected TRY_CAST
performance failure. All630 jobs are now terminal; archive verification is next.


## Verified archive

[Provenance](benchmarks/2026-09-06-infallible-cast/provenance.json):4,912 files,
every source/evidence member hash checked. Archive SHA256
`23c4c07502fcf7fa7cd56b38dbbfc6a777628e0198f07c2c3ff1b7e309510c32`.
Reproduce the harness by applying the three separately archived files listed in
harness-source-hashes.json over the source snapshot. All remaining script
snapshot entries were checked unchanged. The source snapshot preserves the
pre-telemetry implementation; the override files preserve the measured harness.

A newer shared TRY_CAST input-validity experiment now follows in the working
source; none of the630 timings or cap results apply to that new candidate.
