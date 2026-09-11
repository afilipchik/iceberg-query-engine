# Declared build-schema candidate: canonical SF10 provider screen

Frozen candidate703b8564 completes raw Parquet and Iceberg in one session. Native
and Lance remain incomplete. This is a provider screen, not DuckDB leadership or
resource/concurrency certification. The separate paired Q9 diagnostic found a
74.67% raw query-time reduction against2f9ad9f1; it is not a suite-wide improvement.

## Reproducible conditions

Session52333 is terminal (driver exit1 because two provider tracks fail); its
process handle expired, but all four terminal records and final source verification
are present. Audit42325 is terminal0. Candidate release76303 has518verified source
inputs, features lance,gpu; GPU is disabled for this CPU screen. Default disjoint
ownership,16threads, CPU affinity0–15,4GiB query/12GiB process,3measured samples and
one warmup per query, one session. All commands run in the required48GiB capped
scope with swap disabled and TMPDIR inside repository scratch. The typed comparator
audit runs in a separate8GiB scope. Reference is matched DuckDB1.4.4; each query
must finish within10times its matched calibration. Startup allowance is separate.

Driver, commands, environment, schema/data/provider provenance, all samples,
reference plans and independent typed outputs are retained in the archive. No
engine source or benchmark harness changed during measurement. Earlier partial
ownership screens are different experiments and cannot be treated as matched controls.

## Completion and performance

| Track | Valid measured pairs | Correct completed warmups | Geometric mean engine/DuckDB | Suite total ratio |
|---|---:|---:|---:|---:|
| raw_parquet | 66/66 | 22 | 2.993145 | 3.428160 |
| native | 63/66 | 21 | incomplete | incomplete |
| iceberg | 66/66 | 22 | 0.437360 | 0.486399 |
| lance | 57/66 | 19 | incomplete | incomplete |

All252completed measured outputs and84completed warmups independently validate;
no completed output is incorrect. Twelve requested pairs were not run. No completed
measured output exceeded its ceiling. Raw loses22/22queries, worst ratio8.941314
(Q17). Iceberg wins19/22, worst1.806564(Q16). Its one-session bootstrap interval
for geometric mean is0.428265–0.448874; this does not measure between-session
variation. Do not pool the providers or infer CPU kernel leadership from Iceberg.

Failures remain explicit:

- Native Q1 warmup times out; all3dependent measurements are not run.
- Lance Q1 warmup times out; all3dependent measurements are not run.
- Lance Q7 warmup refuses memory scan working space:63176additional bytes,
 410708618used,410718415limit. This is a real engine resource failure, not a timeout.
- Lance Q9 reference calibration fails; no validated ceiling or engine retries.
 This is a reference failure, not an engine speed measurement.

The provider scope records zero memory.max/OOM events, peak19522273280bytes.
Peak is cumulative across sequential tracks, not an individual provider's peak.
No OOM does not prove query-wide reservation coverage.

## Per-query ratios

Ratios use medians of all three valid measured pairs. A missing cell remains
incomplete; it never contributes an invented suite score.

| Query | Raw | Native | Iceberg | Lance |
|---|---:|---:|---:|---:|
| q01 | 3.0879 | incomplete | 0.4061 | incomplete |
| q02 | 1.3130 | 2.7510 | 0.2773 | 1.1145 |
| q03 | 3.0136 | 4.2649 | 0.3763 | 1.6119 |
| q04 | 1.6902 | 2.5671 | 0.2355 | 1.2907 |
| q05 | 2.4855 | 8.4119 | 0.3374 | 1.4050 |
| q06 | 1.9114 | 9.2735 | 0.2274 | 1.1739 |
| q07 | 2.7969 | 4.4113 | 0.3492 | incomplete |
| q08 | 1.6339 | 5.4934 | 0.2866 | 1.4315 |
| q09 | 3.0533 | 3.9873 | 0.4670 | incomplete |
| q10 | 5.5488 | 4.7688 | 0.7583 | 2.4238 |
| q11 | 2.6426 | 4.0424 | 0.4505 | 1.5938 |
| q12 | 6.5719 | 3.3427 | 0.8409 | 1.2196 |
| q13 | 7.9056 | 7.9507 | 1.0743 | 6.4637 |
| q14 | 3.1130 | 1.8085 | 0.3977 | 0.8808 |
| q15 | 1.6973 | 2.7381 | 0.2441 | 2.7619 |
| q16 | 7.3469 | 7.4052 | 1.8066 | 4.3006 |
| q17 | 8.9413 | 9.7527 | 1.0564 | 1.9658 |
| q18 | 3.6179 | 9.2058 | 0.5500 | 8.1703 |
| q19 | 4.5803 | 3.0020 | 0.5717 | 1.1075 |
| q20 | 2.2654 | 2.0536 | 0.3520 | 1.4131 |
| q21 | 1.4344 | 3.8154 | 0.2294 | 1.2785 |
| q22 | 1.5311 | 1.2999 | 0.2415 | 0.6020 |

## Next attribution and acceptance

The full screen restores raw Q12/Q13 completion but leaves their performance far
behind DuckDB. Raw Q13's first sample records444msplanning and2074msexecution;
Q17 records1830msexecution of1838mstotal. These are observed phase metrics, not
root-cause proof. A separate fresh-process diagnostic profiles Q1/Q13/Q17 for raw
and native with an explicitly diagnostic180second watchdog; it cannot clear the
strict screen's failures.

Native's missing admitted scan and whole-segment deletion survivor materialization
remain source-confirmed contract gaps. Fix incremental ownership before advertising
admitted parallel input. Native runtime-filter absence is a separate source finding;
its performance impact still needs attribution. Preserve independent SQL oracles,
exact dictionary/deletion semantics and terminal error behavior.

Full provider three-session acceptance, decoded IPC and GPU evidence for this binary,
resource/concurrency gates, existing spill failures and protected short-query
precision remain open. No default ownership change or leadership promotion.

[Source contracts and paired evidence](build-schema-admission-2026-09-09.md),
[archive manifest](benchmarks/2026-09-09-build-schema-admission-providers/manifest.json).
