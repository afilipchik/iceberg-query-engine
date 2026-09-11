# Corrected correlated aggregates and semi filtering: diagnostic — 2026-09-11

Release72847 completed in8m52s, freezing binary68912c231c1442f79d3f5433bb5c41795cde37b18a2ca777976fb8cb3799b203 with521 source inputs. Sequence82829's diagnostic stage completed successfully:9 independent typed-correct outputs across raw Parquet, native and decoded resident inputs. Source/binary/dataset/provider/driver/harness checks verify after execution. Locked/offline lance,gpu build; GPU disabled for this diagnostic, default disjoint ownership,16threads CPU0–15. Raw/native4/12GiB query/process, decoded resident32/48GiB with preload excluded. A180-second diagnostic watchdog is not matched DuckDB10× acceptance.

| Provider | Q9 ms | Q13 ms | Q17 ms |
|---|---:|---:|---:|
| raw_parquet | 1469.034 | 663.116 | 1704.478 |
| native | 2625.309 | 1803.508 | 925.525 |
| cpu_resident_32g | 1626.492 | 2111.786 | 1022.489 |

These are single instrumented samples, not a paired speedup claim. Q17 plans in all modes contain the true SEMI reduction and the lazy missing-group CASE; raw Q17's aggregate drain still reports one input slot. The fixes therefore activate in canonical SQL without establishing complete parallel admission. Q13 retains the COUNT preaggregation/SUM reduction. Full plans, response metrics and traces are archived; zero aggregate-profile fields must not be interpreted as zero aggregate work.

The same-binary Q1/Q6 route control is running next, followed by a full canonical provider screen. Results remain pending. See [source repair](right-built-semi-runtime-filter-2026-09-11.md), [validation](right-semi-validation-2026-09-11.md), and [diagnostic archive](benchmarks/2026-09-11-right-semi-diagnostic/manifest.json).

Recorded scope resources: `{"memory.events": "low 0\nhigh 0\nmax 0\noom 0\noom_kill 0\noom_group_kill 0\n", "memory.max": "51539607552\n", "memory.peak": "26956623872\n", "memory.swap.max": "0\n", "path": "/sys/fs/cgroup/user.slice/user-1000.slice/user@1000.service/app.slice/safe-build-2694604-654.scope"}`. The sequence shares this scope across stages; this snapshot covers the diagnostic stage before later stages start.
