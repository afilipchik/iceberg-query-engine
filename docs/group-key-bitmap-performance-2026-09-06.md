# Group-key and float bitmap candidate: release validation

The optimized candidate fixes the independent grouping and float comparison
reproducers and restores Q6 performance while retaining Q14's improvement. It is
not yet a full-suite or multi-session acceptance result.

Executable SHA-256:
`e8174895539343b61796925247fedebdfef45f54caa4f133d4496e70df90aae6`.
Frozen source SHA-256:
`0276aa42c545a7a30570141ffbc4db394ea3688dbd387491dc487e8f2e5c1b3f` (612 files).
The control is the validated SUBSTRING/VALUES release
`ad506267227122efe6b67c426d113468e191fdb821f4d3ea8afe95e1fba20585`.

## Correctness and contained execution

735 selected default test executions pass, including 13 spill regressions and the
dedicated IPC test; the historical flatten-dependent-join test remains ignored.
The optimized release passes 89 independent float/date oracle queries, plus all
four 65,536-row grouping/join probes: 3 groups, 2 non-NULL distinct values and
52,429 qualifying rows in each comparison query. These explicit expected results
come from independent DuckDB evaluation of the same Arrow data. The test gates
do not certify every provider, external sort or query-wide ownership boundary.

## Canonical SF10 focused recovery screen

Each mode runs Q6/Q12/Q14/Q19/Q22 with ten steady pairs plus one warmup per side,
16 threads/affinity 0–15, 40 GiB query memory, 48 GiB process cap and a 96 GiB
cgroup. Fresh matched DuckDB calibration supplies typed reference results and
10x query ceilings. No competing heavy jobs ran during latency measurement.
All **330 requests** validate; no missing/failed samples are excluded.

Ratios below are candidate/control. They are not engine/DuckDB leadership ratios.

| Mode | Suite | Geomean | Q6 | Q12 | Q14 | Q19 | Q22 |
|---|---:|---:|---:|---:|---:|---:|---:|
| Decoded IPC | 0.9802 | 0.9576 | 0.9976 | 0.9999 | 0.8003 | 1.0011 | 1.0075 |
| Lance | 0.9303 | 0.9303 | 0.9749 | 0.8628 | 0.7974 | 1.0222 | 1.0163 |
| Raw Parquet | 0.9866 | 0.9831 | 0.9543 | 0.9969 | 0.9264 | 1.0234 | 1.0180 |

The preceding row-wise float builder regressed Q6 by 25–65% across these modes;
component pairing isolated it from constant CAST normalization. Direct packed
Boolean values with separate validity handling recover that loss without reverting
SQL signed-zero/NaN semantics. Q14 remains about 20% faster on IPC/Lance and 7%
faster on Parquet. No query in this focused screen exceeds a 10% median regression.

Detailed paths:
`.scratch/group-key-equivalence-repair/{paired-status.json,run-pairs.py}` and
`.scratch/public-bench/group-key-bitmap-{decoded_ipc,lance,raw_parquet}-01/`.
The [preceding evidence](sql-float-cast-performance-2026-09-06.md) preserves the
failed implementation and component attribution rather than replacing its results.

Required GPU/control validation completed on the separate 600,000-row float
fixture: 40 device samples each report one completed GPU run, zero failures or
fallbacks, and a verified 268,435,456-byte cache target. All 40 matching CPU
control samples also validate. GPU/DuckDB suite ratio is 0.09831; CPU control is
0.77669. This fixture is not canonical SF10 or hard-VRAM certification.

The [verified recovery archive](benchmarks/2026-09-06-group-key-bitmap/evidence.tar.gz)
contains 986 checked members, including exact source/binary, all release oracles,
focused provider samples and GPU evidence. Archive SHA-256:
`fcae9f55f173d43ab53730c8abb5896ff0eaa9f70cbbf1cf94dcb409d7e57f31`.

A full 22-query screen across IPC, raw Parquet, native, Iceberg and Lance is now
running with three steady pairs plus warmup per side. Its results will be retained
separately from the completed focused archive. Full multi-session,
layout/resource/concurrency, larger-scale and additional public-workload gates
remain open; this result does not establish DuckDB leadership.

## Completed full SF10 screen

All five modes completed all 22 queries with three steady pairs and one warmup
per side: **880/880 engine requests validated**, with no missing samples or time
gate failures. This is a development screen, not multi-session acceptance.

| Mode | Candidate/control suite | Candidate/control geomean | Initial >10% regressions | Candidate/DuckDB calibration suite |
|---|---:|---:|---|---:|
| decoded_ipc | 0.95931 | 0.97116 | q16 | 0.47170 |
| iceberg | 0.99043 | 0.99402 | q08 | 0.32219 |
| lance | 0.91447 | 0.93284 | q08, q16, q20 | 1.25244 |
| native | 0.97202 | 0.97612 | None | 3.20636 |
| raw_parquet | 0.99709 | 0.99737 | q02, q18 | 2.38482 |

DuckDB columns use the fresh three-sample calibration for each query. Provider
and residency modes are separate comparisons; do not combine them into one
leadership claim. The Lance reference retains the documented extension/decimal
AVG qualification. Current raw Parquet and native gaps remain substantial.

Seven query/provider cases crossed the 10% median regression threshold. Two
fresh sessions with ten steady pairs each have completed for those cases.
Initial IPC Q16 is 1.11756 (266.991 to 298.378 ms). Saved optimized and physical
plans for IPC Q16 and Parquet Q2/Q18 are identical across binaries; this does
not establish whether their timing differences are persistent.

Full per-query calibration and candidate/control analysis is in
`.scratch/group-key-equivalence-repair/full-analysis.json`; completion commands
are in `full-status.json`. The focused archive predates and excludes this screen;
the verified separate archive below preserves these results.

## Protected rechecks and archived evidence

All 308 recheck requests validated, with zero time-gate failures. None of the
seven cases exceeded 10% in either repeat session. Original slower samples are
preserved; these repeat measurements do not replace the full-screen results.

| Mode/query | Initial screen | Session 1 | Session 2 |
|---|---:|---:|---:|
| decoded_ipc q16 | 1.1176 | 0.9878 | 1.0082 |
| iceberg q08 | 1.1817 | 0.9779 | 0.9672 |
| lance q08 | 1.2853 | 1.0129 | 0.9439 |
| lance q16 | 1.1664 | 0.9884 | 0.9779 |
| lance q20 | 1.1767 | 1.0643 | 1.0885 |
| raw_parquet q02 | 1.1270 | 0.9909 | 1.0141 |
| raw_parquet q18 | 1.1040 | 1.0056 | 0.9954 |

Lance Q20 remains 6.4% and 8.9% slower in the repeats, a retained concern below
the 10% protected threshold. No query-specific change was introduced.

The [full/recheck archive](benchmarks/2026-09-06-group-key-bitmap-full/evidence.tar.gz)
contains 3,510 SHA-verified members. Archive SHA-256:
`ecbb3c545b786ebdba030c24c7992b9c17911cad42135cafe414d1784ad66394`.
The frozen source and binary remain in the earlier focused archive.

A subsequent [small memory probe](result-memory-boundary-findings-2026-09-06.md)
reproduces computed output exceeding the query budget with zero reservations.
This resource gap is open; the performance screen does not certify memory safety.
