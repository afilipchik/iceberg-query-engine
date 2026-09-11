# Incremental native reader: frozen CPU comparison

The new incremental reader repairs eager survivor retention, but this comparison
does not establish a consistent native latency win. All36outputs independently
validate. Native Q1 remains roughly12seconds, and its raw/resident gap persists.
Do not promote these results to DuckDB leadership or a protected regression bound.

## Frozen source and protocol

Release32424 completed in8m52s, freezing
`c20b0648bd15bd0c0a6c9674c7a5aac2482e242ebd0a3382830b14ff075c9b6e`.
All518build inputs match the current source and correctness archive. Only
native_scan.rs, ipc_cache.rs and native_table.rs differ from control703b8564.
Features lance,gpu, locked/offline,48GiB capped build, one build job; no dependency
or ownership-default change. The preceding gate remains1080library/11ignored,
56default native IPC/mutation passes, with separately recorded unresolved failures.

Paired11044 is terminal0: Q1/Q9/Q17, raw/native/decoded-resident, two blocks with
reversed control/candidate order. Each query runs in a fresh process.16threads,
CPU0–15, default disjoint ownership, GPU0. Raw/native4GiB query/12GiB process;
resident32GiB query/48GiB process, preload excluded. Enclosing48GiB scope and
repository TMPDIR. The180second diagnostic watchdog is not the10times DuckDB
acceptance ceiling. Three queries are diagnostic shapes, not a canonical full suite.

All36outputs match independent typed oracles; all1236join traces complete without
unwinding. After-run dataset, binary, source, harness and driver hashes verify.
Scope peak37268877312bytes, swap0, zeroOOM/max events. This cumulative peak is not
per-query memory usage and does not certify query-wide reservations.

## Observed timings

Candidate/control ratios below1favor the candidate. Two observations per binary
cannot establish between-session uncertainty or a confirmed slowdown bound.

| Mode/query | Control samples ms | Candidate samples ms | Ratio of means | Block ratios |
|---|---|---|---:|---|
| raw_parquet/q01 | 614.998, 621.758 | 650.871, 620.887 | 1.028302 | 1.058329, 0.998600 |
| raw_parquet/q09 | 1507.249, 1542.300 | 1480.448, 1505.956 | 0.979294 | 0.982219, 0.976436 |
| raw_parquet/q17 | 1826.773, 1938.837 | 1852.546, 1839.728 | 0.980525 | 1.014108, 0.948882 |
| native/q01 | 12524.668, 11618.829 | 11844.950, 11890.818 | 0.983112 | 0.945730, 1.023409 |
| native/q09 | 3185.590, 2820.634 | 2588.087, 3075.250 | 0.942912 | 0.812436, 1.090269 |
| native/q17 | 1427.606, 1347.407 | 1299.990, 1356.707 | 0.957364 | 0.910608, 1.006902 |
| cpu_resident_32g/q01 | 11046.193, 11083.204 | 11296.622, 11194.940 | 1.016366 | 1.022671, 1.010082 |
| cpu_resident_32g/q09 | 1618.777, 1562.329 | 1590.043, 1610.552 | 1.006127 | 0.982250, 1.030867 |
| cpu_resident_32g/q17 | 1080.686, 1082.248 | 1070.124, 1075.913 | 0.992188 | 0.990227, 0.994147 |

Native Q9's0.942912ratio of means hides opposing block results0.812436 and1.090269.
Calling that a proven5.7%improvement would overstate the evidence. Raw Q1's first
block is5.83%slower while its second is essentially unchanged. Preserve both.

## Next shared bottleneck

Resident Q1 already has16admitted input slots in both binaries. Block1 control
still reports8995.138msaggregate ingestion and6884.753msprocessing wall time;
candidate reports9222.726msingestion and6980.092msprocessing. Both handle59,142,609
rows through four disjoint workers and produce fourgroups without spill. Counters
can overlap; do not add them into a synthetic total. Native scan admission alone
cannot explain away this resident cost.

Current lower_aggregate_cpu chooses the direct Parquet MorselAggregateExec path
when it can extract a Parquet source. Native special routing only supports an
eligible dense integer-key shape; other native and resident aggregates use the
spillable pipeline. Thus provider-dependent kernel/ownership choices are concrete
source facts. Do not expand an unadmitted materializing fast path merely to match
raw timing. Investigate sharing efficient aggregation over admitted batches while
preserving spill, exact dictionary semantics and retained output ownership.

The next performance task is general aggregate ownership/kernel costing, protected
by4and16thread low/high-NDV controls and the known4thread Q18 partial-ownership
regression. The native admitted reader remains necessary for full query-resource
coverage, with compression/alignment/dictionary metadata prerequisites in its design.
A new implementation needs fresh protected/provider/residency/resource gates; this
candidate has not completed those acceptance requirements.

[Incremental contract and correctness](native-incremental-ipc-2026-09-10.md),
[native admission prerequisites](native-admitted-reader-design-2026-09-10.md),
[measurement archive](benchmarks/2026-09-10-native-incremental-paired/manifest.json).
