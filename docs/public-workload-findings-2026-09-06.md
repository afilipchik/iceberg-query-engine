# Public workload correctness gates — 2026-09-06

The first public development gates found systemic correctness gaps despite a
passing canonical SF10 check. Their original failures are preserved below.
After shared aggregate, binder, scalar and regex repairs, the final release
passes **113/113 JOB queries and 43/43 ClickBench queries**, with three measured
pairs per query and no correctness or time-gate failures. These are development
extracts, not full public workload certification.

Final runs: `.scratch/public-bench/final-contracts-01/{job,clickbench}`.
Suite engine/DuckDB ratios are 1.675876× JOB and 2.234947× ClickBench; geometric
means are 1.421927× and 1.671372×. One session does not establish a performance
ranking or leadership. The [repair evidence](benchmarks/2026-09-06-public-contracts/README.md)
keeps intermediate failures and source boundaries explicit.

The following table describes the **first, pre-repair runs**, not current failures.

| Workload | Measured pairs | Typed correct | Wrong answers | Engine errors/unsupported | Reference failures |
|---|---:|---:|---:|---:|---:|
| JOB development extract, all 113 queries | 113 | 77 | 26 | 6 | 4 |
| ClickBench development extract, all 43 queries | 43 | 30 | 1 | 12 | 0 |
| Canonical TPC-H SF10, 22 queries × 3 samples | 66 | 66 | 0 | 0 | 0 |

Counts classify reference failures separately before evaluating engine answers.
One pair means one measured execution from each engine. The archived original
reports and [machine-readable classifications](benchmarks/2026-09-06-public-gates/classification.json)
preserve every query, error and comparison failure.

## Matched conditions and provenance

All three runs used raw Parquet, DuckDB 1.4.4, 16 threads, a 40 GiB query budget,
48 GiB process cap, warm host cache and embedded parse-to-consumed-Arrow timing.
Containment/affinity, fresh calibrations, pairing order, exact commands and data,
SQL, binary and source hashes are in each manifest. All used binary SHA256
`29f66805d43b6b9ba20b9331c6ab642078c487f052d044e3aa803529ee45e4c6`.
The source tree was dirty: the revision alone cannot identify the implementation.

Run starts in UTC: SF10 01:45:32, JOB 01:48:15, ClickBench 01:49:16 on
2026-09-06. JOB and ClickBench used one session and one sample per query;
SF10 used one session and three samples. Public runs are correctness screens,
not reliable performance rankings. The real-data extracts are intentionally
small and biased; see [selection and source provenance](../scripts/benchmark/PUBLIC_WORKLOADS.md).

## JOB: aggregate semantics and physical type coverage

Six queries raised engine errors: 1a, 1b, 1c and 1d report unsupported `MIN`
over Int32; 3c and 5c report unsupported `MIN` over Dictionary(Int32, Utf8).

Twenty-six queries completed but failed exact typed value/multiplicity checks:
4c, 6f, 8c, 8d, 9c, 9d, 12c, 13a, 13b, 13d, 14c, 16b, 16c,
17a, 17b, 17e, 17f, 18c, 19c, 19d, 20c, 22c, 22d, 25c, 26c and 30c.
The recorded mismatches include missing or incorrect NULL/string aggregate
values. This points toward aggregate state/type handling, but these traces
alone do not prove a single root cause for every wrong answer. Preserve
independent DuckDB comparisons when testing any shared fix.

Four additional queries, 15a–15d, failed **in DuckDB's parser** because the
original PostgreSQL alias `at` conflicts with DuckDB 1.4.4's reserved `AT`.
These are reference dialect failures, not four additional proven engine wrong
answers. The other 32 engine failures remain valid independent findings.

Preparation now applies a generic lexical identifier-quoting adapter to the
incompatible alias's declarations and qualified references. It preserves
strings/comments and original SQL bytes/hashes. The same translated SQL is used
for timed engine execution, timed DuckDB execution and the oracle. All 113
queries remain present; only 15a–15d change. The refreshed
`.scratch/public-bench/job-dev-04/dataset.json` reuses the original 21 tables
after hash verification, without redownload. All translated statements parse
with the pinned DuckDB. The original failed run and JOB03 manifest remain
unaltered. This preparation fix is not an engine correctness rerun.

## ClickBench: one wrong answer and twelve capability failures

| Finding | Queries | Recorded evidence |
|---|---|---|
| Wrong aggregate answer | q10 | Floating value/multiplicity mismatch; query includes AVG(ResolutionWidth), SUM, COUNT and COUNT DISTINCT |
| Int16 group key support | q08, q12, q15, q31, q40, q42 | Unsupported group type or group output array |
| Timestamp group output | q43 | Unsupported Timestamp(Microsecond, None) group array |
| Aggregate expression in ORDER BY | q16, q17 | COUNT(*) survives into scalar evaluation and is rejected |
| STRLEN | q28, q29 | Function explicitly unsupported |
| EXTRACT | q19 | Argument type explicitly unsupported |

All 43 reference queries completed. Unordered LIMIT, hidden ORDER BY fields and
OFFSET were validated using complete untimed oracles and typed tie-group slice
matching; no query was excluded to obtain the 30 passes. The q10 failure is a
real answer mismatch requiring an isolated AVG/type reproducer, not grounds to
increase tolerance without evidence.

The original q40/q42/q43 report additionally says “oracle slice contract
mismatch.” This was a secondary reporting defect: the early execution-error
comparison omitted slice metadata. The runner now preserves that metadata while
keeping `ok=false`; a regression test verifies an engine error cannot pass or
acquire a misleading extra contract error. Original reports were not rewritten.

## SF10: correctness passes, leadership and regression gates remain open

The [SF10 report](benchmarks/2026-09-06-public-gates/tpch-sf10-latest-contracts-01/report.json)
records all 66 pairs correct and no completion/time-gate failures. Engine/DuckDB
suite ratio is **2.6859585369**, geometric mean **2.5696890424**. Summed query
medians are 12,319.737678 ms engine and 4,586.719232 ms DuckDB. Only one of 22
queries is faster; worst ratio is q16 at 5.2271×.

Q9's current engine/DuckDB ratio is 2.89475×. Its possible regression against
earlier candidates remains an open investigation; three samples in one session
are insufficient to settle it. Use same-machine paired A/B measurements with
unchanged data, SQL, binary provenance and resource settings before attributing
or accepting a regression. A suite improvement cannot excuse a failing query
or demonstrate broader JOB/ClickBench correctness.

## Validation and evidence retention

After the dialect and reporting changes, all **63 Python harness tests pass**,
including source membership, literal/comment-safe quoting, independent
duplicate/NULL alias equivalence, parsing all JOB queries, generic slice
correctness and failure classification. This is harness coverage; engine
aggregate/binder/scalar changes need their own focused regressions and complete
public workload reruns before closing the findings.

The [evidence directory](benchmarks/2026-09-06-public-gates/) contains readable
manifests/reports/samples/execution traces, worker logs and the captured harness
source snapshots. Three compressed output archives preserve all original run
files except temporary scratch workspace, including Arrow results and plans.
The retained `contracts-latest-source.tar.gz` contains the measured production
source snapshot: all **107 source/dependency files** listed in each of the three
manifests match the archive byte-for-byte by SHA256. The preserved
`benchmark_embedded` binary also matches all three manifests. These checks are
recorded in [source-verification.json](benchmarks/2026-09-06-public-gates/source-verification.json).
Together with the per-run harness snapshots, this identifies the measured
implementation despite the dirty checkout. [SHA256SUMS](benchmarks/2026-09-06-public-gates/SHA256SUMS)
covers the retained artifacts. Dialect preparation and 63-test evidence are in
[the public extract archive](benchmarks/2026-09-05-public-extracts/).

Next acceptance gate: rerun all 113 JOB queries against JOB04, all 43 ClickBench
queries and canonical SF10 after the systemic fixes. Keep wrong answers,
unsupported types, reference failures, timeouts and performance changes
separate. None of these archived development gates certifies native, Iceberg,
Lance, GPU, concurrent execution or memory/refusal safety.
