# Iceberg benchmark registration and query boundary — 2026-09-06

The completed canonical SF10 Iceberg run is a valid measurement of the recorded
warm, persistent-provider APIs: 660 validated pairs, no issues, suite ratio
**0.348762886× DuckDB**, geometric mean **0.337836808×**, and 21 of 22 query wins.
It is not an execution-only comparison or a cold Iceberg open-to-result test.
There is a real registration-boundary distinction, but the saved EXPLAIN timings
do **not** support attributing the large suite advantage to metadata binding.

This is a read-only audit of
[`canonical-sf10-provider-gates-02/iceberg`](../.scratch/public-bench/canonical-sf10-provider-gates-02/iceberg/).
No query, build, test, extension install, or benchmark configuration change was
performed. The [provider evidence](benchmarks/2026-09-06-canonical-provider-sf10/README.md)
records the surrounding conversion and timing work; other modes remain separate.

## What is actually timed

The [manifest](../.scratch/public-bench/canonical-sf10-provider-gates-02/iceberg/manifest.json)
declares `embedded_parse_to_arrow_consumed`, `warm_host`, DuckDB 1.4.4, and engine
binary SHA-256 `5224c0c602f34de66c857b5d6bb2ff8dcb754fff4c34f038857512929ca5ef3b`.
Both persistent workers use 16 threads, a 40 GiB query/engine memory setting and
48 GiB process cap. Registration is outside each query timer on both sides.
The engine times `context.sql`, which collects Arrow batches; the reference times
`con.execute(sql).fetch_arrow_table()`. IPC result-file serialization is outside
both timers. This includes parsing/planning and materializing results on each
side, rather than isolating scan/operator execution.

The [setup](../.scratch/public-bench/canonical-sf10-provider-gates-02/iceberg/setup.json)
pins eight table snapshots. The engine receives the table directory and explicit
snapshot ID; DuckDB receives that snapshot's immutable metadata-file path.
Neither reader is intentionally pointed at a different snapshot. The conversion
gate separately checks complete typed table equivalence; row counts or metadata
hashes alone are not its correctness proof.

| Boundary | Engine | DuckDB reference |
|---|---|---|
| Worker setup | `register_iceberg` opens metadata, chooses the pinned snapshot, reads the manifest list/manifests and registers a `ParquetTable` over those data files | Loads the pinned Avro/Iceberg extensions and creates a view containing `iceberg_scan(explicit_metadata_path)` |
| Retained object | Ordinary Parquet provider with the resolved data-file list | SQL view over the Iceberg table function, in a persistent connection |
| Query request | Plans and executes against the retained provider; it does not reopen Iceberg metadata through `register_iceberg` | Executes SQL referencing that view; any binding/planning and execution performed by DuckDB/its extension lies inside the timer |

These are source-established API differences. They do **not** establish that
DuckDB physically rereads every metadata or manifest file on every query, or that
its extension has no cache. `CREATE VIEW` can itself perform binding work; it is
not evidence that all metadata work is deferred. The saved artifacts do not
trace extension cache hits or filesystem reads, and this audit did not inspect
the pinned extension's implementation. Repeated physical metadata I/O and its
cost therefore remain unverified.

Source evidence:

- [`context.rs`](../src/execution/context.rs): `register_iceberg` retains
  `Arc::new(opened.table)`, not a live Iceberg metadata resolver.
- [`storage/iceberg.rs`](../src/storage/iceberg.rs): `open_table` calls
  `data_files_of` before `ParquetTable::try_from_files`.
- Captured [`providers.py`](../.scratch/public-bench/canonical-sf10-provider-gates-02/iceberg/provenance/providers.py),
  [`duckdb_worker.py`](../.scratch/public-bench/canonical-sf10-provider-gates-02/iceberg/provenance/duckdb_worker.py)
  and [`benchmark_embedded.rs`](../.scratch/public-bench/canonical-sf10-provider-gates-02/iceberg/provenance/benchmark_embedded.rs)
  establish the measured adapters and timers. Their SHA-256 hashes match the
  inspected working copies. Both production source files above also match their
  entries in this run's `engine_source_sha256` manifest.
- [`workers.json`](../.scratch/public-bench/canonical-sf10-provider-gates-02/iceberg/workers.json)
  records Iceberg extension version `1095c1fa`, file SHA-256
  `4c3897422d44060012a1df00342a5be587dc42baf7b26e51346a8bfbc95aab6c`.

## What the saved timings say

Worker setup took 3.36–3.44 ms on the engine and 100.61–112.83 ms on DuckDB.
These totals include different initialization work, notably extension loading,
and are not metadata-only measurements. They are excluded from reported query
ratios, equally by the declared boundary.

The runner saves one ordinary `EXPLAIN` request per query/session, before the
complete oracle, reference warmup, three calibration executions, engine warmup
and ten measured pairs. All 66 EXPLAIN requests completed in **2.23–13.15 ms**.
They include planning and plan-result production, not normal query execution.

| Session | Sum of 22 DuckDB EXPLAIN requests, ms | DuckDB sum of measured query medians, ms | Engine sum of measured query medians, ms |
|---|---:|---:|---:|
| 1 | 115.119 | 33,565.299 | 11,690.018 |
| 2 | 122.759 | 33,657.342 | 11,779.639 |
| 3 | 130.736 | 33,614.747 | 11,703.586 |

The EXPLAIN sum is about 0.34–0.39% of the corresponding reference suite time,
whereas the measured suite difference is about 21.9 seconds. Selected query
details reinforce that scale:

| Query | Median of 3 EXPLAIN requests, ms | Median of 30 DuckDB measurements, ms | Median of 30 engine measurements, ms |
|---|---:|---:|---:|
| Q1 | 2.756 | 1,800.321 | 898.229 |
| Q2 | 12.307 | 326.535 | 56.802 |
| Q8 | 9.631 | 2,384.672 | 439.303 |
| Q16 | 3.720 | 282.787 | 378.406 |
| Q21 | 6.698 | 4,417.101 | 881.772 |

These table medians summarize the raw samples; the report's suite/geometric
aggregation is authoritative. Evidence is in
[`execution.jsonl`](../.scratch/public-bench/canonical-sf10-provider-gates-02/iceberg/execution.jsonl)
(request IDs `sN-qXX-duckdb-plan`),
[`samples.jsonl`](../.scratch/public-bench/canonical-sf10-provider-gates-02/iceberg/samples.jsonl)
and [`report.json`](../.scratch/public-bench/canonical-sf10-provider-gates-02/iceberg/report.json).

EXPLAIN time is **not** a subtractable metadata tax or a rigorous upper bound on
execution-time metadata work: EXPLAIN can omit work deferred until execution,
and the measured queries occur after additional warming. Nevertheless, the
observed planning scale argues against an unsupported claim that ordinary view
rebinding alone explains this result. Scan behavior, encoding, predicate
pushdown, cardinality estimates, join plans and extension execution remain
possible contributors requiring separate attribution.

## Follow-up that preserves the result

Keep this run and its direct Iceberg reference unchanged. Describe the finding
as a warm persistent-provider Iceberg win on these pinned snapshots and this
extension version, with eager engine snapshot resolution. Do not relabel it
execution-only, general Iceberg support/performance certification, or
cross-provider DuckDB leadership.

After frozen gates finish, predeclare a small diagnostic matrix:

1. Profile repeated ordinary queries and EXPLAIN on the pinned extension, and
   trace metadata/manifest opens separately from data-file reads. Verify whether
   work occurs at view creation, binding, scan initialization or subsequent
   execution before choosing any cache-control experiment.
2. Measure a separate fresh-worker open-to-consumed-result boundary, including
   registration on both sides, with explicit host/metadata cache policy. Preserve
   snapshot, SQL, data, limits and correctness checks. This answers cold/startup
   use cases without rewriting the existing warm score.
3. If execution attribution is needed, add a separate matched resolved-file
   Parquet experiment using the exact manifest-listed files on both sides.
   Label it a resolved-file control, not direct Iceberg. Any prepared-query or
   metadata-cache experiment must record its different boundary and equivalent
   setup on both sides; do not assume preparing SQL eliminates extension work.

No adapter change or reference optimizer override is justified by the current
metadata hypothesis alone.
