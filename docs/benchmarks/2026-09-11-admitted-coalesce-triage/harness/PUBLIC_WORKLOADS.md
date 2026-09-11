# Deterministic public development extracts

`python -m benchmark.prepare_public` prepares real-data development extracts.
It is separate from the active runner and does not change its evidence contract.
It never generates synthetic rows, drops failed SQL cases, or treats extraction
as query-engine correctness certification.

## Pinned sources

- JOB SQL/schema: revision `a39603662e023e449cb2121997a5034df9e02ebf`,
  [upstream distribution](https://github.com/gregrahn/join-order-benchmark/tree/a39603662e023e449cb2121997a5034df9e02ebf).
  All 113 queries, schema and README are vendored with SHA256 hashes in
  `public_sources/manifest.json`. This distribution is not maintained by the
  paper's original authors; its README identifies the original May 2013 data.
- JOB data: [original CWI archive](https://event.cwi.nl/da/job/imdb.tgz),
  1,263,193,115 bytes, SHA256
  `25f9d893c54f903366e0c263f88db0d429dbc2b159d4987ebc1e203242a7e988`.
  The hash was established by a complete contained download on 2026-09-05.
- ClickBench SQL/schema/license: revision
  `ccaf12175a6a70eba5e7e23c603ab3bc8c9ffe9a`,
  [upstream](https://github.com/ClickHouse/ClickBench/tree/ccaf12175a6a70eba5e7e23c603ab3bc8c9ffe9a).
  All 43 DuckDB queries, its DDL, README and LICENSE are vendored and hashed.
- ClickBench data: the [official ClickHouse S3 object](https://clickhouse-public-datasets.s3.amazonaws.com/hits_compatible/hits.parquet),
  also referenced by the [ClickHouse author](https://github.com/ClickHouse/ClickHouse/issues/46703).
  Object size 14,779,976,446 bytes; ETag
  `6b028bb94eecf0ff4e6cde62a0f8fa48-829`. The ordinary CDN and its documented
  Athena alternatives returned HTTP 403 during this session; the S3 object
  supports bounded range access.

Repository/source licenses do not relicense the datasets. JOB's pinned
repository contains no LICENSE file; its README links the IMDb data terms.
The preparation manifest records these distinctions rather than inventing a
license grant. Extracted data remains in repository scratch.

## Selection and data validation

JOB defaults to `movie_id % 1000 = 0`, with `title.id` providing the movie ID.
Movie facts and alternate titles retain those movies. Both endpoints of a
movie link must be selected. Name, character, company and keyword dimensions
retain referenced IDs; person information and alternate names retain selected
people; the six small enumerated dimensions remain complete. Episode-parent
closure is not added. This is a deliberately reduced relational workload,
not a representative full-JOB performance score.

The archive is processed in two streaming passes. Each CSV member is spooled
individually inside repository scratch, then read by the pinned DuckDB 1.4.4
strict CSV parser with a 512 MB budget and one thread. Source quotes use
backslash escaping **inside** quoted fields, while backslashes in unquoted
names remain literal. NULL and quoted empty strings remain distinct. Parser
errors fail the entire preparation; no invalid rows are skipped.

ClickBench defaults to the first 1,000,000 physical source rows. HTTP requests
require exact 206/Content-Range responses and the pinned strong ETag. The reader
caps cumulative received bytes and records hashes of every downloaded range.
A server ignoring Range is rejected before reading its body. The entire
14.8 GB source is not needed. A pinned local source file is also supported.

ClickBench's upstream Parquet has physical types requiring normalization to
its published DuckDB DDL. Date stores epoch days and DateTime stores epoch
seconds; normalization uses checked conversions, including seconds-to-microseconds
scaling. These units follow [Date](https://clickhouse.com/docs/reference/data-types/date)
and [DateTime](https://clickhouse.com/docs/reference/data-types/datetime) semantics.
Failed casts, invalid UTF-8, overflow and NULLs in required columns are errors.

Every output schema and every selected typed source row is checked against a
complete Parquet readback, using an order-preserving SHA256 encoding that
distinguishes NULL, empty strings and field boundaries. Output file hashes,
row counts, source pins, schema serialization and SQL hashes are preserved.

## Commands

Run one preparation job at a time and coordinate with benchmark measurements:

```bash
export TMPDIR="$PWD/.scratch"
export PYTHONPATH="$PWD/scripts"

SAFE_BUILD_MEM=4G scripts/claude-safe-build.sh .scratch/venv-lance/bin/python \
  -m benchmark.prepare_public --workload job \
  --source-file .scratch/public-bench/sources/imdb.tgz \
  --source-sha256 25f9d893c54f903366e0c263f88db0d429dbc2b159d4987ebc1e203242a7e988 \
  --output .scratch/public-bench/job-dev-new

SAFE_BUILD_MEM=4G scripts/claude-safe-build.sh .scratch/venv-lance/bin/python \
  -m benchmark.prepare_public --workload clickbench --rows 1000000 \
  --max-download-bytes 1073741824 \
  --output .scratch/public-bench/clickbench-dev-new
```

Omitting JOB `--source-file` downloads the original archive subject to the
byte cap and pinned source hash. New output directories are mandatory. Failed
attempts remain failed; choose another output directory after fixing a cause.

## Validation status and result contracts

The current ClickBench dialect refresh is
`.scratch/public-bench/clickbench-dev-04/dataset.json`, over the same verified
million-row extract. A generic lexical adapter adds empty fourth options to
every three-argument unquoted builtin `REGEXP_REPLACE` call. It preserves nested
expressions, strings/comments and existing four-argument calls. In the pinned
43-query source this affects q29 only; there are no query-ID conditions in the
translation. Original SQL and hashes remain archived, and the same translated
expression runs in timed engine SQL, timed DuckDB SQL and the complete oracle.

This explicitly selects [DuckDB's first-match and backslash-capture behavior](https://duckdb.org/docs/current/sql/functions/regular_expressions),
which differs from [Trino's global replacement and dollar-capture behavior](https://trino.io/docs/current/functions/regexp.html).
The engine's existing three-argument behavior remains separate. Pinned DuckDB
1.4.4 tests compare original and explicit-options results over repeated matches,
captures, literal dollar replacements, empty strings and NULLs; all 43 adapted
statements parse. This translation does not resolve or excuse q29's measured
timeout; engine execution and the unchanged time ceiling must be checked anew.

JOB preparation completed at `.scratch/public-bench/job-dev-03/dataset.json`:
21 tables, 346,929 total rows, 20,016,447 Parquet bytes, all 113 SQL queries.
Source-selection and Parquet readback checks passed. Benchmark query execution
is a separate gate.

The first JOB execution exposed a reference dialect mismatch in 15a–15d:
PostgreSQL permits the unquoted table alias `at`, while DuckDB 1.4.4 reserves
`AT`. The preparation adapter now quotes that alias's declaration and qualified
references, preserving the identifier's PostgreSQL lowercase meaning. It skips
strings, quoted identifiers and comments, and rejects unfamiliar identifier
contexts. Original SQL bytes and hashes remain preserved; translated SQL and
its hash apply identically to the engine, DuckDB and complete oracle. This is
a dialect translation, not a change to filtering, joins or query membership.
The original failed run remains evidence. Other reported JOB engine failures
remain independent open correctness findings until rerun and validated.
The refreshed `.scratch/public-bench/job-dev-04/dataset.json` is prepared over
the same hash-verified 21 tables and preserves all 113 queries. Eight focused
public-workload tests pass, including parsing every translated JOB statement
with pinned DuckDB and comparing a duplicate/NULL alias fixture against an
independently renamed reference. Actual engine reruns remain separate.

ClickBench extraction completed at `.scratch/public-bench/clickbench-dev-02/dataset.json`:
1,000,000 real rows, 93,139,773 Parquet bytes; 138,767,513 bytes fetched in seven
pinned HTTP ranges. Source-selection and exact Parquet readback checks passed.

The original `clickbench-dev-02` manifest remains `prepared_incomplete` as
historical evidence. Its eight comparator gaps are now implemented generically:
q18 unordered LIMIT; q25/q27 hidden ORDER BY fields; q39–q43 OFFSET. The refreshed
`.scratch/public-bench/clickbench-dev-03/dataset.json` is `prepared`, preserves all
43 original timed SQL statements, and reuses hash-verified data. This closes
harness eligibility gaps; engine correctness and performance require execution.

The untimed oracle removes LIMIT/OFFSET and preserves complete ordered results.
For hidden ordering, only the oracle projects the missing EventTime column;
explicit ordinal mappings select the original result columns. Equal ordering
keys form groups. The requested rank window fixes how many rows each group must
contribute. Each corresponding actual segment must match a typed submultiset
of that complete group, preserving duplicate capacity. This permits every legal
tie order while rejecting wrong ordering groups, omitted required rows and
invented duplicates. Unordered LIMIT uses one group over the complete oracle.
Float comparisons retain explicit tolerance and the existing bounded matching
bucket; exceeding that limit is a validation error, not a pass.

The complete oracle and disk-backed comparison are untimed validation work.
They can cost substantially more than a small LIMIT query. The recorded report
checks the slice bounds, output mapping and oracle ordering against the query
manifest. Timed SQL never gains validation-only columns.

To refresh an existing validated extract without another download:

```bash
TMPDIR="$PWD/.scratch" PYTHONPATH="$PWD/scripts" SAFE_BUILD_MEM=4G \
  scripts/claude-safe-build.sh .scratch/venv-lance/bin/python \
  -m benchmark.prepare_public --workload clickbench \
  --reuse-data-manifest .scratch/public-bench/clickbench-dev-02/dataset.json \
  --output .scratch/public-bench/clickbench-dev-new
```

All 60 contained harness tests pass, including 24 comparator tests. An
independent exhaustive test enumerates legal tied permutations, slices and
projects them, then checks acceptance for every candidate across multiple
offsets and limits. Six parser/provenance tests also pass. Failed JOB parser attempts
and the initial unsupported ClickBench physical Date cast are retained in
`.scratch/public-bench/*-dev-0*/` with explicit failed manifests.
