# Frozen aggregate-threshold SF10 provider screen — 2026-09-09

Frozen `1efb2554` completes 319 typed-correct engine outputs: 238 measured and
81 warmups. Only 237 of 264 requested measured pairs meet both correctness and
timing requirements. This is incomplete provider acceptance.

Run43384 is terminal1; supplemental typed audit57471 is terminal0. Each provider
uses one session, three measured pairs per query,16 threads, CPU affinity0–15,
4GiB query and12GiB process caps inside a48GiB scope. Query ceilings remain10×
fresh matched DuckDB1.4.4 calibration. Scope peak is20,556,812,288 bytes, with
zero max/OOM/kill events. All507 source hashes and the binary verify afterward.

| Provider | Valid measured pairs | Correct measured outputs | Correct warmups |
| --- | ---: | ---: | ---: |
| Raw Parquet | 60/66 | 60 | 21 |
| Native | 56/66 | 57 | 19 |
| Iceberg | 63/66 | 63 | 21 |
| Lance | 58/66 | 58 | 20 |

## Failures retained

- Raw Q12 has a correct but late warmup:1,177.087ms against1,097.525ms. Raw Q9
  warmup times out. Neither gets measured retries.
- Native Q1/Q18/Q17 warmups time out. Native Q11's third measured output is
  correct but late:179.170ms against162.617ms. Typed comparison alone would count
  57 completed pairs; the strict timing gate admits only56.
- Native Q18's ceiling is4,459.852ms. The separately labelled extended CPU
  diagnostic completes around5.7s, so removing unnecessary spill still does not
  clear this fresh ceiling.
- Iceberg Q9 DuckDB warmup refuses a262,144-byte allocation with
  OutOfMemoryException. No valid calibration or engine timing is invented.
- Lance Q1 warmup times out. Lance Q18 passes warmup and its first measured
  request (5,324.872ms against5,552.095ms), then the second request times out and
  the third is not run. Completion remains unstable at this boundary.
- Lance Q9 DuckDB calibration1 refuses a134,217,728-byte allocation after one
  earlier calibration succeeds. Dependent engine requests are not run.

There are five engine warmup timeouts, one measured timeout, one late correct
warmup, one late correct measured result, and two distinct reference refusals.
The scope records no OOM events; named DuckDB refusals remain separate outcomes.
No complete-suite leadership, resource/concurrency acceptance or GPU execution
is inferred. The [threshold repair](aggregate-spill-threshold-2026-09-09.md) and
[protected Q6 precision failure](short-query-null-control-2026-09-09.md) remain
separate evidence. Residency gates still belong to this frozen candidate.

The [immutable archive](benchmarks/2026-09-09-aggregate-threshold-providers/manifest.json)
preserves all completed outputs, failures, plans, provenance, the measured harness
and supplemental typed/timing audit. All1,400 archive files verify. Twenty-one empty temporary payload files are
listed separately; no nonempty spill payload is omitted.
