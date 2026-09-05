# SF=10 eight-way TPC-H benchmark, 2026-09-05: engine (Parquet/Native/Iceberg/Lance) vs DuckDB (Native/Parquet/Iceberg/Lance)

Measured 2026-09-05 on the final merged code of the three certification
follow-up epics (`main` @ `ec1839f`+; engine binaries built from that tree:
default features for Parquet/Native/Iceberg, `--features lance` for Lance),
same 32-vCPU / 125G machine as the 2026-08-28 six-way run, DuckDB 1.4.4,
pylance 0.23.2 (a scratch venv — the repo `.venv` no longer exists). Each
leg ran alone, in sequence; best-of-3 per query. The machine carried
interactive desktop load (load average 7-15) throughout, so absolute
numbers are a few percent pessimistic versus the August run.

## Per-query, best-of-3 ms

| Q | Engine/Parquet | Engine/Native | Engine/Iceberg | Engine/Lance | DuckDB/Native | DuckDB/Parquet | DuckDB/Iceberg | DuckDB/Lance |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 01 | 264.5 | 494.1 | 455.8 | 365.6 | 110.0 | 154.0 | 237.5 | 483.4 |
| 02 | 30.3 | 35.7 | 75.5 | 37.7 | 22.5 | 67.4 | 383.8 | 109.6 |
| 03 | 245.7 | 238.3 | 556.9 | 298.6 | 88.3 | 189.5 | 251.6 | 710.4 |
| 04 | 136.2 | 131.1 | 273.4 | 147.9 | 62.7 | 117.1 | 119.7 | 579.5 |
| 05 | 157.5 | 164.0 | 423.7 | 215.6 | 58.5 | 157.2 | 224.9 | 911.9 |
| 06 | 93.3 | 117.8 | 254.6 | 120.7 | 25.9 | 72.7 | 126.6 | 299.7 |
| 07 | 212.1 | 205.6 | 384.3 | 254.6 | 76.8 | 157.2 | 212.7 | 509.3 |
| 08 | 193.1 | 171.4 | 424.2 | 233.4 | 74.6 | 234.6 | 304.4 | 621.1 |
| 09 | 951.1 | 1,226.3 | 1,325.5 | 978.5 | 1,391.9 | 929.1 | 1,314.8 | 1,720.1 |
| 10 | 172.5 | 238.4 | 559.7 | 240.7 | 98.7 | 230.4 | 370.2 | 452.3 |
| 11 | 23.2 | 23.6 | 78.0 | 30.2 | 14.2 | 40.0 | 182.7 | 74.7 |
| 12 | 132.9 | 186.0 | 217.4 | 238.5 | 94.5 | 136.1 | 141.6 | 459.6 |
| 13 | 247.3 | 695.3 | 275.3 | 206.2 | 129.6 | 164.7 | 172.3 | 178.4 |
| 14 | 99.7 | 90.4 | 213.7 | 133.4 | 42.1 | 128.3 | 206.8 | 330.8 |
| 15 | 86.8 | 98.4 | 198.9 | 123.3 | 35.5 | 86.6 | 130.1 | 339.3 |
| 16 | 173.6 | 100.9 | 152.6 | 91.6 | 43.3 | 80.9 | 350.5 | 77.2 |
| 17 | 153.3 | 133.2 | 466.7 | 231.2 | 92.1 | 189.5 | 267.4 | 880.0 |
| 18 | 393.7 | 393.9 | 630.5 | 920.4 | 262.0 | 365.5 | 478.4 | 1,044.4 |
| 19 | 212.8 | 212.4 | 155.6 | 395.3 | 96.8 | 149.9 | 258.8 | 347.9 |
| 20 | 288.1 | 324.9 | 478.2 | 336.8 | 186.4 | 301.9 | 622.9 | 524.1 |
| 21 | 445.2 | 414.5 | 736.8 | 526.0 | 241.9 | 431.5 | 457.6 | 1,701.5 |
| 22 | 114.8 | 60.4 | 253.8 | 57.1 | 39.2 | 136.5 | 204.9 | 132.4 |
| **total** | **4.828s** | **5.757s** | **8.591s** | **6.183s** | **3.288s** | **4.521s** | **7.020s** | **12.488s** |

## Like-for-like and cross-format ratios

| comparison | ratio |
|---|---:|
| Engine/Parquet vs DuckDB/Parquet (same files) | 1.07x |
| Engine/Iceberg vs DuckDB/Iceberg (same warehouse, `iceberg_scan`) | 1.22x |
| Engine/Lance vs DuckDB/Lance (same datasets; DuckDB materialized once, 0.91s load excluded) | 0.50x — engine 2.0x faster |
| Engine/Native vs DuckDB/Native (each engine's own storage) | 1.75x |
| Engine/Native vs DuckDB/Parquet | 1.27x |
| Engine/Parquet vs DuckDB/Native | 1.47x |
| Engine/Native vs Engine/Parquet | 1.19x (Q13 695 vs 247ms is most of it; within ~10% excluding Q13) |

## Correctness (independent oracles, this session)

- Engine/Parquet: **22/22 cell-exact** vs DuckDB `read_parquet`
  (`.scratch/validate_parquet_leg.py`).
- Engine/Native: **22/22 cell-exact** vs DuckDB over the parquet source
  (`native_bench_compare.py`'s built-in `cell_compare`).
- Engine/Lance: **22/22 cell-exact** vs DuckDB over the same Lance datasets
  (`scripts/validate_lance.py`).
- Engine/Iceberg: `scripts/iceberg_bench_compare.py` compares ROW COUNTS
  only (22/22 match); not cell-checked in this run.

## Method (same tooling as 2026-08-28)

- Engine/Parquet: `benchmark-parquet --path data/tpch-10gb --iterations 3
  --sf 10 --save-csv`, `QE_IPC_CACHE` unset (Auto; the committed `.qeipc`
  sidecars were fresh).
- Engine/Native + DuckDB/Parquet + DuckDB/Iceberg (view): one
  `native_bench_compare.py --iceberg-dir data/tpch-10gb-iceberg --iterations 3
  --memory-limit 40G` run (`serve --tables data/tpch-10gb-native` over HTTP).
- Engine/Iceberg + DuckDB/Iceberg: `iceberg_bench_compare.py --iterations 3`
  (`serve --tables data/tpch-10gb-iceberg`; the table above uses this
  script's DuckDB/Iceberg column, 7.02s; `native_bench_compare`'s own
  DuckDB/Iceberg leg measured 7.06s).
- Engine/Lance: `benchmark-lance --path data/tpch-10gb-lance --iterations 3
  --sf 10 --save-csv` with the `--features lance` binary.
- DuckDB/Native: `duckdb_rebaseline.py --data data/tpch-10gb --sf 10`
  (real `TABLE`s, 16 threads, 64GB).
- DuckDB/Lance: `duckdb_lance_bench.py --data data/tpch-10gb-lance
  --iterations 3 --sf 10` (materialized mode).
- Every run under `systemd-run --scope -p MemoryMax=…` with `QE_MEM_CAP`
  set. Raw logs: `.scratch/sixway2/` (gitignored).

## Versus 2026-08-28 (same machine, pre-estimate-fix engine)

| leg | 2026-08-28 | 2026-09-05 |
|---|---:|---:|
| Engine/Parquet | 4.857s | 4.828s |
| Engine/Native | 8.200s (Q12 3.14s, Q13 0.67s) | 5.757s (Q12 0.19s) |
| Engine/Lance | 6.028s | 6.183s |
| Engine/Iceberg | — (not measured) | 8.591s |
| DuckDB/Native | 3.665s | 3.288s |
| DuckDB/Parquet | 4.241s | 4.521s |
| DuckDB/Iceberg | (view, via native_bench_compare) | 7.020s |
| DuckDB/Lance | 10.659s | 12.488s |
