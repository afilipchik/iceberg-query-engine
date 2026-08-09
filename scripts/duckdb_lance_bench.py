#!/usr/bin/env python3
"""
DuckDB-over-Lance reference: run the engine's own TPC-H SQL against Lance
datasets through DuckDB's Arrow integration (the "DuckDB Lance connector").

This serves two purposes:
  1. ORACLE  -- with --save-csv it writes per-query results the engine's Lance
     results are compared against, cell by cell.
  2. BASELINE -- it times DuckDB reading the SAME Lance datasets, which is the
     honest comparison for the engine's Lance reader (the native-table DuckDB
     number prices in DuckDB's own storage format and is not like-for-like).

Queries are extracted from src/tpch/queries.rs so both sides always run
identical SQL.

Usage:
    .venv/bin/python scripts/duckdb_lance_bench.py --data ./data/tpch-1mb-lance
    .venv/bin/python scripts/duckdb_lance_bench.py --data ./data/tpch-10gb-lance \
        --iterations 3 --save-csv .scratch/duck_lance_csv
"""

import argparse
import os
import re
import sys
import time

import duckdb
import lance

TABLES = [
    "nation",
    "region",
    "part",
    "supplier",
    "partsupp",
    "customer",
    "orders",
    "lineitem",
]

QUERIES_RS = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "src",
    "tpch",
    "queries.rs",
)


def load_queries() -> dict:
    """Extract `pub const QN: &str = r#"..."#;` blocks from queries.rs."""
    src = open(QUERIES_RS).read()
    out = {}
    for m in re.finditer(r'pub const Q(\d+): &str = r#"(.*?)"#;', src, re.S):
        out[int(m.group(1))] = m.group(2).strip()
    return out


def register(con, data_dir: str, mode: str) -> dict:
    """Make each Lance dataset queryable from DuckDB.

    mode="materialized" (default): read the Lance dataset into an Arrow table
    and register that. Reliable, and the Lance read cost is paid up front
    rather than inside the timed query.

    mode="direct": register the `lance.LanceDataset` object itself, so DuckDB
    scans Lance lazily through its Arrow replacement scan.

    WARNING on mode="direct" (measured 2026-08-08, duckdb 1.4.4 + pylance
    0.23.2): DuckDB's ARROW_SCAN over a LanceDataset reports **~0 rows** as its
    cardinality estimate (visible in EXPLAIN). Single-table filters and 2-table
    joins are fine, but a 3-table join plus filters — TPC-H Q03 is the minimal
    repro — degenerates into a plan that never finishes: 22.2s of user CPU in
    20s wall at 113% CPU on a 6,000-row dataset, i.e. a genuine spin, not a
    deadlock. It is independent of `threads` and unaffected by disabling
    filter_pushdown or statistics_propagation. Each re-scan also crosses back
    into Python. Use "direct" only for single-table or 2-table probes.
    """
    handles = {}
    for t in TABLES:
        path = os.path.join(data_dir, f"{t}.lance")
        if not os.path.exists(path):
            raise SystemExit(f"missing Lance dataset: {path}")
        ds = lance.dataset(path)
        if mode == "direct":
            handles[t] = ds
            con.register(t, ds)
        else:
            tbl = ds.to_table()
            handles[t] = tbl  # keep alive: DuckDB holds a borrowed reference
            con.register(f"__lance_{t}", tbl)
            con.execute(f"CREATE OR REPLACE VIEW {t} AS SELECT * FROM __lance_{t}")
    return handles


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default="./data/tpch-1mb-lance")
    ap.add_argument("--iterations", type=int, default=3)
    ap.add_argument("--threads", type=int, default=16)
    ap.add_argument("--queries", default="", help="comma list, default all 22")
    ap.add_argument(
        "--sf",
        type=float,
        default=10.0,
        help="scale factor; Q11's HAVING threshold is spec'd for SF=1 and must "
        "be divided by SF or DuckDB returns 0 rows while the engine returns 100 "
        "-- an apples-to-oranges timing row if left unadjusted",
    )
    ap.add_argument("--save-csv", default="", help="write per-query CSV results here")
    ap.add_argument(
        "--mode",
        choices=["materialized", "direct"],
        default="materialized",
        help="how DuckDB reads Lance; see register() -- 'direct' hangs on "
        "3-table joins with duckdb 1.4.4 + pylance 0.23.2",
    )
    args = ap.parse_args()

    queries = load_queries()
    if args.sf != 1.0 and 11 in queries:
        queries[11] = queries[11].replace("0.0001", str(0.0001 / args.sf))
    want = (
        [int(x) for x in args.queries.split(",") if x.strip()]
        if args.queries
        else sorted(queries)
    )

    con = duckdb.connect()
    con.execute(f"SET threads={args.threads}")
    con.execute("SET memory_limit='64GB'")
    t_load = time.perf_counter()
    _handles = register(con, args.data, args.mode)
    load_s = time.perf_counter() - t_load

    if args.save_csv:
        os.makedirs(args.save_csv, exist_ok=True)

    print(f"DuckDB {duckdb.__version__} over Lance {lance.__version__}: {args.data}")
    print(f"mode={args.mode}  load={load_s:.2f}s (Lance read, outside query timings)")
    print(f"{'Query':<7}{'Best(ms)':>10}{'Rows':>10}")
    total = 0.0
    for qn in want:
        sql = queries.get(qn)
        if not sql:
            print(f"Q{qn:02d}     MISSING")
            continue
        best = None
        rows = 0
        for _ in range(args.iterations):
            t0 = time.perf_counter()
            res = con.execute(sql).fetchall()
            dt = (time.perf_counter() - t0) * 1000
            rows = len(res)
            best = dt if best is None else min(best, dt)
        total += best
        print(f"Q{qn:02d}   {best:>10.1f}{rows:>10}")

        if args.save_csv:
            # Match the engine's --save-csv layout so validation can diff them.
            out = os.path.join(args.save_csv, f"q{qn:02d}.csv")
            con.execute(sql).df().to_csv(out, index=False)

    print(f"\nTotal (best of {args.iterations}): {total / 1000:.2f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
