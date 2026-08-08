#!/usr/bin/env python3
"""
Re-measure DuckDB reference times for the 22 spec-compliant TPC-H queries.

Uses the exact DuckDB-dialect SQL from generate_expected_results.py (the same
queries the engine runs, modulo dialect), so the safe_benchmark.sh 10x rule is
judged against a valid baseline. Methodology matches the original
duckdb_benchmark_sf10.py: parquet loaded into native DuckDB tables, 16 threads,
64GB memory limit, best of 3 runs.

Outputs a markdown table (for CLAUDE.md) and a bash associative-array block
(for scripts/safe_benchmark.sh).

Usage:
    python3 scripts/duckdb_rebaseline.py [--data ./data/tpch-10gb] [--sf 10]
"""

import argparse
import os
import sys
import time

import duckdb

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from generate_expected_results import get_queries  # noqa: E402

TABLES = [
    "lineitem",
    "orders",
    "customer",
    "supplier",
    "part",
    "partsupp",
    "nation",
    "region",
]

ITERATIONS = 3
THREADS = 16
MEMORY_LIMIT = "64GB"


def tpch_queries(sf: float):
    """Extract the 22 tpch/* queries in DuckDB dialect, with Q11's
    SF-dependent HAVING threshold substituted (0.0001/SF per spec)."""
    out = {}
    for entry in get_queries():
        name, sql = entry[0], entry[1]
        if not name.startswith("tpch/q"):
            continue
        qnum = int(name.split("/q")[1])
        if qnum == 11:
            factor = 0.0001 / sf
            sql = sql.replace("* 0.0001", f"* {factor:.12f}".rstrip("0"))
        out[qnum] = sql
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default="./data/tpch-10gb")
    ap.add_argument("--sf", type=float, default=10.0)
    args = ap.parse_args()

    queries = tpch_queries(args.sf)
    assert len(queries) == 22, f"expected 22 queries, got {len(queries)}"

    con = duckdb.connect()
    con.execute(f"SET threads TO {THREADS}")
    con.execute(f"SET memory_limit = '{MEMORY_LIMIT}'")

    print(f"Data: {args.data} | SF={args.sf} | threads={THREADS} | mem={MEMORY_LIMIT}")
    print("Loading tables...")
    for table in TABLES:
        path = f"{args.data}/{table}.parquet"
        con.execute(f"CREATE TABLE {table} AS SELECT * FROM read_parquet('{path}')")
        count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"  {table}: {count:,} rows")

    results = {}
    print(f"\n{'Query':<7}{'best ms':>10}{'rows':>10}")
    for qnum in sorted(queries):
        times = []
        rows = 0
        for i in range(ITERATIONS):
            t0 = time.perf_counter()
            res = con.execute(queries[qnum]).fetchall()
            times.append((time.perf_counter() - t0) * 1000)
            if i == 0:
                rows = len(res)
        best = min(times)
        results[qnum] = (best, rows)
        print(f"Q{qnum:<6}{best:>10.1f}{rows:>10}")

    total = sum(b for b, _ in results.values())
    print(f"\nTotal (best): {total:.0f}ms")

    print("\n--- bash block for scripts/safe_benchmark.sh ---")
    ms = {q: max(1, round(b)) for q, (b, _) in results.items()}
    print("declare -A DUCKDB_MS=(")
    for row_start in range(1, 23, 6):
        cells = "".join(
            f"[{q}]={ms[q]:<5}" for q in range(row_start, min(row_start + 6, 23))
        )
        print(f"    {cells.rstrip()}")
    print(")")

    print("\n--- markdown table for CLAUDE.md ---")
    print("| Query | DuckDB Time (SF=10) | Timeout (10x) |")
    print("|-------|---------------------|---------------|")
    for q in sorted(ms):
        t = ms[q] * 10
        timeout = f"{t / 1000:.1f}s" if t >= 1000 else f"{t}ms"
        print(f"| Q{q:02d} | {ms[q]}ms | {timeout} |")


if __name__ == "__main__":
    main()
