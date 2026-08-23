#!/usr/bin/env python3
"""Iceberg-table benchmark: engine (via `serve --tables`) vs DuckDB
`iceberg_scan`, over the SAME Iceberg warehouse — not plain parquet.

Companion to the plain-parquet SF=10 comparison already in CLAUDE.md; this
answers a different question (manifest/snapshot indirection overhead on
both sides), per this program's "report multiple premises separately"
convention (cache-on/off, native/like-for-like, and now iceberg/parquet).

`benchmark-parquet` does NOT auto-detect Iceberg directories (confirmed:
it hardcodes `<table>.parquet` lookups). The engine's Iceberg auto-detect
only lives in `serve --tables <dir>` (`is_iceberg_dir` / `register_iceberg`,
src/main.rs ~1238-1253), so this script drives that HTTP surface instead
of shelling out to a CLI benchmark subcommand.

Usage:
  .venv/bin/python scripts/iceberg_bench_compare.py \
      --iceberg-dir data/tpch-10gb-iceberg --sf 10 \
      --binary target/release/query_engine --iterations 2

Requires: `INSTALL iceberg; LOAD iceberg;` support (DuckDB 1.4+), the repo
.venv (duckdb, requests), and a built query_engine binary with the default
feature set (Iceberg reading needs no --features flag).
"""
import argparse
import glob
import os
import re
import socket
import subprocess
import sys
import time

import duckdb
import requests

TABLES = [
    "customer",
    "lineitem",
    "nation",
    "orders",
    "part",
    "partsupp",
    "region",
    "supplier",
]


def get_queries(sf: float) -> dict[int, str]:
    src = open("src/tpch/queries.rs").read()
    queries = {}
    for m in re.finditer(r'pub const Q(\d+): &str = r#"(.*?)"#;', src, re.S):
        queries[int(m.group(1))] = m.group(2).strip()
    assert len(queries) >= 22, len(queries)
    # Q11's HAVING threshold is the SF=1 constant 0.0001 in source; the
    # spec wants 0.0001/SF (same adjustment .scratch/validate22.py and
    # generate_expected_results.py make for the plain-parquet leg).
    queries[11] = queries[11].replace("0.0001", str(0.0001 / sf))
    return queries


def latest_metadata_json(iceberg_dir: str, table: str) -> str:
    files = sorted(
        glob.glob(os.path.join(iceberg_dir, table, "metadata", "*.metadata.json"))
    )
    if not files:
        raise FileNotFoundError(f"no metadata json for {table} under {iceberg_dir}")
    return files[-1]


def free_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def duckdb_side(iceberg_dir: str, queries: dict, iterations: int):
    con = duckdb.connect()
    con.execute("INSTALL iceberg; LOAD iceberg;")
    for t in TABLES:
        meta = latest_metadata_json(iceberg_dir, t)
        con.execute(f"CREATE VIEW {t} AS SELECT * FROM iceberg_scan('{meta}')")
    per_query_ms = {}
    per_query_rows = {}
    for q in range(1, 23):
        sql = queries[q]
        best = None
        rows = None
        for _ in range(iterations):
            t0 = time.time()
            res = con.execute(sql).fetchall()
            dt = (time.time() - t0) * 1000.0
            if best is None or dt < best:
                best = dt
                rows = len(res)
        per_query_ms[q] = best
        per_query_rows[q] = rows
    return per_query_ms, per_query_rows


def wait_ready(base_url: str, timeout_s: float = 60.0):
    deadline = time.time() + timeout_s
    last_err = None
    while time.time() < deadline:
        try:
            r = requests.get(f"{base_url}/readyz", timeout=2)
            if r.status_code == 200:
                return
        except requests.RequestException as e:
            last_err = e
        time.sleep(0.25)
    raise RuntimeError(f"server never became ready: {last_err}")


def engine_side(binary: str, iceberg_dir: str, queries: dict, iterations: int):
    port = free_port()
    base_url = f"http://127.0.0.1:{port}"
    env = dict(os.environ)
    proc = subprocess.Popen(
        [
            binary,
            "serve",
            "--bind",
            f"127.0.0.1:{port}",
            "--tables",
            iceberg_dir,
            "--flight-bind",
            "none",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env=env,
        text=True,
    )
    try:
        wait_ready(base_url)
        per_query_ms = {}
        per_query_rows = {}
        for q in range(1, 23):
            sql = queries[q]
            best = None
            rows = None
            for _ in range(iterations):
                t0 = time.time()
                r = requests.post(
                    f"{base_url}/sql",
                    params={"format": "csv", "distributed": "0"},
                    data=sql.encode("utf-8"),
                    timeout=120,
                )
                dt = (time.time() - t0) * 1000.0
                if r.status_code != 200:
                    raise RuntimeError(f"Q{q:02d} HTTP {r.status_code}: {r.text[:300]}")
                n = max(0, r.text.count("\n") - 1)
                if best is None or dt < best:
                    best = dt
                    rows = n
            per_query_ms[q] = best
            per_query_rows[q] = rows
        return per_query_ms, per_query_rows
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--iceberg-dir", default="data/tpch-10gb-iceberg")
    ap.add_argument("--sf", type=float, default=10.0)
    ap.add_argument("--binary", default="target/release/query_engine")
    ap.add_argument("--iterations", type=int, default=2)
    args = ap.parse_args()

    queries = get_queries(args.sf)

    print(f"=== Iceberg-table benchmark: {args.iceberg_dir} (SF={args.sf}) ===")
    print(f"engine binary: {args.binary}")
    print(f"iterations: {args.iterations} (best-of-N per query)\n")

    print("--- engine (serve --tables, Iceberg auto-detect) ---")
    eng_ms, eng_rows = engine_side(args.binary, args.iceberg_dir, queries, args.iterations)

    print("--- DuckDB (iceberg_scan) ---")
    duck_ms, duck_rows = duckdb_side(args.iceberg_dir, queries, args.iterations)

    print(f"\n{'Q':>4} {'engine ms':>12} {'duckdb ms':>12} {'ratio':>8} {'rows(e/d)':>14} {'match':>6}")
    total_e = 0.0
    total_d = 0.0
    mismatches = []
    for q in range(1, 23):
        e = eng_ms[q]
        d = duck_ms[q]
        total_e += e
        total_d += d
        ratio = e / d if d > 0 else float("nan")
        match = eng_rows[q] == duck_rows[q]
        if not match:
            mismatches.append(q)
        print(
            f"Q{q:02d} {e:12.2f} {d:12.2f} {ratio:8.2f} "
            f"{str(eng_rows[q]) + '/' + str(duck_rows[q]):>14} {'OK' if match else 'MISMATCH':>6}"
        )

    print(f"\nTOTAL {total_e:.2f}ms engine vs {total_d:.2f}ms duckdb-iceberg = {total_e/total_d:.2f}x")
    if mismatches:
        print(f"ROW COUNT MISMATCHES on: {mismatches}")
        sys.exit(1)
    else:
        print("Row counts match on all 22 queries (engine-iceberg vs duckdb-iceberg).")


if __name__ == "__main__":
    main()
