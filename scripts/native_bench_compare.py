#!/usr/bin/env python3
"""Native-table benchmark + cell-exact validation (task 008 of the
native-tables-foundation epic).

Two modes:

1. `--write`: convert each TPC-H table under --source-dir (plain Parquet,
   one `<table>.parquet` per table, the `benchmark-parquet` layout) into a
   native table under --native-dir via `write-native --from-parquet`
   (task 003's write path). One native table directory per TPC-H table,
   so `serve --tables <native-dir>` auto-registers all eight
   (`is_native_table_dir` detection, task 004).

2. default (no `--write`): benchmark + validate. Starts
   `serve --tables <native-dir>` (mirrors `scripts/iceberg_bench_compare.py`,
   this session's directly-comparable precedent for driving the HTTP
   surface rather than a CLI benchmark subcommand, since there is no
   `benchmark-native`), runs the 22 TPC-H queries over HTTP, and compares:
     - timing vs DuckDB reading the SAME plain-parquet source
       (`read_parquet` views over --source-dir)
     - timing vs DuckDB reading the engine's OWN Iceberg tables
       (`iceberg_scan`, if --iceberg-dir is given) -- the standing
       "DuckDB reading plain parquet AND DuckDB reading Iceberg" convention
       from `duckdb-parity-2`.
     - CELL-EXACT correctness: every value of every engine-native result,
       column-by-column, row-by-row, against the DuckDB-over-parquet
       oracle (an INDEPENDENT engine, not the engine checking itself).

Usage:
  # one-time conversion (repeat per scale factor / data dir)
  .venv/bin/python scripts/native_bench_compare.py --write \
      --source-dir data/tpch-10gb --native-dir data/tpch-10gb-native \
      --binary target/release/query_engine

  # benchmark + cell-exact validate
  .venv/bin/python scripts/native_bench_compare.py \
      --native-dir data/tpch-10gb-native --source-dir data/tpch-10gb \
      --iceberg-dir data/tpch-10gb-iceberg --sf 10 \
      --binary target/release/query_engine --memory-limit 40G --iterations 2

  # GPU vs CPU split: pass a --features gpu binary, toggle QE_GPU via --env
  .venv/bin/python scripts/native_bench_compare.py \
      --native-dir data/tpch-10gb-native --source-dir data/tpch-10gb --sf 10 \
      --binary target/release/query_engine_gpu --memory-limit 40G \
      --iterations 3 --env QE_GPU=0 --no-duckdb --no-iceberg
"""
import argparse
import csv
import datetime
import glob
import io
import math
import os
import re
import socket
import subprocess
import sys
import time

TABLES = [
    "nation",
    "region",
    "customer",
    "orders",
    "partsupp",
    "supplier",
    "lineitem",
    "part",
]


def get_queries(sf: float) -> dict[int, str]:
    src = open("src/tpch/queries.rs").read()
    queries = {}
    for m in re.finditer(r'pub const Q(\d+): &str = r#"(.*?)"#;', src, re.S):
        queries[int(m.group(1))] = m.group(2).strip()
    assert len(queries) >= 22, len(queries)
    # Q11's HAVING threshold is the SF=1 constant 0.0001 in source; the
    # spec wants 0.0001/SF (same adjustment every other validation script
    # in this repo makes).
    if sf != 1.0:
        queries[11] = queries[11].replace("0.0001", str(0.0001 / sf))
    return queries


def write_native_tables(binary: str, source_dir: str, native_dir: str, mode: str) -> dict:
    os.makedirs(native_dir, exist_ok=True)
    timings = {}
    for t in TABLES:
        src = os.path.join(source_dir, f"{t}.parquet")
        if not os.path.exists(src):
            raise FileNotFoundError(src)
        out = os.path.join(native_dir, t)
        t0 = time.time()
        r = subprocess.run(
            [binary, "write-native", "--from-parquet", src, "--out", out, "--mode", mode],
            capture_output=True,
            text=True,
        )
        dt = time.time() - t0
        if r.returncode != 0:
            raise RuntimeError(f"write-native {t} failed (exit {r.returncode}):\n{r.stdout}\n{r.stderr}")
        timings[t] = dt
        # write-native prints rows/segments/table_id/version to stdout.
        summary = r.stdout.strip().splitlines()[-1] if r.stdout.strip() else ""
        print(f"  {t}: {dt:.2f}s  {summary}")
    return timings


def free_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def wait_ready(base_url: str, timeout_s: float = 180.0):
    import requests

    deadline = time.time() + timeout_s
    last_err = None
    while time.time() < deadline:
        try:
            r = requests.get(f"{base_url}/readyz", timeout=3)
            if r.status_code == 200:
                return
            last_err = f"HTTP {r.status_code}: {r.text[:300]}"
        except Exception as e:  # noqa: BLE001
            last_err = e
        time.sleep(0.5)
    raise RuntimeError(f"server never became ready: {last_err}")


def engine_native_side(
    binary: str,
    native_dir: str,
    queries: dict,
    iterations: int,
    memory_limit: str | None,
    env_overrides: dict | None,
    query_timeout_s: float,
    only_queries=None,
):
    import requests

    port = free_port()
    base_url = f"http://127.0.0.1:{port}"
    env = dict(os.environ)
    if env_overrides:
        env.update(env_overrides)
    cmd = [
        binary,
        "serve",
        "--bind",
        f"127.0.0.1:{port}",
        "--tables",
        native_dir,
        "--flight-bind",
        "none",
    ]
    if memory_limit:
        cmd += ["--memory-limit", memory_limit]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=env, text=True)
    try:
        wait_ready(base_url)
        per_query_ms = {}
        per_query_csv = {}
        per_query_err = {}
        qlist = only_queries if only_queries else range(1, 23)
        for q in qlist:
            sql = queries[q]
            best = None
            best_text = None
            for _ in range(iterations):
                t0 = time.time()
                try:
                    r = requests.post(
                        f"{base_url}/sql",
                        params={"format": "csv", "distributed": "0"},
                        data=sql.encode("utf-8"),
                        timeout=query_timeout_s,
                    )
                except requests.RequestException as e:
                    per_query_err[q] = str(e)
                    break
                dt = (time.time() - t0) * 1000.0
                if r.status_code != 200:
                    per_query_err[q] = f"HTTP {r.status_code}: {r.text[:400]}"
                    break
                if best is None or dt < best:
                    best = dt
                    best_text = r.text
            if q not in per_query_err:
                per_query_ms[q] = best
                per_query_csv[q] = best_text
        return per_query_ms, per_query_csv, per_query_err, proc
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()


def duckdb_parquet_side(source_dir: str, queries: dict, iterations: int, only_queries=None):
    import duckdb

    con = duckdb.connect()
    con.execute("SET threads=16")
    for t in TABLES:
        path = os.path.join(source_dir, f"{t}.parquet")
        con.execute(f"CREATE OR REPLACE VIEW {t} AS SELECT * FROM read_parquet('{path}')")
    per_query_ms = {}
    per_query_rows = {}
    qlist = only_queries if only_queries else range(1, 23)
    for q in qlist:
        sql = queries[q]
        best = None
        rows = None
        for _ in range(iterations):
            t0 = time.time()
            res = con.execute(sql).fetchall()
            dt = (time.time() - t0) * 1000.0
            if best is None or dt < best:
                best = dt
                rows = res
        per_query_ms[q] = best
        per_query_rows[q] = rows
    return per_query_ms, per_query_rows


def latest_metadata_json(iceberg_dir: str, table: str) -> str:
    files = sorted(glob.glob(os.path.join(iceberg_dir, table, "metadata", "*.metadata.json")))
    if not files:
        raise FileNotFoundError(f"no metadata json for {table} under {iceberg_dir}")
    return files[-1]


def duckdb_iceberg_side(iceberg_dir: str, queries: dict, iterations: int, only_queries=None):
    import duckdb

    con = duckdb.connect()
    con.execute("INSTALL iceberg; LOAD iceberg;")
    for t in TABLES:
        meta = latest_metadata_json(iceberg_dir, t)
        con.execute(f"CREATE OR REPLACE VIEW {t} AS SELECT * FROM iceberg_scan('{meta}')")
    per_query_ms = {}
    per_query_rows = {}
    qlist = only_queries if only_queries else range(1, 23)
    for q in qlist:
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


def norm(v):
    """Normalize a value so DuckDB and engine-CSV text compare equal."""
    if v is None or v == "" or v == "\\N":
        return ""
    if isinstance(v, (datetime.date, datetime.datetime)):
        return str(v)[:10]
    try:
        f = float(v)
        if math.isnan(f):
            return "NaN"
        return round(f, 2)
    except (ValueError, TypeError):
        return v


def cell_compare(duck_rows, engine_csv_text):
    if engine_csv_text is None:
        return "ENGINE CSV MISSING"
    reader = csv.reader(io.StringIO(engine_csv_text))
    rows = list(reader)
    if not rows:
        erows = []
    else:
        erows = rows[1:]  # drop header

    if len(duck_rows) != len(erows):
        return f"ROW COUNT engine={len(erows)} duckdb={len(duck_rows)}"

    for i, (drow, erow) in enumerate(zip(duck_rows, erows)):
        if len(drow) != len(erow):
            return f"row {i}: col count engine={len(erow)} duckdb={len(drow)}"
        for j, (dv, ev) in enumerate(zip(drow, erow)):
            dn, en = norm(dv), norm(ev)
            if isinstance(dn, float) and isinstance(en, float):
                # Absolute tolerance for small values; relative for large ones
                # (SF=100 aggregates land in the trillions, where FP summation
                # -order noise of ~1e-13 relative dwarfs a flat 0.02 absolute
                # tolerance -- matches this repo's own established
                # cell-exact-comparison convention, e.g. sf100_engine_validate.py).
                tol = max(0.02, abs(dn) * 1e-9)
                if abs(dn - en) > tol:
                    return f"row {i} col {j}: engine={ev} duckdb={dv}"
            elif isinstance(dn, float) or isinstance(en, float):
                # one side parsed numeric, other didn't -- try string fallback
                if str(dn) != str(en):
                    return f"row {i} col {j}: engine={ev!r} duckdb={dv!r}"
            elif str(dn) != str(en):
                return f"row {i} col {j}: engine={ev!r} duckdb={dv!r}"
    return None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--write", action="store_true", help="convert source-dir -> native-dir and exit")
    ap.add_argument("--source-dir", default="data/tpch-10gb")
    ap.add_argument("--native-dir", default="data/tpch-10gb-native")
    ap.add_argument("--iceberg-dir", default=None)
    ap.add_argument("--sf", type=float, default=10.0)
    ap.add_argument("--binary", default="target/release/query_engine")
    ap.add_argument("--iterations", type=int, default=2)
    ap.add_argument("--memory-limit", default=None)
    ap.add_argument("--mode", default="overwrite")
    ap.add_argument("--no-duckdb", action="store_true", help="skip the DuckDB-parquet leg (timing only)")
    ap.add_argument("--no-iceberg", action="store_true")
    ap.add_argument("--no-cell-exact", action="store_true")
    ap.add_argument("--env", action="append", default=[], help="KEY=VALUE env override, may repeat")
    ap.add_argument("--query-timeout", type=float, default=600.0)
    ap.add_argument("--queries", default=None, help="comma list, e.g. 1,6,14 (default: all 22)")
    args = ap.parse_args()

    if args.write:
        print(f"=== writing native tables: {args.source_dir} -> {args.native_dir} (mode={args.mode}) ===")
        timings = write_native_tables(args.binary, args.source_dir, args.native_dir, args.mode)
        print(f"total write time: {sum(timings.values()):.2f}s")
        return 0

    only_queries = [int(x) for x in args.queries.split(",")] if args.queries else None
    queries = get_queries(args.sf)
    env_overrides = {}
    for kv in args.env:
        k, _, v = kv.partition("=")
        env_overrides[k] = v

    print(f"=== native-table benchmark: {args.native_dir} (SF={args.sf}) ===")
    print(f"engine binary: {args.binary}")
    print(f"memory-limit: {args.memory_limit}")
    print(f"env overrides: {env_overrides}")
    print(f"iterations: {args.iterations} (best-of-N per query)\n")

    print("--- engine (serve --tables, native-table auto-detect) ---")
    eng_ms, eng_csv, eng_err, _ = engine_native_side(
        args.binary,
        args.native_dir,
        queries,
        args.iterations,
        args.memory_limit,
        env_overrides,
        args.query_timeout,
        only_queries,
    )

    duck_ms = {}
    duck_rows = {}
    if not args.no_duckdb:
        print("--- DuckDB (read_parquet, plain parquet source) ---")
        duck_ms, duck_rows = duckdb_parquet_side(args.source_dir, queries, args.iterations, only_queries)

    duck_iceberg_ms = {}
    if args.iceberg_dir and not args.no_iceberg:
        print("--- DuckDB (iceberg_scan) ---")
        duck_iceberg_ms, _ = duckdb_iceberg_side(args.iceberg_dir, queries, args.iterations, only_queries)

    qlist = only_queries if only_queries else list(range(1, 23))
    header = f"\n{'Q':>4} {'engine ms':>12}"
    if not args.no_duckdb:
        header += f" {'duck-pq ms':>12} {'ratio-pq':>9}"
    if duck_iceberg_ms:
        header += f" {'duck-ice ms':>12} {'ratio-ice':>10}"
    if not args.no_cell_exact and not args.no_duckdb:
        header += f" {'cell-exact':>30}"
    print(header)

    total_e = 0.0
    total_d = 0.0
    total_di = 0.0
    bad = []
    for q in qlist:
        if q in eng_err:
            print(f"Q{q:02d}  ERROR: {eng_err[q]}")
            bad.append(q)
            continue
        e = eng_ms[q]
        total_e += e
        line = f"Q{q:02d} {e:12.2f}"
        if not args.no_duckdb:
            d = duck_ms[q]
            total_d += d
            ratio = e / d if d > 0 else float("nan")
            line += f" {d:12.2f} {ratio:9.2f}"
        if duck_iceberg_ms:
            di = duck_iceberg_ms[q]
            total_di += di
            ratioi = e / di if di > 0 else float("nan")
            line += f" {di:12.2f} {ratioi:10.2f}"
        if not args.no_cell_exact and not args.no_duckdb:
            msg = cell_compare(duck_rows[q], eng_csv[q])
            if msg is None:
                line += f" {'OK (' + str(len(duck_rows[q])) + ' rows)':>30}"
            else:
                line += f" {'MISMATCH: ' + msg:>30}"
                bad.append(q)
        print(line)

    print(f"\nTOTAL engine={total_e:.2f}ms", end="")
    if not args.no_duckdb and total_d > 0:
        print(f"  duckdb-parquet={total_d:.2f}ms  ratio={total_e/total_d:.2f}x", end="")
    if duck_iceberg_ms and total_di > 0:
        print(f"  duckdb-iceberg={total_di:.2f}ms  ratio={total_e/total_di:.2f}x", end="")
    print()

    if bad:
        print(f"\nFAILED queries: {bad}")
        return 1
    print("\nALL QUERIES OK" + ("" if args.no_cell_exact or args.no_duckdb else " (cell-exact vs DuckDB/parquet)"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
