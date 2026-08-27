#!/usr/bin/env python3
"""Task 002 (native-table-pruning epic): independent DuckDB cross-check for
`examples/native_pruning_sweep.rs`'s broader predicate-shape sweep.

Runs the (already-built) Rust example with `QE_DEBUG_NATIVE_PRUNING=1`
(so its real per-segment skip/scan traces are captured, not just its
`RESULT` lines), parses each `RESULT <table>.<label> <count>` line, and
independently recomputes the identical predicate against the ORIGINAL
source parquet (`data/tpch-10gb/{lineitem,orders}.parquet`) via a fresh
DuckDB connection -- an engine-independent oracle, not a self-check.

Usage:
  .venv/bin/python scripts/native_pruning_sweep_check.py
"""
import re
import subprocess
import sys

import duckdb

BINARY = "target/release/examples/native_pruning_sweep"
SOURCE_DIR = "data/tpch-10gb"

# label -> WHERE-clause predicate, identical text to the Rust example's own
# SQL (DuckDB and this engine's binder both accept standard `DATE 'YYYY-MM-DD'`
# literals and BETWEEN, so no translation is needed).
LINEITEM_PREDICATES = {
    "range": "l_orderkey <= 300000",
    "equality": "l_orderkey = 14500000",
    "multi_column_and": "l_orderkey BETWEEN 5000000 AND 5500000 AND l_discount > 0.095",
    "no_stats_string": "l_shipmode = 'AIR'",
    "spans_multiple_segments": "l_orderkey BETWEEN 300000 AND 2000000",
    "date_range_q4_shape": "l_shipdate BETWEEN DATE '1996-03-01' AND DATE '1996-03-31'",
}
ORDERS_PREDICATES = {
    "range": "o_orderkey <= 500000",
    "equality": "o_orderkey = 14000000",
}


def main() -> int:
    proc = subprocess.run(
        [BINARY],
        env={"QE_DEBUG_NATIVE_PRUNING": "1", "PATH": "/usr/bin:/bin"},
        capture_output=True,
        text=True,
        timeout=1200,
    )
    combined = proc.stdout + "\n" + proc.stderr
    print(combined)
    if proc.returncode != 0:
        print(f"FAIL: example exited {proc.returncode}", file=sys.stderr)
        return 1

    results = {}
    for m in re.finditer(r"^RESULT (\S+)\.(\S+) (-?\d+)$", combined, re.M):
        table, label, count = m.group(1), m.group(2), int(m.group(3))
        results[(table, label)] = count

    # Real, traced skip evidence -- summary lines look like:
    #   [native_pruning] table=data/tpch-10gb-native/lineitem scanned=8 skipped=50 total=58
    traces = re.findall(
        r"\[native_pruning\] table=(\S+) scanned=(\d+) skipped=(\d+) total=(\d+)",
        combined,
    )

    con = duckdb.connect()
    ok = True

    def check(table: str, label: str, sql_where: str | None, parquet: str):
        engine_count = results.get((table, label))
        if engine_count is None:
            print(f"FAIL: no RESULT line for {table}.{label}")
            nonlocal ok
            ok = False
            return
        where = f"WHERE {sql_where}" if sql_where else ""
        q = f"SELECT COUNT(*) FROM read_parquet('{SOURCE_DIR}/{parquet}.parquet') {where}"
        duck_count = con.execute(q).fetchone()[0]
        status = "PASS" if duck_count == engine_count else "FAIL"
        if status == "FAIL":
            ok = False
        print(f"{status} {table}.{label}: engine={engine_count} duckdb={duck_count}")

    check("lineitem", "unfiltered", None, "lineitem")
    for label, sql in LINEITEM_PREDICATES.items():
        check("lineitem", label, sql, "lineitem")
    check("orders", "unfiltered", None, "orders")
    for label, sql in ORDERS_PREDICATES.items():
        check("orders", label, sql, "orders")

    print("\n--- real traced segment skip/scan evidence ---")
    for table, scanned, skipped, total in traces:
        print(f"table={table} scanned={scanned} skipped={skipped} total={total}")

    if ok:
        print("\nPASS: every engine RESULT is cell-exact vs. an independent DuckDB oracle.")
    else:
        print("\nFAIL: see above.")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
