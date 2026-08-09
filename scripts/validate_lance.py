#!/usr/bin/env python3
"""
Cell-exact validation of the engine's Lance reader against DuckDB reading the
SAME Lance datasets.

The engine writes per-query CSVs via `benchmark-lance --save-csv`; this script
runs the identical SQL (extracted from src/tpch/queries.rs) through DuckDB over
the Lance datasets and compares every cell.

Both sides therefore read Lance, so a mismatch is a reader bug rather than a
data-conversion difference.

Note on DuckDB's Lance access: datasets are materialized to Arrow before
registration. Registering a `lance.LanceDataset` object directly makes DuckDB's
ARROW_SCAN report ~0 rows as its cardinality estimate, and 3-table joins then
spin forever (see scripts/duckdb_lance_bench.py for the full measurement). The
data is identical either way, so this remains a valid oracle.

Usage:
    .venv/bin/python scripts/validate_lance.py \
        --lance ./data/tpch-10gb-lance --csv .scratch/lance_csv --sf 10
"""

import argparse
import csv
import datetime
import math
import os
import re
import sys

import duckdb
import lance

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


def norm(v):
    """Normalize a value so DuckDB and CSV text compare equal."""
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


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--lance", default="./data/tpch-10gb-lance")
    ap.add_argument("--csv", default=".scratch/lance_csv")
    ap.add_argument("--sf", type=float, default=10.0)
    args = ap.parse_args()

    con = duckdb.connect()
    con.execute("SET threads=16")
    keep = []
    for t in TABLES:
        path = os.path.join(args.lance, f"{t}.lance")
        if not os.path.exists(path):
            print(f"missing Lance dataset: {path}")
            return 2
        tbl = lance.dataset(path).to_table()
        keep.append(tbl)
        con.register(f"__lance_{t}", tbl)
        con.execute(f"CREATE OR REPLACE VIEW {t} AS SELECT * FROM __lance_{t}")

    src = open("src/tpch/queries.rs").read()
    queries = {}
    for m in re.finditer(r'pub const Q(\d+): &str = r#"(.*?)"#;', src, re.S):
        queries[int(m.group(1))] = m.group(2)
    if len(queries) < 22:
        print(f"only extracted {len(queries)} queries")
        return 2

    # Q11's HAVING threshold is scale-dependent: the spec value is for SF=1.
    if args.sf != 1.0:
        queries[11] = queries[11].replace("0.0001", str(0.0001 / args.sf))

    bad = 0
    for q in range(1, 23):
        duck = con.execute(queries[q]).fetchall()
        path = os.path.join(args.csv, f"q{q:02d}.csv")
        if not os.path.exists(path):
            print(f"Q{q:02d}: MISSING engine CSV {path}")
            bad += 1
            continue
        with open(path) as f:
            rows = list(csv.reader(f))[1:]

        if len(duck) != len(rows):
            print(f"Q{q:02d}: ROW COUNT engine={len(rows)} duckdb={len(duck)}")
            bad += 1
            continue

        mismatch = None
        for i, (drow, erow) in enumerate(zip(duck, rows)):
            if len(drow) != len(erow):
                mismatch = f"row {i}: col count {len(erow)} vs {len(drow)}"
                break
            for j, (dv, ev) in enumerate(zip(drow, erow)):
                dn, en = norm(dv), norm(ev)
                if isinstance(dn, float) and isinstance(en, float):
                    if abs(dn - en) > 0.02:
                        mismatch = f"row {i} col {j}: engine={ev} duckdb={dv}"
                        break
                elif str(dn) != str(en):
                    mismatch = f"row {i} col {j}: engine={ev!r} duckdb={dv!r}"
                    break
            if mismatch:
                break

        if mismatch:
            print(f"Q{q:02d}: MISMATCH {mismatch}")
            bad += 1
        else:
            print(f"Q{q:02d}: OK ({len(rows)} rows)")

    print("ALL 22 CELL-EXACT (engine/Lance vs DuckDB/Lance)" if bad == 0 else f"{bad} QUERIES FAILED")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
