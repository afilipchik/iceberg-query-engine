#!/usr/bin/env python3
"""
Convert a TPC-H parquet directory into Lance datasets, one per table.

The engine's Lance reader (src/storage/lance.rs) and the DuckDB-over-Lance
oracle (scripts/duckdb_lance_bench.py) both read what this writes, so the two
sides are always compared on byte-identical inputs.

Usage:
    .venv/bin/python scripts/lance_convert.py \
        --parquet ./data/tpch-1mb --out ./data/tpch-1mb-lance
"""

import argparse
import os
import shutil
import sys
import time

import lance
import pyarrow.parquet as pq

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


def convert(parquet_dir: str, out_dir: str, max_rows_per_file: int) -> int:
    os.makedirs(out_dir, exist_ok=True)
    total_rows = 0
    for table in TABLES:
        src = os.path.join(parquet_dir, f"{table}.parquet")
        if not os.path.exists(src):
            src_dir = os.path.join(parquet_dir, table)
            if os.path.isdir(src_dir):
                src = src_dir
            else:
                print(f"  SKIP {table}: no {src}")
                continue

        dst = os.path.join(out_dir, f"{table}.lance")
        if os.path.exists(dst):
            shutil.rmtree(dst)

        t0 = time.time()
        tbl = pq.read_table(src)
        # Lance keeps the Arrow schema as-is; no type coercion here, so any
        # mismatch against the parquet source is a reader bug, not a writer one.
        lance.write_dataset(
            tbl,
            dst,
            max_rows_per_file=max_rows_per_file,
            mode="create",
        )
        rows = tbl.num_rows
        total_rows += rows
        size = sum(
            os.path.getsize(os.path.join(dp, f))
            for dp, _, fs in os.walk(dst)
            for f in fs
        )
        print(
            f"  {table:10} {rows:>10,} rows  {size / 1e6:8.1f} MB  "
            f"{time.time() - t0:6.1f}s"
        )
    return total_rows


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--parquet", default="./data/tpch-1mb")
    ap.add_argument("--out", default="./data/tpch-1mb-lance")
    ap.add_argument(
        "--max-rows-per-file",
        type=int,
        default=1024 * 1024,
        help="Lance fragment size; mirrors the parquet row-group size so the "
        "two formats give the engine comparable parallelism opportunities.",
    )
    args = ap.parse_args()

    print(f"Converting {args.parquet} -> {args.out} (lance {lance.__version__})")
    t0 = time.time()
    rows = convert(args.parquet, args.out, args.max_rows_per_file)
    print(f"Done: {rows:,} rows in {time.time() - t0:.1f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
