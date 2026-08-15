#!/usr/bin/env python3
"""Generate real, spec-compliant Apache Iceberg tables from TPC-H parquet fixtures.

Usage:
    .venv/bin/python scripts/iceberg_gen.py <src_parquet_dir> <out_warehouse_dir>

Example:
    .venv/bin/python scripts/iceberg_gen.py data/tpch-1mb data/tpch-1mb-iceberg
    .venv/bin/python scripts/iceberg_gen.py data/tpch-10mb data/tpch-10mb-iceberg

Produces, for each of the 8 TPC-H tables (region, nation, customer, orders,
lineitem, part, partsupp, supplier):

    <out>/<table>/metadata/   Iceberg metadata: NNNNN-<uuid>.metadata.json,
                              snap-<snapshot-id>-0-<uuid>.avro (manifest lists),
                              <uuid>-m0.avro (manifest files).
                              NOTE: no version-hint.text is written (this is a
                              SqlCatalog layout, not a HadoopCatalog layout).
    <out>/<table>/data/       parquet data files written by pyiceberg.

The SQLite catalog database is throwaway plumbing and lives in
.scratch/iceberg_catalog/ inside the repo; the warehouse directories above are
self-contained and readable without the catalog (point a reader at the
highest-numbered *.metadata.json in metadata/).

MULTI-SNAPSHOT TABLE: for the *orders* table when the source dir name contains
"1mb", a second append of the first 100 source rows is performed, so:
    snapshot 1 (first append):  1500 rows
    snapshot 2 (current):       1600 rows  (1500 + 100 duplicated rows)
All other tables have exactly 1 snapshot and match the source parquet exactly.

Validation: run with --validate (done automatically after generation) compares
COUNT(*) plus SUM() checksums of every numeric column between the source
parquet and the Iceberg table read through DuckDB's iceberg extension
(fallback: pyiceberg .scan().to_arrow()).
"""

import glob
import math
import os
import shutil
import sys
import uuid

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

TABLES = [
    "region",
    "nation",
    "customer",
    "orders",
    "lineitem",
    "part",
    "partsupp",
    "supplier",
]

# Table that gets a second append (2 snapshots) and the slice size, applied
# only for warehouses generated from a source dir whose name contains this key.
MULTI_SNAPSHOT_TABLE = "orders"
MULTI_SNAPSHOT_SRC_KEY = "1mb"
MULTI_SNAPSHOT_EXTRA_ROWS = 100


def load_source(src_dir: str, table: str) -> pa.Table:
    path = os.path.join(src_dir, f"{table}.parquet")
    if os.path.isfile(path):
        return pq.read_table(path)
    # per-table directory of parquet files
    dpath = os.path.join(src_dir, table)
    files = sorted(glob.glob(os.path.join(dpath, "*.parquet")))
    if not files:
        raise FileNotFoundError(f"no parquet for table {table} in {src_dir}")
    return pa.concat_tables(pq.read_table(f) for f in files)


def generate(src_dir: str, out_dir: str) -> None:
    from pyiceberg.catalog.sql import SqlCatalog

    src_dir = os.path.abspath(src_dir)
    out_dir = os.path.abspath(out_dir)
    if os.path.exists(out_dir):
        shutil.rmtree(out_dir)
    os.makedirs(out_dir)

    catalog_dir = os.path.join(REPO, ".scratch", "iceberg_catalog")
    os.makedirs(catalog_dir, exist_ok=True)
    catalog_db = os.path.join(catalog_dir, f"catalog_{uuid.uuid4().hex}.db")

    catalog = SqlCatalog(
        "tpch",
        uri=f"sqlite:///{catalog_db}",
        warehouse=f"file://{out_dir}",
    )
    catalog.create_namespace("tpch")

    multi_snapshot = MULTI_SNAPSHOT_SRC_KEY in os.path.basename(src_dir)

    for table_name in TABLES:
        arrow_table = load_source(src_dir, table_name)
        tbl = catalog.create_table(
            f"tpch.{table_name}",
            schema=arrow_table.schema,
            location=f"file://{os.path.join(out_dir, table_name)}",
        )
        tbl.append(arrow_table)
        n = len(arrow_table)
        if multi_snapshot and table_name == MULTI_SNAPSHOT_TABLE:
            extra = arrow_table.slice(0, MULTI_SNAPSHOT_EXTRA_ROWS)
            tbl.append(extra)
            print(
                f"  {table_name}: 2 snapshots "
                f"(snapshot 1: {n} rows, current: {n + len(extra)} rows)"
            )
        else:
            print(f"  {table_name}: 1 snapshot, {n} rows")


def expected_arrow(src_dir: str, table: str) -> pa.Table:
    """Source data with the multi-snapshot duplicate slice applied."""
    t = load_source(src_dir, table)
    if MULTI_SNAPSHOT_SRC_KEY in os.path.basename(os.path.abspath(src_dir)) and (
        table == MULTI_SNAPSHOT_TABLE
    ):
        t = pa.concat_tables([t, t.slice(0, MULTI_SNAPSHOT_EXTRA_ROWS)])
    return t


def aggregates(t: pa.Table):
    """(count, {numeric_col: sum}) over an arrow table."""
    sums = {}
    for field in t.schema:
        if pa.types.is_integer(field.type) or pa.types.is_floating(field.type):
            sums[field.name] = pc.sum(t.column(field.name)).as_py()
    return len(t), sums


def latest_metadata_json(out_dir: str, table: str) -> str:
    files = sorted(glob.glob(os.path.join(out_dir, table, "metadata", "*.metadata.json")))
    if not files:
        raise FileNotFoundError(f"no metadata json for {table}")
    return files[-1]


def read_back_duckdb(out_dir: str, table: str) -> pa.Table:
    import duckdb

    con = duckdb.connect()
    con.execute("INSTALL iceberg; LOAD iceberg;")
    meta = latest_metadata_json(out_dir, table)
    return con.execute(f"SELECT * FROM iceberg_scan('{meta}')").fetch_arrow_table()


def read_back_pyiceberg(out_dir: str, table: str) -> pa.Table:
    from pyiceberg.table import StaticTable

    return StaticTable.from_metadata(latest_metadata_json(out_dir, table)).scan().to_arrow()


def close_enough(a, b) -> bool:
    if a == b:
        return True
    if isinstance(a, float) or isinstance(b, float):
        return math.isclose(a, b, rel_tol=1e-9, abs_tol=1e-6)
    return False


def validate(src_dir: str, out_dir: str) -> bool:
    src_dir, out_dir = os.path.abspath(src_dir), os.path.abspath(out_dir)
    try:
        import duckdb  # noqa: F401
        reader, engine = read_back_duckdb, "duckdb-iceberg"
        import duckdb as _d
        _d.connect().execute("INSTALL iceberg; LOAD iceberg;")
    except Exception as e:  # pragma: no cover
        print(f"duckdb iceberg extension unavailable ({e}); using pyiceberg")
        reader, engine = read_back_pyiceberg, "pyiceberg"

    print(f"\nValidating {out_dir} against {src_dir} via {engine}")
    all_ok = True
    for table in TABLES:
        exp_count, exp_sums = aggregates(expected_arrow(src_dir, table))
        try:
            got = reader(out_dir, table)
        except Exception:
            got = read_back_pyiceberg(out_dir, table)
            print(f"  [{table}] duckdb read failed, fell back to pyiceberg")
        got_count, got_sums = aggregates(got)
        problems = []
        if exp_count != got_count:
            problems.append(f"count {exp_count} != {got_count}")
        for col, v in exp_sums.items():
            if col not in got_sums or not close_enough(v, got_sums[col]):
                problems.append(f"sum({col}) {v} != {got_sums.get(col)}")
        status = "PASS" if not problems else "FAIL " + "; ".join(problems)
        checked = ", ".join(sorted(exp_sums))
        print(f"  {table:<10} rows={got_count:<7} sums[{checked}] {status}")
        all_ok &= not problems
    return all_ok


def validate_snapshots(out_dir: str) -> bool:
    """Check the multi-snapshot orders table: snapshot 1 vs current row counts."""
    from pyiceberg.table import StaticTable

    meta = latest_metadata_json(out_dir, MULTI_SNAPSHOT_TABLE)
    tbl = StaticTable.from_metadata(meta)
    snaps = tbl.metadata.snapshots
    if len(snaps) != 2:
        print(f"  FAIL: expected 2 snapshots on {MULTI_SNAPSHOT_TABLE}, got {len(snaps)}")
        return False
    n1 = len(tbl.scan(snapshot_id=snaps[0].snapshot_id).to_arrow())
    n2 = len(tbl.scan(snapshot_id=snaps[1].snapshot_id).to_arrow())
    ok = n2 == n1 + MULTI_SNAPSHOT_EXTRA_ROWS
    print(
        f"  {MULTI_SNAPSHOT_TABLE} snapshots: snapshot1={n1} rows, "
        f"current={n2} rows -> {'PASS' if ok else 'FAIL'}"
    )
    return ok


def main() -> int:
    if len(sys.argv) != 3:
        print(__doc__)
        return 2
    src_dir, out_dir = sys.argv[1], sys.argv[2]
    print(f"Generating Iceberg warehouse {out_dir} from {src_dir}")
    generate(src_dir, out_dir)
    ok = validate(src_dir, out_dir)
    if MULTI_SNAPSHOT_SRC_KEY in os.path.basename(os.path.abspath(src_dir)):
        ok &= validate_snapshots(os.path.abspath(out_dir))
    print("\nOVERALL:", "PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
