#!/usr/bin/env python3
"""Independent reference + cell-exact comparator for
`examples/native_rollup_multi_shape_check.rs` (native-tables-rollups
epic, task 004 QA close-out).

Recomputes THREE differently-shaped GROUP BY queries -- the same three
`CREATE MATERIALIZED VIEW`-registered rollup shapes the Rust example
registers simultaneously against one native table -- as REAL DuckDB SQL
against the SAME source parquet file (`data/tpch-1gb/lineitem.parquet`,
real SF=1 scale), a wholly independent engine, not this one checking
itself. For each shape, THREE pairwise comparisons are made, all with the
same numeric tolerance: rollup-answered result vs. DuckDB oracle,
direct-base-table-computation result vs. DuckDB oracle, and
rollup-answered vs. direct-computation directly. All three CSVs (rollup,
direct) are produced by the Rust example, which itself asserts the
rollup-answered leg was actually answered by the correct rollup
(`QueryMetrics::rollup_answered`) and the direct-computation leg used a
context with zero rollups registered.

Float tolerance follows this repo's own established, precedented
convention (`scripts/native_bench_compare.py`'s `cell_compare`:
`tol = max(0.02, abs(v) * 1e-9)`) for exactly the reason documented there
and in `scripts/native_rollup_cell_exact_check.py`: float64 summation
order differs between this engine's parallel/morsel accumulation and
DuckDB's own internal reduction (and even between two DIFFERENT
ExecutionContext instances of THIS engine, since morsel/partition
scheduling is not guaranteed deterministic run to run), producing
last-few-bits noise on large sums that a flat small absolute tolerance
would not absorb. Integer and date columns are compared EXACTLY (no
tolerance) -- COUNT/MIN/MAX have no summation-order ambiguity.

Usage:
    scripts/claude-safe-build.sh cargo build --release \\
        --example native_rollup_multi_shape_check
    scripts/claude-safe-build.sh \\
        ./target/release/examples/native_rollup_multi_shape_check
    .venv/bin/python scripts/native_rollup_multi_shape_check.py
"""
import sys

import duckdb

SOURCE = "data/tpch-1gb/lineitem.parquet"
CSV_DIR = ".scratch/rollup004"
FLOAT_TOL = "GREATEST(0.02, ABS(a.{c}) * 1e-9)"


def duck_type(c, float_cols, int_cols, date_cols):
    if c in float_cols:
        return "DOUBLE"
    if c in int_cols:
        return "BIGINT"
    if c in date_cols:
        return "DATE"
    return "VARCHAR"


def load_csv(con, table_name, csv_path, all_cols, float_cols, int_cols, date_cols, create_cols):
    con.execute(f"DROP TABLE IF EXISTS {table_name}")
    con.execute(f"CREATE TABLE {table_name} ({create_cols})")
    select_cast = ", ".join(
        f"{c}::{duck_type(c, float_cols, int_cols, date_cols)}" for c in all_cols
    )
    names_list = ", ".join(f"'{c}'" for c in all_cols)
    con.execute(
        f"""
        INSERT INTO {table_name}
        SELECT {select_cast}
        FROM read_csv('{csv_path}', header=true, all_varchar=true,
                       names=[{names_list}])
        """
    )
    return con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]


def compare_tables(con, label, table_a, table_b, key_cols, float_cols, int_cols, date_cols):
    """Cell-exact (within tolerance) comparison of two already-loaded
    DuckDB tables, joined on `key_cols`. Returns True on PASS."""
    n_a = con.execute(f"SELECT COUNT(*) FROM {table_a}").fetchone()[0]
    n_b = con.execute(f"SELECT COUNT(*) FROM {table_b}").fetchone()[0]
    if n_a != n_b:
        print(f"  [{label}] FAIL: row count mismatch: {table_a}={n_a} {table_b}={n_b}")
        return False

    key_join = " AND ".join(f"a.{k} = b.{k}" for k in key_cols)
    key_cols_sel = ", ".join(key_cols)
    only_in_a = con.execute(
        f"SELECT {key_cols_sel} FROM {table_a} EXCEPT SELECT {key_cols_sel} FROM {table_b}"
    ).fetchall()
    only_in_b = con.execute(
        f"SELECT {key_cols_sel} FROM {table_b} EXCEPT SELECT {key_cols_sel} FROM {table_a}"
    ).fetchall()
    if only_in_a or only_in_b:
        print(f"  [{label}] FAIL: group-key set mismatch")
        print(f"    only in {table_a}: {only_in_a}")
        print(f"    only in {table_b}: {only_in_b}")
        return False

    checks = []
    if float_cols:
        checks.append(
            " OR ".join(f"ABS(a.{c} - b.{c}) > {FLOAT_TOL.format(c=c)}" for c in float_cols)
        )
    if int_cols:
        checks.append(" OR ".join(f"a.{c} <> b.{c}" for c in int_cols))
    if date_cols:
        checks.append(" OR ".join(f"a.{c} <> b.{c}" for c in date_cols))
    where_clause = " OR ".join(f"({c})" for c in checks)

    mismatches = con.execute(
        f"SELECT * FROM {table_a} a JOIN {table_b} b ON {key_join} WHERE {where_clause}"
    ).fetchall()
    if mismatches:
        print(f"  [{label}] FAIL: {len(mismatches)} group(s) exceed tolerance:")
        for row in mismatches[:10]:
            print(f"    {row}")
        return False

    print(f"  [{label}] PASS: cell-exact, {n_a} group(s)")
    return True


def check_shape(con, name, reference_sql, rollup_csv, direct_csv, create_cols, key_cols,
                 float_cols, int_cols, date_cols, all_cols):
    print(f"=== {name} ===")
    con.execute("DROP TABLE IF EXISTS duckdb_ref")
    con.execute(f"CREATE TABLE duckdb_ref AS {reference_sql}")
    n_ref = con.execute("SELECT COUNT(*) FROM duckdb_ref").fetchone()[0]
    n_rollup = load_csv(
        con, "rollup_result", rollup_csv, all_cols, float_cols, int_cols, date_cols, create_cols
    )
    n_direct = load_csv(
        con, "direct_result", direct_csv, all_cols, float_cols, int_cols, date_cols, create_cols
    )
    print(
        f"  DuckDB reference: {n_ref} group(s); rollup-answered: {n_rollup} group(s); "
        f"direct: {n_direct} group(s)"
    )

    ok = True
    ok &= compare_tables(
        con, "rollup vs. DuckDB", "rollup_result", "duckdb_ref", key_cols, float_cols, int_cols,
        date_cols
    )
    ok &= compare_tables(
        con, "direct vs. DuckDB", "direct_result", "duckdb_ref", key_cols, float_cols, int_cols,
        date_cols
    )
    ok &= compare_tables(
        con, "rollup vs. direct", "rollup_result", "direct_result", key_cols, float_cols,
        int_cols, date_cols
    )
    return ok


def main() -> int:
    con = duckdb.connect()
    all_pass = True

    # --- Shape A: rollup_by_flag_status ---
    # Matches the Rust example's own query_sql column order/aliases exactly.
    ref_sql_a = f"""
        SELECT
            l_linestatus,
            l_returnflag,
            COUNT(*) AS n,
            SUM(l_quantity) AS q,
            SUM(l_extendedprice) AS base_price
        FROM read_parquet('{SOURCE}')
        GROUP BY l_linestatus, l_returnflag
    """
    all_pass &= check_shape(
        con,
        "rollup_by_flag_status",
        ref_sql_a,
        f"{CSV_DIR}/rollup_a_flag_status.csv",
        f"{CSV_DIR}/rollup_a_flag_status_direct.csv",
        "l_linestatus VARCHAR, l_returnflag VARCHAR, n BIGINT, q DOUBLE, base_price DOUBLE",
        key_cols=["l_linestatus", "l_returnflag"],
        float_cols=["q", "base_price"],
        int_cols=["n"],
        date_cols=[],
        all_cols=["l_linestatus", "l_returnflag", "n", "q", "base_price"],
    )

    # --- Shape B: rollup_by_shipmode (incl. MIN/MAX on a DATE column) ---
    ref_sql_b = f"""
        SELECT
            COUNT(*) AS cnt,
            MAX(l_shipdate) AS latest,
            MIN(l_shipdate) AS earliest,
            SUM(l_quantity) AS total_qty,
            l_shipmode AS mode
        FROM read_parquet('{SOURCE}')
        GROUP BY l_shipmode
    """
    all_pass &= check_shape(
        con,
        "rollup_by_shipmode",
        ref_sql_b,
        f"{CSV_DIR}/rollup_b_shipmode.csv",
        f"{CSV_DIR}/rollup_b_shipmode_direct.csv",
        "cnt BIGINT, latest VARCHAR, earliest VARCHAR, total_qty DOUBLE, mode VARCHAR",
        key_cols=["mode"],
        float_cols=["total_qty"],
        int_cols=["cnt"],
        date_cols=["latest", "earliest"],
        all_cols=["cnt", "latest", "earliest", "total_qty", "mode"],
    )

    # --- Shape C: rollup_by_status_mode_flag (3-column composite key) ---
    ref_sql_c = f"""
        SELECT
            l_returnflag,
            l_shipmode,
            l_linestatus,
            COUNT(*) AS cnt,
            SUM(l_extendedprice * (1 - l_discount)) AS disc_price
        FROM read_parquet('{SOURCE}')
        GROUP BY l_returnflag, l_shipmode, l_linestatus
    """
    all_pass &= check_shape(
        con,
        "rollup_by_status_mode_flag",
        ref_sql_c,
        f"{CSV_DIR}/rollup_c_status_mode_flag.csv",
        f"{CSV_DIR}/rollup_c_status_mode_flag_direct.csv",
        "l_returnflag VARCHAR, l_shipmode VARCHAR, l_linestatus VARCHAR, cnt BIGINT, "
        "disc_price DOUBLE",
        key_cols=["l_returnflag", "l_shipmode", "l_linestatus"],
        float_cols=["disc_price"],
        int_cols=["cnt"],
        date_cols=[],
        all_cols=["l_returnflag", "l_shipmode", "l_linestatus", "cnt", "disc_price"],
    )

    if not all_pass:
        print("\nOVERALL: FAIL -- see per-shape output above")
        return 1

    print(
        "\nOVERALL: PASS -- all 3 distinctly-shaped rollups (varied GROUP BY column count/"
        "order, varied aggregate sets incl. MIN/MAX on a date column), simultaneously "
        "registered via real CREATE MATERIALIZED VIEW DDL against one base table, cell-exact "
        "(rollup vs. DuckDB, direct vs. DuckDB, AND rollup vs. direct) at real SF=1 scale"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
