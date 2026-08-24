#!/usr/bin/env python3
"""Independent reference + cell-exact comparator for
`examples/native_mutation_cell_exact_check.rs` (task 006, native-tables-
mutation epic QA close-out).

Recomputes the IDENTICAL four-statement mutation sequence
(CREATE/INSERT/DELETE/UPDATE) as REAL DuckDB DML against the SAME source
parquet file (`data/tpch-10gb/orders.parquet`, 1,500,000 rows -- real
scale, not the ~1500-row fixtures the individual mutation tasks used) --
a wholly independent engine, not the query engine checking itself -- then
compares the resulting table, cell-by-cell, against the engine's own CSV
dump (produced by the Rust example) via a DuckDB `EXCEPT` set-difference
in both directions. Zero rows in both directions is the cell-exact
verdict, matching this repo's established DuckDB-oracle convention
(`scripts/native_bench_compare.py`'s `cell_compare`, `.scratch/validate22.py`
pattern referenced throughout CLAUDE.md).

Usage:
    scripts/claude-safe-build.sh cargo build --release \\
        --example native_mutation_cell_exact_check
    scripts/claude-safe-build.sh \\
        ./target/release/examples/native_mutation_cell_exact_check
    .venv/bin/python scripts/native_mutation_cell_exact_check.py
"""
import sys

import duckdb

SOURCE = "data/tpch-10gb/orders.parquet"
ENGINE_CSV = ".scratch/qa006/orders_native_result.csv"

COLUMNS = [
    ("o_orderkey", "BIGINT"),
    ("o_custkey", "BIGINT"),
    ("o_orderstatus", "VARCHAR"),
    ("o_totalprice", "DOUBLE"),
    ("o_orderdate", "DATE"),
    ("o_orderpriority", "VARCHAR"),
    ("o_clerk", "VARCHAR"),
    ("o_shippriority", "INTEGER"),
    ("o_comment", "VARCHAR"),
]


def main() -> int:
    con = duckdb.connect()

    # --- Independent reference: real DuckDB DML, same source file ---
    con.execute(
        f"CREATE TABLE ref AS SELECT * FROM read_parquet('{SOURCE}') "
        "WHERE o_orderkey % 5 <> 0"
    )
    n0 = con.execute("SELECT COUNT(*) FROM ref").fetchone()[0]
    con.execute(
        f"INSERT INTO ref SELECT * FROM read_parquet('{SOURCE}') "
        "WHERE o_orderkey % 5 = 0"
    )
    n1 = con.execute("SELECT COUNT(*) FROM ref").fetchone()[0]
    con.execute("DELETE FROM ref WHERE o_orderstatus = 'F' AND o_totalprice < 50000")
    n2 = con.execute("SELECT COUNT(*) FROM ref").fetchone()[0]
    con.execute(
        "UPDATE ref SET o_totalprice = o_totalprice * 1.05, "
        "o_orderpriority = '1-URGENT' WHERE o_orderdate >= DATE '1998-01-01'"
    )
    n3 = con.execute("SELECT COUNT(*) FROM ref").fetchone()[0]
    print(
        f"DuckDB reference: base(create)={n0} after_insert={n1} "
        f"after_delete={n2} after_update(unchanged count)={n3}"
    )
    print(f"  -> inserted {n1 - n0}, deleted {n1 - n2}")

    # --- Engine result: the Rust example's CSV dump, forced-typed ---
    # An explicit CREATE TABLE + positional INSERT (rather than `SELECT
    # x::T AS x`) sidesteps a DuckDB binder quirk where casting a column
    # and re-aliasing it to its OWN source name in one SELECT list is
    # rejected ("referenced ... before it is defined"). `names=[...]`
    # overrides whatever the CSV's own header text is -- a bare `SELECT *`
    # in the engine's own final query reuses the scan's qualified Arrow
    # field names (e.g. "orders_native.o_orderkey", not "o_orderkey"; see
    # CLAUDE.md's Native Tables section), so the header row is NOT plain
    # column names and must not be trusted for binding.
    create_cols = ", ".join(f"{c} {t}" for c, t in COLUMNS)
    con.execute(f"CREATE TABLE engine_result ({create_cols})")
    select_cast = ", ".join(f"{c}::{t}" for c, t in COLUMNS)
    names_list = ", ".join(f"'{c}'" for c, _ in COLUMNS)
    con.execute(
        f"""
        INSERT INTO engine_result
        SELECT {select_cast}
        FROM read_csv('{ENGINE_CSV}', header=true, all_varchar=true,
                       names=[{names_list}])
        """
    )
    n_engine = con.execute("SELECT COUNT(*) FROM engine_result").fetchone()[0]
    print(f"Engine result (native table, post CREATE+INSERT+DELETE+UPDATE): {n_engine} rows")

    if n_engine != n3:
        print(f"\nFAIL: row count mismatch: engine={n_engine} duckdb_ref={n3}")
        return 1

    # Round the float column to 2dp on both sides -- currency values;
    # this guards against float-print/parse round-tripping through CSV
    # text producing a false negative, not against any real computation
    # difference (matches this repo's own established cell-compare
    # convention, e.g. scripts/native_bench_compare.py's `norm()`).
    cmp_cols = ", ".join(
        f"ROUND({c}, 2) AS {c}" if t == "DOUBLE" else c for c, t in COLUMNS
    )
    only_in_ref = con.execute(
        f"SELECT {cmp_cols} FROM ref EXCEPT SELECT {cmp_cols} FROM engine_result"
    ).fetchall()
    only_in_engine = con.execute(
        f"SELECT {cmp_cols} FROM engine_result EXCEPT SELECT {cmp_cols} FROM ref"
    ).fetchall()

    print(f"Rows only in DuckDB reference (missing from engine): {len(only_in_ref)}")
    print(f"Rows only in engine result (not in DuckDB reference): {len(only_in_engine)}")
    if only_in_ref:
        print("  sample (up to 5):", only_in_ref[:5])
    if only_in_engine:
        print("  sample (up to 5):", only_in_engine[:5])

    if only_in_ref or only_in_engine:
        print("\nFAIL: cell-exact mismatch")
        return 1

    print(
        f"\nPASS: cell-exact match, {n_engine} rows x 9 columns, real SF=10 scale "
        f"(CREATE {n0} -> INSERT +{n1 - n0} -> DELETE -{n1 - n2} -> UPDATE {n3} "
        "rows recomputed in place, all independently verified against DuckDB DML "
        "over the same source parquet)"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
