#!/usr/bin/env python3
"""Independent reference + cell-exact comparator for
`examples/native_rollup_cell_exact_check.rs` (native-tables-rollups
epic, task 001).

Recomputes the IDENTICAL GROUP BY query (the PRD's own worked example:
`lineitem` grouped by `l_returnflag`/`l_linestatus`, SUM/COUNT
aggregates) as REAL DuckDB SQL against the SAME source parquet file
(`data/tpch-1gb/lineitem.parquet`, real SF=1 scale, not the small
`data/tpch-1mb` fixture the Rust-level integration tests use) -- a
wholly independent engine, not the query engine checking itself -- then
compares the resulting table, cell-by-cell, against the engine's own CSV
dump (produced by the Rust example, and asserted BY THAT EXAMPLE to have
actually been answered by the rollup, not a silent base-table fallback).

Tolerance, not bit-exact string equality, for the float SUM columns --
and this is a DELIBERATE, PRECEDENTED choice, not a loosening invented
for this check: `scripts/native_bench_compare.py`'s own `cell_compare`
already establishes `tol = max(0.02, abs(v) * 1e-9)` (absolute floor for
small values, relative for large ones) as this repo's standing
cell-exact-comparison convention, with the SAME justification stated
there -- summing ~1,000,000 float64 values in a different order (this
engine's own parallel/morsel accumulation vs. DuckDB's own internal
reduction) produces last-few-bits differences on the order of 1e-13
RELATIVE, which a flat small absolute tolerance (e.g. round to 2dp) does
NOT absorb once the summed magnitude reaches 1e10 (this data's own
`SUM(l_extendedprice)` per group is ~5e10, where an absolute 0.02
tolerance is far tighter than the float noise floor). This was measured
directly here, not assumed: a first pass at this script used a flat
`ROUND(x, 6)` and reported spurious mismatches at the ~1e-10 absolute /
~1e-15 relative level -- exactly the class of noise
`native_bench_compare.py`'s own comment already documents.

Usage:
    scripts/claude-safe-build.sh cargo build --release \\
        --example native_rollup_cell_exact_check
    scripts/claude-safe-build.sh \\
        ./target/release/examples/native_rollup_cell_exact_check
    .venv/bin/python scripts/native_rollup_cell_exact_check.py
"""
import sys

import duckdb

SOURCE = "data/tpch-1gb/lineitem.parquet"
ENGINE_CSV = ".scratch/rollup001/lineitem_rollup_result.csv"

KEY_COLUMNS = ["l_linestatus", "l_returnflag"]
FLOAT_COLUMNS = ["q", "base_price", "disc_price"]
INT_COLUMNS = ["n"]
# Matches the Rust example's own SELECT list order/aliases exactly.
ALL_COLUMNS = ["l_linestatus", "l_returnflag", "n", "q", "base_price", "disc_price"]


def main() -> int:
    con = duckdb.connect()

    # --- Independent reference: real DuckDB SQL, same source file ---
    reference_sql = f"""
        SELECT
            l_linestatus,
            l_returnflag,
            COUNT(*) AS n,
            SUM(l_quantity) AS q,
            SUM(l_extendedprice) AS base_price,
            SUM(l_extendedprice * (1 - l_discount)) AS disc_price
        FROM read_parquet('{SOURCE}')
        GROUP BY l_linestatus, l_returnflag
    """
    con.execute(f"CREATE TABLE ref AS {reference_sql}")
    n_ref = con.execute("SELECT COUNT(*) FROM ref").fetchone()[0]
    print(f"DuckDB reference: {n_ref} group(s) (over {SOURCE})")

    # --- Engine result: the Rust example's CSV dump, forced-typed ---
    # See scripts/native_mutation_cell_exact_check.py's own comment for
    # why an explicit CREATE TABLE + positional INSERT is used instead of
    # a bare `SELECT x::T AS x` (DuckDB binder quirk with self-aliasing
    # casts), and why `names=[...]` overrides the CSV header text.
    create_cols = "l_linestatus VARCHAR, l_returnflag VARCHAR, n BIGINT, q DOUBLE, " \
        "base_price DOUBLE, disc_price DOUBLE"
    con.execute(f"CREATE TABLE engine_result ({create_cols})")
    select_cast = ", ".join(
        f"{c}::{'DOUBLE' if c in FLOAT_COLUMNS else 'BIGINT' if c in INT_COLUMNS else 'VARCHAR'}"
        for c in ALL_COLUMNS
    )
    names_list = ", ".join(f"'{c}'" for c in ALL_COLUMNS)
    con.execute(
        f"""
        INSERT INTO engine_result
        SELECT {select_cast}
        FROM read_csv('{ENGINE_CSV}', header=true, all_varchar=true,
                       names=[{names_list}])
        """
    )
    n_engine = con.execute("SELECT COUNT(*) FROM engine_result").fetchone()[0]
    print(f"Engine result (rollup-answered, per its own provenance check): {n_engine} group(s)")

    if n_engine != n_ref:
        print(f"\nFAIL: row count mismatch: engine={n_engine} duckdb_ref={n_ref}")
        return 1

    # Key-set equality first (every group present on both sides, exactly
    # once) -- a FULL OUTER JOIN would otherwise let a missing/extra key
    # silently produce a NULL-vs-NULL "match" in the tolerance predicate
    # below.
    key_join = " AND ".join(f"r.{k} = e.{k}" for k in KEY_COLUMNS)
    key_cols_sel = ", ".join(KEY_COLUMNS)
    only_in_ref_keys = con.execute(
        f"SELECT {key_cols_sel} FROM ref EXCEPT SELECT {key_cols_sel} FROM engine_result"
    ).fetchall()
    only_in_engine_keys = con.execute(
        f"SELECT {key_cols_sel} FROM engine_result EXCEPT SELECT {key_cols_sel} FROM ref"
    ).fetchall()
    if only_in_ref_keys or only_in_engine_keys:
        print(f"\nFAIL: group-key set mismatch")
        print(f"  only in DuckDB reference: {only_in_ref_keys}")
        print(f"  only in engine result: {only_in_engine_keys}")
        return 1

    # Per-column tolerance check, joined on the group key.
    tol_checks = " OR ".join(
        f"ABS(r.{c} - e.{c}) > GREATEST(0.02, ABS(r.{c}) * 1e-9)" for c in FLOAT_COLUMNS
    )
    int_checks = " OR ".join(f"r.{c} <> e.{c}" for c in INT_COLUMNS)
    mismatches = con.execute(
        f"""
        SELECT r.l_returnflag, r.l_linestatus,
               r.n AS ref_n, e.n AS engine_n,
               r.q AS ref_q, e.q AS engine_q,
               r.base_price AS ref_base_price, e.base_price AS engine_base_price,
               r.disc_price AS ref_disc_price, e.disc_price AS engine_disc_price
        FROM ref r JOIN engine_result e ON {key_join}
        WHERE ({tol_checks}) OR ({int_checks})
        """
    ).fetchall()

    if mismatches:
        print(f"\nFAIL: {len(mismatches)} group(s) exceed tolerance:")
        for row in mismatches[:10]:
            print(f"  {row}")
        return 1

    print(
        f"\nPASS: cell-exact match (within this repo's own established float-SUM tolerance, "
        f"tol=max(0.02, |v|*1e-9)), {n_engine} group(s) x {len(ALL_COLUMNS)} columns, real "
        f"SF=1 scale ({SOURCE}) -- rollup-answered result independently verified against a "
        "fresh DuckDB computation over the same source parquet, with order-independent "
        "GROUP BY, different aliases, and a reordered SELECT list vs. the rollup's own "
        "defining query"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
