#!/usr/bin/env python3
"""
Compare query engine performance with DuckDB on simple TPC-H queries.
"""

import subprocess
import time
import sys
import os

# TPC-H Queries (simplified for testing)
Q1 = """
SELECT
    l_returnflag,
    l_linestatus,
    SUM(l_quantity) AS sum_qty,
    SUM(l_extendedprice) AS sum_base_price,
    COUNT(*) AS count_order
FROM lineitem
WHERE l_shipdate <= DATE '1998-09-02'
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus
"""

Q3 = """
SELECT
    l_orderkey,
    SUM(l_extendedprice * (1 - l_discount)) AS revenue,
    o_orderdate,
    o_shippriority
FROM orders, lineitem
WHERE l_orderkey = o_orderkey
  AND o_orderdate < DATE '1995-03-15'
  AND l_shipdate > DATE '1995-03-15'
GROUP BY l_orderkey, o_orderdate, o_shippriority
ORDER BY revenue DESC, o_orderdate
LIMIT 10
"""

def run_query_engine(sql: str) -> dict:
    """Run query through our query engine via CLI."""
    # Write SQL to temp file
    with open("/tmp/benchmark_query.sql", "w") as f:
        f.write(sql)

    start = time.time()
    result = subprocess.run(
        ["./target/release/query_engine", "sql", "/tmp/benchmark_query.sql", "--sf", "0.01"],
        capture_output=True,
        text=True,
        cwd="/Users/elonazoulay/src/trino/otherdirs/iceberg-query-engine"
    )
    elapsed = time.time() - start

    return {
        "time": elapsed,
        "stdout": result.stdout,
        "stderr": result.stderr,
        "success": result.returncode == 0
    }

def run_duckdb(sql: str) -> dict:
    """Run query through DuckDB."""
    # For DuckDB, we'd need to generate/load TPC-H data first
    # Using simple CSV-based approach for now
    ddb_script = f"""
    -- Install TPC-H extension and generate data
    INSTALL tpch;
    LOAD tpch;
    CALL dbgen(sf=0.01);

    -- Run query
    {sql};

    -- Show timing info
    SELECT * FROM duckdb_queries();
    """

    start = time.time()
    result = subprocess.run(
        ["python3", "-c", f"import duckdb; conn = duckdb.connect(); conn.execute('INSTALL tpch; LOAD tpch; CALL dbgen(sf=0.01);'); result = conn.execute(\"\"\"{sql}\"\"\").fetchall(); print(f'Rows: {{len(result)}}'); conn.execute(\"\"\"SELECT * FROM duckdb_queries();\"\"\").fetchall()"],
        capture_output=True,
        text=True,
        timeout=60
    )
    elapsed = time.time() - start

    return {
        "time": elapsed,
        "stdout": result.stdout,
        "stderr": result.stderr,
        "success": result.returncode == 0
    }

def main():
    print("=" * 60)
    print("Query Engine vs DuckDB Performance Comparison")
    print("=" * 60)
    print()

    queries = [("Q1", Q1), ("Q3", Q3)]

    for name, sql in queries:
        print(f"\n{'─' * 60}")
        print(f"Query: {name}")
        print(f"{'─' * 60}")

        # Run with our query engine
        print("\n🦀 Query Engine:")
        print("-" * 40)
        qe_result = run_query_engine(sql)
        if qe_result["success"]:
            print(f"✓ Time: {qe_result['time']:.3f}s")
            # Extract row count from output
            for line in qe_result["stdout"].split("\n"):
                if "row count:" in line.lower() or "rows:" in line.lower():
                    print(f"  {line.strip()}")
        else:
            print(f"✗ Error: {qe_result['stderr'][:200]}")

        # Run with DuckDB
        print("\n🦆 DuckDB:")
        print("-" * 40)
        try:
            dd_result = run_duckdb(sql)
            if dd_result["success"]:
                print(f"✓ Time: {dd_result['time']:.3f}s")
                print(f"  {dd_result['stdout'][:200]}")
            else:
                print(f"✗ Error: {dd_result['stderr'][:200]}")
        except Exception as e:
            print(f"✗ Exception: {e}")

        print()

if __name__ == "__main__":
    main()
