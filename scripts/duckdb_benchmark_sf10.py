#!/usr/bin/env python3
"""
Benchmark specific TPC-H queries against DuckDB at SF=10.
Runs each query 3 times and reports best time.
"""

import duckdb
import time
import sys

DATA_DIR = "/media/afilipchik/nvme6tb/src/afilipchik/iceberg-query-engine/data/tpch-10gb"

TABLES = ["lineitem", "orders", "customer", "supplier", "part", "partsupp", "nation", "region"]

# Exact queries from src/tpch/queries.rs
# Q11 adjusted for SF=10: threshold = 0.0001/10 = 0.00001
QUERIES = {
    5: """
SELECT
    n_name,
    SUM(l_extendedprice * (1 - l_discount)) AS revenue
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'ASIA'
    AND o_orderdate >= DATE '1994-01-01'
    AND o_orderdate < DATE '1995-01-01'
GROUP BY
    n_name
ORDER BY
    revenue DESC
""",

    8: """
SELECT
    EXTRACT(YEAR FROM o_orderdate) AS o_year,
    SUM(CASE WHEN n2.n_name = 'BRAZIL' THEN l_extendedprice * (1 - l_discount) ELSE 0.0 END) / SUM(l_extendedprice * (1 - l_discount)) AS mkt_share
FROM
    part,
    supplier,
    lineitem,
    orders,
    customer,
    nation n1,
    nation n2,
    region
WHERE
    p_partkey = l_partkey
    AND s_suppkey = l_suppkey
    AND l_orderkey = o_orderkey
    AND o_custkey = c_custkey
    AND c_nationkey = n1.n_nationkey
    AND n1.n_regionkey = r_regionkey
    AND r_name = 'AMERICA'
    AND s_nationkey = n2.n_nationkey
    AND o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
    AND p_type = 'STANDARD ANODIZED TIN'
GROUP BY
    EXTRACT(YEAR FROM o_orderdate)
ORDER BY
    o_year
""",

    11: """
SELECT
    ps_partkey,
    SUM(ps_supplycost * ps_availqty) AS value
FROM
    partsupp,
    supplier,
    nation
WHERE
    ps_suppkey = s_suppkey
    AND s_nationkey = n_nationkey
    AND n_name = 'GERMANY'
GROUP BY
    ps_partkey
HAVING
    SUM(ps_supplycost * ps_availqty) > (
        SELECT SUM(ps_supplycost * ps_availqty) * 0.00001
        FROM partsupp, supplier, nation
        WHERE ps_suppkey = s_suppkey
        AND s_nationkey = n_nationkey
        AND n_name = 'GERMANY'
    )
ORDER BY
    value DESC
LIMIT 100
""",

    13: """
SELECT
    c_count,
    COUNT(*) AS custdist
FROM (
    SELECT
        c_custkey,
        COUNT(o_orderkey) AS c_count
    FROM
        customer
        LEFT OUTER JOIN orders ON c_custkey = o_custkey
    GROUP BY
        c_custkey
) AS c_orders
GROUP BY
    c_count
ORDER BY
    custdist DESC,
    c_count DESC
LIMIT 100
""",

    15: """
WITH revenue AS (
    SELECT
        l_suppkey,
        SUM(l_extendedprice * (1 - l_discount)) AS total_revenue
    FROM
        lineitem
    WHERE
        l_shipdate >= DATE '1996-01-01'
        AND l_shipdate < DATE '1996-04-01'
    GROUP BY
        l_suppkey
)
SELECT
    s_suppkey,
    s_name,
    s_address,
    s_phone,
    total_revenue
FROM
    supplier,
    revenue
WHERE
    s_suppkey = revenue.l_suppkey
    AND total_revenue = (
        SELECT MAX(total_revenue)
        FROM revenue
    )
ORDER BY
    total_revenue DESC
""",

    17: """
SELECT
    SUM(l_extendedprice) / 7.0 AS avg_yearly
FROM
    lineitem,
    part
WHERE
    p_partkey = l_partkey
    AND p_brand = 'Brand#23'
    AND p_container = 'MED BOX'
    AND l_quantity < (
        SELECT 0.2 * AVG(l_quantity)
        FROM lineitem
        WHERE l_partkey = p_partkey
    )
""",

    20: """
SELECT
    s_name,
    s_address
FROM
    supplier,
    nation
WHERE
    s_suppkey IN (
        SELECT ps_suppkey
        FROM partsupp
        WHERE ps_partkey IN (
            SELECT p_partkey
            FROM part
            WHERE p_name LIKE 'Part 1%'
        )
        AND ps_availqty > (
            SELECT 0.5 * SUM(l_quantity)
            FROM lineitem
            WHERE l_partkey = ps_partkey
            AND l_suppkey = ps_suppkey
            AND l_shipdate >= DATE '1994-01-01'
            AND l_shipdate < DATE '1995-01-01'
        )
    )
    AND s_nationkey = n_nationkey
    AND n_name = 'CANADA'
ORDER BY
    s_name
""",

    22: """
SELECT
    cntrycode,
    COUNT(*) AS numcust,
    SUM(c_acctbal) AS totacctbal
FROM (
    SELECT
        SUBSTRING(c_phone FROM 1 FOR 2) AS cntrycode,
        c_acctbal
    FROM
        customer
    WHERE
        SUBSTRING(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17')
        AND c_acctbal > (
            SELECT AVG(c_acctbal)
            FROM customer
            WHERE c_acctbal > 0.00
            AND SUBSTRING(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17')
        )
        AND NOT EXISTS (
            SELECT *
            FROM orders
            WHERE o_custkey = c_custkey
        )
) AS custsale
GROUP BY
    cntrycode
ORDER BY
    cntrycode
""",
}

ITERATIONS = 3

def main():
    print(f"DuckDB TPC-H Benchmark (SF=10)")
    print(f"Data directory: {DATA_DIR}")
    print(f"Iterations per query: {ITERATIONS}")
    print(f"Queries: {sorted(QUERIES.keys())}")
    print("=" * 70)

    # Create connection and load data
    con = duckdb.connect()

    # Set threads to use all cores and cap memory at 64GB for fair comparison
    con.execute("SET threads TO 16")
    con.execute("SET memory_limit = '64GB'")

    print("\nLoading tables...")
    for table in TABLES:
        path = f"{DATA_DIR}/{table}.parquet"
        t0 = time.perf_counter()
        con.execute(f"CREATE TABLE {table} AS SELECT * FROM read_parquet('{path}')")
        elapsed = time.perf_counter() - t0
        count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"  {table}: {count:,} rows ({elapsed:.2f}s)")

    print("\n" + "=" * 70)
    print(f"{'Query':<8} {'Best (ms)':<12} {'Med (ms)':<12} {'All times (ms)'}")
    print("-" * 70)

    results = {}

    for qnum in sorted(QUERIES.keys()):
        sql = QUERIES[qnum]
        times = []

        for i in range(ITERATIONS):
            t0 = time.perf_counter()
            result = con.execute(sql).fetchall()
            elapsed_ms = (time.perf_counter() - t0) * 1000
            times.append(elapsed_ms)

            if i == 0:
                row_count = len(result)

        best = min(times)
        times_sorted = sorted(times)
        median = times_sorted[len(times_sorted) // 2]
        times_str = ", ".join(f"{t:.1f}" for t in times)

        results[qnum] = {
            "best": best,
            "median": median,
            "all": times,
            "rows": row_count,
        }

        print(f"Q{qnum:<7} {best:<12.1f} {median:<12.1f} [{times_str}]  ({row_count} rows)")

    print("\n" + "=" * 70)
    print("\nSummary (best times):")
    print("-" * 40)
    total = 0
    for qnum in sorted(results.keys()):
        best = results[qnum]["best"]
        total += best
        print(f"  Q{qnum:02d}: {best:>8.1f} ms")
    print(f"  {'TOTAL':>4}: {total:>8.1f} ms")

    con.close()

if __name__ == "__main__":
    main()
