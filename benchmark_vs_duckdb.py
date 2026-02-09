#!/usr/bin/env python3
"""
TPC-H Benchmark: Our Engine vs DuckDB
Compares query execution times and validates result row counts.
"""

import subprocess
import time
import duckdb
import re
from pathlib import Path

DATA_DIR = "data/tpch-benchmark"

# TPC-H Queries (same as used by our engine)
QUERIES = {
    1: """SELECT l_returnflag, l_linestatus, SUM(l_quantity) AS sum_qty, SUM(l_extendedprice) AS sum_base_price, SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price, SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge, AVG(l_quantity) AS avg_qty, AVG(l_extendedprice) AS avg_price, AVG(l_discount) AS avg_disc, COUNT(*) AS count_order FROM lineitem WHERE l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus""",

    2: """SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment FROM part, supplier, partsupp, nation, region WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND p_size = 15 AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = 'EUROPE' ORDER BY s_acctbal DESC, n_name, s_name, p_partkey LIMIT 100""",

    3: """SELECT l_orderkey, SUM(l_extendedprice * (1 - l_discount)) AS revenue, o_orderdate, o_shippriority FROM customer, orders, lineitem WHERE c_mktsegment = 'BUILDING' AND c_custkey = o_custkey AND l_orderkey = o_orderkey AND o_orderdate < DATE '1995-03-15' AND l_shipdate > DATE '1995-03-15' GROUP BY l_orderkey, o_orderdate, o_shippriority ORDER BY revenue DESC, o_orderdate LIMIT 10""",

    4: """SELECT o_orderpriority, COUNT(*) AS order_count FROM orders WHERE o_orderdate >= DATE '1993-07-01' AND o_orderdate < DATE '1993-10-01' AND EXISTS (SELECT * FROM lineitem WHERE l_orderkey = o_orderkey AND l_commitdate < l_receiptdate) GROUP BY o_orderpriority ORDER BY o_orderpriority""",

    5: """SELECT n_name, SUM(l_extendedprice * (1 - l_discount)) AS revenue FROM customer, orders, lineitem, supplier, nation, region WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = 'ASIA' AND o_orderdate >= DATE '1994-01-01' AND o_orderdate < DATE '1995-01-01' GROUP BY n_name ORDER BY revenue DESC""",

    6: """SELECT SUM(l_extendedprice * l_discount) AS revenue FROM lineitem WHERE l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01' AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24""",

    7: """SELECT supp_nation, cust_nation, l_year, SUM(volume) AS revenue FROM (SELECT n1.n_name AS supp_nation, n2.n_name AS cust_nation, EXTRACT(YEAR FROM l_shipdate) AS l_year, l_extendedprice * (1 - l_discount) AS volume FROM supplier, lineitem, orders, customer, nation n1, nation n2 WHERE s_suppkey = l_suppkey AND o_orderkey = l_orderkey AND c_custkey = o_custkey AND s_nationkey = n1.n_nationkey AND c_nationkey = n2.n_nationkey AND ((n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY') OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE')) AND l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31') AS shipping GROUP BY supp_nation, cust_nation, l_year ORDER BY supp_nation, cust_nation, l_year""",

    8: """SELECT o_year, SUM(CASE WHEN nation = 'BRAZIL' THEN volume ELSE 0 END) / SUM(volume) AS mkt_share FROM (SELECT EXTRACT(YEAR FROM o_orderdate) AS o_year, l_extendedprice * (1 - l_discount) AS volume, n2.n_name AS nation FROM part, supplier, lineitem, orders, customer, nation n1, nation n2, region WHERE p_partkey = l_partkey AND s_suppkey = l_suppkey AND l_orderkey = o_orderkey AND o_custkey = c_custkey AND c_nationkey = n1.n_nationkey AND n1.n_regionkey = r_regionkey AND r_name = 'AMERICA' AND s_nationkey = n2.n_nationkey AND o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31' AND p_type = 'STANDARD ANODIZED TIN') AS all_nations GROUP BY o_year ORDER BY o_year""",

    9: """SELECT nation, o_year, SUM(amount) AS sum_profit FROM (SELECT n_name AS nation, EXTRACT(YEAR FROM o_orderdate) AS o_year, l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount FROM part, supplier, lineitem, partsupp, orders, nation WHERE s_suppkey = l_suppkey AND ps_suppkey = l_suppkey AND ps_partkey = l_partkey AND p_partkey = l_partkey AND o_orderkey = l_orderkey AND s_nationkey = n_nationkey AND p_name LIKE '%green%') AS profit GROUP BY nation, o_year ORDER BY nation, o_year DESC""",

    10: """SELECT c_custkey, c_name, SUM(l_extendedprice * (1 - l_discount)) AS revenue, c_acctbal, n_name, c_address, c_phone, c_comment FROM customer, orders, lineitem, nation WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND o_orderdate >= DATE '1993-10-01' AND o_orderdate < DATE '1994-01-01' AND l_returnflag = 'R' AND c_nationkey = n_nationkey GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment ORDER BY revenue DESC LIMIT 20""",

    11: """SELECT ps_partkey, SUM(ps_supplycost * ps_availqty) AS value FROM partsupp, supplier, nation WHERE ps_suppkey = s_suppkey AND s_nationkey = n_nationkey AND n_name = 'GERMANY' GROUP BY ps_partkey HAVING SUM(ps_supplycost * ps_availqty) > (SELECT SUM(ps_supplycost * ps_availqty) * 0.0001 FROM partsupp, supplier, nation WHERE ps_suppkey = s_suppkey AND s_nationkey = n_nationkey AND n_name = 'GERMANY') ORDER BY value DESC LIMIT 100""",

    12: """SELECT l_shipmode, SUM(CASE WHEN o_orderpriority = '1-URGENT' OR o_orderpriority = '2-HIGH' THEN 1 ELSE 0 END) AS high_line_count, SUM(CASE WHEN o_orderpriority <> '1-URGENT' AND o_orderpriority <> '2-HIGH' THEN 1 ELSE 0 END) AS low_line_count FROM orders, lineitem WHERE o_orderkey = l_orderkey AND l_shipmode IN ('MAIL', 'SHIP') AND l_commitdate < l_receiptdate AND l_shipdate < l_commitdate AND l_receiptdate >= DATE '1994-01-01' AND l_receiptdate < DATE '1995-01-01' GROUP BY l_shipmode ORDER BY l_shipmode""",

    13: """SELECT c_count, COUNT(*) AS custdist FROM (SELECT c_custkey, COUNT(o_orderkey) AS c_count FROM customer LEFT OUTER JOIN orders ON c_custkey = o_custkey AND o_comment NOT LIKE '%special%requests%' GROUP BY c_custkey) AS c_orders GROUP BY c_count ORDER BY custdist DESC, c_count DESC LIMIT 100""",

    14: """SELECT 100.00 * SUM(CASE WHEN p_type LIKE 'PROMO%' THEN l_extendedprice * (1 - l_discount) ELSE 0 END) / SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue FROM lineitem, part WHERE l_partkey = p_partkey AND l_shipdate >= DATE '1995-09-01' AND l_shipdate < DATE '1995-10-01'""",

    15: """WITH revenue AS (SELECT l_suppkey AS supplier_no, SUM(l_extendedprice * (1 - l_discount)) AS total_revenue FROM lineitem WHERE l_shipdate >= DATE '1996-01-01' AND l_shipdate < DATE '1996-04-01' GROUP BY l_suppkey) SELECT s_suppkey, s_name, s_address, s_phone, total_revenue FROM supplier, revenue WHERE s_suppkey = supplier_no AND total_revenue = (SELECT MAX(total_revenue) FROM revenue) ORDER BY s_suppkey""",

    16: """SELECT p_brand, p_type, p_size, COUNT(DISTINCT ps_suppkey) AS supplier_cnt FROM partsupp, part WHERE p_partkey = ps_partkey AND p_brand <> 'Brand#45' AND p_type NOT LIKE 'MEDIUM POLISHED%' AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9) AND ps_suppkey NOT IN (SELECT s_suppkey FROM supplier WHERE s_comment LIKE '%Customer%Complaints%') GROUP BY p_brand, p_type, p_size ORDER BY supplier_cnt DESC, p_brand, p_type, p_size LIMIT 100""",

    17: """SELECT SUM(l_extendedprice) / 7.0 AS avg_yearly FROM lineitem, part WHERE p_partkey = l_partkey AND p_brand = 'Brand#23' AND p_container = 'MED BOX' AND l_quantity < (SELECT 0.2 * AVG(l_quantity) FROM lineitem WHERE l_partkey = p_partkey)""",

    18: """SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, SUM(l_quantity) FROM customer, orders, lineitem WHERE o_orderkey IN (SELECT l_orderkey FROM lineitem GROUP BY l_orderkey HAVING SUM(l_quantity) > 300) AND c_custkey = o_custkey AND o_orderkey = l_orderkey GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice ORDER BY o_totalprice DESC, o_orderdate LIMIT 100""",

    19: """SELECT SUM(l_extendedprice* (1 - l_discount)) AS revenue FROM lineitem, part WHERE (p_partkey = l_partkey AND p_brand = 'Brand#12' AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') AND l_quantity >= 1 AND l_quantity <= 11 AND p_size BETWEEN 1 AND 5 AND l_shipmode IN ('AIR', 'REG AIR') AND l_shipinstruct = 'DELIVER IN PERSON') OR (p_partkey = l_partkey AND p_brand = 'Brand#23' AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') AND l_quantity >= 10 AND l_quantity <= 20 AND p_size BETWEEN 1 AND 10 AND l_shipmode IN ('AIR', 'REG AIR') AND l_shipinstruct = 'DELIVER IN PERSON') OR (p_partkey = l_partkey AND p_brand = 'Brand#34' AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') AND l_quantity >= 20 AND l_quantity <= 30 AND p_size BETWEEN 1 AND 15 AND l_shipmode IN ('AIR', 'REG AIR') AND l_shipinstruct = 'DELIVER IN PERSON')""",

    20: """SELECT s_name, s_address FROM supplier, nation WHERE s_suppkey IN (SELECT ps_suppkey FROM partsupp WHERE ps_partkey IN (SELECT p_partkey FROM part WHERE p_name LIKE 'Part 1%') AND ps_availqty > (SELECT 0.5 * SUM(l_quantity) FROM lineitem WHERE l_partkey = ps_partkey AND l_suppkey = ps_suppkey AND l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01')) AND s_nationkey = n_nationkey AND n_name = 'CANADA' ORDER BY s_name""",

    21: """SELECT s_name, COUNT(*) AS numwait FROM supplier, lineitem l1, orders, nation WHERE s_suppkey = l1.l_suppkey AND o_orderkey = l1.l_orderkey AND o_orderstatus = 'F' AND l1.l_receiptdate > l1.l_commitdate AND EXISTS (SELECT * FROM lineitem l2 WHERE l2.l_orderkey = l1.l_orderkey AND l2.l_suppkey <> l1.l_suppkey) AND NOT EXISTS (SELECT * FROM lineitem l3 WHERE l3.l_orderkey = l1.l_orderkey AND l3.l_suppkey <> l1.l_suppkey AND l3.l_receiptdate > l3.l_commitdate) AND s_nationkey = n_nationkey AND n_name = 'SAUDI ARABIA' GROUP BY s_name ORDER BY numwait DESC, s_name LIMIT 100""",

    22: """SELECT cntrycode, COUNT(*) AS numcust, SUM(c_acctbal) AS totacctbal FROM (SELECT SUBSTRING(c_phone FROM 1 FOR 2) AS cntrycode, c_acctbal FROM customer WHERE SUBSTRING(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17') AND c_acctbal > (SELECT AVG(c_acctbal) FROM customer WHERE c_acctbal > 0.00 AND SUBSTRING(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17')) AND NOT EXISTS (SELECT * FROM orders WHERE o_custkey = c_custkey)) AS custsale GROUP BY cntrycode ORDER BY cntrycode""",
}


def run_engine_benchmark() -> dict:
    """Run all TPC-H queries using our engine's benchmark-parquet command"""
    cmd = [
        "./target/release/query_engine",
        "benchmark-parquet",
        "--path", DATA_DIR,
        "--iterations", "1"
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
    output = result.stdout + result.stderr

    # Parse output: "Q01:        6 rows in   37.076ms"
    results = {}
    for line in output.split('\n'):
        match = re.match(r'Q(\d+):\s+(\d+)\s+rows\s+in\s+([0-9.]+)ms', line)
        if match:
            q = int(match.group(1))
            rows = int(match.group(2))
            time_ms = float(match.group(3))
            results[q] = {"rows": rows, "time_ms": time_ms}
        elif "ERROR" in line:
            match = re.match(r'Q(\d+):', line)
            if match:
                q = int(match.group(1))
                results[q] = {"rows": -1, "time_ms": -1, "error": line}

    return results


def run_duckdb_benchmark(conn: duckdb.DuckDBPyConnection) -> dict:
    """Run all TPC-H queries using DuckDB"""
    results = {}
    for q, query in QUERIES.items():
        try:
            start = time.perf_counter()
            result = conn.execute(query).fetchall()
            elapsed_ms = (time.perf_counter() - start) * 1000
            results[q] = {"rows": len(result), "time_ms": elapsed_ms}
        except Exception as e:
            results[q] = {"rows": -1, "time_ms": -1, "error": str(e)}
    return results


def setup_duckdb() -> duckdb.DuckDBPyConnection:
    """Create DuckDB connection and load TPC-H tables"""
    conn = duckdb.connect(":memory:")
    tables = ["nation", "region", "part", "supplier", "partsupp", "customer", "orders", "lineitem"]
    for table in tables:
        path = f"{DATA_DIR}/{table}.parquet"
        conn.execute(f"CREATE TABLE {table} AS SELECT * FROM read_parquet('{path}')")
    return conn


def main():
    print("# TPC-H Benchmark: Our Engine vs DuckDB (SF=0.1)")
    print(f"# Data: {DATA_DIR}")
    print()

    # Run our engine benchmark
    print("Running our engine benchmark...", end=" ", flush=True)
    engine_results = run_engine_benchmark()
    print("done.")

    # Setup and run DuckDB benchmark
    print("Running DuckDB benchmark...", end=" ", flush=True)
    conn = setup_duckdb()
    duck_results = run_duckdb_benchmark(conn)
    conn.close()
    print("done.")
    print()

    # Print comparison table
    print("| Query | Engine (ms) | DuckDB (ms) | Ratio | Engine Rows | DuckDB Rows | Match | Notes |")
    print("|-------|-------------|-------------|-------|-------------|-------------|-------|-------|")

    total_engine = 0
    total_duck = 0

    for q in range(1, 23):
        e = engine_results.get(q, {"rows": -1, "time_ms": -1})
        d = duck_results.get(q, {"rows": -1, "time_ms": -1})

        e_time = e["time_ms"]
        d_time = d["time_ms"]
        e_rows = e["rows"]
        d_rows = d["rows"]

        # Calculate ratio
        if e_time > 0 and d_time > 0:
            ratio = f"{e_time / d_time:.1f}"
            total_engine += e_time
            total_duck += d_time
        else:
            ratio = "N/A"

        # Check match
        if e_rows == d_rows:
            match = "Yes"
        elif e_rows < 0 or d_rows < 0:
            match = "ERR"
        else:
            match = f"**{e_rows} vs {d_rows}**"

        # Notes
        notes = ""
        if "error" in e:
            notes = "Engine err"
        if "error" in d:
            notes += " Duck err"

        # Format times
        e_str = f"{e_time:.0f}" if e_time >= 0 else "ERR"
        d_str = f"{d_time:.0f}" if d_time >= 0 else "ERR"

        print(f"| Q{q:02d} | {e_str:>11} | {d_str:>11} | {ratio:>5}x | {e_rows:>11} | {d_rows:>11} | {match:>5} | {notes} |")

    # Summary
    print()
    print("## Summary")
    if total_duck > 0:
        overall_ratio = total_engine / total_duck
        print(f"- **Total Engine Time**: {total_engine:.0f}ms ({total_engine/1000:.1f}s)")
        print(f"- **Total DuckDB Time**: {total_duck:.0f}ms ({total_duck/1000:.2f}s)")
        print(f"- **Overall Ratio**: {overall_ratio:.1f}x slower")

    matches = sum(1 for q in range(1, 23)
                  if engine_results.get(q, {}).get("rows") == duck_results.get(q, {}).get("rows"))
    print(f"- **Row Count Matches**: {matches}/22")


if __name__ == "__main__":
    main()
