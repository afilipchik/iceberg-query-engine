#!/usr/bin/env python3
"""Generate expected query results using DuckDB as the oracle.

Loads TPC-H parquet data into DuckDB, runs all test queries,
and writes CSV expected results for the Rust integration test suite.

Usage:
    python scripts/generate_expected_results.py --data-dir data/tpch-1mb --output-dir tests/expected_results
"""

import argparse
import json
import os
import sys
import duckdb


def get_queries():
    """Return all test queries as a list of (id, sql, ordered) tuples.

    `ordered` is True if the query has ORDER BY (so row order matters).
    """
    queries = []

    # =========================================================================
    # TPC-H Queries (Q01-Q22) — copied exactly from src/tpch/queries.rs
    # =========================================================================

    queries.append(("tpch/q01", """
SELECT
    l_returnflag,
    l_linestatus,
    SUM(l_quantity) AS sum_qty,
    SUM(l_extendedprice) AS sum_base_price,
    SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
    SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
    AVG(l_quantity) AS avg_qty,
    AVG(l_extendedprice) AS avg_price,
    AVG(l_discount) AS avg_disc,
    COUNT(*) AS count_order
FROM
    lineitem
WHERE
    l_shipdate <= DATE '1998-09-02'
GROUP BY
    l_returnflag,
    l_linestatus
ORDER BY
    l_returnflag,
    l_linestatus
""", True))

    queries.append(("tpch/q02", """
SELECT
    s_acctbal,
    s_name,
    n_name,
    p_partkey,
    p_mfgr,
    s_address,
    s_phone,
    s_comment
FROM
    part,
    supplier,
    partsupp,
    nation,
    region
WHERE
    p_partkey = ps_partkey
    AND s_suppkey = ps_suppkey
    AND p_size = 15
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'EUROPE'
ORDER BY
    s_acctbal DESC,
    n_name,
    s_name,
    p_partkey
LIMIT 100
""", True))

    queries.append(("tpch/q03", """
SELECT
    l_orderkey,
    SUM(l_extendedprice * (1 - l_discount)) AS revenue,
    o_orderdate,
    o_shippriority
FROM
    customer,
    orders,
    lineitem
WHERE
    c_mktsegment = 'BUILDING'
    AND c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND o_orderdate < DATE '1995-03-15'
    AND l_shipdate > DATE '1995-03-15'
GROUP BY
    l_orderkey,
    o_orderdate,
    o_shippriority
ORDER BY
    revenue DESC,
    o_orderdate
LIMIT 10
""", True))

    queries.append(("tpch/q04", """
SELECT
    o_orderpriority,
    COUNT(*) AS order_count
FROM
    orders
WHERE
    o_orderdate >= DATE '1993-07-01'
    AND o_orderdate < DATE '1993-10-01'
GROUP BY
    o_orderpriority
ORDER BY
    o_orderpriority
""", True))

    queries.append(("tpch/q05", """
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
""", True))

    queries.append(("tpch/q06", """
SELECT
    SUM(l_extendedprice * l_discount) AS revenue
FROM
    lineitem
WHERE
    l_shipdate >= DATE '1994-01-01'
    AND l_shipdate < DATE '1995-01-01'
    AND l_discount >= 0.05
    AND l_discount <= 0.07
    AND l_quantity < 24
""", False))

    queries.append(("tpch/q07", """
SELECT
    n1.n_name AS supp_nation,
    n2.n_name AS cust_nation,
    EXTRACT(YEAR FROM l_shipdate) AS l_year,
    SUM(l_extendedprice * (1 - l_discount)) AS revenue
FROM
    supplier,
    lineitem,
    orders,
    customer,
    nation n1,
    nation n2
WHERE
    s_suppkey = l_suppkey
    AND o_orderkey = l_orderkey
    AND c_custkey = o_custkey
    AND s_nationkey = n1.n_nationkey
    AND c_nationkey = n2.n_nationkey
    AND ((n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
        OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE'))
    AND l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
GROUP BY
    n1.n_name,
    n2.n_name,
    EXTRACT(YEAR FROM l_shipdate)
ORDER BY
    supp_nation,
    cust_nation,
    l_year
""", True))

    queries.append(("tpch/q08", """
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
""", True))

    queries.append(("tpch/q09", """
SELECT
    n_name AS nation,
    EXTRACT(YEAR FROM o_orderdate) AS o_year,
    SUM(l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity) AS sum_profit
FROM
    part,
    supplier,
    lineitem,
    partsupp,
    orders,
    nation
WHERE
    s_suppkey = l_suppkey
    AND ps_suppkey = l_suppkey
    AND ps_partkey = l_partkey
    AND p_partkey = l_partkey
    AND o_orderkey = l_orderkey
    AND s_nationkey = n_nationkey
    AND p_name LIKE 'Part 1%'
GROUP BY
    n_name,
    EXTRACT(YEAR FROM o_orderdate)
ORDER BY
    nation,
    o_year DESC
""", True))

    queries.append(("tpch/q10", """
SELECT
    c_custkey,
    c_name,
    SUM(l_extendedprice * (1 - l_discount)) AS revenue,
    c_acctbal,
    n_name,
    c_address,
    c_phone,
    c_comment
FROM
    customer,
    orders,
    lineitem,
    nation
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND o_orderdate >= DATE '1993-10-01'
    AND o_orderdate < DATE '1994-01-01'
    AND l_returnflag = 'R'
    AND c_nationkey = n_nationkey
GROUP BY
    c_custkey,
    c_name,
    c_acctbal,
    c_phone,
    n_name,
    c_address,
    c_comment
ORDER BY
    revenue DESC
LIMIT 20
""", True))

    queries.append(("tpch/q11", """
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
        SELECT SUM(ps_supplycost * ps_availqty) * 0.0001
        FROM partsupp, supplier, nation
        WHERE ps_suppkey = s_suppkey
        AND s_nationkey = n_nationkey
        AND n_name = 'GERMANY'
    )
ORDER BY
    value DESC
LIMIT 100
""", True))

    queries.append(("tpch/q12", """
SELECT
    l_shipmode,
    SUM(CASE
        WHEN o_orderpriority = '1-URGENT'
            OR o_orderpriority = '2-HIGH'
            THEN 1
        ELSE 0
    END) AS high_line_count,
    SUM(CASE
        WHEN o_orderpriority <> '1-URGENT'
            AND o_orderpriority <> '2-HIGH'
            THEN 1
        ELSE 0
    END) AS low_line_count
FROM
    orders,
    lineitem
WHERE
    o_orderkey = l_orderkey
    AND l_shipmode IN ('MAIL', 'SHIP')
    AND l_commitdate < l_receiptdate
    AND l_shipdate < l_commitdate
    AND l_receiptdate >= DATE '1994-01-01'
    AND l_receiptdate < DATE '1995-01-01'
GROUP BY
    l_shipmode
ORDER BY
    l_shipmode
""", True))

    queries.append(("tpch/q13", """
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
""", True))

    queries.append(("tpch/q14", """
SELECT
    SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue
FROM
    lineitem,
    part
WHERE
    l_partkey = p_partkey
    AND l_shipdate >= DATE '1995-09-01'
    AND l_shipdate < DATE '1995-10-01'
""", False))

    queries.append(("tpch/q15", """
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
""", True))

    queries.append(("tpch/q16", """
SELECT
    p_brand,
    p_type,
    p_size,
    COUNT(ps_suppkey) AS supplier_cnt
FROM
    partsupp,
    part
WHERE
    p_partkey = ps_partkey
    AND p_brand <> 'Brand#45'
    AND p_size >= 1
GROUP BY
    p_brand,
    p_type,
    p_size
ORDER BY
    supplier_cnt DESC,
    p_brand,
    p_type,
    p_size
LIMIT 100
""", True))

    queries.append(("tpch/q17", """
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
""", False))

    queries.append(("tpch/q18", """
SELECT
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice,
    SUM(l_quantity) AS total_quantity
FROM
    customer,
    orders,
    lineitem
WHERE
    c_custkey = o_custkey
    AND o_orderkey = l_orderkey
GROUP BY
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice
ORDER BY
    o_totalprice DESC,
    o_orderdate
LIMIT 100
""", True))

    queries.append(("tpch/q19", """
SELECT
    SUM(l_extendedprice * (1 - l_discount)) AS revenue
FROM
    lineitem,
    part
WHERE
    p_partkey = l_partkey
    AND (
        (p_brand = 'Brand#12'
        AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
        AND l_quantity >= 1 AND l_quantity <= 11)
        OR (p_brand = 'Brand#23'
        AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
        AND l_quantity >= 10 AND l_quantity <= 20)
        OR (p_brand = 'Brand#34'
        AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
        AND l_quantity >= 20 AND l_quantity <= 30)
    )
    AND (
        (p_brand = 'Brand#12' AND l_shipmode IN ('AIR', 'AIR REG'))
        OR (p_brand = 'Brand#23' AND l_shipmode IN ('AIR', 'AIR REG'))
        OR (p_brand = 'Brand#34' AND l_shipmode IN ('AIR', 'AIR REG'))
    )
""", False))

    queries.append(("tpch/q20", """
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
""", True))

    queries.append(("tpch/q21", """
SELECT
    s_name,
    COUNT(*) AS numwait
FROM
    supplier,
    lineitem l1,
    orders,
    nation
WHERE
    s_suppkey = l1.l_suppkey
    AND o_orderkey = l1.l_orderkey
    AND o_orderstatus = 'F'
    AND l1.l_receiptdate > l1.l_commitdate
    AND EXISTS (
        SELECT *
        FROM lineitem l2
        WHERE l2.l_orderkey = l1.l_orderkey
        AND l2.l_suppkey <> l1.l_suppkey
    )
    AND NOT EXISTS (
        SELECT *
        FROM lineitem l3
        WHERE l3.l_orderkey = l1.l_orderkey
        AND l3.l_suppkey <> l1.l_suppkey
        AND l3.l_receiptdate > l3.l_commitdate
    )
    AND s_nationkey = n_nationkey
    AND n_name = 'SAUDI ARABIA'
GROUP BY
    s_name
ORDER BY
    numwait DESC,
    s_name
LIMIT 100
""", True))

    queries.append(("tpch/q22", """
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
""", True))

    # =========================================================================
    # Basic SELECT & WHERE (8 queries)
    # =========================================================================

    queries.append(("basic/select_all_nation", """
SELECT * FROM nation ORDER BY n_nationkey
""", True))

    queries.append(("basic/select_columns", """
SELECT n_nationkey, n_name FROM nation ORDER BY n_nationkey
""", True))

    queries.append(("basic/where_equals", """
SELECT n_nationkey, n_name FROM nation WHERE n_regionkey = 1 ORDER BY n_nationkey
""", True))

    queries.append(("basic/where_comparison", """
SELECT n_nationkey, n_name FROM nation WHERE n_nationkey > 10 AND n_nationkey <= 20 ORDER BY n_nationkey
""", True))

    queries.append(("basic/where_and_or", """
SELECT n_nationkey, n_name, n_regionkey FROM nation
WHERE (n_regionkey = 0 OR n_regionkey = 1) AND n_nationkey < 10
ORDER BY n_nationkey
""", True))

    queries.append(("basic/where_like", """
SELECT p_partkey, p_name FROM part WHERE p_name LIKE 'Part 1%' ORDER BY p_partkey LIMIT 20
""", True))

    queries.append(("basic/where_between", """
SELECT o_orderkey, o_totalprice FROM orders
WHERE o_totalprice BETWEEN 100000.0 AND 200000.0
ORDER BY o_orderkey
LIMIT 20
""", True))

    queries.append(("basic/where_in_list", """
SELECT n_nationkey, n_name FROM nation
WHERE n_name IN ('FRANCE', 'GERMANY', 'BRAZIL', 'JAPAN')
ORDER BY n_nationkey
""", True))

    # =========================================================================
    # ORDER BY & LIMIT (6 queries)
    # =========================================================================

    queries.append(("orderby/asc_desc", """
SELECT n_nationkey, n_name FROM nation ORDER BY n_name ASC LIMIT 10
""", True))

    queries.append(("orderby/desc", """
SELECT n_nationkey, n_name FROM nation ORDER BY n_nationkey DESC LIMIT 5
""", True))

    queries.append(("orderby/limit_offset", """
SELECT n_nationkey, n_name FROM nation ORDER BY n_nationkey LIMIT 5 OFFSET 10
""", True))

    queries.append(("orderby/multi_column", """
SELECT n_regionkey, n_nationkey, n_name FROM nation ORDER BY n_regionkey ASC, n_name ASC
""", True))

    queries.append(("orderby/expression", """
SELECT n_nationkey, n_name, n_regionkey FROM nation ORDER BY n_regionkey * 100 + n_nationkey
""", True))

    queries.append(("orderby/alias", """
SELECT n_nationkey AS id, n_name AS name FROM nation ORDER BY id DESC LIMIT 5
""", True))

    # =========================================================================
    # Aggregates & GROUP BY (10 queries)
    # =========================================================================

    queries.append(("agg/count_star", """
SELECT COUNT(*) AS cnt FROM lineitem
""", False))

    queries.append(("agg/count_column", """
SELECT COUNT(l_comment) AS cnt FROM lineitem
""", False))

    queries.append(("agg/sum_avg_min_max", """
SELECT
    SUM(l_quantity) AS total_qty,
    AVG(l_quantity) AS avg_qty,
    MIN(l_quantity) AS min_qty,
    MAX(l_quantity) AS max_qty
FROM lineitem
""", False))

    queries.append(("agg/count_distinct", """
SELECT COUNT(DISTINCT l_returnflag) AS distinct_flags FROM lineitem
""", False))

    queries.append(("agg/group_by_single", """
SELECT l_returnflag, COUNT(*) AS cnt FROM lineitem GROUP BY l_returnflag ORDER BY l_returnflag
""", True))

    queries.append(("agg/group_by_multiple", """
SELECT l_returnflag, l_linestatus, COUNT(*) AS cnt
FROM lineitem
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus
""", True))

    queries.append(("agg/having", """
SELECT l_returnflag, COUNT(*) AS cnt
FROM lineitem
GROUP BY l_returnflag
HAVING COUNT(*) > 1000
ORDER BY l_returnflag
""", True))

    queries.append(("agg/with_where", """
SELECT n_regionkey, COUNT(*) AS cnt, MIN(n_nationkey) AS min_key
FROM nation
WHERE n_nationkey > 5
GROUP BY n_regionkey
ORDER BY n_regionkey
""", True))

    queries.append(("agg/expression_group", """
SELECT n_regionkey, COUNT(*) AS cnt
FROM nation
GROUP BY n_regionkey
ORDER BY cnt DESC, n_regionkey
""", True))

    queries.append(("agg/empty_result", """
SELECT n_regionkey, COUNT(*) AS cnt, SUM(n_nationkey) AS total
FROM nation
WHERE n_nationkey > 9999
GROUP BY n_regionkey
ORDER BY n_regionkey
""", True))

    # =========================================================================
    # Joins (10 queries)
    # =========================================================================

    queries.append(("join/inner", """
SELECT n.n_name, r.r_name
FROM nation n, region r
WHERE n.n_regionkey = r.r_regionkey
ORDER BY n.n_name
""", True))

    queries.append(("join/left", """
SELECT c_custkey, o_orderkey
FROM customer
LEFT JOIN orders ON c_custkey = o_custkey
ORDER BY c_custkey, o_orderkey
LIMIT 20
""", True))

    queries.append(("join/right", """
SELECT c_custkey, o_orderkey, c_name
FROM orders
RIGHT JOIN customer ON o_custkey = c_custkey
ORDER BY c_custkey, o_orderkey
LIMIT 20
""", True))

    queries.append(("join/full_outer", """
SELECT n.n_name, r.r_name
FROM nation n
FULL OUTER JOIN region r ON n.n_regionkey = r.r_regionkey
ORDER BY n.n_name, r.r_name
LIMIT 30
""", True))

    queries.append(("join/cross", """
SELECT r1.r_name AS r1_name, r2.r_name AS r2_name
FROM region r1, region r2
ORDER BY r1_name, r2_name
""", True))

    queries.append(("join/self", """
SELECT n1.n_name AS name1, n2.n_name AS name2
FROM nation n1, nation n2
WHERE n1.n_regionkey = n2.n_regionkey AND n1.n_nationkey < n2.n_nationkey
ORDER BY name1, name2
LIMIT 20
""", True))

    queries.append(("join/multi_way", """
SELECT n.n_name, r.r_name, s.s_name
FROM nation n, region r, supplier s
WHERE n.n_regionkey = r.r_regionkey AND s.s_nationkey = n.n_nationkey
ORDER BY r.r_name, n.n_name, s.s_name
LIMIT 20
""", True))

    queries.append(("join/with_aggregate", """
SELECT n.n_name, COUNT(*) AS supplier_count
FROM nation n, supplier s
WHERE n.n_nationkey = s.s_nationkey
GROUP BY n.n_name
ORDER BY supplier_count DESC, n.n_name
LIMIT 10
""", True))

    queries.append(("join/with_filter", """
SELECT c_name, o_totalprice
FROM customer, orders
WHERE c_custkey = o_custkey AND o_totalprice > 200000.0
ORDER BY o_totalprice DESC, c_name
LIMIT 10
""", True))

    queries.append(("join/inequality", """
SELECT n1.n_name AS nation1, n2.n_name AS nation2
FROM nation n1, nation n2
WHERE n1.n_regionkey = n2.n_regionkey AND n1.n_nationkey < n2.n_nationkey
ORDER BY nation1, nation2
""", True))

    # =========================================================================
    # Subqueries (8 queries)
    # =========================================================================

    queries.append(("subquery/scalar", """
SELECT n_name
FROM nation
WHERE n_nationkey = (SELECT MIN(n_nationkey) FROM nation WHERE n_regionkey = 0)
""", False))

    queries.append(("subquery/in", """
SELECT s_name
FROM supplier
WHERE s_nationkey IN (SELECT n_nationkey FROM nation WHERE n_regionkey = 1)
ORDER BY s_name
LIMIT 10
""", True))

    queries.append(("subquery/not_in", """
SELECT n_name
FROM nation
WHERE n_nationkey NOT IN (SELECT s_nationkey FROM supplier)
ORDER BY n_name
""", True))

    queries.append(("subquery/exists", """
SELECT n_name
FROM nation n
WHERE EXISTS (
    SELECT 1 FROM supplier s WHERE s.s_nationkey = n.n_nationkey
)
ORDER BY n_name
""", True))

    queries.append(("subquery/not_exists", """
SELECT n_name
FROM nation n
WHERE NOT EXISTS (
    SELECT 1 FROM supplier s WHERE s.s_nationkey = n.n_nationkey
)
ORDER BY n_name
""", True))

    queries.append(("subquery/correlated_scalar", """
SELECT s.s_name, s.s_acctbal, n.n_name AS nation_name
FROM supplier s, nation n
WHERE n.n_nationkey = s.s_nationkey
ORDER BY s_name
LIMIT 10
""", True))

    queries.append(("subquery/from_clause", """
SELECT avg_price
FROM (
    SELECT AVG(l_extendedprice) AS avg_price
    FROM lineitem
    WHERE l_quantity > 20
) AS sub
""", False))

    queries.append(("subquery/nested", """
SELECT s_name
FROM supplier
WHERE s_nationkey IN (
    SELECT n_nationkey FROM nation
    WHERE n_regionkey IN (SELECT r_regionkey FROM region WHERE r_name = 'EUROPE')
)
ORDER BY s_name
LIMIT 10
""", True))

    # =========================================================================
    # Expressions & Functions (8 queries)
    # =========================================================================

    queries.append(("expr/arithmetic", """
SELECT
    SUM(l_extendedprice * (1 - l_discount)) AS total_disc_price,
    SUM(l_extendedprice + l_tax) AS total_price_tax,
    SUM(l_quantity * 2) AS total_double_qty,
    SUM(l_extendedprice - l_discount) AS total_minus_disc
FROM lineitem
WHERE l_orderkey <= 10
""", False))

    queries.append(("expr/case_when", """
SELECT l_returnflag,
    CASE
        WHEN l_returnflag = 'R' THEN 'Returned'
        WHEN l_returnflag = 'A' THEN 'Accepted'
        ELSE 'None'
    END AS flag_desc,
    COUNT(*) AS cnt
FROM lineitem
GROUP BY l_returnflag
ORDER BY l_returnflag
""", True))

    queries.append(("expr/coalesce_nullif", """
SELECT
    COALESCE(NULL, 'default') AS c1,
    COALESCE(NULL, NULL, 'fallback') AS c2,
    NULLIF(1, 1) AS n1,
    NULLIF(1, 2) AS n2
FROM nation
LIMIT 1
""", False))

    queries.append(("expr/cast", """
SELECT
    CAST(SUM(l_quantity) AS BIGINT) AS total_qty_int,
    CAST(42 AS DOUBLE) AS int_to_double,
    CAST(100 AS VARCHAR) AS int_to_str
FROM lineitem
WHERE l_orderkey = 1
""", False))

    queries.append(("expr/string_functions", """
SELECT
    n_nationkey,
    UPPER(n_name) AS upper_name,
    LOWER(n_name) AS lower_name,
    LENGTH(n_name) AS name_len,
    SUBSTRING(n_name FROM 1 FOR 3) AS name_prefix
FROM nation
ORDER BY n_nationkey
LIMIT 10
""", True))

    queries.append(("expr/math_functions", """
SELECT
    ABS(-42) AS abs_val,
    ROUND(3.7) AS rounded,
    FLOOR(3.7) AS floored,
    CEIL(3.2) AS ceiled
FROM nation
LIMIT 1
""", False))

    queries.append(("expr/date_extract", """
SELECT
    o_orderkey,
    CAST(EXTRACT(YEAR FROM o_orderdate) AS BIGINT) AS order_year,
    CAST(EXTRACT(MONTH FROM o_orderdate) AS BIGINT) AS order_month,
    CAST(EXTRACT(DAY FROM o_orderdate) AS BIGINT) AS order_day
FROM orders
ORDER BY o_orderkey
LIMIT 10
""", True))

    queries.append(("expr/trim_replace", """
SELECT
    TRIM('  hello  ') AS trimmed,
    REPLACE('hello world', 'world', 'rust') AS replaced,
    POSITION('llo' IN 'hello') AS pos
FROM nation
LIMIT 1
""", False))

    # =========================================================================
    # DISTINCT (3 queries)
    # =========================================================================

    queries.append(("distinct/simple", """
SELECT DISTINCT l_returnflag FROM lineitem ORDER BY l_returnflag
""", True))

    queries.append(("distinct/multi_column", """
SELECT DISTINCT l_returnflag, l_linestatus FROM lineitem ORDER BY l_returnflag, l_linestatus
""", True))

    queries.append(("distinct/with_expression", """
SELECT DISTINCT n_regionkey FROM nation ORDER BY n_regionkey
""", True))

    # =========================================================================
    # Set Operations (4 queries)
    # =========================================================================

    queries.append(("setop/union", """
SELECT n_name FROM nation WHERE n_regionkey = 0
UNION
SELECT n_name FROM nation WHERE n_regionkey = 1
ORDER BY n_name
""", True))

    queries.append(("setop/union_all", """
SELECT n_regionkey FROM nation WHERE n_nationkey < 5
UNION ALL
SELECT n_regionkey FROM nation WHERE n_nationkey >= 20
ORDER BY n_regionkey
""", True))

    queries.append(("setop/intersect", """
SELECT n_regionkey FROM nation WHERE n_nationkey < 10
INTERSECT
SELECT n_regionkey FROM nation WHERE n_nationkey >= 5
ORDER BY n_regionkey
""", True))

    queries.append(("setop/except", """
SELECT n_regionkey FROM nation WHERE n_nationkey < 15
EXCEPT
SELECT n_regionkey FROM nation WHERE n_nationkey >= 10
ORDER BY n_regionkey
""", True))

    # =========================================================================
    # Complex / Multi-Feature (7 queries)
    # =========================================================================

    queries.append(("complex/nested_subquery_agg", """
SELECT n_name, total_balance
FROM (
    SELECT n.n_name, SUM(s.s_acctbal) AS total_balance
    FROM nation n, supplier s
    WHERE n.n_nationkey = s.s_nationkey
    GROUP BY n.n_name
) sub
WHERE total_balance > 0
ORDER BY total_balance DESC
LIMIT 10
""", True))

    queries.append(("complex/multi_join_having", """
SELECT r.r_name, COUNT(DISTINCT s.s_suppkey) AS num_suppliers
FROM region r, nation n, supplier s
WHERE r.r_regionkey = n.n_regionkey AND n.n_nationkey = s.s_nationkey
GROUP BY r.r_name
HAVING COUNT(DISTINCT s.s_suppkey) > 1
ORDER BY num_suppliers DESC
""", True))

    queries.append(("complex/cte_with_join", """
WITH european_nations AS (
    SELECT n.n_nationkey, n.n_name
    FROM nation n, region r
    WHERE n.n_regionkey = r.r_regionkey AND r.r_name = 'EUROPE'
)
SELECT en.n_name, COUNT(*) AS supplier_count
FROM european_nations en, supplier s
WHERE en.n_nationkey = s.s_nationkey
GROUP BY en.n_name
ORDER BY supplier_count DESC, en.n_name
""", True))

    queries.append(("complex/derived_table_chain", """
SELECT region_name, avg_balance
FROM (
    SELECT r.r_name AS region_name, AVG(s.s_acctbal) AS avg_balance
    FROM region r, nation n, supplier s
    WHERE r.r_regionkey = n.n_regionkey AND n.n_nationkey = s.s_nationkey
    GROUP BY r.r_name
) sub
ORDER BY avg_balance DESC
""", True))

    queries.append(("complex/case_in_aggregate", """
SELECT
    n_regionkey,
    COUNT(*) AS total_nations,
    SUM(CASE WHEN n_nationkey < 10 THEN 1 ELSE 0 END) AS small_key_count,
    SUM(CASE WHEN n_nationkey >= 10 THEN 1 ELSE 0 END) AS large_key_count
FROM nation
GROUP BY n_regionkey
ORDER BY n_regionkey
""", True))

    queries.append(("complex/exists_with_aggregate", """
SELECT n_name
FROM nation n
WHERE EXISTS (
    SELECT 1
    FROM supplier s
    WHERE s.s_nationkey = n.n_nationkey
    AND s.s_acctbal > (
        SELECT AVG(s2.s_acctbal) FROM supplier s2
    )
)
ORDER BY n_name
""", True))

    queries.append(("complex/union_with_aggregate", """
SELECT 'high' AS category, COUNT(*) AS cnt
FROM orders WHERE o_totalprice > 200000
UNION ALL
SELECT 'medium' AS category, COUNT(*) AS cnt
FROM orders WHERE o_totalprice BETWEEN 100000 AND 200000
UNION ALL
SELECT 'low' AS category, COUNT(*) AS cnt
FROM orders WHERE o_totalprice < 100000
ORDER BY category
""", True))

    return queries


def format_value(val):
    """Format a single value for CSV output.

    - NULL → \\N
    - Floats at full precision
    - Dates as YYYY-MM-DD
    - Booleans as true/false
    """
    if val is None:
        return "\\N"
    if isinstance(val, float):
        # Full precision, but strip trailing zeros for cleanliness
        return f"{val:.15g}"
    if isinstance(val, bool):
        return "true" if val else "false"
    return str(val)


def main():
    parser = argparse.ArgumentParser(description="Generate expected query results using DuckDB")
    parser.add_argument("--data-dir", default="data/tpch-1mb",
                       help="Path to TPC-H parquet data directory")
    parser.add_argument("--output-dir", default="tests/expected_results",
                       help="Output directory for CSV files and manifest")
    args = parser.parse_args()

    data_dir = args.data_dir
    output_dir = args.output_dir

    # Verify data directory
    tables = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]
    for t in tables:
        path = os.path.join(data_dir, f"{t}.parquet")
        if not os.path.exists(path):
            print(f"ERROR: Missing parquet file: {path}", file=sys.stderr)
            print(f"Generate data first: cargo run --release -- generate-parquet --sf 0.01 --output {data_dir}", file=sys.stderr)
            sys.exit(1)

    # Create output directory structure
    os.makedirs(output_dir, exist_ok=True)
    for subdir in ["tpch", "basic", "orderby", "agg", "join", "subquery", "expr", "distinct", "setop", "complex"]:
        os.makedirs(os.path.join(output_dir, subdir), exist_ok=True)

    # Connect to DuckDB and load tables
    con = duckdb.connect(":memory:")
    for t in tables:
        path = os.path.join(data_dir, f"{t}.parquet")
        con.execute(f"CREATE TABLE {t} AS SELECT * FROM read_parquet('{path}')")
        row_count = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        print(f"  Loaded {t}: {row_count} rows")

    print()

    # Get all queries
    queries = get_queries()
    manifest = []
    success_count = 0
    fail_count = 0

    for query_id, sql, ordered in queries:
        csv_filename = f"{query_id}.csv"
        csv_path = os.path.join(output_dir, csv_filename)

        try:
            result = con.execute(sql.strip())
            columns = [desc[0] for desc in result.description]
            rows = result.fetchall()

            # Write CSV
            with open(csv_path, "w", newline="") as f:
                # Write header
                f.write(",".join(columns) + "\n")
                # Write data rows
                for row in rows:
                    formatted = [format_value(v) for v in row]
                    # Escape commas and quotes in values
                    escaped = []
                    for v in formatted:
                        if "," in v or '"' in v or "\n" in v:
                            escaped.append('"' + v.replace('"', '""') + '"')
                        else:
                            escaped.append(v)
                    f.write(",".join(escaped) + "\n")

            manifest.append({
                "id": query_id,
                "sql": sql.strip(),
                "ordered": ordered,
                "row_count": len(rows),
                "file": csv_filename,
            })
            success_count += 1
            print(f"  OK  {query_id}: {len(rows)} rows")
        except Exception as e:
            fail_count += 1
            print(f"  FAIL {query_id}: {e}", file=sys.stderr)

    # Write manifest
    manifest_path = os.path.join(output_dir, "manifest.json")
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)

    print()
    print(f"Results: {success_count} succeeded, {fail_count} failed")
    print(f"Manifest: {manifest_path}")
    print(f"CSV files: {output_dir}/")

    if fail_count > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
