#!/usr/bin/env python3
"""Probe the engine with standard-SQL features and record what fails."""
import os, subprocess, json, sys

BINARY = os.environ.get("QE_BINARY", "./target/release/query_engine")

PROBES = [
    # Window functions: ranking
    ("ROW_NUMBER", "SELECT o_orderkey, ROW_NUMBER() OVER (ORDER BY o_totalprice) AS rn FROM orders LIMIT 5"),
    ("RANK", "SELECT o_orderkey, RANK() OVER (ORDER BY o_orderpriority) AS r FROM orders LIMIT 5"),
    ("DENSE_RANK", "SELECT DENSE_RANK() OVER (ORDER BY o_orderpriority) AS r FROM orders LIMIT 5"),
    ("PERCENT_RANK", "SELECT PERCENT_RANK() OVER (ORDER BY o_totalprice) AS r FROM orders LIMIT 5"),
    ("CUME_DIST", "SELECT CUME_DIST() OVER (ORDER BY o_totalprice) AS r FROM orders LIMIT 5"),
    ("NTILE", "SELECT NTILE(4) OVER (ORDER BY o_totalprice) AS r FROM orders LIMIT 5"),
    # Window: navigation
    ("LAG", "SELECT LAG(o_totalprice) OVER (ORDER BY o_orderkey) AS p FROM orders LIMIT 5"),
    ("LAG_OFFSET_DEFAULT", "SELECT LAG(o_totalprice, 2, 0.0) OVER (ORDER BY o_orderkey) AS p FROM orders LIMIT 5"),
    ("LEAD", "SELECT LEAD(o_totalprice) OVER (ORDER BY o_orderkey) AS n FROM orders LIMIT 5"),
    ("FIRST_VALUE", "SELECT FIRST_VALUE(o_totalprice) OVER (PARTITION BY o_custkey ORDER BY o_orderkey) AS f FROM orders LIMIT 5"),
    ("LAST_VALUE", "SELECT LAST_VALUE(o_totalprice) OVER (ORDER BY o_orderkey) AS l FROM orders LIMIT 5"),
    ("NTH_VALUE", "SELECT NTH_VALUE(o_totalprice, 2) OVER (ORDER BY o_orderkey) AS v FROM orders LIMIT 5"),
    # Window: aggregates over windows + partitions
    ("SUM_OVER_PARTITION", "SELECT o_orderkey, SUM(o_totalprice) OVER (PARTITION BY o_custkey) AS s FROM orders LIMIT 5"),
    ("AVG_OVER_ORDER", "SELECT AVG(o_totalprice) OVER (ORDER BY o_orderkey) AS running FROM orders LIMIT 5"),
    ("COUNT_OVER_EMPTY", "SELECT COUNT(*) OVER () AS c FROM orders LIMIT 5"),
    ("MIN_MAX_OVER", "SELECT MIN(o_totalprice) OVER (PARTITION BY o_orderpriority) AS lo, MAX(o_totalprice) OVER (PARTITION BY o_orderpriority) AS hi FROM orders LIMIT 5"),
    # Frames
    ("ROWS_FRAME", "SELECT SUM(o_totalprice) OVER (ORDER BY o_orderkey ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS s FROM orders LIMIT 5"),
    ("ROWS_UNBOUNDED", "SELECT SUM(o_totalprice) OVER (ORDER BY o_orderkey ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS s FROM orders LIMIT 5"),
    ("RANGE_FRAME", "SELECT SUM(o_totalprice) OVER (ORDER BY o_orderkey RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS s FROM orders LIMIT 5"),
    ("ROWS_FOLLOWING", "SELECT SUM(o_totalprice) OVER (ORDER BY o_orderkey ROWS BETWEEN CURRENT ROW AND 3 FOLLOWING) AS s FROM orders LIMIT 5"),
    # Named windows
    ("WINDOW_CLAUSE", "SELECT SUM(o_totalprice) OVER w AS s FROM orders WINDOW w AS (PARTITION BY o_custkey) LIMIT 5"),
    # Other standard SQL possibly missing
    ("FETCH_FIRST", "SELECT o_orderkey FROM orders ORDER BY o_orderkey FETCH FIRST 3 ROWS ONLY"),
    ("OFFSET", "SELECT o_orderkey FROM orders ORDER BY o_orderkey LIMIT 3 OFFSET 2"),
    ("VALUES_CLAUSE", "SELECT * FROM (VALUES (1,'a'),(2,'b')) AS t(x,y)"),
    ("CTE", "WITH big AS (SELECT * FROM orders WHERE o_totalprice > 100000) SELECT COUNT(*) AS n FROM big"),
    ("RECURSIVE_CTE", "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM t WHERE n < 5) SELECT SUM(n) AS s FROM t"),
    ("INTERSECT", "SELECT o_custkey FROM orders INTERSECT SELECT c_custkey FROM customer LIMIT 5"),
    ("EXCEPT", "SELECT c_custkey FROM customer EXCEPT SELECT o_custkey FROM orders LIMIT 5"),
    ("GROUPING_SETS", "SELECT o_orderpriority, o_orderstatus, COUNT(*) AS n FROM orders GROUP BY GROUPING SETS ((o_orderpriority),(o_orderstatus),())"),
    ("ROLLUP", "SELECT o_orderpriority, COUNT(*) AS n FROM orders GROUP BY ROLLUP (o_orderpriority)"),
    ("CUBE", "SELECT o_orderpriority, o_orderstatus, COUNT(*) AS n FROM orders GROUP BY CUBE (o_orderpriority, o_orderstatus)"),
    ("HAVING", "SELECT o_custkey, COUNT(*) AS n FROM orders GROUP BY o_custkey HAVING COUNT(*) > 5 LIMIT 5"),
    ("DISTINCT_ON_AGG", "SELECT COUNT(DISTINCT o_custkey) AS n FROM orders"),
    ("CASE_SEARCHED", "SELECT CASE WHEN o_totalprice > 100000 THEN 'big' ELSE 'small' END AS c FROM orders LIMIT 3"),
    ("NULLS_FIRST", "SELECT o_orderkey FROM orders ORDER BY o_comment NULLS FIRST LIMIT 3"),
    ("IS_DISTINCT_FROM", "SELECT COUNT(*) AS n FROM orders WHERE o_orderstatus IS DISTINCT FROM 'F'"),
    ("BETWEEN_SYMMETRIC", "SELECT COUNT(*) AS n FROM orders WHERE o_totalprice BETWEEN SYMMETRIC 200000 AND 100000"),
    ("CROSS_JOIN", "SELECT COUNT(*) AS n FROM region CROSS JOIN nation"),
    ("NATURAL_JOIN", "SELECT COUNT(*) AS n FROM nation NATURAL JOIN region"),
    ("USING_JOIN", "SELECT COUNT(*) AS n FROM nation JOIN region USING (r_regionkey)"),
    ("FULL_OUTER", "SELECT COUNT(*) AS n FROM nation FULL OUTER JOIN region ON n_regionkey = r_regionkey"),
    ("LATERAL", "SELECT o.o_orderkey, t.m FROM orders o, LATERAL (SELECT MAX(l_extendedprice) AS m FROM lineitem WHERE l_orderkey = o.o_orderkey) t LIMIT 3"),
    ("EXTRACT_EPOCH", "SELECT EXTRACT(YEAR FROM DATE '2020-05-01') AS y"),
    ("INTERVAL", "SELECT DATE '2020-05-01' + INTERVAL '3' DAY AS d"),
    ("CAST_DECIMAL", "SELECT CAST(1.5 AS DECIMAL(10,2)) AS d"),
    ("TRIM_SPEC", "SELECT TRIM(BOTH 'x' FROM 'xxhixx') AS t"),
    ("POSITION_IN", "SELECT POSITION('b' IN 'abc') AS p"),
    ("OVERLAY", "SELECT OVERLAY('hello' PLACING 'XX' FROM 2 FOR 2) AS o"),
    ("SUBSTRING_FROM", "SELECT SUBSTRING('hello' FROM 2 FOR 3) AS s"),
    ("LIKE_ESCAPE", "SELECT COUNT(*) AS n FROM orders WHERE o_comment LIKE '%!%%' ESCAPE '!'"),
    ("EXISTS_SUBQ", "SELECT COUNT(*) AS n FROM customer c WHERE EXISTS (SELECT 1 FROM orders o WHERE o.o_custkey = c.c_custkey)"),
    ("QUANTIFIED_ANY", "SELECT COUNT(*) AS n FROM orders WHERE o_totalprice > ANY (SELECT AVG(o_totalprice) FROM orders)"),
    ("QUANTIFIED_ALL", "SELECT COUNT(*) AS n FROM orders WHERE o_totalprice > ALL (SELECT MIN(o_totalprice) FROM orders)"),
    ("IN_LIST", "SELECT COUNT(*) AS n FROM orders WHERE o_orderstatus IN ('F','O')"),
    ("ORDER_BY_ALIAS", "SELECT o_totalprice AS tp FROM orders ORDER BY tp LIMIT 3"),
    ("ORDER_BY_ORDINAL", "SELECT o_orderkey, o_totalprice FROM orders ORDER BY 2 DESC LIMIT 3"),
    ("GROUP_BY_ORDINAL", "SELECT o_orderstatus, COUNT(*) AS n FROM orders GROUP BY 1"),
    ("SCALAR_SUBQ_SELECT", "SELECT o_orderkey, (SELECT MAX(l_linenumber) FROM lineitem WHERE l_orderkey = o_orderkey) AS ml FROM orders LIMIT 3"),
    ("CORRELATED_IN", "SELECT COUNT(*) AS n FROM orders WHERE o_orderkey IN (SELECT l_orderkey FROM lineitem WHERE l_quantity > 40)"),
]

results = {}
for name, sql in PROBES:
    p = subprocess.run(
        [BINARY, "sql", sql, "--sf", "0.001"],
        capture_output=True, text=True, timeout=120)
    out = p.stdout + p.stderr
    failed = ("error" in out.lower() or "Error" in out) and "rows in" not in out
    results[name] = {"ok": not failed, "msg": ""}
    if failed:
        for line in out.splitlines():
            if "rror" in line:
                results[name]["msg"] = line.strip()[:160]
                break

ok = [k for k, v in results.items() if v["ok"]]
bad = [(k, results[k]["msg"]) for k, v in results.items() if not results[k]["ok"]]
print(f"=== WORKS ({len(ok)}): {', '.join(ok)}\n")
print(f"=== FAILS ({len(bad)}):")
for k, m in bad:
    print(f"  {k}: {m}")
