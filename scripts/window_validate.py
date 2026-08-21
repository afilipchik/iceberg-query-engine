"""Window / grouping / expression acceptance gate vs DuckDB.

Every case runs on the engine binary (QE_BINARY, default release) and in
DuckDB over the same parquet, compared cell-by-cell after canonical full-row
sort (this generator's lineitem has duplicate keys, so outer ORDER BY can
tie). Floats compare at 1e-6 relative — the project's distributed convention.
"""
import os, duckdb, subprocess, re, sys

BINARY = os.environ.get("QE_BINARY", "./target/release/query_engine")

con = duckdb.connect()
for t in ["orders","lineitem","customer"]:
    con.execute(f"CREATE VIEW {t} AS SELECT * FROM read_parquet('data/tpch-1mb/{t}.parquet')")

CASES = [
    # ranking
    "SELECT o_orderkey, RANK() OVER (ORDER BY o_orderpriority) AS r FROM orders",
    "SELECT o_orderkey, DENSE_RANK() OVER (PARTITION BY o_orderstatus ORDER BY o_orderpriority) AS r FROM orders",
    "SELECT o_orderkey, PERCENT_RANK() OVER (PARTITION BY o_orderstatus ORDER BY o_orderpriority) AS r FROM orders",
    "SELECT o_orderkey, CUME_DIST() OVER (PARTITION BY o_orderstatus ORDER BY o_orderpriority) AS r FROM orders",
    "SELECT o_orderkey, NTILE(4) OVER (ORDER BY o_totalprice) AS r FROM orders",
    "SELECT o_orderkey, NTILE(7) OVER (PARTITION BY o_orderstatus ORDER BY o_orderkey) AS r FROM orders",
    # navigation
    "SELECT o_orderkey, LAG(o_totalprice) OVER (ORDER BY o_orderkey) AS v FROM orders",
    "SELECT o_orderkey, LAG(o_totalprice, 3) OVER (PARTITION BY o_custkey ORDER BY o_orderkey) AS v FROM orders",
    "SELECT o_orderkey, LAG(o_totalprice, 2, -1.0) OVER (ORDER BY o_orderkey) AS v FROM orders",
    "SELECT o_orderkey, LEAD(o_orderstatus) OVER (ORDER BY o_orderkey) AS v FROM orders",
    "SELECT o_orderkey, FIRST_VALUE(o_totalprice) OVER (PARTITION BY o_custkey ORDER BY o_orderkey) AS v FROM orders",
    "SELECT o_orderkey, LAST_VALUE(o_totalprice) OVER (ORDER BY o_orderkey) AS v FROM orders",
    "SELECT o_orderkey, LAST_VALUE(o_totalprice) OVER (ORDER BY o_orderkey ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS v FROM orders",
    "SELECT o_orderkey, NTH_VALUE(o_totalprice, 2) OVER (PARTITION BY o_orderstatus ORDER BY o_orderkey) AS v FROM orders",
    # aggregates over windows
    "SELECT o_orderkey, COUNT(*) OVER () AS v FROM orders",
    "SELECT o_orderkey, COUNT(*) OVER (PARTITION BY o_custkey) AS v FROM orders",
    "SELECT o_orderkey, SUM(o_totalprice) OVER (PARTITION BY o_custkey) AS v FROM orders",
    "SELECT o_orderkey, SUM(o_totalprice) OVER (ORDER BY o_orderkey) AS v FROM orders",
    "SELECT o_orderkey, AVG(o_totalprice) OVER (ORDER BY o_orderkey ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS v FROM orders",
    "SELECT o_orderkey, SUM(o_totalprice) OVER (ORDER BY o_orderkey ROWS BETWEEN CURRENT ROW AND 3 FOLLOWING) AS v FROM orders",
    "SELECT o_orderkey, SUM(o_totalprice) OVER (ORDER BY o_orderkey ROWS BETWEEN 5 FOLLOWING AND 8 FOLLOWING) AS v FROM orders",
    "SELECT o_orderkey, MIN(o_totalprice) OVER (PARTITION BY o_orderstatus ORDER BY o_orderkey ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS v FROM orders",
    "SELECT o_orderkey, MAX(o_orderstatus) OVER (PARTITION BY o_custkey) AS v FROM orders",
    "SELECT o_orderkey, COUNT(o_comment) OVER (ORDER BY o_orderkey ROWS BETWEEN 10 PRECEDING AND CURRENT ROW) AS v FROM orders",
    # RANGE frames
    "SELECT o_orderkey, SUM(o_totalprice) OVER (ORDER BY o_orderkey RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS v FROM orders",
    "SELECT o_orderkey, SUM(o_totalprice) OVER (ORDER BY o_custkey RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS v FROM orders",
    "SELECT o_orderkey, COUNT(*) OVER (ORDER BY o_custkey RANGE BETWEEN 10 PRECEDING AND 10 FOLLOWING) AS v FROM orders",
    "SELECT o_orderkey, SUM(o_totalprice) OVER (ORDER BY o_custkey DESC RANGE BETWEEN 5 PRECEDING AND CURRENT ROW) AS v FROM orders",
    # combined / expression / named windows / multiple windows
    "SELECT o_orderkey, o_totalprice - LAG(o_totalprice, 1, 0.0) OVER (ORDER BY o_orderkey) AS delta FROM orders",
    "SELECT o_orderkey, ROW_NUMBER() OVER w AS a, SUM(o_totalprice) OVER w AS b FROM orders WINDOW w AS (PARTITION BY o_orderstatus ORDER BY o_orderkey)",
    "SELECT o_orderkey, ROW_NUMBER() OVER (ORDER BY o_orderkey) AS a, RANK() OVER (PARTITION BY o_orderstatus ORDER BY o_totalprice DESC) AS b FROM orders",
    # ties / NULLS ordering
    "SELECT l_orderkey, l_linenumber, RANK() OVER (ORDER BY l_quantity) AS r FROM lineitem",
    "SELECT l_orderkey, l_linenumber, SUM(l_quantity) OVER (PARTITION BY l_orderkey ORDER BY l_linenumber) AS v FROM lineitem",
    # --- extended coverage (task 008) ---
    "SELECT o_orderkey, NTILE(3) OVER (ORDER BY o_totalprice DESC) AS r FROM orders",
    "SELECT o_orderkey, LEAD(o_totalprice, 4, 0.0) OVER (PARTITION BY o_orderstatus ORDER BY o_orderkey) AS v FROM orders",
    "SELECT o_orderkey, FIRST_VALUE(o_totalprice) OVER (ORDER BY o_orderkey ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS v FROM orders",
    "SELECT o_orderkey, LAST_VALUE(o_totalprice) OVER (PARTITION BY o_custkey ORDER BY o_orderkey ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) AS v FROM orders",
    "SELECT o_orderkey, LAG(o_orderpriority) OVER (ORDER BY o_orderkey) AS v FROM orders",
    "SELECT o_orderkey, SUM(o_totalprice) OVER (ORDER BY o_orderdate RANGE BETWEEN 30 PRECEDING AND CURRENT ROW) AS v FROM orders",
    "SELECT o_orderkey, RANK() OVER (ORDER BY o_orderstatus DESC, o_orderpriority ASC) AS r FROM orders",
    "SELECT o_orderkey, ROW_NUMBER() OVER (PARTITION BY o_orderstatus, o_orderpriority ORDER BY o_orderkey) AS r FROM orders",
    "SELECT o_orderkey, AVG(o_totalprice) OVER (PARTITION BY o_custkey) AS v FROM orders",
    "SELECT o_orderkey, MIN(o_orderdate) OVER (PARTITION BY o_orderstatus ORDER BY o_orderkey) AS v FROM orders",
    "SELECT o_orderkey, COUNT(*) OVER (ORDER BY o_orderkey DESC ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS v FROM orders",
    "SELECT c_custkey, SUM(c_acctbal) OVER (PARTITION BY c_nationkey ORDER BY c_custkey) AS v FROM customer",
    "SELECT c_custkey, CUME_DIST() OVER (ORDER BY c_acctbal) AS v FROM customer",
    "SELECT c_custkey, PERCENT_RANK() OVER (PARTITION BY c_mktsegment ORDER BY c_acctbal) AS v FROM customer",
    # grouping extensions
    "SELECT o_orderpriority, COUNT(*) AS n FROM orders GROUP BY ROLLUP (o_orderpriority)",
    "SELECT o_orderpriority, o_orderstatus, COUNT(*) AS n, SUM(o_totalprice) AS s FROM orders GROUP BY ROLLUP (o_orderpriority, o_orderstatus)",
    "SELECT o_orderpriority, o_orderstatus, COUNT(*) AS n FROM orders GROUP BY CUBE (o_orderpriority, o_orderstatus)",
    "SELECT o_orderpriority, o_orderstatus, COUNT(*) AS n FROM orders GROUP BY GROUPING SETS ((o_orderpriority),(o_orderstatus),())",
    "SELECT o_orderpriority, GROUPING(o_orderpriority) AS g, COUNT(*) AS n FROM orders GROUP BY ROLLUP (o_orderpriority)",
    "SELECT o_orderpriority, o_orderstatus, GROUPING(o_orderpriority) AS ga, GROUPING(o_orderstatus) AS gb, COUNT(*) AS n FROM orders GROUP BY CUBE (o_orderpriority, o_orderstatus)",
    # expression forms
    "SELECT COUNT(*) AS n FROM orders WHERE o_orderstatus IS DISTINCT FROM 'F'",
    "SELECT COUNT(*) AS n FROM orders WHERE o_comment IS NOT DISTINCT FROM o_comment",
    "SELECT COUNT(*) AS n FROM orders WHERE o_totalprice > ANY (SELECT AVG(o_totalprice) FROM orders)",
    "SELECT COUNT(*) AS n FROM orders WHERE o_totalprice > ALL (SELECT MIN(o_totalprice) FROM orders)",
    "SELECT COUNT(*) AS n FROM orders WHERE o_totalprice > ANY (SELECT o_totalprice FROM orders WHERE 1=0)",
    "SELECT COUNT(*) AS n FROM orders WHERE o_totalprice > ALL (SELECT o_totalprice FROM orders WHERE 1=0)",
    "SELECT COUNT(*) AS n FROM orders WHERE o_orderstatus = ANY (SELECT DISTINCT o_orderstatus FROM orders WHERE o_totalprice > 300000)",
    "SELECT o_orderkey, o_orderdate + INTERVAL '30' DAY AS d FROM orders WHERE o_orderkey < 100",
    "SELECT o_orderstatus, COUNT(*) AS n FROM orders GROUP BY 1",
    "SELECT o_orderpriority, o_orderstatus, COUNT(*) AS n FROM orders GROUP BY 1, 2",
]


import datetime

def cell_key(v):
    """Parse a cell (engine string or duck value) to a comparable key."""
    if v is None or v == "":
        return (0, "")
    if isinstance(v, datetime.datetime):
        # DuckDB widens date+interval to timestamp; the engine keeps dates.
        v = v.date().isoformat() if (v.hour, v.minute, v.second) == (0, 0, 0) else v.isoformat()
    elif isinstance(v, datetime.date):
        v = v.isoformat()
    if isinstance(v, bool):
        return (2, "true" if v else "false")
    try:
        return (1, float(v))
    except (TypeError, ValueError):
        return (2, str(v))

def cells_equal(e, d):
    ek, dk = cell_key(e), cell_key(d)
    if ek[0] != dk[0]:
        return False
    if ek[0] == 1:
        a, b = ek[1], dk[1]
        return a == b or abs(a - b) <= 1e-6 * max(abs(a), abs(b), 1.0)
    return ek[1] == dk[1]

bad = 0
for sql in CASES:
    ncols = sql.split(" FROM ")[0].count(",") + 1
    order_cols = ", ".join(str(i + 1) for i in range(min(2, ncols)))
    order_wrap = f"SELECT * FROM ({sql}) q ORDER BY {order_cols}"
    duck = con.execute(order_wrap).fetchall()
    out = subprocess.run([BINARY, "sql", order_wrap, "--sf","0.001"],
                         capture_output=True, text=True)
    text = out.stdout + out.stderr
    if "rror" in text and "Row count" not in text:
        line = next((l for l in text.splitlines() if "rror" in l), "?")
        print(f"ENGINE ERROR: {sql[:80]}\n   {line[:140]}")
        bad += 1
        continue
    # The pretty table is: +--+ / header / +--+ / data rows / +--+.
    lines = text.splitlines()
    seps = [i for i, l in enumerate(lines) if re.match(r'^\+[-+]+\+$', l)]
    rows = []
    if len(seps) >= 3:
        for line in lines[seps[1] + 1 : seps[2]]:
            rows.append([c.strip() for c in line.strip('|').split('|')])
    if len(rows) != len(duck):
        print(f"ROWCOUNT MISMATCH {len(rows)} vs {len(duck)}: {sql[:80]}")
        bad += 1
        continue
    # Outer ORDER BY may tie (duplicate lineitem keys in this generator), so
    # canonicalize both sides by full-row sort on parsed keys.
    rows = sorted(rows, key=lambda r: [cell_key(c) for c in r])
    duck = sorted(duck, key=lambda r: [cell_key(c) for c in r])
    mism = 0
    for er, dr in zip(rows, duck):
        for ec, dc in zip(er, dr):
            if not cells_equal(ec, dc):
                mism += 1
                if mism <= 2:
                    print(f"  cell: engine={ec!r} duck={dc!r}")
    if mism:
        print(f"MISMATCH ({mism} cells): {sql[:90]}")
        bad += 1
    else:
        print(f"ok: {sql[:90]}")
print(f"\n{'ALL PASS' if bad==0 else f'{bad} FAILURES'} of {len(CASES)}")
sys.exit(1 if bad else 0)
