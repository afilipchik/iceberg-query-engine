#!/usr/bin/env python3
"""M2 acceptance evidence: correctness, balance, and wall-time spread.

Three questions, answered with numbers rather than assurances:

1. **Is the distributed answer right?** Every query is run against the cluster
   with `?distributed=1` — which never falls back to a local answer — from EVERY
   node in turn, and compared cell-by-cell against DuckDB reading ALL of the
   data. Integers, strings and NULLs must match exactly; floating point is
   compared with the same 1e-6 relative tolerance the project's DuckDB-validated
   suite uses (`tests/duckdb_validated.rs`). That tolerance is arithmetic, not
   slack: `SUM` over `f64` is not associative, so adding a column in three
   shard-sized pieces and adding it in one differ in the last bits. Any parallel
   engine has this property, DuckDB included.

2. **Was the work divided equally?** `GET /splits?table=T&nodes=N` reports
   `imbalance = max_node_bytes / mean_node_bytes` for any N, without needing N
   nodes running — it is metadata arithmetic. The gate is <= 1.10.

3. **Did the nodes finish together?** The `x-qe-distribution` header carries
   each node's own wall time, so `max_node_ms / mean_node_ms` is measured, not
   modelled. NOTE: N nodes on one box share its memory bandwidth, disks and page
   cache, so this measures coordination, NOT scaling. No speedup is claimed.

Usage:
    scripts/distributed_validate.py --data ./data/tpch-1mb --nodes 3
    scripts/distributed_validate.py --data ./data/tpch-10gb --nodes 3 \
        --balance-nodes 3,8 --base-port 17700
"""

import argparse
import json
import math
import subprocess
import sys
import urllib.error
import urllib.request

TOL_REL = 1e-6
TOL_ABS = 1e-9
BALANCE_GATE = 1.10

# (name, sql). Every one is a supported shape; the rejection list is checked
# separately below.
QUERIES = [
    ("count_star", "SELECT COUNT(*) AS n FROM lineitem"),
    ("count_filtered", "SELECT COUNT(*) AS n FROM lineitem WHERE l_shipdate < '1995-01-01'"),
    ("count_matches_nothing", "SELECT COUNT(*) AS n FROM lineitem WHERE l_orderkey < 0"),
    ("sum", "SELECT SUM(l_quantity) AS s, SUM(l_extendedprice) AS p FROM lineitem"),
    ("min_max", "SELECT MIN(l_extendedprice) AS lo, MAX(l_extendedprice) AS hi, "
                "MIN(l_shipmode) AS ml, MAX(l_shipmode) AS mh FROM lineitem"),
    ("avg", "SELECT AVG(l_quantity) AS aq, AVG(l_discount) AS ad, "
            "AVG(l_extendedprice) AS ap FROM lineitem"),
    ("avg_of_empty", "SELECT AVG(l_quantity) AS aq FROM lineitem WHERE l_orderkey < 0"),
    ("group_by_q1_shaped",
     "SELECT l_returnflag, l_linestatus, SUM(l_quantity) AS sum_qty, "
     "SUM(l_extendedprice) AS sum_base_price, "
     "SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price, "
     "AVG(l_quantity) AS avg_qty, AVG(l_extendedprice) AS avg_price, "
     "AVG(l_discount) AS avg_disc, COUNT(*) AS count_order "
     "FROM lineitem WHERE l_shipdate <= '1998-09-01' "
     "GROUP BY l_returnflag, l_linestatus"),
    ("group_by_high_cardinality",
     "SELECT l_shipmode, COUNT(*) AS c, SUM(l_quantity) AS s, AVG(l_discount) AS a "
     "FROM lineitem GROUP BY l_shipmode"),
    ("group_by_having",
     "SELECT l_shipmode, COUNT(*) AS c FROM lineitem GROUP BY l_shipmode HAVING COUNT(*) > 100"),
    ("orders_table", "SELECT o_orderstatus, COUNT(*) AS c, SUM(o_totalprice) AS t, "
                     "AVG(o_totalprice) AS a FROM orders GROUP BY o_orderstatus"),
    ("tiny_table", "SELECT COUNT(*) AS c, MIN(n_name) AS lo, MAX(n_name) AS hi FROM nation"),
    ("projection_passthrough",
     "SELECT l_orderkey, l_linenumber FROM lineitem WHERE l_quantity > 49.9"),
]

# Shapes the exact scatter-gather planner refuses but the GATHER path (M2.5)
# answers: workers stream their shard of every referenced table to the
# initiator, which runs the original statement. All deterministic, so they are
# validated cell-exact against DuckDB exactly like QUERIES above.
GATHER_QUERIES = [
    ("cross-shard join (explicit)",
     "SELECT COUNT(*) AS n FROM lineitem JOIN orders ON l_orderkey = o_orderkey"),
    ("cross-shard join (comma)",
     "SELECT COUNT(*) AS n FROM lineitem, orders WHERE l_orderkey = o_orderkey"),
    ("COUNT(DISTINCT)", "SELECT COUNT(DISTINCT l_orderkey) AS n FROM lineitem"),
    ("uncorrelated subquery",
     "SELECT COUNT(*) AS n FROM lineitem WHERE l_orderkey IN (SELECT o_orderkey FROM orders)"),
    ("correlated subquery",
     "SELECT COUNT(*) AS n FROM lineitem l WHERE EXISTS "
     "(SELECT 1 FROM orders o WHERE o.o_orderkey = l.l_orderkey)"),
    ("scalar subquery",
     "SELECT COUNT(*) AS n FROM lineitem "
     "WHERE l_quantity > (SELECT AVG(l_quantity) FROM lineitem)"),
    ("global ORDER BY + LIMIT",
     "SELECT l_orderkey FROM lineitem ORDER BY l_orderkey LIMIT 10"),
    ("STDDEV", "SELECT STDDEV(l_quantity) AS s FROM lineitem"),
    ("SELECT DISTINCT", "SELECT DISTINCT l_returnflag FROM lineitem"),
    ("UNION", "SELECT COUNT(*) AS n FROM lineitem UNION ALL SELECT COUNT(*) FROM orders"),
    ("CTE", "WITH x AS (SELECT * FROM lineitem) SELECT COUNT(*) AS n FROM x"),
    ("derived table", "SELECT COUNT(*) AS n FROM (SELECT * FROM lineitem) t"),
    ("three-way join + group",
     "SELECT n_name, COUNT(*) AS c FROM customer "
     "JOIN orders ON c_custkey = o_custkey "
     "JOIN nation ON c_nationkey = n_nationkey "
     "GROUP BY n_name ORDER BY c DESC, n_name"),
]

# Shapes that MUST still be refused. The needle is the substring the message
# must contain; the point is that the reason is specific, not merely an error.
# The list is short on purpose: gather widened distributed support to exactly
# the local engine's envelope, so what remains is what the ENGINE cannot run
# (windows), what has nothing to shard, and what is not a SELECT.
REJECTIONS = [
    ("window function", "SELECT SUM(l_quantity) OVER () FROM lineitem", "indow"),
    ("no base table", "SELECT 1", "no base table"),
    ("non-SELECT", "DROP TABLE lineitem", "only SELECT"),
]

GREEN, RED, YELLOW, BOLD, NC = "\033[0;32m", "\033[0;31m", "\033[1;33m", "\033[1m", "\033[0m"


def human_bytes(n):
    for unit, size in (("GB", 1 << 30), ("MB", 1 << 20), ("KB", 1 << 10)):
        if n >= size:
            return f"{n / size:.1f}{unit}"
    return f"{n}B"


def post_sql(addr, sql, distributed="1", fmt="csv", timeout=1800):
    req = urllib.request.Request(
        f"http://{addr}/sql?format={fmt}&distributed={distributed}",
        data=sql.encode(),
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read().decode(), dict(r.headers)
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode(), dict(e.headers)


def get_json(addr, path, timeout=300):
    with urllib.request.urlopen(f"http://{addr}{path}", timeout=timeout) as r:
        return json.loads(r.read().decode())


def duckdb_csv(data_dir, sql, binary="duckdb"):
    """Run `sql` in DuckDB over ALL of the data, as views over the same files."""
    views = "\n".join(
        f"CREATE VIEW {t} AS SELECT * FROM read_parquet('{data_dir}/{t}.parquet');"
        for t in ("lineitem", "orders", "nation", "customer", "part",
                  "partsupp", "region", "supplier")
    )
    script = f"{views}\n{sql};\n"
    # `-csv` must precede `-c`: the CLI applies output-mode flags in order, and
    # dot-commands are not accepted inside a `-c` script.
    out = subprocess.run([binary, "-csv", "-c", script], capture_output=True, text=True)
    if out.returncode != 0:
        raise RuntimeError(f"duckdb failed: {out.stderr}")
    return out.stdout


def cells(csv_text):
    rows = [r for r in csv_text.strip().splitlines() if r.strip()]
    if not rows:
        return [], []
    return rows[0].split(","), sorted(rows[1:])


# CSV spellings of NULL. Arrow's writer emits an empty field, DuckDB's emits
# the literal `NULL`. Both mean absent, and an AVG over a filter that matches
# nothing legitimately produces one — normalizing here compares VALUES rather
# than two writers' opinions about how to render nothing.
NULLS = {"", '""', "NULL", "null", "\\N"}


def match(a, b):
    if a in NULLS and b in NULLS:
        return True
    if a in NULLS or b in NULLS:
        return False
    if a == b:
        return True
    try:
        x, y = float(a), float(b)
    except ValueError:
        return False
    if math.isnan(x) and math.isnan(y):
        return True
    return abs(x - y) <= max(TOL_REL * max(abs(x), abs(y)), TOL_ABS)


def compare(got, expected):
    """Returns None when equal, else a human-readable first difference."""
    gh, gr = cells(got)
    eh, er = cells(expected)
    if len(gr) != len(er):
        return f"row count {len(gr)} != {len(er)}"
    if len(gh) != len(eh):
        return f"column count {len(gh)} != {len(eh)}"
    for i, (g, e) in enumerate(zip(gr, er)):
        gc, ec = g.split(","), e.split(",")
        if len(gc) != len(ec):
            return f"row {i}: column count {len(gc)} != {len(ec)}"
        for j, (a, b) in enumerate(zip(gc, ec)):
            if not match(a, b):
                return f"row {i} col {j} ({gh[j] if j < len(gh) else '?'}): {a!r} vs {b!r}"
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default="./data/tpch-1mb")
    ap.add_argument("--nodes", type=int, default=3)
    ap.add_argument("--base-port", type=int, default=17700)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--duckdb", default="duckdb")
    ap.add_argument("--balance-nodes", default="3,8",
                    help="comma-separated node counts to report imbalance for")
    ap.add_argument("--balance-table", default="lineitem")
    ap.add_argument("--skip-correctness", action="store_true")
    args = ap.parse_args()

    addrs = [f"{args.host}:{args.base_port + i}" for i in range(args.nodes)]
    failures = []

    # ---- 1. balance -------------------------------------------------------
    print(f"{BOLD}=== 1/4  work division ({args.balance_table}, {args.data}) ==={NC}")
    for n in [int(x) for x in args.balance_nodes.split(",") if x.strip()]:
        try:
            s = get_json(addrs[0], f"/splits?table={args.balance_table}&nodes={n}")
        except Exception as e:  # noqa: BLE001
            print(f"{RED}FAIL{NC} /splits at {n} nodes: {e}")
            failures.append(f"splits n={n}")
            continue
        imb = s["imbalance"]
        per = [p["bytes"] for p in s["per_node"]]
        verdict = f"{GREEN}PASS{NC}" if imb <= BALANCE_GATE else f"{RED}FAIL{NC}"
        if imb > BALANCE_GATE:
            failures.append(f"imbalance {imb:.4f} at {n} nodes")
        print(f"  {verdict} {n:>2} nodes  imbalance {imb:.4f}  "
              f"(gate <= {BALANCE_GATE})  splits={s['total_splits']:>5}  "
              f"target={human_bytes(s['target_split_bytes'])}  "
              f"idle={s['idle_nodes']}")
        print(f"        per-node: {[human_bytes(b) for b in per]}")

    # ---- 2. correctness ---------------------------------------------------
    if not args.skip_correctness:
        print(f"\n{BOLD}=== 2/4  cell-exact vs DuckDB reading ALL the data ==={NC}")
        for name, sql in QUERIES:
            try:
                expected = duckdb_csv(args.data, sql, args.duckdb)
            except Exception as e:  # noqa: BLE001
                print(f"{RED}FAIL{NC} {name}: DuckDB: {e}")
                failures.append(name)
                continue
            ok = True
            for i, addr in enumerate(addrs):
                status, body, hdrs = post_sql(addr, sql)
                if status != 200:
                    print(f"{RED}FAIL{NC} {name} @node{i}: HTTP {status} {body[:200]}")
                    ok = False
                    break
                if hdrs.get("x-qe-distributed") != "true":
                    print(f"{RED}FAIL{NC} {name} @node{i}: answered locally, not distributed")
                    ok = False
                    break
                diff = compare(body, expected)
                if diff:
                    print(f"{RED}FAIL{NC} {name} @node{i}: {diff}")
                    ok = False
                    break
            if ok:
                shards = hdrs.get("x-qe-shards", "?")
                print(f"{GREEN}PASS{NC} {name:<28} identical on all {len(addrs)} nodes "
                      f"and to DuckDB ({shards} shards)")
            else:
                failures.append(name)

    # ---- 3. gather path + rejections --------------------------------------
    print(f"\n{BOLD}=== 3/4  gather path (joins, subqueries, DISTINCT, ORDER BY) "
          f"vs DuckDB ==={NC}")
    for name, sql in GATHER_QUERIES:
        try:
            expected = duckdb_csv(args.data, sql, args.duckdb)
        except Exception as e:  # noqa: BLE001
            print(f"{RED}FAIL{NC} {name}: DuckDB: {e}")
            failures.append(f"gather {name}")
            continue
        status, body, hdrs = post_sql(addrs[0], sql)
        if status != 200:
            print(f"{RED}FAIL{NC} {name}: HTTP {status} {body[:200]}")
            failures.append(f"gather {name}")
            continue
        if hdrs.get("x-qe-distributed") != "true":
            print(f"{RED}FAIL{NC} {name}: answered locally, not distributed")
            failures.append(f"gather {name}")
            continue
        diff = compare(body, expected)
        if diff:
            print(f"{RED}FAIL{NC} {name}: {diff}")
            failures.append(f"gather {name}")
        else:
            shape = "?"
            if "x-qe-distribution" in hdrs:
                shape = json.loads(hdrs["x-qe-distribution"]).get("shape", "?")
            print(f"{GREEN}PASS{NC} {name:<28} identical to DuckDB (shape={shape})")

    print(f"\n{BOLD}===      what remains refused, is refused by name ==={NC}")
    for name, sql, needle in REJECTIONS:
        status, body, _ = post_sql(addrs[0], sql)
        if status == 200:
            print(f"{RED}FAIL{NC} {name}: answered with HTTP 200 instead of refusing")
            failures.append(f"reject {name}")
            continue
        try:
            msg = json.loads(body).get("error", body)
        except json.JSONDecodeError:
            msg = body
        if status != 501 or needle.lower() not in msg.lower():
            print(f"{RED}FAIL{NC} {name}: HTTP {status}, message did not mention "
                  f"{needle!r}: {msg[:160]}")
            failures.append(f"reject {name}")
        else:
            print(f"{GREEN}PASS{NC} {name:<28} 501 — {msg.split(';')[0]}")

    # ---- 4. wall time -----------------------------------------------------
    print(f"\n{BOLD}=== 4/4  per-node wall time (coordination, NOT scaling) ==={NC}")
    print(f"{YELLOW}note{NC}: all nodes share one machine's memory bandwidth, disks and "
          f"page cache.\n      These numbers show whether the nodes FINISH TOGETHER. "
          f"They are not a speedup.")
    for name, sql in QUERIES[:1] + [q for q in QUERIES if q[0] == "group_by_q1_shaped"]:
        status, _, hdrs = post_sql(addrs[0], sql)
        if status != 200 or "x-qe-distribution" not in hdrs:
            print(f"{RED}FAIL{NC} {name}: no distribution header")
            failures.append(f"timing {name}")
            continue
        d = json.loads(hdrs["x-qe-distribution"])
        per = [(n["shard_index"], n["elapsed_ms"], n["assigned_bytes"]) for n in d["nodes"]]
        total = float(hdrs["x-qe-elapsed-ms"])
        print(f"  {name}: total {total:.1f}ms  imbalance(bytes) {d['imbalance']:.4f}  "
              f"spread(time) {d['wall_time_spread']:.3f}")
        for idx, ms, by in per:
            print(f"      shard {idx}: {ms:8.1f} ms   {human_bytes(by):>9}")

    print()
    if failures:
        print(f"{RED}{BOLD}M2 GATE: {len(failures)} FAILURE(S){NC}: {', '.join(failures)}")
        return 1
    print(f"{GREEN}{BOLD}M2 GATE: PASS{NC}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
