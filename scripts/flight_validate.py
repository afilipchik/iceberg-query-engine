#!/usr/bin/env python3
"""Arrow Flight acceptance gate: Flight answers must equal POST /sql answers.

For every query, the SAME statement is executed twice against the SAME node —
once over Arrow Flight (GetFlightInfo → DoGet), once over HTTP
(`POST /sql?format=arrow`) — and the two results are compared cell by cell,
along with the execution facts (distributed? how many shards? why skipped?)
that Flight carries in its trailing metadata message and HTTP carries in
x-qe-* headers. Both doors run the identical execute_statement path in the
server, so any disagreement is a transport bug by construction.

Comparison rules follow the project's distributed conventions
(scripts/distributed_validate.py): exact for integers/strings/NULLs, 1e-6
relative tolerance for floats — two runs of a distributed SUM may divide the
data identically but the comparison stays honest about float addition.

Usage:
    # Against a running node (cluster_local.sh addresses):
    flight_validate.py --node 127.0.0.1:17800 --http 127.0.0.1:17700 [--quick]

    # Spawn a single-node server on ephemeral ports, run the full sweep:
    flight_validate.py --single --data ./data/tpch-1mb \
        [--binary ./target/release/query_engine]

--quick runs 3 shape-representative checks (scatter aggregate, forced gather
join, forced-local) — the per-node check `cluster_local.sh verify` uses.
The default runs all 22 TPC-H queries (extracted from src/tpch/queries.rs).

Exit status: 0 = all green; 1 = any mismatch (named); 2 = setup failure.
Requires pyarrow >= 4 (flight client API; the repo .venv ships 23.x).
"""

import argparse
import json
import re
import subprocess
import sys
import time
import urllib.request

import pyarrow as pa
import pyarrow.flight as fl
import pyarrow.ipc

TOL_REL = 1e-6
TOL_ABS = 1e-9


def http_query(http_addr, sql, mode):
    """POST /sql?format=arrow → (table, facts dict from x-qe-* headers)."""
    url = f"http://{http_addr}/sql?format=arrow&distributed={mode}"
    req = urllib.request.Request(url, data=sql.encode(), method="POST")
    with urllib.request.urlopen(req, timeout=300) as resp:
        body = resp.read()
        headers = resp.headers
    table = pa.ipc.open_stream(body).read_all()
    facts = {
        "rows": int(headers.get("x-qe-rows", "-1")),
        "distributed": headers.get("x-qe-distributed") == "true",
        "shards": int(headers["x-qe-shards"]) if headers.get("x-qe-shards") else None,
        "skipped_reason": headers.get("x-qe-distributed-skipped"),
    }
    return table, facts


def flight_query(client, sql, mode):
    """GetFlightInfo → DoGet → (table, facts dict from trailing metadata)."""
    if mode == "auto":
        cmd = sql.encode()
    else:
        cmd = json.dumps({"sql": sql, "mode": mode}).encode()
    info = client.get_flight_info(fl.FlightDescriptor.for_command(cmd))
    reader = client.do_get(info.endpoints[0].ticket)
    batches, meta = [], None
    while True:
        try:
            chunk = reader.read_chunk()
        except StopIteration:
            break
        if chunk.data is not None:
            batches.append(chunk.data)
        if chunk.app_metadata is not None:
            meta = json.loads(chunk.app_metadata.to_pybytes())
    table = pa.Table.from_batches(batches, schema=reader.schema)
    if meta is None:
        raise RuntimeError("DoGet stream carried no trailing metadata message")
    facts = {
        "rows": meta.get("rows", -1),
        "distributed": meta.get("distributed", False),
        "shards": meta.get("shards"),
        "skipped_reason": meta.get("skipped_reason"),
    }
    return table, facts


def cells_equal(a, b):
    if a is None or b is None:
        return a is None and b is None
    if isinstance(a, float) or isinstance(b, float):
        try:
            fa, fb = float(a), float(b)
        except (TypeError, ValueError):
            return a == b
        if fa == fb:
            return True
        return abs(fa - fb) <= max(TOL_ABS, TOL_REL * max(abs(fa), abs(fb)))
    return a == b


def canonical_sort(table):
    """Sort by every column so two answers to an ORDER-BY-less query compare
    stably. Falls back to the original order for unsortable column types."""
    try:
        return table.sort_by([(c, "ascending") for c in table.schema.names])
    except pa.ArrowError:
        return table


def compare_tables(name, ft, ht):
    if ft.schema.names != ht.schema.names:
        return [f"{name}: column names differ: flight={ft.schema.names} http={ht.schema.names}"]
    if ft.num_rows != ht.num_rows:
        return [f"{name}: row counts differ: flight={ft.num_rows} http={ht.num_rows}"]
    ft, ht = canonical_sort(ft), canonical_sort(ht)
    problems = []
    fcols = [c.to_pylist() for c in ft.columns]
    hcols = [c.to_pylist() for c in ht.columns]
    for ci, col in enumerate(ft.schema.names):
        for ri in range(ft.num_rows):
            if not cells_equal(fcols[ci][ri], hcols[ci][ri]):
                problems.append(
                    f"{name}: cell mismatch at row {ri} col {col}: "
                    f"flight={fcols[ci][ri]!r} http={hcols[ci][ri]!r}"
                )
                if len(problems) >= 5:
                    return problems
    return problems


def compare_facts(name, ff, hf):
    problems = []
    if ff["distributed"] != hf["distributed"]:
        problems.append(
            f"{name}: distributed flag differs: flight={ff['distributed']} http={hf['distributed']}"
        )
    if ff["shards"] != hf["shards"]:
        problems.append(f"{name}: shards differ: flight={ff['shards']} http={hf['shards']}")
    if ff["rows"] != hf["rows"]:
        problems.append(f"{name}: reported rows differ: flight={ff['rows']} http={hf['rows']}")
    return problems


HTTP_MODE = {"auto": "auto", "force": "1", "off": "0"}


def run_one(client, http_addr, name, sql, mode, expect_distributed=None):
    ft, ff = flight_query(client, sql, mode)
    ht, hf = http_query(http_addr, sql, HTTP_MODE[mode])
    problems = compare_tables(name, ft, ht) + compare_facts(name, ff, hf)
    if expect_distributed is not None and ff["distributed"] != expect_distributed:
        problems.append(
            f"{name}: expected distributed={expect_distributed}, got {ff['distributed']} "
            f"(reason: {ff['skipped_reason']})"
        )
    return problems


QUICK_CHECKS = [
    # Scatter-eligible aggregate (no global ORDER BY — that shape is not
    # scatterable); on a >=2 member cluster auto must distribute it.
    ("scatter_agg",
     "SELECT l_returnflag, COUNT(*) AS n, SUM(l_quantity) AS q "
     "FROM lineitem GROUP BY l_returnflag",
     "auto", None),
    # A join is not an exact scatter shape; force pushes it down the gather path.
    ("gather_join",
     "SELECT o_orderpriority, COUNT(*) AS n FROM orders, lineitem "
     "WHERE o_orderkey = l_orderkey AND l_shipdate < '1995-01-01' "
     "GROUP BY o_orderpriority ORDER BY o_orderpriority",
     "force", True),
    # Forced local must say so on both doors.
    ("forced_local", "SELECT COUNT(*) AS n FROM orders", "off", False),
]


def tpch_queries():
    src = open("src/tpch/queries.rs").read()
    queries = {}
    for m in re.finditer(r'pub const Q(\d+): &str = r#"(.*?)"#;', src, re.S):
        queries[int(m.group(1))] = m.group(2)
    if len(queries) < 22:
        raise RuntimeError(f"only extracted {len(queries)} TPC-H queries from src/tpch/queries.rs")
    return queries


def run_sweep(client, http_addr, quick, member_count):
    failures = []
    if quick:
        for name, sql, mode, expect in QUICK_CHECKS:
            if expect is None:
                # Scatter under auto distributes only when the cluster has peers.
                expect = member_count is not None and member_count >= 2
            probs = run_one(client, http_addr, name, sql, mode, expect)
            for p in probs:
                print(f"  FAIL {p}")
            failures.extend(probs)
            if not probs:
                print(f"  ok {name}")
        return failures

    for q, sql in sorted(tpch_queries().items()):
        name = f"Q{q:02d}"
        probs = run_one(client, http_addr, name, sql, "auto")
        for p in probs:
            print(f"  FAIL {p}")
        failures.extend(probs)
        if not probs:
            print(f"  ok {name}")
    return failures


def member_count_of(http_addr):
    try:
        with urllib.request.urlopen(f"http://{http_addr}/cluster", timeout=10) as r:
            return json.load(r).get("member_count")
    except Exception:
        return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--node", help="flight address host:port of a running node")
    ap.add_argument("--http", help="HTTP address host:port of the same node")
    ap.add_argument("--quick", action="store_true", help="3 shape checks instead of all 22")
    ap.add_argument("--single", action="store_true", help="spawn a lone server and sweep it")
    ap.add_argument("--data", default="./data/tpch-1mb")
    ap.add_argument("--binary", default="./target/release/query_engine")
    args = ap.parse_args()

    proc = None
    try:
        if args.single:
            proc = subprocess.Popen(
                [args.binary, "serve", "--bind", "127.0.0.1:17960",
                 "--flight-bind", "127.0.0.1:17961", "--data", args.data],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )
            args.http, args.node = "127.0.0.1:17960", "127.0.0.1:17961"
            deadline = time.time() + 120
            while time.time() < deadline:
                try:
                    with urllib.request.urlopen(f"http://{args.http}/readyz", timeout=2) as r:
                        if r.status == 200:
                            break
                except Exception:
                    time.sleep(0.2)
            else:
                print("spawned server never became ready")
                return 2
        elif not (args.node and args.http):
            print("need --node and --http, or --single")
            return 2

        client = fl.connect(f"grpc://{args.node}")
        members = member_count_of(args.http)
        failures = run_sweep(client, args.http, args.quick, members)
        if failures:
            print(f"FLIGHT GATE: {len(failures)} FAILURE(S)")
            return 1
        print("FLIGHT GATE: PASS")
        return 0
    finally:
        if proc is not None:
            proc.terminate()
            try:
                proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                proc.kill()


if __name__ == "__main__":
    sys.exit(main())
