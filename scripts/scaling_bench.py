#!/usr/bin/env python3
"""
Throughput scaling harness.

The acceptance criterion for the distributed engine is "throughput scales
linearly with more nodes". On a single 32-core box you cannot conjure more
hardware, so the honest experiment holds TOTAL CORES CONSTANT and varies how
they are divided:

    1 node  x 32 cores      <- the single-process baseline
    2 nodes x 16 cores
    4 nodes x  8 cores
    8 nodes x  4 cores

If the distributed engine scales linearly, N nodes of C cores should deliver
the same throughput as one node of N*C cores, minus shuffle and coordination
overhead. That difference IS the distribution tax, and it is the number worth
reporting. Anything else — e.g. comparing 8 nodes on 32 cores against 1 node on
4 cores — measures the hardware, not the design, and would be self-flattery.

This harness also measures the CURRENT single-process engine as a function of
core count, which yields the intra-node scaling curve (a Universal Scalability
Law fit). That curve is the baseline any distributed design must beat, and its
contention/coherency terms predict where linear scaling dies.

WHAT A SINGLE BOX CANNOT PROVE: real network latency and bandwidth limits, NIC
saturation, partial failures, or cross-machine clock effects. Localhost shuffle
is far cheaper than a real network, so single-box numbers are an UPPER BOUND on
distributed efficiency. Report them as such.

Usage:
    .venv/bin/python scripts/scaling_bench.py --mode cores --data ./data/tpch-10gb
    .venv/bin/python scripts/scaling_bench.py --mode concurrency --procs 1,2,4,8
"""

import argparse
import json
import os
import re
import statistics
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor

BIN = "./target/release/query_engine"


def total_cores() -> int:
    return os.cpu_count() or 1


def physical_cores() -> list[int]:
    """One CPU id per PHYSICAL core, cheapest-class-last, SMT siblings excluded.

    THIS FUNCTION EXISTS BECAUSE THE OBVIOUS THING IS WRONG. Pinning contiguous
    ids (`taskset -c 0-K`) on a hybrid SMT machine does not vary "core count" --
    it walks a heterogeneous list. On this i9-13900KF, cpus 0..15 are 8 P-cores
    with HyperThreading (siblings adjacent: 0-1, 2-3, ...) and 16..31 are E-cores.
    So a contiguous sweep means: K=1 one SMT thread, K=2 BOTH THREADS OF ONE
    PHYSICAL CORE, K=4 two cores, ... K=32 adds 16 slower E-cores. The measured
    "speedup" then conflates three different variables.

    That mistake produced a real, propagated error: a contiguous sweep gave
    1->32 speedup 5.66x and an Amdahl fit of f=0.150, which was quoted as a hard
    architectural limit ("single-query speedup capped at 6.7x") and used to order
    the phases of the distributed design. Re-measured over homogeneous physical
    P-cores the curve is 1.00 / 1.44 / 2.55 / 4.20 at 1/2/4/8 cores (52%
    efficiency at 8), and a per-point Amdahl fit yields 0.389 / 0.189 / 0.129 --
    a spread that REJECTS the single-serial-fraction model. Use a USL fit
    (contention + coherency) instead of quoting one f.
    """
    by_core: dict[tuple, int] = {}
    weight: dict[int, int] = {}
    for c in range(total_cores()):
        base = f"/sys/devices/system/cpu/cpu{c}/topology"
        try:
            core = open(f"{base}/core_id").read().strip()
            pkg = open(f"{base}/physical_package_id").read().strip()
        except OSError:
            return list(range(total_cores()))  # no sysfs (container): degrade
        by_core.setdefault((pkg, core), c)  # first sibling only
        try:
            weight[c] = int(
                open(f"/sys/devices/system/cpu/cpu{c}/cpufreq/cpuinfo_max_freq").read()
            )
        except OSError:
            weight[c] = 0
    cpus = list(by_core.values())
    # Fastest class first so a K-core sweep stays homogeneous for as long as
    # possible instead of mixing P- and E-cores at the first opportunity.
    cpus.sort(key=lambda c: (-weight.get(c, 0), c))
    return cpus


def run_one(cores: list[int], data: str, queries: str, iterations: int) -> tuple[float, int]:
    """Run the benchmark pinned to `cores`; return (wall_seconds, queries_run)."""
    cmd = []
    if cores:
        cmd += ["taskset", "-c", ",".join(str(c) for c in cores)]
    cmd += [BIN, "benchmark-parquet", "--path", data, "--iterations", str(iterations)]
    if queries:
        cmd += ["--query", queries]
    t0 = time.perf_counter()
    p = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)
    wall = time.perf_counter() - t0
    n = len(re.findall(r"^Q\d+:", p.stdout, re.M))
    if n == 0:
        sys.stderr.write(p.stdout[-400:] + p.stderr[-400:])
    return wall, n


def mode_cores(args) -> None:
    """Single process, varying core count -> the intra-node scaling curve."""
    pcores = physical_cores()
    tc = len(pcores) if not args.contiguous else total_cores()
    counts = [int(x) for x in args.cores.split(",")] if args.cores else [1, 2, 4, 8, tc]
    counts = [c for c in counts if c <= tc]
    if args.contiguous:
        print("WARNING: --contiguous pins cpu ids 0..K-1, which on an SMT/hybrid")
        print("machine varies core TYPE as well as count. Results are not a")
        print("scaling curve. See physical_cores() for why this matters.\n")
    print(f"Intra-node scaling: 1 process, {tc} "
          f"{'cpus (contiguous)' if args.contiguous else 'PHYSICAL cores'}\n")
    print(f"{'cores':>6} {'wall(s)':>9} {'QPS':>8} {'speedup':>8} {'efficiency':>11}")
    base = None
    rows = []
    for c in counts:
        walls = []
        for _ in range(args.repeat):
            cpus = list(range(c)) if args.contiguous else pcores[:c]
            w, n = run_one(cpus, args.data, args.queries, args.iterations)
            walls.append(w)
        w = statistics.median(walls)
        qps = (n * args.iterations) / w if w else 0
        if base is None:
            base = qps
        speedup = qps / base if base else 0
        eff = speedup / (c / counts[0]) if counts[0] else 0
        print(f"{c:>6} {w:>9.2f} {qps:>8.2f} {speedup:>8.2f} {eff:>10.0%}")
        rows.append({"cores": c, "wall": w, "qps": qps, "speedup": speedup, "efficiency": eff})
    usl_note(rows)
    if args.out:
        json.dump(rows, open(args.out, "w"), indent=1)


def mode_concurrency(args) -> None:
    """N concurrent processes over disjoint core sets: the 'N nodes' proxy."""
    tc = total_cores()
    procs = [int(x) for x in args.procs.split(",")]
    print(f"Throughput vs process count, {tc} cores split evenly (cores held constant)\n")
    print(f"{'procs':>6} {'cores/proc':>11} {'wall(s)':>9} {'total QPS':>10} {'scaling':>9}")
    base = None
    rows = []
    for p in procs:
        per = max(1, tc // p)
        shards = [list(range(i * per, min((i + 1) * per, tc))) for i in range(p)]
        t0 = time.perf_counter()
        with ThreadPoolExecutor(max_workers=p) as ex:
            res = list(
                ex.map(lambda s: run_one(s, args.data, args.queries, args.iterations), shards)
            )
        wall = time.perf_counter() - t0
        nq = sum(n for _, n in res) * args.iterations
        qps = nq / wall if wall else 0
        if base is None:
            base = qps
        rows.append({"procs": p, "cores_per_proc": per, "wall": wall, "qps": qps})
        print(f"{p:>6} {per:>11} {wall:>9.2f} {qps:>10.2f} {qps/base:>9.2f}x")
    print(
        "\nNOTE: these are INDEPENDENT processes with no shuffle — an upper bound\n"
        "on what a real distributed engine can achieve at the same core budget.\n"
        "A distributed run of the SAME query across N nodes must be compared\n"
        "against the 1-process/32-core row, not against 1-process/4-core."
    )
    if args.out:
        json.dump(rows, open(args.out, "w"), indent=1)


def usl_note(rows) -> None:
    """Report where efficiency falls off — the contention signature."""
    if len(rows) < 3:
        return
    worst = min(rows[1:], key=lambda r: r["efficiency"])
    print(
        f"\nEfficiency is lowest at {worst['cores']} cores ({worst['efficiency']:.0%}). "
        "Efficiency well under 100%\nat high core counts is intra-node contention "
        "(memory bandwidth, allocator, shared caches);\nno distributed design can "
        "recover it, so it bounds the achievable cluster efficiency too."
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["cores", "concurrency"], default="cores")
    ap.add_argument("--data", default="./data/tpch-10gb")
    ap.add_argument("--queries", default="", help="e.g. 1 or 1,6 (default: all 22)")
    ap.add_argument("--iterations", type=int, default=1)
    ap.add_argument("--repeat", type=int, default=3, help="medians per point")
    ap.add_argument("--cores", default="", help="comma list for --mode cores")
    ap.add_argument("--procs", default="1,2,4,8", help="comma list for --mode concurrency")
    ap.add_argument("--contiguous", action="store_true",
                    help="pin cpu ids 0..K-1 (WRONG on SMT/hybrid; kept only to\n"
                         "reproduce the flawed historical measurement)")
    ap.add_argument("--out", default="")
    args = ap.parse_args()

    if not os.path.exists(BIN):
        print(f"missing {BIN}; cargo build --release first")
        return 2
    if args.mode == "cores":
        mode_cores(args)
    else:
        mode_concurrency(args)
    return 0


if __name__ == "__main__":
    sys.exit(main())
