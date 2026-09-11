#!/usr/bin/env python3
"""Warm SF10 baseline; invoke ONLY through scripts/claude-safe-build.sh.

Requires duckdb, pyarrow and requests. Keeps all samples and typed Arrow answers.
This is the repository's custom TPC-H-derived workload, not official TPC-H.
"""
import argparse
from collections import Counter, defaultdict
from decimal import Decimal
import hashlib
import json
import math
import multiprocessing
import os
from pathlib import Path
import platform
import random
import re
import signal
import socket
import statistics
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import requests

ROOT = Path(__file__).resolve().parents[1]
TABLES = ("region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem")
KEYS = dict(zip(TABLES, ("r_regionkey", "n_nationkey", "s_suppkey", "c_custkey",
                        "p_partkey", "ps_partkey", "o_orderkey", "l_orderkey")))
REL_TOL, ABS_TOL = 1e-10, 1e-8


def now():
    return datetime.now(timezone.utc).isoformat()


def save(path, value):
    path.write_text(json.dumps(value, indent=2, default=str, allow_nan=False) + "\n")


def sha(path):
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(8 * 1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def family(t):
    if pa.types.is_dictionary(t):
        return family(t.value_type)
    if pa.types.is_integer(t) or pa.types.is_decimal(t):
        return "exact_number"
    if pa.types.is_floating(t):
        return "float"
    if pa.types.is_string(t) or pa.types.is_large_string(t) or pa.types.is_string_view(t):
        return "string"
    if pa.types.is_date(t):
        return "date"
    return str(t)


def compare(actual, expected):
    """Projection-position comparison of typed bags; output order is not certified.

    Exact values partition the bag; only floating columns use explicit tolerance.
    Multiplicity is retained. Names/nullability and numeric widths are recorded
    separately. Integers/decimals compare exactly, never by conversion to float.
    """
    result = {"ok": False, "rows_engine": actual.num_rows, "rows_duckdb": expected.num_rows,
              "schema_engine": str(actual.schema), "schema_duckdb": str(expected.schema),
              "schema_identical": actual.schema.equals(expected.schema, check_metadata=False),
              "max_float_abs_error": 0.0, "max_float_rel_error": 0.0}
    if actual.num_columns != expected.num_columns or actual.num_rows != expected.num_rows:
        return dict(result, reason="shape mismatch")
    af = [family(x.type) for x in actual.schema]
    ef = [family(x.type) for x in expected.schema]
    if af != ef:
        return dict(result, reason=f"incompatible logical types: {af} vs {ef}")
    floating = [i for i, f in enumerate(af) if f == "float"]
    exact = [i for i, f in enumerate(af) if f != "float"]

    def rows(table):
        columns = [c.to_pylist() for c in table.columns]
        return list(zip(*columns))

    ar, er = rows(actual), rows(expected)
    if not floating:
        result["ok"] = Counter(ar) == Counter(er)
        if not result["ok"]:
            result["reason"] = "exact value/multiplicity mismatch"
            result["engine_only"] = repr(list((Counter(ar) - Counter(er)).items())[:3])
            result["duckdb_only"] = repr(list((Counter(er) - Counter(ar)).items())[:3])
        return result
    groups = defaultdict(list)
    for row in er:
        groups[tuple(row[i] for i in exact)].append(row)
    for row in ar:
        key = tuple(row[i] for i in exact)
        candidates = groups.get(key, [])
        matched = None
        for j, other in enumerate(candidates):
            good = True
            for i in floating:
                a, b = row[i], other[i]
                if a is None or b is None:
                    good = a is b
                elif math.isnan(a) or math.isnan(b):
                    good = math.isnan(a) and math.isnan(b)
                else:
                    good = math.isclose(a, b, rel_tol=REL_TOL, abs_tol=ABS_TOL)
                if not good:
                    break
            if good:
                matched = j
                break
        if matched is None:
            return dict(result, reason="typed bag mismatch", engine_row=repr(row),
                        duckdb_candidates=repr(candidates[:3]))
        other = candidates.pop(matched)
        for i in floating:
            a, b = row[i], other[i]
            if a is not None and b is not None and math.isfinite(a) and math.isfinite(b):
                delta = abs(a - b)
                result["max_float_abs_error"] = max(result["max_float_abs_error"], delta)
                result["max_float_rel_error"] = max(
                    result["max_float_rel_error"], delta / max(abs(a), abs(b), 1e-300))
    result["ok"] = all(not candidates for candidates in groups.values())
    return result


def selftest():
    def tab(vals, typ):
        return pa.table({"v": pa.array(vals, type=typ)})
    assert compare(tab([2**60 + 1], pa.int64()), tab([2**60 + 1], pa.decimal128(38, 0)))["ok"]
    assert not compare(tab([2**60 + 1], pa.int64()), tab([2**60], pa.int64()))["ok"]
    assert not compare(tab([None], pa.string()), tab([""], pa.string()))["ok"]
    assert not compare(tab(["01"], pa.string()), tab(["1"], pa.string()))["ok"]
    assert not compare(tab([1, 1, 2], pa.int64()), tab([1, 2, 2], pa.int64()))["ok"]
    assert compare(tab([1, None, 2], pa.int64()), tab([2, 1, None], pa.int64()))["ok"]
    assert compare(tab([1.0 + 1e-11], pa.float64()), tab([1.0], pa.float64()))["ok"]
    assert not compare(tab([1.0 + 1e-5], pa.float64()), tab([1.0], pa.float64()))["ok"]
    assert not compare(tab([None], pa.float64()), tab([0.0], pa.float64()))["ok"]
    assert compare(tab([float("nan")], pa.float64()), tab([float("nan")], pa.float64()))["ok"]
    assert not compare(tab([float("nan")], pa.float64()), tab([0.0], pa.float64()))["ok"]
    assert compare(tab([Decimal("1.23")], pa.decimal128(10, 2)),
                   tab([Decimal("1.230")], pa.decimal128(10, 3)))["ok"]
    print("Comparator self-tests: 12 passed", flush=True)


def proc_snapshot(pid):
    try:
        status = Path(f"/proc/{pid}/status").read_text()
        fields = {}
        for key in ("VmRSS", "VmHWM", "Threads", "Cpus_allowed_list", "THP_enabled"):
            m = re.search(rf"^{key}:\s*(.+)$", status, re.M)
            if m:
                fields[key] = m[1]
        fields["loadavg"] = os.getloadavg()
        fields["io"] = Path(f"/proc/{pid}/io").read_text()
        return fields
    except (FileNotFoundError, PermissionError):
        return {}



def iceberg_metadata(table):
    directory = ROOT / "data/tpch-10gb-iceberg" / table / "metadata"
    hint = directory / "version-hint.text"
    if hint.is_file():
        version = hint.read_text().strip().lstrip("v")
        path = directory / f"v{version}.metadata.json"
        if not path.is_file():
            raise FileNotFoundError(path)
        return path
    candidates = list(directory.glob("*.metadata.json"))
    return max(candidates, key=lambda p: (json.loads(p.read_text()).get("last-updated-ms", 0), str(p)))


def duck_worker(connection):
    """Extension execution in an owned, killable process; no forked DB state."""
    con = duckdb.connect()
    try:
        while True:
            request = connection.recv()
            if request is None:
                break
            try:
                table, detail = duck_run(con, request)
                connection.send((True, table, detail))
            except Exception as exc:
                connection.send((False, str(exc), None))
    finally:
        con.close()
        connection.close()


class DuckDBWorker:
    def __init__(self):
        context = multiprocessing.get_context("spawn")
        self.connection, child = context.Pipe()
        self.process = context.Process(target=duck_worker, args=(child,))
        self.process.start()
        child.close()
        self.result = None

    def query(self, sql, timeout=60):
        self.connection.send(sql)
        if not self.connection.poll(timeout + 5):
            self.close(force=True)
            raise TimeoutError(f"DuckDB extension worker exceeded {timeout + 5}s: {sql[:150]}")
        ok, table, detail = self.connection.recv()
        if not ok:
            raise RuntimeError(table)
        return table, detail

    def execute(self, sql):
        self.result, _ = self.query(sql)
        return self

    def fetchall(self):
        return list(zip(*(c.to_pylist() for c in self.result.columns)))

    def close(self, force=False):
        if self.process.is_alive() and not force:
            try:
                self.connection.send(None)
                self.process.join(timeout=5)
            except (EOFError, BrokenPipeError):
                pass
        if self.process.is_alive():
            self.process.terminate()
            self.process.join(timeout=5)
        if self.process.is_alive():
            self.process.kill()
            self.process.join(timeout=5)
        self.connection.close()


class Engine:
    def __init__(self, binary, track, out):
        self.binary, self.track, self.out = binary, track, out
        self.proc, self.log = None, None
        self.session = requests.Session()
        self.session.trust_env = False
        self.start_count = 0

    def start(self):
        self.stop()
        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            port = s.getsockname()[1]
        self.url = f"http://127.0.0.1:{port}"
        env = os.environ.copy()
        env["QE_IPC_CACHE"] = "0" if self.track in ("raw_parquet", "iceberg", "lance") else "auto"
        if self.track.startswith("gpu_"):
            env["QE_GPU"] = "1" if self.track == "gpu_assisted" else "0"
            env["QE_GPU_DEBUG"] = "1"
            env["QE_GPU_CACHE_MB"] = "24576"
            nvrtc = str(ROOT / ".venv/lib/python3.12/site-packages/nvidia/cuda_nvrtc/lib")
            env["LD_LIBRARY_PATH"] = nvrtc + ":" + env.get("LD_LIBRARY_PATH", "")
        env["QE_MEM_CAP"] = "48G"
        args = [str(self.binary), "serve", "--bind", f"127.0.0.1:{port}",
                "--flight-bind", "none", "--memory-limit", "40G"]
        if self.track in ("native", "iceberg", "lance"):
            args += ["--tables", str(ROOT / ("data/tpch-10gb-" + self.track))]
        else:
            args += ["--data", str(ROOT / "data/tpch-10gb")]
        if self.track.startswith("gpu_"):
            args.remove("serve")
            position = args.index("--flight-bind")
            del args[position:position + 2]
        self.start_count += 1
        self.log = (self.out / f"engine-{self.start_count}.log").open("w")
        self.proc = subprocess.Popen(args, cwd=ROOT, env=env, stdout=self.log,
                                     stderr=subprocess.STDOUT, start_new_session=True)
        started = time.monotonic()
        while time.monotonic() - started < 120:
            if self.proc.poll() is not None:
                raise RuntimeError(f"engine startup exited {self.proc.returncode}; see {self.log.name}")
            try:
                if self.session.get(self.url + "/readyz", timeout=1).ok:
                    save(self.out / f"engine-{self.start_count}-startup.json",
                         {"command": args, "cache_mode": env["QE_IPC_CACHE"],
                          "ready_seconds": time.monotonic() - started,
                          "process": proc_snapshot(self.proc.pid),
                          "gpu_environment": {k: env[k] for k in
                              ("QE_GPU", "QE_GPU_DEBUG", "QE_GPU_CACHE_MB", "LD_LIBRARY_PATH") if k in env}})
                    return
            except requests.RequestException:
                pass
            time.sleep(0.1)
        self.stop()
        raise TimeoutError("engine readiness timeout")

    def stop(self):
        if self.proc and self.proc.poll() is None:
            try:
                os.killpg(self.proc.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(self.proc.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                self.proc.wait(timeout=10)
        if self.log:
            self.log.close()
        self.proc, self.log = None, None

    def run(self, sql, limit_ms):
        if not self.proc or self.proc.poll() is not None:
            self.start()
        before = proc_snapshot(self.proc.pid)
        log_path = Path(self.log.name)
        log_offset = log_path.stat().st_size
        start = time.perf_counter()
        # Transport allowance is separate from the strict server-time 10x gate.
        try:
            response = self.session.post(self.url + "/sql?format=arrow&distributed=0",
                                         data=sql.encode(), timeout=(3, limit_ms / 1000 + 2))
        except requests.RequestException as exc:
            wall_ms = (time.perf_counter() - start) * 1000
            # HTTP disconnect does not cancel engine work: stop the owned process.
            self.stop()
            return None, {"status": "transport_failure", "error": str(exc), "wall_ms": wall_ms,
                          "limit_ms": limit_ms, "process_before": before}
        wire_ms = (time.perf_counter() - start) * 1000
        if not response.ok:
            return None, {"status": "query_error", "http_status": response.status_code,
                          "error": response.text[:4000], "wall_ms": wire_ms,
                          "limit_ms": limit_ms, "process_before": before}
        table = pa.ipc.open_stream(response.content).read_all()
        client_ms = (time.perf_counter() - start) * 1000
        elapsed_ms = float(response.headers["x-qe-elapsed-ms"])
        assert response.headers["x-qe-distributed"] == "false"
        assert int(response.headers["x-qe-rows"]) == table.num_rows
        record = None
        query_id = response.headers.get("x-qe-query-id")
        if query_id:
            detail = self.session.get(self.url + "/queries/" + query_id, timeout=5)
            if detail.ok:
                record = detail.json()
        with log_path.open("rb") as trace_file:
            trace_file.seek(log_offset)
            trace = trace_file.read().decode(errors="replace")
        return table, {"status": "completed", "ms": elapsed_ms, "wall_ms": client_ms,
                       "gpu_trace": trace if self.track.startswith("gpu_") else "",
                       "gpu_run_ok_count": trace.count("[gpu-trace] run OK"),
                       "gpu_not_ready_count": trace.count("[gpu-trace] not ready"),
                       "gpu_run_failed_count": trace.count("[gpu-trace] run FAILED"),
                       "wire_ms": wire_ms, "query_id": query_id, "query_record": record,
                       "limit_ms": limit_ms, "within_10x": elapsed_ms <= limit_ms,
                       "process_before": before, "process_after": proc_snapshot(self.proc.pid)}


def duck_run(con, sql, timeout=60):
    if isinstance(con, DuckDBWorker):
        return con.query(sql, timeout)
    timer = threading.Timer(timeout, con.interrupt)
    timer.daemon = True
    timer.start()
    before = proc_snapshot(os.getpid())
    start = time.perf_counter()
    try:
        table = con.execute(sql).fetch_arrow_table()
        ms = (time.perf_counter() - start) * 1000
        return table, {"status": "completed", "ms": ms, "wall_ms": ms,
                       "process_before": before, "process_after": proc_snapshot(os.getpid())}
    finally:
        timer.cancel()
        timer.join()


def save_arrow(path, table):
    with pa.OSFile(str(path), "wb") as sink:
        with pa.ipc.new_stream(sink, table.schema) as writer:
            writer.write_table(table)


def distribution(values):
    if not values:
        return None
    ordered = sorted(values)
    n = len(values)
    def percentile(p):
        k = (n - 1) * p
        lower = int(k)
        return ordered[lower] + (ordered[min(lower + 1, n - 1)] - ordered[lower]) * (k - lower)
    return {"n": n, "median": statistics.median(values), "min": min(values),
            "max": max(values), "p25": percentile(.25), "p75": percentile(.75),
            "p95": percentile(.95)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path)
    parser.add_argument("--binary", type=Path, default=ROOT / "target-rel/release/query_engine")
    parser.add_argument("--samples", type=int, default=10)
    parser.add_argument("--tracks", default="raw_parquet,ipc_cache,native")
    parser.add_argument("--queries", default=",".join(str(i) for i in range(1, 23)))
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--oracle-dir", type=Path, help="Earlier source-Parquet Arrow results")
    args = parser.parse_args()
    if "safe-build-" not in Path("/proc/self/cgroup").read_text():
        parser.error("run through scripts/claude-safe-build.sh (memory-capped cgroup required)")
    selftest()
    if args.self_test:
        return 0
    if not args.out or args.samples < 1:
        parser.error("--out required and --samples must be positive")
    tracks = args.tracks.split(",")
    if any(t not in ("raw_parquet", "ipc_cache", "native", "iceberg", "lance", "gpu_assisted", "gpu_cpu_control") for t in tracks):
        parser.error("unknown track")
    affinity = sorted(os.sched_getaffinity(0))
    if len(affinity) != 16 or os.environ.get("RAYON_NUM_THREADS") != "16":
        parser.error("pin to 16 CPUs and set RAYON_NUM_THREADS=16 before launch")
    if os.environ.get("TMPDIR") != str(ROOT / ".scratch"):
        parser.error("TMPDIR must be the absolute repository .scratch path")
    out = args.out.resolve()
    out.mkdir(parents=True, exist_ok=False)
    binary = args.binary.resolve()
    sql_source = (ROOT / "src/tpch/queries.rs").read_text()
    queries = {int(m.group(1)): m.group(2).strip() for m in re.finditer(
        r'pub const Q(\d+): &str = r#"(.*?)"#;', sql_source, re.S)}
    assert len(queries) == 22
    assert queries[11].count("0.0001") == 1
    queries[11] = queries[11].replace("0.0001", "0.00001")
    query_ids = [int(q) for q in args.queries.split(",")]
    assert all(q in queries for q in query_ids)
    (out / "sql").mkdir()
    for q in query_ids:
        (out / "sql" / f"q{q:02}.sql").write_text(queries[q] + "\n")
    manifest = {
        "start_utc": now(), "argv": sys.argv, "cwd": str(ROOT),
        "source_head": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        "source_diff": subprocess.check_output(["git", "diff", "--", "src", "Cargo.toml", "Cargo.lock"],
                                               cwd=ROOT, text=True),
        "binary": str(binary), "binary_sha256": sha(binary),
        "driver_sha256": sha(Path(__file__)), "cargo_lock_sha256": sha(ROOT / "Cargo.lock"),
        "sql_source_sha256": sha(ROOT / "src/tpch/queries.rs"),
        "workload": "custom TPC-H-derived SF10, existing data, Q11 threshold 0.00001",
        "duckdb_version": duckdb.__version__, "pyarrow_version": pa.__version__,
        "python_version": platform.python_version(), "platform": platform.platform(),
        "lscpu": subprocess.check_output(["lscpu"], text=True),
        "cpu_affinity": affinity, "threads": 16, "execution_memory_bytes": 40 * 1024**3,
        "qe_mem_cap": "48G", "shared_cgroup_memory_max": os.environ.get("SAFE_BUILD_MEM"),
        "environment": {k: v for k, v in os.environ.items() if k.startswith(
            ("QE_", "QUERY_ENGINE_", "RAYON_", "TOKIO_", "MIMALLOC_", "OMP_", "SAFE_BUILD_", "RUST"))
            or k == "TMPDIR"},
        "cgroup": Path("/proc/self/cgroup").read_text(),
        "initial_process": proc_snapshot(os.getpid()),
        "samples_per_query": args.samples, "seed": 20260905, "tracks": tracks,
        "timing": "engine x-qe-elapsed-ms; DuckDB execute+Arrow materialization; client wall also retained",
        "protocol": "DuckDB warmup + 3 calibration samples; engine warmup; paired random order samples",
        "cache": "warm; no OS cache drops; preflight and hash reads excluded",
        "comparison": {"mode": "typed multiset", "float_rel_tol": REL_TOL, "float_abs_tol": ABS_TOL,
                       "order_checked": False, "schema_differences_recorded": True},
        "data": {}, "native_manifests": {}, "ipc_cache_inventory": [], "ipc_cache_freshness": {},
        "extra_inputs": {}, "source_oracle": {}, "reference_extensions": {},
        "gpu_adapter_sha256": sha(ROOT / "examples/sf10_gpu_serve.rs")
            if any(t.startswith("gpu_") for t in tracks) else None,
    }
    print("Hashing inputs and recording provenance (outside timings)", flush=True)
    for table in TABLES:
        path = ROOT / "data/tpch-10gb" / f"{table}.parquet"
        metadata = pq.read_metadata(path)
        manifest["data"][table] = {"path": str(path), "bytes": path.stat().st_size,
                                  "sha256": sha(path), "rows": metadata.num_rows,
                                  "schema": str(pq.read_schema(path))}
        sidecar = Path(str(path) + ".qeipc")
        expected_stamp = f"v2:{path.stat().st_size}:{int(path.stat().st_mtime)}"
        fresh = ((sidecar / ".complete").is_file()
                 and (sidecar / ".complete").read_text() == expected_stamp)
        missing = [i for i in range(metadata.num_row_groups)
                   if not (sidecar / f"rg_{i:05}.arrow").is_file()]
        manifest["ipc_cache_freshness"][table] = {
            "fresh": fresh, "expected_stamp": expected_stamp,
            "row_groups": metadata.num_row_groups, "missing_row_groups": missing}
        if any(t in ("ipc_cache", "gpu_assisted", "gpu_cpu_control") for t in tracks) and (not fresh or missing):
            raise RuntimeError(f"IPC sidecar missing/stale for {table}; refusing mixed cache baseline")
        native = ROOT / "data/tpch-10gb-native" / table / "_manifest.json"
        manifest["native_manifests"][table] = {"sha256": sha(native), "content": json.loads(native.read_text())}
    for path in sorted((ROOT / "data/tpch-10gb").glob("*.qeipc/*")):
        if path.is_file():
            manifest["ipc_cache_inventory"].append({"path": str(path.relative_to(ROOT)),
                "bytes": path.stat().st_size, "mtime_ns": path.stat().st_mtime_ns, "sha256": sha(path)})
    for track in tracks:
        if track in ("iceberg", "lance"):
            directory = ROOT / ("data/tpch-10gb-" + track)
            manifest["extra_inputs"][track] = []
            for path in sorted(directory.rglob("*")):
                if path.is_file():
                    manifest["extra_inputs"][track].append({
                        "path": str(path.relative_to(ROOT)), "bytes": path.stat().st_size,
                        "sha256": sha(path)})
    if "iceberg" in tracks:
        manifest["iceberg_snapshots"] = {
            t: {"metadata": str(iceberg_metadata(t)), "sha256": sha(iceberg_metadata(t)),
                "snapshot_id": json.loads(iceberg_metadata(t).read_text())["current-snapshot-id"]}
            for t in TABLES}
    if "lance" in tracks:
        import lance
        manifest["pylance_version_for_metadata"] = lance.__version__
        manifest["lance_versions"] = {
            t: {"version": lance.dataset(str(ROOT / "data/tpch-10gb-lance" / f"{t}.lance")).version,
                "schema": str(lance.dataset(str(ROOT / "data/tpch-10gb-lance" / f"{t}.lance")).schema)}
            for t in TABLES}
    if any(t.startswith("gpu_") for t in tracks):
        manifest["nvidia_smi_start"] = subprocess.check_output(
            ["nvidia-smi", "--query-gpu=name,memory.total,memory.free,driver_version",
             "--format=csv,noheader"], text=True)
        manifest["gpu_cache_bytes"] = 24576 * 1024**2
        manifest["gpu_adapter"] = "Benchmark-only HTTP loader opts context into GPU; stock serve disables GPU"
    oracle_tables = {}
    if args.oracle_dir:
        oracle_out = out / "source-oracle"
        oracle_out.mkdir()
        for q in query_ids:
            sql_path = args.oracle_dir.parent / "sql" / f"q{q:02}.sql"
            assert sql_path.read_text().strip() == queries[q]
            path = args.oracle_dir / f"q{q:02}-duckdb.arrow"
            oracle_tables[q] = pa.ipc.open_stream(path.read_bytes()).read_all()
            save_arrow(oracle_out / path.name, oracle_tables[q])
            manifest["source_oracle"][str(q)] = {"path": str(path.resolve()), "sha256": sha(path)}
    save(out / "manifest.json", manifest)
    summaries, failures = [], []
    rng = random.Random(20260905)
    traces = (out / "samples.jsonl").open("w")
    try:
        for track in tracks:
            track_out = out / track
            track_out.mkdir()
            engine = Engine(binary, track, track_out)
            con = DuckDBWorker() if track in ("iceberg", "lance") else duckdb.connect()
            con.execute("SET threads=16")
            con.execute(f"SET memory_limit='{40 * 1024**3}B'")
            duck_temp = out / "duckdb-temp"
            duck_temp.mkdir(exist_ok=True)
            con.execute("SET temp_directory='" + str(duck_temp).replace("'", "''") + "'")
            try:
                load_start = time.perf_counter()
                con.execute("SET autoinstall_known_extensions=false")
                if track == "iceberg":
                    con.execute("LOAD avro")
                    con.execute("LOAD iceberg")
                if track == "lance":
                    con.execute("LOAD lance")
                for table in TABLES:
                    if track == "iceberg":
                        path = str(iceberg_metadata(table)).replace("'", "''")
                        con.execute(f"CREATE VIEW {table} AS SELECT * FROM iceberg_scan('{path}')")
                    elif track == "lance":
                        path = str(ROOT / "data/tpch-10gb-lance" / f"{table}.lance").replace("'", "''")
                        con.execute(f"CREATE VIEW {table} AS SELECT * FROM '{path}'")
                    else:
                        path = str(ROOT / "data/tpch-10gb" / f"{table}.parquet").replace("'", "''")
                        kind = "TABLE" if track == "native" else "VIEW"
                        con.execute(f"CREATE {kind} {table} AS SELECT * FROM read_parquet('{path}')")
                schemas = {t: con.execute("DESCRIBE " + t).fetchall() for t in TABLES}
                save(track_out / "duckdb-input-schemas.json", schemas)
                manifest["reference_extensions"][track] = con.execute(
                    "SELECT extension_name, extension_version, install_path FROM duckdb_extensions() WHERE loaded").fetchall()
                save(out / "manifest.json", manifest)
                save(track_out / "duckdb-setup.json", {"load_seconds": time.perf_counter() - load_start,
                    "settings": con.execute("SELECT name, value FROM duckdb_settings() WHERE name IN "
                                           "('threads','memory_limit','temp_directory','preserve_insertion_order')").fetchall()})
                engine.start()
                # Independent storage identity smoke checks, excluded from timings.
                checks = []
                for table in TABLES:
                    key = KEYS[table]
                    sql = (f"SELECT COUNT(*) AS n, COUNT({key}) AS nonnull, MIN({key}) AS lo, "
                           f"MAX({key}) AS hi, SUM({key}) AS total FROM {table}")
                    d, _ = duck_run(con, sql)
                    e, detail = engine.run(sql, 60000)
                    check = compare(e, d) if e is not None else {"ok": False, "error": detail}
                    checks.append({"table": table, "check": check})
                save(track_out / "data-preflight.json", checks)
                if not all(c["check"]["ok"] for c in checks):
                    raise RuntimeError(f"{track}: storage identity smoke check failed")
                for q in query_ids:
                    sql = queries[q]
                    print(f"{track} Q{q:02}: warmup/calibration", flush=True)
                    expected, _ = duck_run(con, sql)
                    calibration = []
                    for _ in range(3):
                        _, d = duck_run(con, sql)
                        calibration.append(d["ms"])
                    ref_ms = statistics.median(calibration)
                    ceiling = 10 * ref_ms
                    actual, warm = engine.run(sql, ceiling)
                    warm_check = compare(actual, expected) if actual is not None else {"ok": False}
                    gpu_warmups = [{"attempt": 0, "engine": warm, "comparison": warm_check}]
                    if track == "gpu_assisted":
                        for attempt in range(20):
                            # Retire asynchronous uploads before paired timings. A completed device
                            # trace proves use; unsupported shapes have no GPU wrapper and need none.
                            plan = (warm.get("query_record") or {}).get("physical_plan", "")
                            if warm["status"] != "completed":
                                break
                            if "GpuAggExec" not in plan or (warm.get("gpu_run_ok_count", 0) > 0
                                                          and warm.get("gpu_not_ready_count", 0) == 0
                                                          and warm.get("gpu_run_failed_count", 0) == 0):
                                break
                            time.sleep(.1)
                            actual, warm = engine.run(sql, ceiling)
                            warm_check = compare(actual, expected) if actual is not None else {"ok": False}
                            gpu_warmups.append({"attempt": attempt + 1, "engine": warm, "comparison": warm_check})
                            if actual is None:
                                break
                    oracle_check = compare(expected, oracle_tables[q]) if q in oracle_tables else {"ok": True}
                    save(track_out / f"q{q:02}-warmup.json", {
                        "duckdb_calibration_ms": calibration, "reference_ms": ref_ms,
                        "engine": warm, "comparison": warm_check, "extra_gpu_warmups": gpu_warmups,
                        "reference_vs_source_oracle": oracle_check})
                    save_arrow(track_out / f"q{q:02}-duckdb.arrow", expected)
                    if actual is not None:
                        save_arrow(track_out / f"q{q:02}-engine.arrow", actual)
                    plan = con.execute("EXPLAIN " + sql).fetchall()
                    save(track_out / f"q{q:02}-duckdb-plan.json", plan)
                    plan = (warm.get("query_record") or {}).get("physical_plan", "")
                    warm_ready = warm["status"] == "completed"
                    if track == "gpu_assisted" and "GpuAggExec" in plan:
                        warm_ready = (warm_ready and warm.get("gpu_run_ok_count", 0) > 0
                                      and warm.get("gpu_not_ready_count", 0) == 0
                                      and warm.get("gpu_run_failed_count", 0) == 0)
                    if not warm_ready:
                        # Stop pending uploads/work, retain failure, and still attempt later queries.
                        engine.stop()
                        row = {"track": track, "query": q, "engine_ms": None, "engine_client_ms": None,
                               "duckdb_ms": None, "ratio": None, "correct": False, "within_10x": False,
                               "reference_ms": ref_ms, "completed_samples": 0, "rows": expected.num_rows,
                               "gpu_samples_with_device_execution": 0, "gpu_device_aggregate_runs": 0,
                               "gpu_samples_requesting_upload": 0, "max_float_abs_error": 0,
                               "status": "warmup_failed_or_gpu_not_ready"}
                        summaries.append(row)
                        failures.append({"track": track, "query": q, "reason": row["status"]})
                        save(out / "summary.json", {"queries": summaries, "failures": failures})
                        print(f"{track} Q{q:02}: {row['status']}; no warm samples accepted", flush=True)
                        continue
                    samples, comparisons = [], []
                    for iteration in range(args.samples):
                        order = ["engine", "duckdb"]
                        rng.shuffle(order)
                        pair = {}
                        for side in order:
                            if side == "duckdb":
                                table_result, detail = duck_run(con, sql)
                            else:
                                table_result, detail = engine.run(sql, ceiling)
                            pair[side] = (table_result, detail)
                        e, ed = pair["engine"]
                        d, dd = pair["duckdb"]
                        check = compare(e, d) if e is not None else {"ok": False, "reason": ed["status"]}
                        if q in oracle_tables:
                            check["engine_vs_source_oracle"] = compare(e, oracle_tables[q]) if e is not None else {"ok": False}
                            check["reference_vs_source_oracle"] = compare(d, oracle_tables[q])
                            check["ok"] = (check["ok"] and check["engine_vs_source_oracle"]["ok"]
                                           and check["reference_vs_source_oracle"]["ok"])
                        comparisons.append(check)
                        sample = {"utc": now(), "track": track, "query": q,
                                  "iteration": iteration + 1, "order": order, "reference_ms": ref_ms,
                                  "engine": ed, "duckdb": dd, "comparison": check}
                        traces.write(json.dumps(sample, default=str, allow_nan=False) + "\n")
                        traces.flush()
                        samples.append(sample)
                        if not check["ok"] and e is not None:
                            save_arrow(track_out / f"q{q:02}-failure-{iteration + 1}-engine.arrow", e)
                            save_arrow(track_out / f"q{q:02}-failure-{iteration + 1}-duckdb.arrow", d)
                        if ed["status"] != "completed":
                            # Do not label a restarted engine's first query a warm sample.
                            break
                    es = distribution([s["engine"]["ms"] for s in samples if s["engine"]["status"] == "completed"])
                    ds = distribution([s["duckdb"]["ms"] for s in samples])
                    ew = distribution([s["engine"]["wall_ms"] for s in samples if s["engine"]["status"] == "completed"])
                    valid = (all(c["ok"] for c in comparisons) and warm_check["ok"] and oracle_check["ok"]
                             and all(w["comparison"]["ok"] for w in gpu_warmups))
                    perf = (len(samples) == args.samples and
                            all(s["engine"].get("within_10x", False) for s in samples))
                    row = {"track": track, "query": q, "engine_ms": es, "engine_client_ms": ew,
                           "duckdb_ms": ds, "ratio": es["median"] / ds["median"] if es else None,
                           "correct": valid, "within_10x": perf, "reference_ms": ref_ms,
                           "completed_samples": es["n"] if es else 0,
                           "rows": expected.num_rows,
                           "gpu_samples_with_device_execution": sum(s["engine"].get("gpu_run_ok_count", 0) > 0 for s in samples),
                           "gpu_device_aggregate_runs": sum(s["engine"].get("gpu_run_ok_count", 0) for s in samples),
                           "gpu_samples_requesting_upload": sum(s["engine"].get("gpu_not_ready_count", 0) > 0 for s in samples),
                           "max_float_abs_error": max(c.get("max_float_abs_error", 0) for c in comparisons)}
                    summaries.append(row)
                    if not valid or not perf:
                        failures.append({"track": track, "query": q, "correct": valid, "within_10x": perf})
                    save(out / "summary.json", {"queries": summaries, "failures": failures})
                    em = f"{es['median']:.3f}" if es else "FAILED"
                    print(f"{track} Q{q:02}: engine={em} ms duckdb={ds['median']:.3f} ms "
                          f"ratio={row['ratio']} correct={valid} within_10x={perf} "
                          f"gpu_samples={row['gpu_samples_with_device_execution']}", flush=True)
            finally:
                engine.stop()
                con.close()
    finally:
        traces.close()
        manifest["end_utc"] = now()
        manifest["final_process"] = proc_snapshot(os.getpid())
        manifest["gpu_measured_samples_with_device_execution"] = sum(
            r["gpu_samples_with_device_execution"] for r in summaries)
        if "gpu_assisted" in tracks and not manifest["gpu_measured_samples_with_device_execution"]:
            failures.append({"track": "gpu_assisted", "reason": "No measured device execution"})
        manifest["completed_query_tracks"] = len(summaries)
        manifest["expected_query_tracks"] = len(query_ids) * len(tracks)
        save(out / "manifest.json", manifest)
        save(out / "summary.json", {"queries": summaries, "failures": failures,
             "complete": len(summaries) == len(query_ids) * len(tracks)})
    print(f"Saved baseline to {out}; failed query-tracks={len(failures)}", flush=True)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
