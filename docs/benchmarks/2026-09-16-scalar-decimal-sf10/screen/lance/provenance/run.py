"""Contained paired embedded runs. Missing/failed cases stay in the evidence.

CPU provider tracks require separately validated conversions and matched native
or direct-extension references. GPU/residency timing, streamed result budgets and
resource/concurrency certification remain explicit follow-up work.
"""
import json
import os
import platform
import random
import selectors
import shutil
import signal
import statistics
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from .compare import compare_files
from .contract import ContractError, TIMING, digest, positive, report, require
from .prepare import DUCKDB_VERSION, write_json
from .providers import provider_setup, require_supported
from .gpu_residency import execution_telemetry


def containment():
    entry = next((line.split(":", 2)[2] for line in Path("/proc/self/cgroup").read_text().splitlines()
                  if line.startswith("0::")), None)
    require(entry is not None, "cgroup v2 required")
    path = Path("/sys/fs/cgroup") / entry.lstrip("/")
    limits = {name: (path / name).read_text().strip()
              for name in ("memory.max", "memory.swap.max")}
    require(limits["memory.max"] != "max" and int(limits["memory.max"]) > 0,
            "run through scripts/claude-safe-build.sh with a finite memory cap")
    require(limits["memory.swap.max"] == "0", "benchmark scope must disable swap")
    return {"path": str(path), **limits}


def memory_events(capped):
    root = Path(capped["path"])
    return {name: (root / name).read_text().strip()
            for name in ("memory.events", "memory.peak", "memory.current")}


class Worker:
    def __init__(self, command, environment, log_path, events, startup_seconds=120):
        self.command, self.events = command, events
        self.gpu_trace = environment.get("QE_GPU_DEBUG") == "1" and environment.get("QE_GPU") == "1"
        self.log = Path(log_path).open("wb")
        self.process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                        stderr=self.log, env=environment, start_new_session=True)
        self.selector = selectors.DefaultSelector()
        self.selector.register(self.process.stdout, selectors.EVENT_READ)
        self.buffer = b""
        self.ready = self.read(startup_seconds)
        self.lifecycle = {"command": command, "ready": self.ready, "stderr": str(log_path)}
        if hasattr(self, "teardown"):
            self.lifecycle["teardown"] = self.teardown
        self.events.append(self.lifecycle)
        if self.ready.get("status") != "ready":
            self.close(force=True)

    def read(self, seconds):
        deadline = time.monotonic() + seconds
        while b"\n" not in self.buffer:
            remaining = deadline - time.monotonic()
            if remaining <= 0 or not self.selector.select(remaining):
                self.close(force=True)
                return {"status": "timeout", "error": "worker response deadline exceeded"}
            block = os.read(self.process.stdout.fileno(), 65536)
            if not block:
                code = self.process.wait()
                # SIGKILL alone does not establish OOM; retain the exact signal.
                return {"status": "crash", "exit_code": code}
            self.buffer += block
            if len(self.buffer) > 1024 * 1024:
                self.close(force=True)
                return {"status": "crash", "error": "protocol line exceeds 1 MiB"}
        line, self.buffer = self.buffer.split(b"\n", 1)
        try:
            value = json.loads(line)
            require(isinstance(value, dict), "worker response must be an object")
            return value
        except (ValueError, TypeError) as error:
            self.close(force=True)
            return {"status": "crash", "error": f"invalid worker protocol: {error}"}

    def query(self, request, ceiling_ms=None):
        if self.ready.get("status") != "ready":
            # No SQL ran: retain the actual setup failure rather than replacing
            # a named refusal with an unavailable-worker error. Each record is
            # explicitly a dependent, unexecuted request, not another refusal.
            return {**self.ready, "id": request["id"], "phase": "setup",
                    "executed": False, "setup_failure_reused": True}
        trace_start = Path(self.log.name).stat().st_size if self.gpu_trace else 0
        if self.process.poll() is not None:
            return {"status": "startup_error", "error": "worker unavailable", "exit_code": self.process.returncode}
        try:
            self.process.stdin.write((json.dumps(request) + "\n").encode())
            self.process.stdin.flush()
        except (BrokenPipeError, OSError) as error:
            return {"status": "crash", "error": str(error)}
        phase, wait = "dispatch", 30.0
        while True:
            result = self.read(wait)
            if result.get("status") in ("timeout", "crash"):
                result["phase"] = phase
                return result
            if result.get("id") != request["id"]:
                self.close(force=True)
                return {"status": "crash", "error": "response ID mismatch"}
            if result.get("event") == "started":
                phase = "query"
                # 100 ms pipe/scheduling grace affects the watchdog only. The
                # exact engine elapsed gate has no floor or grace period.
                wait = ceiling_ms / 1000 + .1 if ceiling_ms is not None else 120.0
            elif result.get("event") == "query_finished":
                phase, wait = "serialization", 120.0
            else:
                trace = ""
                if self.gpu_trace:
                    with Path(self.log.name).open("rb") as log:
                        log.seek(trace_start)
                        trace = log.read().decode(errors="replace")
                if self.gpu_trace or result.get("gpu_resident_evidence") is not None:
                    result["gpu_execution"] = execution_telemetry(result, trace)
                    if self.gpu_trace:
                        result["gpu_execution"].update(
                            trace_start=trace_start,
                            trace_end=Path(self.log.name).stat().st_size)
                return result

    def restart_for_next_query(self):
        # A query-time crash can be SQL-dependent. A failed identical setup is
        # session-wide evidence; retrying it for every query adds no coverage.
        return self.ready.get("status") == "ready" and self.process.poll() is not None

    def close(self, *, force=False, grace_seconds=10):
        # Startup refusal, per-query failure and outer finally may all own cleanup.
        if getattr(self, "_closed", False):
            return
        self._closed = True
        teardown = {"requested": "abort" if force else "graceful", "forced": False}
        if self.process.poll() is None and not force:
            # Normal EOF lets in-memory engines release spill files and joined
            # workers. Deadlines/protocol failures explicitly request abort;
            # this grace never extends a timed query's watchdog.
            try:
                self.process.stdin.close()
            except (BrokenPipeError, OSError):
                pass
            try:
                self.process.wait(timeout=grace_seconds)
            except subprocess.TimeoutExpired:
                teardown["grace_expired"] = True
        if self.process.poll() is None:
            teardown["forced"] = True
            try:
                os.killpg(self.process.pid, signal.SIGKILL)
            except ProcessLookupError:
                # The child exited between poll and kill; still reap it.
                pass
            self.process.wait()
        teardown["exit_code"] = self.process.returncode
        self.teardown = teardown
        # Startup read failures can close before __init__ publishes its record.
        if hasattr(self, "lifecycle"):
            self.lifecycle["teardown"] = teardown
        self.selector.close()
        for stream in (self.process.stdin, self.process.stdout, self.log):
            try:
                stream.close()
            except (BrokenPipeError, OSError):
                pass


def verify_dataset(path):
    path = Path(path).resolve()
    dataset = json.loads(path.read_text())
    require(dataset.get("version") == 1 and dataset.get("status") == "prepared", "dataset not prepared")
    require(dataset.get("duckdb_version") == DUCKDB_VERSION, "dataset DuckDB version mismatch")
    require(dataset.get("queries") and dataset.get("tables"), "dataset membership required")
    for table in dataset["tables"]:
        require(digest(path.parent / table["path"]) == table["sha256"], f"table hash mismatch: {table['name']}")
    for query in dataset["queries"]:
        for prefix in ("sql", "original_sql", "oracle_sql"):
            require(digest(path.parent / query[prefix + "_path"]) == query[prefix + "_sha256"],
                    f"SQL hash mismatch: {query['id']}/{prefix}")
    return dataset


def compare_execution(actual, expected, query, scratch):
    complete_oracle = query['result_policy'] in ('limit_ties', 'unordered_limit') or query.get('oracle_output_columns') is not None
    if actual.get('status') != 'completed' or expected.get('status') != 'completed':
        return {'ok': False, 'policy': query['result_policy'], 'status': 'validation_error',
                'reason': 'execution did not complete', 'limit_rows': query['limit_rows'],
                'offset_rows': query.get('offset_rows', 0),
                'oracle_output_columns': query.get('oracle_output_columns'),
                'oracle_order_by': query['order_by'], 'expected_is_complete': complete_oracle}
    return compare_files(actual['output'], expected['output'], scratch=scratch,
                         policy=query['result_policy'], order_by=query['order_by'],
                         limit_rows=query['limit_rows'], offset_rows=query.get('offset_rows', 0),
                         oracle_output_columns=query.get('oracle_output_columns'),
                         expected_is_complete=complete_oracle)


def run(args):
    require_supported(args.track)
    residency = getattr(args, "gpu_residency", "mixed")
    require(residency in ("mixed", "required"), "invalid GPU residency policy")
    required = residency == "required"
    preloaded = required or getattr(args, "host_arrow_preloaded", False)
    require(not required or args.track == "gpu", "required residency is only valid for gpu")
    require(not preloaded or args.track in ("gpu", "gpu_control"), "explicit preload is only for GPU/control")
    preparation_timeout = getattr(args, "gpu_preparation_timeout_seconds", 120)
    require(type(preparation_timeout) is int and preparation_timeout > 0, "positive integer preparation deadline required")
    provider_track = "decoded_ipc" if preloaded else args.track
    capped = containment()
    dataset_path = Path(args.dataset).resolve()
    dataset = verify_dataset(dataset_path)
    tables, provider_provenance = provider_setup(dataset_path, dataset, provider_track, getattr(args, "provider_manifest", None))
    from .reference_runtime import reference_runtime
    runtime = reference_runtime(args.track, getattr(args, "reference_lance_io_limit", None), os.environ)
    require(args.samples > 0 and args.sessions > 0 and args.threads > 0, "positive run dimensions required")
    binary = Path(args.engine_binary).resolve()
    require(binary.is_file() and os.access(binary, os.X_OK), "executable embedded adapter required")
    root = Path(args.output).resolve()
    root.mkdir(parents=True, exist_ok=False)
    scratch = root / "scratch"
    scratch.mkdir()
    env = dict(os.environ, TMPDIR=str(scratch), QE_IPC_CACHE="0", QE_GPU="1" if args.track == "gpu" else "0",
               QE_GPU_DEBUG="1" if args.track == "gpu" else "0",
               QE_MEM_CAP=f"{args.process_cap_gib}G", RAYON_NUM_THREADS=str(args.threads))
    require(args.process_cap_gib >= args.memory_gib > 0, "positive query budget must fit process cap")
    setup = {"track": args.track, "threads": args.threads, "memory_limit": f"{args.memory_gib * 1024**3}B",
             "process_cap_bytes": args.process_cap_gib * 1024**3,
             "temp_directory": str(scratch / "duckdb-temp"),
             "tables": tables, **runtime}
    if preloaded:
        setup.update(host_arrow_preloaded=True, gpu_residency_required=required)
    control_provenance = None
    if args.track == "gpu":
        control_path = Path(getattr(args, "gpu_control_manifest", None) or "")
        require(control_path.is_file(), "GPU requires --gpu-control-manifest from a completed same-binary gpu_control run")
        control = json.loads(control_path.read_text())
        control_samples = control_path.parent / "samples.jsonl"
        with control_samples.open() as samples:
            fresh_control_report = report(control, (json.loads(line) for line in samples if line.strip()))
        require(control.get("tracks") == ["gpu_control"] and fresh_control_report.get("complete") is True,
                "GPU CPU control must complete correctly")
        require(control.get("timing_boundary") == TIMING, "GPU control timing boundary differs")
        require(control.get("samples_per_query") == args.samples and len(control.get("sessions", [])) == args.sessions,
                "GPU control sample/session dimensions differ")
        require(control.get("engine_binary_sha256") == digest(binary), "GPU control must use the same binary")
        require(control.get("dataset_manifest_sha256") == digest(dataset_path), "GPU control must use the same dataset")
        old_setup = control["environment"]["setup"]
        require(all(old_setup.get(key) == setup[key] for key in ("threads", "memory_limit", "process_cap_bytes", "tables")),
                "GPU control execution settings differ")
        require(old_setup.get("host_arrow_preloaded", False) == preloaded, "GPU control host residency differs")
        require(old_setup.get("gpu_residency_required", False) is False, "GPU control cannot require device residency")
        if preloaded:
            require(control.get("provider_conversion", {}).get("sha256") == provider_provenance["sha256"], "GPU control conversion differs")
        require(control["environment"]["affinity"] == sorted(os.sched_getaffinity(0)), "GPU control CPU affinity differs")
        control_provenance = {"path": str(control_path.resolve()), "sha256": digest(control_path),
                              "samples_sha256": digest(control_samples), "fresh_completion": fresh_control_report["complete"]}
    write_json(root / "setup.json", setup)
    queries = dataset["queries"]
    from .resident_runner import manifest_residency
    manifest = {"version": 1, "profile": "development",
                **manifest_residency(required, preloaded, preparation_timeout),
                "tracks": [args.track], "sessions": [f"s{i + 1}" for i in range(args.sessions)],
                "samples_per_query": args.samples, "timing_boundary": TIMING,
                "source_revision": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
                "working_tree": subprocess.check_output(["git", "status", "--short"], text=True),
                "engine_binary_sha256": digest(binary), "duckdb_version": DUCKDB_VERSION,
                "dataset_manifest_sha256": digest(dataset_path), "dataset_path": str(dataset_path),
                "provider_conversion": provider_provenance, "gpu_cpu_control": control_provenance,
                "workloads": [{"id": dataset["id"], "queries": queries}],
                "created_at": datetime.now(timezone.utc).isoformat(),
                "environment": {"platform": platform.platform(), "affinity": sorted(os.sched_getaffinity(0)),
                                "setup": setup, "containment": capped},
                "limitations": ["CPU provider development run; residency/resource/concurrency certification pending",
                                "both APIs collect Arrow results; query-wide result reservations pending",
                                "operator-level resource attribution and per-query RSS sampling pending",
                                "engine plan text may be capped by the production QueryMetrics API",
                                "GPU residency is observed per query; isolated synchronous cold-upload timing is not implemented"],
                "seed": 20260905, "command": sys.argv}
    if preloaded:
        manifest["limitations"].append("explicit Arrow preload on both sides; fresh worker per query; preparation excluded from query timing and reported separately")
    provenance = root / "provenance"
    provenance.mkdir()
    sources = (list(Path("scripts/benchmark").glob("*.py"))
        + sorted(Path("examples/benchmark_support").rglob("*.rs"))
        + [Path("examples/benchmark_embedded.rs"),
        Path("scripts/benchmark/tpch_sql_sha256.json"),
        Path("scripts/benchmark/licenses/duckdb-tpch-v1.4.4-LICENSE")])
    for source in sources:
        shutil.copyfile(source, provenance / source.name)
    manifest["implementation_sha256"] = {str(source): digest(source) for source in sources}
    from .source_manifest import engine_source_hashes
    manifest["engine_source_sha256"] = engine_source_hashes()
    manifest["scope_before"] = memory_events(capped)
    write_json(root / "manifest.json", manifest)
    rows, events = [], []
    rng = random.Random(manifest["seed"])
    workers = {}
    output = (root / "samples.jsonl").open("w")
    trace = (root / "execution.jsonl").open("w")

    def execute(side, sql, label, ceiling=None, resident_session=None):
        request = {"id": label, "sql": sql, "output": str(root / (label + ".arrow"))}
        if resident_session is not None:
            request["session_id"] = resident_session
        result = workers[side].query(request, ceiling)
        trace.write(json.dumps({"side": side, "request": request, "result": result}) + "\n")
        trace.flush()
        return result

    def compare(actual, expected, query):
        return compare_execution(actual, expected, query, scratch)

    try:
        for session in manifest["sessions"]:
            commands = {"engine": [str(binary), str(root / "setup.json")],
                        "duckdb": [sys.executable, "-m", "benchmark.duckdb_worker", str(root / "setup.json")]}
            if not preloaded:
                for side, command in commands.items():
                    workers[side] = Worker(command, env, root / f"{session}-{side}.stderr", events)
            query_order = list(queries)
            rng.shuffle(query_order)
            for query in query_order:
                if preloaded:
                    from .resident_runner import run_query
                    prefix = f"{session}-{query['id']}"
                    worker_id = prefix + "-fresh-engine"
                    for side, command in commands.items():
                        workers[side] = Worker(command, env, root / f"{prefix}-{side}.stderr", events)
                    try:
                        for row in run_query(workers=workers, query=query,
                                sql=(dataset_path.parent / query["sql_path"]).read_text(),
                                oracle_sql=(dataset_path.parent / query["oracle_sql_path"]).read_text(),
                                prefix=prefix, worker_id=worker_id, required=required,
                                preparation_timeout_seconds=preparation_timeout, samples=args.samples,
                                rng=rng, execute=execute, compare=compare, trace=trace,
                                workload=dataset["id"], track=args.track, session=session,
                                record_preparation=lambda value: write_json(root / f"{prefix}-preparation.json", value)):
                            rows.append(row)
                            output.write(json.dumps(row, allow_nan=False) + "\n")
                            output.flush()
                    finally:
                        for worker in workers.values():
                            worker.close()
                        workers.clear()
                    print(json.dumps({"session": session, "query": query["id"],
                                      "engine": row["engine"]["status"], "correct": row["comparison"]["ok"]}), flush=True)
                    continue
                # A failed worker must not manufacture failures for later SQL.
                for side, worker in list(workers.items()):
                    if worker.restart_for_next_query():
                        worker.close()
                        workers[side] = Worker(commands[side], env,
                                               root / f"{session}-{query['id']}-{side}-restart.stderr", events)
                prefix = f"{session}-{query['id']}"
                sql = (dataset_path.parent / query["sql_path"]).read_text()
                oracle_sql = (dataset_path.parent / query["oracle_sql_path"]).read_text()
                from .ordinary_runner import run_query
                for row in run_query(workers=workers, query=query, sql=sql,
                        oracle_sql=oracle_sql, prefix=prefix, samples=args.samples,
                        rng=rng, execute=execute, compare=compare, trace=trace,
                        workload=dataset["id"], track=args.track, session=session):
                    rows.append(row)
                    output.write(json.dumps(row, allow_nan=False) + "\n")
                    output.flush()
                print(json.dumps({"session": session, "query": query["id"],
                                  "engine": row["engine"]["status"], "correct": row["comparison"]["ok"]}), flush=True)
            for worker in workers.values():
                worker.close()
            workers.clear()
    finally:
        for worker in workers.values():
            worker.close()
        output.close()
        trace.close()
        write_json(root / "workers.json", events)
        write_json(root / "scope-after.json", memory_events(capped))
        try:
            verify_dataset(dataset_path)
            provider_setup(dataset_path, dataset, provider_track, getattr(args, "provider_manifest", None))
        except (ContractError, OSError, ValueError, KeyError) as error:
            manifest["input_integrity_error"] = str(error)
            for row in rows:
                row["comparison"].update(ok=False, input_integrity_error=str(error))
            with (root / "samples.jsonl").open("w") as corrected:
                for row in rows:
                    corrected.write(json.dumps(row, allow_nan=False) + "\n")
            write_json(root / "manifest.json", manifest)
        write_json(root / "report.json", report(manifest, rows))
    return report(manifest, rows)
