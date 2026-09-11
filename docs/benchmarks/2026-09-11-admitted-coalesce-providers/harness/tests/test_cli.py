import contextlib
import io
import hashlib
import json
import os
import sys
import subprocess
import tempfile
import unittest
from pathlib import Path

from benchmark.__main__ import main
from benchmark.contract import ContractError, report, validate_manifest
from benchmark.prepare import query_metadata
from benchmark.run import Worker
from test_contract import fixture


class CliTests(unittest.TestCase):
    def test_report_exit_codes_and_no_stale_success(self):
        with tempfile.TemporaryDirectory(dir=os.environ["TMPDIR"]) as directory:
            root = Path(directory)
            manifest, rows = fixture()
            (root / "manifest.json").write_text(json.dumps(manifest))
            samples, output = root / "samples.jsonl", root / "report.json"
            arguments = ["report", "--manifest", str(root / "manifest.json"),
                         "--samples", str(samples), "--output", str(output)]
            samples.write_text("".join(json.dumps(row) + "\n" for row in rows))
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(main(arguments + ["--gate", "complete"]), 0)
                self.assertEqual(main(arguments), 1)  # Development is never certification.
                samples.write_text(json.dumps(rows[0]) + "\n")
                self.assertEqual(main(arguments + ["--gate", "complete"]), 1)
            self.assertFalse(json.loads(output.read_text())["complete"])
            samples.write_text("{invalid json")
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(main(arguments), 2)
            self.assertFalse(json.loads(output.read_text())["complete"])

    def test_legacy_arguments_fail_before_engine_execution(self):
        result = subprocess.run(["bash", "scripts/safe_benchmark.sh", "--data", "data/tpch-10gb"],
                                capture_output=True, text=True)
        self.assertEqual(result.returncode, 2)
        self.assertIn("legacy", result.stderr.lower())

    def test_invalid_inputs_fail_closed(self):
        manifest, rows = fixture()
        for value in (None, [], {"workload": []}):
            self.assertFalse(report(manifest, [value])["complete"])
        for value in (None, [], "ok"):
            rows[0]["comparison"] = value
            self.assertFalse(report(manifest, rows)["complete"])
        manifest["workloads"] = [None]
        with self.assertRaises(ContractError):
            validate_manifest(manifest)
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(main(["validate"]), 2)

    def test_query_metadata_scale_and_limit(self):
        sql, oracle, metadata = query_metadata(11, "SELECT 0.0001000000 ORDER BY value DESC;", 10)
        self.assertIn("0.00001", sql)
        self.assertEqual(sql, oracle)
        self.assertEqual(metadata["result_policy"], "ordered")
        sql, oracle, metadata = query_metadata(2, "SELECT * FROM supplier ORDER BY s_acctbal DESC LIMIT 100;", 1)
        self.assertNotIn("LIMIT", oracle)
        self.assertEqual(metadata["limit_rows"], 100)
        self.assertEqual([x["column"] for x in metadata["order_by"]], [0, 2, 1, 3])
        with self.assertRaises(ContractError):
            query_metadata(2, "SELECT * FROM supplier ORDER BY s_acctbal DESC LIMIT 99;", 1)

    def test_installed_tpch_sql_matches_reviewed_contract(self):
        import duckdb
        from benchmark.prepare import DUCKDB_VERSION
        self.assertEqual(duckdb.__version__, DUCKDB_VERSION)
        pinned = json.loads(Path("scripts/benchmark/tpch_sql_sha256.json").read_text())
        with duckdb.connect() as connection:
            connection.execute("SET autoinstall_known_extensions=false")
            connection.execute("LOAD tpch")
            queries = connection.execute("SELECT query_nr, query FROM tpch_queries()").fetchall()
        self.assertEqual(len(queries), 22)
        for number, sql in queries:
            self.assertEqual(hashlib.sha256(sql.encode()).hexdigest(), pinned[f"q{number:02}"])
            for scale in (1, 10, 100):
                query_metadata(number, sql, scale)

    def worker(self, code, root):
        # Children inherit the suite's capped cgroup; no bare engine invocation.
        return Worker([sys.executable, "-u", "-c", code], dict(os.environ), root / "stderr", [], .5)

    def test_crashed_worker_does_not_count_as_completion(self):
        with tempfile.TemporaryDirectory(dir=os.environ["TMPDIR"]) as directory:
            worker = self.worker("import sys; sys.exit(7)", Path(directory))
            self.assertEqual(worker.ready, {"status": "crash", "exit_code": 7})
            worker.close()

    def test_query_deadline_kills_worker_and_records_phase(self):
        code = ('import json,sys,time; print(json.dumps({"status":"ready"})); '
                'r=json.loads(sys.stdin.readline()); '
                'print(json.dumps({"id":r["id"],"event":"started"})); time.sleep(30)')
        with tempfile.TemporaryDirectory(dir=os.environ["TMPDIR"]) as directory:
            worker = self.worker(code, Path(directory))
            try:
                result = worker.query({"id": "q", "sql": "select 1", "output": "unused"}, 1)
                self.assertEqual(result["status"], "timeout")
                self.assertEqual(result["phase"], "query")
                self.assertIsNotNone(worker.process.poll())
            finally:
                worker.close()

    def test_wrong_protocol_id_fails(self):
        code = ('import json,sys; print(json.dumps({"status":"ready"})); sys.stdin.readline(); '
                'print(json.dumps({"id":"wrong","status":"completed","ms":1}))')
        with tempfile.TemporaryDirectory(dir=os.environ["TMPDIR"]) as directory:
            worker = self.worker(code, Path(directory))
            try:
                self.assertEqual(worker.query({"id": "q"})["status"], "crash")
            finally:
                worker.close()
