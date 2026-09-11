import copy
import unittest
from benchmark.contract import ContractError, CPU_TRACKS, TIMING, report, validate_manifest


def fixture(tracks=None, sessions=None, samples=2):
    tracks, sessions = tracks or ["raw_parquet"], sessions or ["s1"]
    m = {"version": 1, "tracks": tracks, "sessions": sessions,
         "samples_per_query": samples, "timing_boundary": TIMING,
         "profile": "development", "cache_state": "warm_host",
         "source_revision": "source", "engine_binary_sha256": "a" * 64,
         "duckdb_version": "1.4.4", "dataset_manifest_sha256": "b" * 64,
         "workloads": [{"id": "fixture", "queries": [
             {"id": q, "sql_sha256": "c" * 64, "result_policy": "bag"} for q in ("q1", "q2")]}]}
    rows = [{"workload": "fixture", "query": q, "track": t, "session": s,
             "iteration": i, "sql_sha256": "c" * 64, "timing_boundary": TIMING,
             "calibration_ms": [100., 100., 100.],
             "engine": {"status": "completed", "ms": 80.},
             "duckdb": {"status": "completed", "ms": 100.},
             "comparison": {"ok": True, "policy": "bag"}}
            for q in ("q1", "q2") for t in tracks for s in sessions for i in range(1, samples + 1)]
    return m, rows


class ContractTests(unittest.TestCase):
    def test_development_is_not_leadership(self):
        m, rows = fixture()
        r = report(m, rows, 40)
        self.assertTrue(r["complete"])
        self.assertAlmostEqual(r["groups"][0]["suite_ratio"], .8)
        self.assertFalse(r["latency_leadership"])

    def test_missing_duplicate_unexpected_never_score(self):
        for kind in ("missing", "duplicate", "unexpected"):
            with self.subTest(kind=kind):
                m, rows = fixture()
                if kind == "missing": rows.pop()
                elif kind == "duplicate": rows.append(copy.deepcopy(rows[0]))
                else: rows[0]["query"] = "unknown"
                r = report(m, rows, 20)
                self.assertFalse(r["complete"])
                self.assertEqual(r["groups"], [])

    def test_failure_statuses_are_not_wins(self):
        for status in ("timeout", "oom", "crash", "refused", "unsupported", "query_error", "startup_error"):
            for side in ("engine", "duckdb"):
                m, rows = fixture()
                rows[0][side] = {"status": status, "ms": 0}
                self.assertFalse(report(m, rows, 20)["complete"])

    def test_bad_evidence(self):
        mutations = [lambda r: r["comparison"].update(ok=False),
                     lambda r: r["comparison"].update(policy="ordered"),
                     lambda r: r.update(sql_sha256="d" * 64),
                     lambda r: r.update(timing_boundary="http"),
                     lambda r: r.update(calibration_ms=[1, 2]),
                     lambda r: r.update(iteration=True)]
        for mutate in mutations:
            m, rows = fixture()
            mutate(rows[0])
            self.assertFalse(report(m, rows, 20)["complete"])

    def test_invalid_elapsed(self):
        for value in (0, -1, float("nan"), float("inf"), True, None):
            m, rows = fixture()
            rows[0]["engine"]["ms"] = value
            self.assertFalse(report(m, rows, 20)["complete"])

    def test_ceiling_has_no_startup_floor(self):
        m, rows = fixture()
        for r in rows:
            r["calibration_ms"] = [.1, .1, .1]
            r["engine"]["ms"] = 1.001
        self.assertFalse(report(m, rows, 20)["complete"])
        for r in rows: r["engine"]["ms"] = 1.0
        self.assertTrue(report(m, rows, 20)["complete"])

    def test_bad_session_prevents_leadership(self):
        m, rows = fixture(sessions=["a", "b", "c"], samples=10)
        for r in rows:
            if r["session"] == "c": r["engine"]["ms"] = 101
        result = report(m, rows, 40)
        self.assertTrue(result["complete"])
        self.assertFalse(result["groups"][0]["leadership"])

    def test_fast_tiny_fixture_cannot_claim_full_leadership(self):
        m, rows = fixture(tracks=list(CPU_TRACKS), sessions=["a", "b", "c"], samples=10)
        result = report(m, rows, 40)
        self.assertTrue(all(g["leadership"] for g in result["groups"]))
        self.assertFalse(result["latency_leadership"])

    def test_manifest_membership_is_mandatory(self):
        for key, value in (("tracks", []), ("sessions", ["a", "a"]), ("samples_per_query", 0),
                           ("version", 2), ("timing_boundary", "server_only")):
            m, _ = fixture()
            m[key] = value
            with self.assertRaises(ContractError): validate_manifest(m)

    def test_paired_bootstrap_preserves_ratio(self):
        m, rows = fixture(samples=10)
        for r in rows:
            t = 10. * r["iteration"]
            r["engine"]["ms"], r["duckdb"]["ms"] = t * .8, t
        for limits in report(m, rows, 80)["groups"][0]["ci95"].values():
            for limit in limits: self.assertAlmostEqual(limit, .8)

    def test_slice_report_requires_exact_oracle_policy_evidence(self):
        m, rows = fixture()
        query = m["workloads"][0]["queries"][0]
        query.update(result_policy="limit_ties", limit_rows=2, offset_rows=1,
                     oracle_output_columns=[0], order_by=[{"column":1,"direction":"asc","nulls":"last"}])
        for row in rows:
            if row["query"] == "q1":
                row["comparison"].update(policy="limit_ties", limit_rows=2, offset_rows=1,
                    oracle_output_columns=[0], oracle_order_by=query["order_by"], expected_is_complete=True)
        self.assertTrue(report(m,rows,20)["complete"])
        for key, value in (("offset_rows",0),("oracle_output_columns",None),("oracle_order_by",[]),("expected_is_complete",False),("limit_rows",1)):
            bad = copy.deepcopy(rows)
            bad[0]["comparison"][key] = value
            self.assertFalse(report(m,bad,20)["complete"], key)

    def test_unordered_limit_manifest_requires_a_real_slice(self):
        m, _ = fixture()
        query = m["workloads"][0]["queries"][0]
        query.update(result_policy="unordered_limit", limit_rows=2, offset_rows=0, order_by=[])
        validate_manifest(m)
        for field,value in (("limit_rows",None),("offset_rows",-1),("offset_rows",True),("order_by",[{"column":0,"direction":"asc","nulls":"last"}])):
            bad = copy.deepcopy(m)
            bad["workloads"][0]["queries"][0][field] = value
            with self.assertRaises(ContractError): validate_manifest(bad)

    def test_failed_execution_preserves_slice_metadata_without_passing(self):
        from benchmark.run import compare_execution
        m, rows = fixture()
        query = m['workloads'][0]['queries'][0]
        query.update(result_policy='limit_ties', limit_rows=2, offset_rows=1,
                     oracle_output_columns=[0], order_by=[{'column':1,'direction':'asc','nulls':'last'}])
        for row in rows:
            if row['query'] == 'q1':
                row['engine'] = {'status': 'query_error', 'error': 'unsupported operator'}
                row['comparison'] = compare_execution(row['engine'], row['duckdb'], query, None)
                self.assertFalse(row['comparison']['ok'])
        result = report(m, rows, 20)
        self.assertFalse(result['complete'])
        self.assertNotIn('oracle slice contract mismatch', str(result['issues']))
