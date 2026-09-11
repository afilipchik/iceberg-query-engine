"""Protocol orchestration tests; no engine/device or comparator implementation mock proof."""
import io
import random
import unittest
from benchmark.resident_runner import run_query, manifest_residency
from benchmark.contract import report, TIMING


class Process:
    def __init__(self): self.closed = False
    def poll(self): return 0 if self.closed else None


class Worker:
    def __init__(self, engine, prepare_status='prepared'):
        self.process = Process()
        self.requests = []
        self.prepare_status = prepare_status
        self.ready = {'status': 'ready', 'residency': 'host_arrow_preloaded',
                      'gpu_residency_required': True, 'reference': {'reader': 'decoded_ipc'}}
    def close(self): self.process.closed = True
    def query(self, request, ceiling=None):
        self.requests.append((request, ceiling))
        if request['operation'] == 'release_gpu_resident': return {'status': 'released'}
        return {'status': self.prepare_status, 'preparation_ms': 7,
                'preparation': {'session_id': 3, 'rows': 2, 'groups': 1,
                                'column_bytes': 16, 'codes_bytes': 0, 'columns': ['v']}}


class ResidentRunnerTests(unittest.TestCase):
    def fixture(self, prepare='prepared', fail_sample=False, fallback=False):
        workers = {'engine': Worker(True, prepare), 'duckdb': Worker(False)}
        calls = []
        def execute(side, sql, label, ceiling=None, token=None):
            calls.append((side, sql, label, ceiling, token))
            if side == 'engine' and fail_sample and '-1-engine' in label:
                return {'status': 'timeout'}
            evidence = {'session_id': 3, 'matched_operators': 1, 'attempted_device_runs': 1,
                        'completed_device_runs': 0 if fallback and '-1-engine' in label else 1,
                        'failures': 0, 'failure_reason': None}
            return {'status': 'completed', 'ms': 2, 'gpu_resident_evidence': evidence}
        def compare(a, b, q): return {'ok': a['status'] == b['status'] == 'completed', 'policy': 'bag'}
        stream = run_query(workers=workers, query={'id': 'q', 'sql_sha256': 'a' * 64},
                           sql='original sql', oracle_sql='complete oracle', prefix='s1-q',
                           worker_id='unique', required=True, preparation_timeout_seconds=11,
                           samples=3, rng=random.Random(1), execute=execute, compare=compare,
                           trace=io.StringIO(), workload='fixture', track='gpu', session='s1')
        return list(stream), workers, calls

    def test_success_prepares_once_and_preserves_original_timed_sql_and_ceiling(self):
        rows, workers, calls = self.fixture()
        self.assertEqual(len(rows), 3)
        self.assertEqual([r['iteration'] for r in rows], [1, 2, 3])
        self.assertEqual([r['engine']['status'] for r in rows], ['completed'] * 3)
        self.assertEqual([r['gpu_worker_id'] for r in rows], ['unique'] * 3)
        self.assertEqual([r[0]['operation'] for r in workers['engine'].requests],
                         ['prepare_gpu_resident', 'release_gpu_resident'])
        self.assertEqual(workers['engine'].requests[0][1], 11000)
        self.assertTrue(all(c[1] == 'original sql' and c[3] == 20 and c[4] == 3
                            for c in calls if c[0] == 'engine'))
        self.assertTrue(any(c[1] == 'complete oracle' for c in calls))

    def test_produced_required_metadata_and_samples_pass_actual_report(self):
        rows, _, _ = self.fixture()
        manifest = {'version': 1, 'profile': 'development',
                    **manifest_residency(True, True, 11),
                    'tracks': ['gpu'], 'sessions': ['s1'], 'samples_per_query': 3,
                    'timing_boundary': TIMING, 'source_revision': 'fixture',
                    'engine_binary_sha256': 'b' * 64, 'dataset_manifest_sha256': 'c' * 64,
                    'duckdb_version': '1.4.4',
                    'environment': {'setup': {'gpu_residency_required': True,
                                              'host_arrow_preloaded': True}},
                    'workloads': [{'id': 'fixture', 'queries': [
                        {'id': 'q', 'sql_sha256': 'a' * 64, 'result_policy': 'bag'}]}]}
        self.assertEqual(manifest['cache_state'], 'resident')
        result = report(manifest, rows, repetitions=5)
        self.assertTrue(result['complete'], result['issues'])
        rows[0]['engine']['gpu_resident_evidence']['completed_device_runs'] = 0
        self.assertFalse(report(manifest, rows, repetitions=5)['complete'])


    def test_failed_preparation_retains_every_not_run_sample_without_warmup_retry(self):
        rows, workers, calls = self.fixture(prepare='preparation_error')
        self.assertEqual(len(rows), 3)
        self.assertTrue(all(r['engine']['status'] == 'not_run' for r in rows))
        self.assertTrue(all(r['gpu_preparation']['status'] == 'preparation_error' for r in rows))
        self.assertFalse(any(c[0] == 'engine' for c in calls))
        self.assertEqual(len(workers['engine'].requests), 1)
        self.assertTrue(workers['engine'].process.closed)

    def test_timeout_or_zero_device_success_is_preserved_then_no_more_engine_requests(self):
        for option in ({'fail_sample': True}, {'fallback': True}):
            rows, workers, calls = self.fixture(**option)
            self.assertEqual(len(rows), 3)
            self.assertEqual(rows[0]['engine']['status'], 'timeout' if option.get('fail_sample') else 'completed')
            self.assertEqual([r['engine']['status'] for r in rows[1:]], ['not_run', 'not_run'])
            self.assertEqual(len([c for c in calls if c[0] == 'engine']), 2)  # one warmup + one measured
            self.assertTrue(all(r['duckdb']['status'] == 'completed' for r in rows))
            self.assertTrue(workers['engine'].process.closed)


if __name__ == '__main__':
    unittest.main()
