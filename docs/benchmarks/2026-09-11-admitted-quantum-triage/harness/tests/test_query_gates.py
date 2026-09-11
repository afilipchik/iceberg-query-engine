import io
import random
import unittest
from benchmark.ordinary_runner import run_query
from benchmark.resident_runner import run_query as resident_query


class Worker:
    def __init__(self):
        self.closed = False
        self.process = self
        self.ready = {'status': 'ready', 'residency': 'host_arrow_preloaded',
                      'reference': {'reader': 'decoded_ipc'}}
    def close(self): self.closed = True
    def poll(self): return 0 if self.closed else None


def fixture(overrides=None, resident=False, required=False, engine_ready=None):
    calls = []
    workers = {s: Worker() for s in ['engine', 'duckdb']}
    def execute(side, sql, label, ceiling=None, token=None):
        calls.append((side, label, ceiling))
        return dict((overrides or {}).get((side, label), {'status': 'completed', 'ms': 2}))
    def compare(a, b, q):
        return {'ok': a.get('status') == b.get('status') == 'completed'
                and not a.get('wrong') and not b.get('wrong')}
    args = dict(workers=workers, query={'id': 'q', 'sql_sha256': 'a' * 64},
                sql='query', oracle_sql='oracle', prefix='s-q', samples=3,
                rng=random.Random(1), execute=execute, compare=compare,
                trace=io.StringIO(), workload='fixture', track='raw_parquet', session='s')
    if resident:
        args.update(worker_id='w', required=required, preparation_timeout_seconds=10)
        if required:
            workers['engine'].ready['gpu_residency_required'] = True
    if engine_ready is not None:
        workers['engine'].ready = engine_ready
    rows = list((resident_query if resident else run_query)(**args))
    return rows, calls, workers


class QueryGateTests(unittest.TestCase):
    def test_resident_startup_failure_preserves_cause_without_preparation(self):
        for required in [False, True]:
            for status in ['crash', 'timeout', 'refused']:
                with self.subTest(required=required, status=status):
                    rows, calls, workers = fixture(resident=True, required=required,
                        engine_ready={'status': status, 'exit_code': 1,
                                      'error': 'runtime initialization failed'})
                    self.assertFalse(any(c[0] == 'engine' for c in calls))
                    self.assertTrue(workers['engine'].closed)
                    for row in rows:
                        self.assertIn('engine startup failed', row['engine']['error'])
                        self.assertIn(status, row['engine']['error'])
                        self.assertIn('runtime initialization failed', row['engine']['error'])
                        if required:
                            self.assertEqual(row['gpu_preparation']['error'], row['engine']['error'])

    def test_invalid_reference_prevents_required_gpu_preparation(self):
        # The fake has no query method: attempting GPU preparation would fail.
        rows, calls, workers = fixture({('duckdb', 's-q-oracle'): {'status': 'refused'}},
                                       resident=True, required=True)
        self.assertEqual(len(calls), 2)
        self.assertTrue(workers['engine'].closed)
        self.assertTrue(all(r['gpu_preparation']['status'] == 'not_run' for r in rows))

    def test_resident_late_or_wrong_measured_engine_result_is_not_retried(self):
        for response in [{'status': 'completed', 'ms': 21},
                         {'status': 'completed', 'ms': 2, 'wrong': True}]:
            rows, calls, workers = fixture({('engine', 's-q-1-engine'): response}, resident=True)
            self.assertEqual(len([c for c in calls if c[0] == 'engine']), 2)
            self.assertTrue(workers['engine'].closed)
            self.assertTrue(all(r['engine']['status'] == 'not_run' for r in rows[1:]))

    def test_reference_failure_stops_setup_and_calibration_immediately(self):
        labels = ['s-q-duckdb-plan', 's-q-oracle', 's-q-duckdb-warmup',
                  's-q-cal0', 's-q-cal1', 's-q-cal2']
        for resident in [False, True]:
            for index, label in enumerate(labels):
                with self.subTest(resident=resident, label=label):
                    rows, calls, workers = fixture({('duckdb', label): {'status': 'refused'}}, resident=resident)
                    self.assertEqual([c[1] for c in calls], labels[:index + 1])
                    self.assertTrue(workers['duckdb'].closed)
                    self.assertTrue(all(r[s]['status'] == 'not_run' for r in rows for s in ['engine', 'duckdb']))

    def test_wrong_reference_warmup_stops_calibration(self):
        for resident in [False, True]:
            rows, calls, workers = fixture({('duckdb', 's-q-duckdb-warmup'):
                {'status': 'completed', 'ms': 2, 'wrong': True}}, resident=resident)
            self.assertEqual(len(calls), 3)
            self.assertTrue(workers['duckdb'].closed)
            self.assertTrue(all(r['engine']['status'] == 'not_run' for r in rows))

    def test_resident_measured_reference_failure_stops_dependent_requests(self):
        rows, calls, workers = fixture({('duckdb', 's-q-1-duckdb'): {'status': 'refused'}}, resident=True)
        self.assertTrue(workers['duckdb'].closed)
        self.assertFalse(rows[0]['comparison']['ok'])
        self.assertTrue(all(r[s]['status'] == 'not_run' for r in rows[1:] for s in ['engine', 'duckdb']))

    def test_valid_calibration_gates_warmup_and_all_engine_samples(self):
        rows, calls, _ = fixture()
        self.assertEqual(len(rows), 3)
        engine = [c for c in calls if c[0] == 'engine']
        self.assertEqual(len(engine), 4)
        self.assertTrue(all(c[2] == 20 for c in engine))
        self.assertTrue(all(r['comparison']['ok'] for r in rows))

    def test_invalid_reference_never_issues_engine_or_measured_requests(self):
        bad = [{'status': 'timeout'}, {'status': 'completed', 'ms': 2, 'wrong': True}]
        bad += [{'status': 'completed', 'ms': v} for v in [None, 0, -1, float('nan'), float('inf'), True]]
        for response in bad:
            with self.subTest(response=response):
                rows, calls, workers = fixture({('duckdb', 's-q-cal1'): response})
                self.assertFalse(any(c[0] == 'engine' for c in calls))
                self.assertTrue(workers['duckdb'].closed)
                self.assertEqual(len(rows), 3)
                self.assertTrue(all(r[s]['status'] == 'not_run' and r[s]['executed'] is False
                                    for r in rows for s in ['engine', 'duckdb']))
        rows, calls, _ = fixture({('duckdb', 's-q-oracle'): {'status': 'timeout'}})
        self.assertFalse(any(c[0] == 'engine' for c in calls))
        overflow = {('duckdb', f's-q-cal{i}'): {'status': 'completed', 'ms': 1e308} for i in range(3)}
        self.assertFalse(any(c[0] == 'engine' for c in fixture(overflow)[1]))

    def test_failed_or_late_warmup_is_preserved_without_measured_retry(self):
        for response in [{'status': 'timeout'}, {'status': 'completed', 'ms': 21},
                         {'status': 'completed', 'ms': 2, 'wrong': True}]:
            rows, calls, workers = fixture({('engine', 's-q-warmup'): response})
            self.assertEqual(len([c for c in calls if c[0] == 'engine']), 1)
            self.assertTrue(workers['engine'].closed)
            self.assertTrue(all(r['engine']['status'] == 'not_run' for r in rows))
            self.assertTrue(all(r['engine_warmup'] == response for r in rows))

    def test_measured_late_or_wrong_output_stops_later_engine_requests(self):
        for response in [{'status': 'completed', 'ms': 21},
                         {'status': 'completed', 'ms': 2, 'wrong': True}]:
            rows, calls, _ = fixture({('engine', 's-q-1-engine'): response})
            self.assertEqual(rows[0]['engine'], response)
            self.assertEqual(len([c for c in calls if c[0] == 'engine']), 2)
            self.assertEqual([r['engine']['status'] for r in rows[1:]], ['not_run'] * 2)

    def test_measured_reference_failure_prevents_dependent_retries(self):
        rows, calls, workers = fixture({('duckdb', 's-q-1-duckdb'): {'status': 'refused'}})
        self.assertFalse(rows[0]['comparison']['ok'])
        self.assertTrue(all(r[s]['status'] == 'not_run' for r in rows[1:] for s in ['engine', 'duckdb']))
        self.assertTrue(workers['duckdb'].closed,
                        'a failed reference must be retired before the next SQL')

    def test_resident_completed_but_late_warmup_is_not_accepted(self):
        rows, calls, _ = fixture({('engine', 's-q-warmup'): {'status': 'completed', 'ms': 21}}, resident=True)
        self.assertEqual(len([c for c in calls if c[0] == 'engine']), 1)
        self.assertTrue(all(r['engine']['status'] == 'not_run' for r in rows))
