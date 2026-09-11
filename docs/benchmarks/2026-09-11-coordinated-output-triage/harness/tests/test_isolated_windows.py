import unittest
from benchmark.isolated_windows import IsolationError, run_isolated_pair


class FakeWorker:
    def __init__(self, side, owner, *, ready=True, stuck=False):
        if owner['live']:
            raise AssertionError('overlapping engine processes')
        owner['live'] = side
        owner['events'].append(('start', side))
        self.side, self.owner, self.stuck = side, owner, stuck
        self.ready = {'status': 'ready' if ready else 'refused', 'error': 'named startup failure'}
        self.process = self
        self.returncode = None
        self.teardown = {}

    def poll(self):
        return self.returncode

    def close(self, *, force=False):
        self.owner['events'].append(('close', self.side))
        self.teardown = {'forced': force}
        if not self.stuck:
            self.returncode = -9 if force else 0
            self.owner['live'] = None


def result(ms=10, correct=True):
    return {'response': {'status': 'completed', 'ms': ms},
            'comparison': {'ok': correct}, 'time_gate_pass': True}


class IsolatedWindowContract(unittest.TestCase):
    def run_case(self, execute=None, **kwargs):
        owner = {'live': None, 'events': []}
        self.owner = owner
        def start(side):
            return FakeWorker(side, owner, **kwargs.get('worker_options', {}))
        def query(worker, side, i):
            self.assertEqual(owner['live'], side)
            owner['events'].append(('query', side, i))
            return execute(side, i) if execute else result(1000 if i == 0 else 10)
        record = run_isolated_pair(kwargs.get('order', ('before', 'after')), 3, 40,
                                   kwargs.get('start', start), query, snapshot=lambda _: {'observed': True})
        return owner, record

    def test_one_worker_at_a_time_with_complete_eof_cleanup(self):
        for order in [('before', 'after'), ('after', 'before')]:
            owner, r = self.run_case(order=order)
            self.assertIsNone(owner['live'])
            self.assertEqual(owner['events'],
                             [event for side in order for event in
                              [('start', side), *[('query', side, i) for i in range(4)], ('close', side)]])
            self.assertTrue(r['complete'])

    def test_warmup_is_not_exposure_and_windows_never_extend(self):
        owner, r = self.run_case()
        self.assertTrue(r['complete'])
        self.assertFalse(r['precision_sufficient'])
        self.assertEqual([w['measured_ms'] for w in r['windows']], [30, 30])
        self.assertEqual(sum(e[0] == 'query' for e in owner['events']), 8)

    def test_enough_fixed_exposure_is_recorded(self):
        _, r = self.run_case(lambda side, i: result(20))
        self.assertTrue(r['precision_sufficient'])
        self.assertEqual([w['measured_ms'] for w in r['windows']], [60, 60])

    def test_wrong_answer_preserves_failed_and_dependent_slots(self):
        owner, r = self.run_case(lambda side, i: result(correct=not (side == 'before' and i == 1)))
        self.assertFalse(r['complete'])
        self.assertEqual([x['response']['status'] for x in r['windows'][0]['attempts']],
                         ['completed', 'completed', 'not_run', 'not_run'])
        self.assertTrue(r['windows'][1]['complete'])
        self.assertNotIn(('query', 'before', 2), owner['events'])

    def test_stuck_cleanup_prevents_starting_the_other_process(self):
        with self.assertRaises(IsolationError) as raised:
            self.run_case(worker_options={'stuck': True})
        owner, r = self.owner, raised.exception.result
        self.assertFalse(r['complete'])
        self.assertIn('remains live', r['isolation_error'])
        self.assertNotIn(('start', 'after'), owner['events'])
        self.assertEqual(len(r['windows'][1]['attempts']), 4)

    def test_unknown_startup_ownership_prevents_second_start(self):
        calls = []
        def start(side):
            calls.append(side)
            raise RuntimeError('startup failed before returning handle')
        with self.assertRaises(IsolationError) as raised:
            self.run_case(start=start)
        r = raised.exception.result
        self.assertEqual(calls, ['before'])
        self.assertFalse(r['complete'])
        self.assertIn('ownership unverified', r['isolation_error'])

    def test_startup_refusal_is_preserved_without_query_execution(self):
        owner, r = self.run_case(worker_options={'ready': False})
        self.assertFalse(r['complete'])
        self.assertEqual(r['windows'][0]['ready']['error'], 'named startup failure')
        self.assertFalse(any(e[0] == 'query' for e in owner['events']))

    def test_invalid_time_and_failed_gate_stop_the_window(self):
        for bad in [result(ms=True), result(ms=0), result(ms=float('nan')),
                    dict(result(), time_gate_pass=False)]:
            with self.subTest(bad=bad):
                _, r = self.run_case(lambda side, i: bad if side == 'before' else result())
                self.assertFalse(r['complete'])
                self.assertEqual(r['windows'][0]['attempts'][1]['response']['status'], 'not_run')

    def test_invalid_configuration_is_rejected_before_start(self):
        def never(*_):
            self.fail('configuration error started a worker')
        for count, minimum in [(True, 1), (0, 1), (65537, 1), (1, True), (1, float('inf'))]:
            with self.assertRaises(ValueError):
                run_isolated_pair(('before', 'after'), count, minimum, never, never)

    def test_primary_ratio_includes_every_measured_burst(self):
        _, r = self.run_case(lambda side, i: result(2000 if i == 0 else
                            ([10, 10, 100][i - 1] if side == 'before' else 20)))
        self.assertTrue(r['precision_sufficient'])
        self.assertEqual(r['after_over_before'], 0.5)
        self.assertEqual(r['median_after_over_before'], 2.0)

    def test_isolation_failure_stops_the_entire_caller_loop(self):
        owner = {'live': None, 'events': []}
        starts = []
        def start(side):
            starts.append(side)
            return FakeWorker(side, owner, stuck=True)
        with self.assertRaisesRegex(RuntimeError, 'isolation'):
            for block in range(3):
                run_isolated_pair(('before', 'after'), 1, 1, start,
                                  lambda *_: result(), snapshot=lambda _: {})
        self.assertEqual(starts, ['before'])
