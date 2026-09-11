import copy
import unittest
from benchmark.paired import pair_order, summarize_case


def fixture():
    return {'attempts': [
        {'side': side, 'iteration': i, 'warmup': i == 0, 'ok': True,
         'response': {'status': 'completed', 'ms': ms}}
        for i, ms in enumerate((1000.0, 10.0, 10.0, 100.0))
        for side in ('before', 'after')]}


class PairedContract(unittest.TestCase):
    def test_each_side_runs_first_equally_for_even_steady_samples(self):
        for ordinal in range(22):
            for offset in (0, 1):
                orders = [pair_order(i, ordinal, offset) for i in range(1, 11)]
                self.assertEqual(sum(order[0] == 'before' for order in orders), 5)
                self.assertTrue(all(set(order) == {'before', 'after'} for order in orders))
                for i in range(11):
                    self.assertEqual(pair_order(i, ordinal, offset),
                                     tuple(reversed(pair_order(i, ordinal, 1-offset))))

    def test_cleanup_bursts_remain_in_mean_and_total(self):
        s = summarize_case(fixture(), 3)
        self.assertTrue(s['complete'])
        self.assertEqual(s['before_median_ms'], 10)
        self.assertEqual(s['before_mean_ms'], 40)
        self.assertEqual(s['before_total_ms'], 120)
        self.assertEqual(s['before_max_ms'], 100)

    def test_duplicate_cannot_replace_missing_sample(self):
        r = fixture()
        r['attempts'][-1] = copy.deepcopy(r['attempts'][-3])
        self.assertFalse(summarize_case(r, 3)['complete'])

    def test_warmup_cannot_be_relabelled_as_steady(self):
        r = fixture()
        r['attempts'][0]['warmup'] = False
        self.assertFalse(summarize_case(r, 3)['complete'])

    def test_failure_missing_and_invalid_time_do_not_produce_ratios(self):
        cases = []
        r = fixture(); r['attempts'].pop(); cases.append(r)
        r = fixture(); r['attempts'][2]['ok'] = False; cases.append(r)
        r = fixture(); r['error'] = 'oracle failure'; cases.append(r)
        for value in (0, -1, float('nan'), float('inf'), True):
            r = fixture(); r['attempts'][2]['response']['ms'] = value; cases.append(r)
        for r in cases:
            with self.subTest(result=r):
                s = summarize_case(r, 3)
                self.assertFalse(s['complete'])
                self.assertIsNone(s['after_over_before'])


if __name__ == '__main__':
    unittest.main()
