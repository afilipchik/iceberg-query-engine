import os
import tempfile
import unittest
from datetime import date
from decimal import Decimal
from pathlib import Path
import pyarrow as pa
from benchmark.compare import compare_files, matching


class ComparisonTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(dir=os.environ["TMPDIR"])
        self.root = Path(self.directory.name)

    def tearDown(self): self.directory.cleanup()

    def check(self, actual, expected, **kwargs):
        paths = []
        for name, table in (("actual", actual), ("expected", expected)):
            path = self.root / (name + ".arrow")
            with pa.OSFile(str(path), "wb") as f, pa.ipc.new_stream(f, table.schema) as writer:
                writer.write_table(table, max_chunksize=2)
            paths.append(path)
        return compare_files(*paths, scratch=self.root, **kwargs)

    def test_large_integers_and_decimals(self):
        a = pa.table({"k": pa.array([2**60, 2**60 + 1], type=pa.int64())})
        b = pa.table({"k": pa.array([Decimal(2**60 + 1), Decimal(2**60)], type=pa.decimal128(38, 0))})
        self.assertTrue(self.check(a, b)["ok"])
        self.assertFalse(self.check(a, pa.table({"k": [2**60, 2**60]}))["ok"])

    def test_decimal_38_digits(self):
        a = pa.table({"k": pa.array([Decimal("12345678901234567890123456789012345678")], type=pa.decimal128(38, 0))})
        b = pa.table({"k": pa.array([Decimal("12345678901234567890123456789012345679")], type=pa.decimal128(38, 0))})
        self.assertFalse(self.check(a, b)["ok"])

    def test_null_empty_numeric_string(self):
        a = pa.table({"k": pa.array([None, "", "01"], type=pa.string())})
        for values in (["", "", "01"], [None, "", "1"]):
            self.assertFalse(self.check(a, pa.table({"k": values}))["ok"])

    def test_dictionary_and_dates(self):
        a = pa.table({"s": pa.array(["b", "a", None]).dictionary_encode(),
                      "d": pa.array([date(2020, 1, 1), None, date(2021, 1, 1)], type=pa.date32())})
        b = pa.table({"s": ["b", "a", None],
                      "d": pa.array([date(2020, 1, 1), None, date(2021, 1, 1)], type=pa.date64())})
        self.assertTrue(self.check(a, b)["ok"])

    def test_empty(self):
        a = pa.table({"x": pa.array([], type=pa.int64())})
        self.assertTrue(self.check(a, a)["ok"])

    def test_schema(self):
        self.assertFalse(self.check(pa.table({"x": [1]}), pa.table({"x": ["1"]}))["ok"])

    def test_multiplicity_across_batches(self):
        a = pa.table({"x": [1, 1, 2, 3, 3, 3]})
        self.assertTrue(self.check(a, pa.table({"x": [3, 2, 3, 1, 3, 1]}))["ok"])
        self.assertFalse(self.check(a, pa.table({"x": [3, 2, 3, 1, 2, 1]}))["ok"])

    def test_order_and_ties(self):
        order = [{"column": 0, "direction": "asc", "nulls": "last"}]
        a = pa.table({"k": [1, 1, 2, None], "v": ["a", "b", "c", "d"]})
        b = pa.table({"k": [1, 1, 2, None], "v": ["b", "a", "c", "d"]})
        self.assertTrue(self.check(a, b, policy="ordered", order_by=order)["ok"])
        c = pa.table({"k": [1, 2, 1, None], "v": ["a", "c", "b", "d"]})
        r = self.check(c, a, policy="ordered", order_by=order)
        self.assertFalse(r["ok"])
        self.assertIn("order violation", r["reason"])
        self.assertTrue(self.check(c, a)["ok"])

    def test_desc_nulls(self):
        order = [{"column": 0, "direction": "desc", "nulls": "first"}]
        a = pa.table({"k": [None, 2, 1]})
        self.assertTrue(self.check(a, a, policy="ordered", order_by=order)["ok"])
        self.assertFalse(self.check(a, a, policy="ordered", order_by=[])["ok"])

    def test_augmenting_float_match(self):
        self.assertTrue(matching([(0.5,), (0.0,)], [(0.0,), (1.0,)], 0, .6))
        self.assertFalse(matching([(0.0,), (0.0,)], [(0.0,), (1.0,)], 0, .1))
        self.assertTrue(self.check(pa.table({"f": [.5, 0.]}), pa.table({"f": [0., 1.]}), rel_tol=0, abs_tol=.6)["ok"])

    def test_nonfinite_and_null_float(self):
        a = pa.table({"f": pa.array([None, float("nan"), float("inf"), -float("inf")], type=pa.float64())})
        self.assertTrue(self.check(a, a)["ok"])

    def test_float_bound_is_explicit(self):
        a = pa.table({"f": [1., 2., 3.]})
        r = self.check(a, a, max_float_bucket=2)
        self.assertFalse(r["ok"])
        self.assertIn("validation bound", r["reason"])

    def test_limit_ties_require_oracle(self):
        a = pa.table({"k": [1]})
        self.assertFalse(self.check(a, a, policy="limit_ties")["ok"])

    def test_limit_boundary_allows_alternative_tied_rows(self):
        oracle = pa.table({"k": [1, 2, 2, 2, 3], "v": ["a", "b", "c", "d", "e"]})
        opts = dict(policy="limit_ties", order_by=[{"column": 0, "direction": "asc", "nulls": "last"}],
                    limit_rows=3, expected_is_complete=True)
        for values in (["b", "c"], ["c", "d"]):
            actual = pa.table({"k": [1, 2, 2], "v": ["a", *values]})
            self.assertTrue(self.check(actual, oracle, **opts)["ok"])
        for keys, values in (([2, 2, 2], ["b", "c", "d"]),
                             ([1, 2, 3], ["a", "b", "e"]),
                             ([1, 2, 2], ["a", "b", "b"]),
                             ([1, 2], ["a", "b"])):
            self.assertFalse(self.check(pa.table({"k": keys, "v": values}), oracle, **opts)["ok"])

    def test_limit_zero_and_short_oracle(self):
        oracle = pa.table({"k": [1, 2]})
        opts = dict(policy="limit_ties", order_by=[{"column": 0, "direction": "asc", "nulls": "last"}],
                    expected_is_complete=True)
        self.assertTrue(self.check(oracle.slice(0, 0), oracle, limit_rows=0, **opts)["ok"])
        self.assertTrue(self.check(oracle, oracle, limit_rows=10, **opts)["ok"])
        self.assertFalse(self.check(oracle.slice(0, 1), oracle, limit_rows=10, **opts)["ok"])

    def test_limit_float_boundary_and_mandatory_matching(self):
        oracle = pa.table({"k": [1, 2, 2], "f": [.1, .2, .3]})
        actual = pa.table({"k": [1, 2], "f": [.1, .3]})
        opts = dict(policy="limit_ties", order_by=[{"column": 0, "direction": "asc", "nulls": "last"}],
                    limit_rows=2, expected_is_complete=True)
        self.assertTrue(self.check(actual, oracle, **opts)["ok"])
        self.assertFalse(matching([(1.,)], [(0.,), (1.,)], 0, .1, required=[True, False]))
        self.assertTrue(matching([(.5,), (0.,)], [(0.,), (1.,), (2.,)], 0, .6,
                                 required=[True, False, False]))

    def test_unordered_limit_uses_complete_bag_and_multiplicity(self):
        oracle = pa.table({"v": pa.array(["a", "a", "b", None], type=pa.string())})
        options = dict(policy="unordered_limit", limit_rows=2, expected_is_complete=True)
        for values in (["b", None], ["a", "a"], ["a", "b"]):
            self.assertTrue(self.check(pa.table({"v": pa.array(values, type=pa.string())}), oracle, **options)["ok"])
        for values in (["b", "b"], ["a", "missing"], ["a"]):
            self.assertFalse(self.check(pa.table({"v": pa.array(values, type=pa.string())}), oracle, **options)["ok"])
        self.assertFalse(self.check(oracle.slice(0,2), oracle, policy="unordered_limit", limit_rows=2)["ok"])

    def test_offset_can_cut_both_boundary_tie_groups(self):
        oracle = pa.table({"k":[1,1,2,2,3], "v":["a","b","c","d","e"]})
        options = dict(policy="limit_ties", limit_rows=2, offset_rows=1, expected_is_complete=True,
                       order_by=[{"column":0,"direction":"asc","nulls":"last"}])
        for values in (["a","c"], ["b","d"]):
            self.assertTrue(self.check(pa.table({"k":[1,2],"v":values}), oracle, **options)["ok"])
        for keys, values in (([1,1],["a","b"]), ([2,2],["c","d"]), ([1,3],["a","e"])):
            self.assertFalse(self.check(pa.table({"k":keys,"v":values}), oracle, **options)["ok"])

    def test_hidden_order_keys_enforce_actual_sequence_and_duplicate_capacity(self):
        oracle = pa.table({"v":["same","early","same","late"], "hidden":[1,1,2,2]})
        options = dict(policy="limit_ties", limit_rows=2, offset_rows=1, expected_is_complete=True,
                       oracle_output_columns=[0], order_by=[{"column":1,"direction":"asc","nulls":"last"}])
        for values in (["early","late"], ["same","same"], ["same","late"]):
            self.assertTrue(self.check(pa.table({"v":values}), oracle, **options)["ok"])
        for values in (["late","early"], ["early","early"], ["late","late"]):
            self.assertFalse(self.check(pa.table({"v":values}), oracle, **options)["ok"])

    def test_hidden_order_all_legal_tie_permutations_against_exhaustive_oracle(self):
        # Independent definition: enumerate every legal sorted order and slice
        # it, then project away the hidden key. No group-count implementation.
        import itertools
        groups = [[("a",1),("b",1)],[("a",2),("c",2)]]
        permutations = [left + right for left in itertools.permutations(groups[0]) for right in itertools.permutations(groups[1])]
        oracle = pa.table({"v":["a","b","a","c"], "hidden":[1,1,2,2]})
        for offset in range(6):
            for limit in range(4):
                legal = {tuple(row[0] for row in order[offset:offset+limit]) for order in permutations}
                length = min(limit,max(0,4-offset))
                options = dict(policy="limit_ties", limit_rows=limit, offset_rows=offset, expected_is_complete=True,
                               oracle_output_columns=[0], order_by=[{"column":1,"direction":"asc","nulls":"last"}])
                for candidate in itertools.product("abc", repeat=length):
                    actual = pa.table({"v":pa.array(candidate, type=pa.string())})
                    self.assertEqual(self.check(actual, oracle, **options)["ok"], candidate in legal,
                                     (offset,limit,candidate,legal))

    def test_offset_past_end_zero_limit_and_empty_oracle(self):
        oracle = pa.table({"k":pa.array([1,2],type=pa.int64())})
        empty = oracle.slice(0,0)
        options = dict(policy="limit_ties", order_by=[{"column":0,"direction":"asc","nulls":"last"}], expected_is_complete=True)
        self.assertTrue(self.check(empty,oracle,limit_rows=4,offset_rows=9,**options)["ok"])
        self.assertTrue(self.check(empty,oracle,limit_rows=0,offset_rows=1,**options)["ok"])
        self.assertTrue(self.check(empty,empty,limit_rows=4,offset_rows=1,**options)["ok"])
        self.assertFalse(self.check(oracle.slice(0,1),oracle,limit_rows=4,offset_rows=9,**options)["ok"])

    def test_hidden_descending_nulls_and_unordered_offset(self):
        oracle = pa.table({"v":["null","high","low"],"hidden":pa.array([None,2,1],type=pa.int64())})
        options = dict(policy="limit_ties", limit_rows=2, expected_is_complete=True, oracle_output_columns=[0],
                       order_by=[{"column":1,"direction":"desc","nulls":"first"}])
        self.assertTrue(self.check(pa.table({"v":["null","high"]}),oracle,**options)["ok"])
        self.assertFalse(self.check(pa.table({"v":["high","null"]}),oracle,**options)["ok"])
        self.assertTrue(self.check(pa.table({"v":["low"]}),oracle,policy="unordered_limit",limit_rows=10,
                                   offset_rows=2,expected_is_complete=True,oracle_output_columns=[0])["ok"])

    def test_slice_float_tolerance_uses_augmenting_matches(self):
        oracle = pa.table({"f":[0.,1.,2.],"hidden":[1,1,1]})
        options = dict(policy="limit_ties", limit_rows=2, offset_rows=1, expected_is_complete=True,
                       oracle_output_columns=[0],order_by=[{"column":1,"direction":"asc","nulls":"last"}],rel_tol=0,abs_tol=.6)
        self.assertTrue(self.check(pa.table({"f":[.5,0.]}),oracle,**options)["ok"])
        self.assertFalse(self.check(pa.table({"f":[0.,0.]}),oracle,**options)["ok"])

    def test_slice_invalid_mapping_schema_and_order_rejected(self):
        actual = pa.table({"v":["a"]})
        oracle = pa.table({"v":["a"],"hidden":[1]})
        options = dict(policy="limit_ties",limit_rows=1,offset_rows=1,expected_is_complete=True,
                       order_by=[{"column":1,"direction":"asc","nulls":"last"}])
        for mapping in ([],[3],[True],[0,1]):
            self.assertFalse(self.check(actual,oracle,oracle_output_columns=mapping,**options)["ok"])
        self.assertFalse(self.check(actual,oracle,oracle_output_columns=[1],**options)["ok"])
        self.assertFalse(self.check(actual,oracle,**options)["ok"])
        self.assertFalse(self.check(actual,oracle,oracle_output_columns=[0],policy="limit_ties",limit_rows=1,
                                    offset_rows=-1,expected_is_complete=True,order_by=options["order_by"])["ok"])
