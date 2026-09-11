import os
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa

from benchmark import compare as existing
from benchmark.exact_bag import try_exact_bag


class ExactBagTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(dir=os.environ["TMPDIR"])
        self.root = Path(self.directory.name)

    def tearDown(self):
        self.directory.cleanup()

    def write(self, name, table):
        path = self.root / name
        with pa.OSFile(str(path), "wb") as sink, pa.ipc.new_stream(sink, table.schema) as writer:
            writer.write_table(table, max_chunksize=3)
        return path

    def check(self, actual, expected, correct):
        a, e = self.write("a.arrow", actual), self.write("e.arrow", expected)
        # Force the independent original Python/SQLite oracle even after the
        # dispatch is integrated. Assert a SQLite database was actually opened.
        decline = {"attempted": False, "decline_reason": "independent oracle forced"}
        with patch("benchmark.exact_bag.try_exact_bag", return_value=decline), \
             patch.object(existing, "try_exact_bag", return_value=decline, create=True), \
             patch.object(existing, "aligned_exact_streams", return_value={"equal": False}), \
             patch.object(existing.sqlite3, "connect", wraps=existing.sqlite3.connect) as sqlite:
            oracle = existing.compare_files(a, e, scratch=self.root, rel_tol=0, abs_tol=0)
            self.assertTrue(sqlite.called)
        result = try_exact_bag(a, e, scratch=self.root)
        self.assertEqual(result["ok"], correct, result)
        self.assertEqual(result["ok"], oracle["ok"], (result, oracle))
        self.assertTrue(result["attempted"], result)
        self.assertFalse(list(self.root.glob("exact-bag-*")), "temporary staging must be removed")
        return result

    def test_exact_integer_float_decimal_date_and_string_domains(self):
        types = [(pa.bool_(), [True, False, True, None]),
                 *[(t, [0, 1, 1, None]) for t in [pa.int8(), pa.int16(), pa.int32(), pa.uint8(), pa.uint16(), pa.uint32()]],
                 (pa.int64(), [-2**63, 2**53+1, 2**63-1, None]),
                 (pa.uint64(), [0, 2**64-1, 2**64-2, None]),
                 (pa.decimal128(38, 2), [Decimal("123456789012345678901234567890123456.78"), Decimal("-0.01"), Decimal("-0.01"), None]),
                 (pa.string(), ["a", "A", "é", None]),
                 (pa.float32(), [float("nan"), -0., float("inf"), None]),
                 (pa.float64(), [float("nan"), -float("inf"), 0., None]),
                 (pa.date32(), [-719162, 0, 2932896, None])]
        for data_type, values in types:
            with self.subTest(data_type=data_type):
                a = pa.table({"key": pa.array(values, type=data_type)})
                self.check(a, a.take(pa.array([3, 2, 1, 0])), True)
                self.check(a, a.take(pa.array([3, 2, 0, 0])), False)

    def test_adjacent_exact_values_do_not_collapse(self):
        for data_type, values in [
            (pa.int64(), [2**53, 2**53+1]), (pa.uint64(), [2**64-2, 2**64-1]),
            (pa.decimal128(38, 2), [Decimal("123456789012345678901234567890123456.78"), Decimal("123456789012345678901234567890123456.79")]),
            (pa.float64(), [1., 1.0000000000000002]),
        ]:
            a = pa.table({"x": pa.array(values, type=data_type)})
            self.check(a, a.take(pa.array([0, 0])), False)

    def test_full_rows_binary_collation_and_ordinal_duplicate_names(self):
        a = pa.Table.from_arrays([pa.array(["x\x00y", "x|y", "é", "e\u0301"]), pa.array([1, 2, 3, 4])], names=["same", "same"])
        self.check(a, a.take(pa.array([3, 2, 1, 0])), True)
        self.check(pa.table({"a": [1, 2], "b": ["x", "y"]}),
                   pa.table({"a": [1, 2], "b": ["y", "x"]}), False)
        self.check(pa.table({"x": ["a", "A"]}), pa.table({"x": ["a", "a"]}), False)
        self.check(pa.table({"x": ["é", "e\u0301"]}), pa.table({"x": ["é", "é"]}), False)

    def test_empty_all_null_and_float_zero_semantics(self):
        for values in [[], [None, None]]:
            a = pa.table({"x": pa.array(values, type=pa.int64())})
            self.check(a, a, True)
        self.check(pa.table({"x": [0., -0., float("nan")]}),
                   pa.table({"x": [-0., 0., float("nan")]}), True)

    def test_unsupported_domains_and_nonidentical_schema_decline(self):
        for data_type in [pa.large_string(), pa.timestamp("ns"), pa.list_(pa.int64()),
                          pa.dictionary(pa.int32(), pa.string()), pa.decimal128(10, -2), pa.binary()]:
            a = self.write("a.arrow", pa.table({"x": pa.array([], type=data_type)}))
            result = try_exact_bag(a, a, scratch=self.root)
            self.assertFalse(result["attempted"], result)
            self.assertIn("decline_reason", result)
        a = self.write("a.arrow", pa.table({"x": pa.array([1], type=pa.int32())}))
        e = self.write("e.arrow", pa.table({"x": pa.array([1], type=pa.int64())}))
        self.assertFalse(try_exact_bag(a, e, scratch=self.root)["attempted"])
        bad_date = self.write("date.arrow", pa.table({"x": pa.array([2**31-1], type=pa.date32())}))
        self.assertFalse(try_exact_bag(bad_date, bad_date, scratch=self.root)["attempted"])

    def test_nonnullable_null_and_resource_error_are_not_declines(self):
        table = pa.Table.from_arrays([pa.array([None, 1], type=pa.int64())], schema=pa.schema([pa.field("x", pa.int64(), nullable=False)]))
        a = self.write("a.arrow", table)
        result = try_exact_bag(a, a, scratch=self.root)
        self.assertTrue(result["attempted"])
        self.assertFalse(result["ok"])
        self.assertIn("nonnullable", result["reason"])
        a = self.write("a.arrow", pa.table({"x": [1, 2, 3]}))
        result = try_exact_bag(a, a, scratch=self.root, memory_limit_bytes=1024)
        self.assertTrue(result["attempted"], result)
        self.assertEqual(result["status"], "validation_error")
        self.assertEqual(result["error_kind"], "OutOfMemoryException", result)
        self.assertFalse(list(self.root.glob("exact-bag-*")))

    @unittest.skipUnless(os.environ.get("BENCHMARK_TEST_EXACT_BAG_SPILL") == "1",
                         "set BENCHMARK_TEST_EXACT_BAG_SPILL=1 for contained high-cardinality spill test")
    def test_actual_spill_preserves_multiplicity_and_detects_late_corruption(self):
        schema = pa.schema([("u", pa.uint64()), ("s", pa.string())])
        paths = []
        for name, reverse, corrupt in [("a.arrow", False, False), ("e.arrow", True, False), ("bad.arrow", True, True)]:
            path = self.root / name
            paths.append(path)
            with pa.OSFile(str(path), "wb") as sink, pa.ipc.new_stream(sink, schema) as writer:
                for start in range(0, 3000000, 16384):
                    values = list(range(start, min(3000000, start+16384)))
                    if reverse: values.reverse()
                    if corrupt and start+16384 >= 3000000: values[-1] = 0
                    writer.write_batch(pa.record_batch([
                        pa.array([2**64-1-i for i in values], type=pa.uint64()),
                        pa.array([f"key-{i:09d}-" + "x"*80 for i in values]),
                    ], schema=schema))
        for expected, correct, mismatches in [(paths[1], True, 0), (paths[2], False, 2)]:
            result = try_exact_bag(paths[0], expected, scratch=self.root)
            self.assertEqual(result["ok"], correct, result)
            self.assertEqual(result["mismatching_groups"], mismatches)
            self.assertGreater(result["resources"]["system_peak_temp_dir_size"], 0)
            self.assertLessEqual(result["resources"]["system_peak_buffer_memory"], 512*1024**2)
            self.assertFalse(list(self.root.glob("exact-bag-*")))
