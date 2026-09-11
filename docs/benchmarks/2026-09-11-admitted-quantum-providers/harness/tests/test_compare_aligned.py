import os
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
from benchmark.compare import compare_files


class AlignedExactTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(dir=os.environ["TMPDIR"])
        self.root = Path(self.directory.name)

    def tearDown(self):
        self.directory.cleanup()

    def write(self, name, schema, batches):
        path = self.root / name
        with pa.OSFile(str(path), "wb") as sink, pa.ipc.new_stream(sink, schema) as writer:
            for batch in batches:
                writer.write_batch(batch)
        return path

    def compare(self, actual, expected, *, left_chunk=3, right_chunk=5, native_only=False, **options):
        paths = [self.write(name, table.schema, table.to_batches(max_chunksize=size))
                 for name, table, size in [("a.arrow", actual, left_chunk), ("e.arrow", expected, right_chunk)]]
        with patch("benchmark.compare._ALIGNED_SLICE_ROWS", 2):
            if native_only:
                with patch("benchmark.compare.sqlite3.connect", side_effect=AssertionError("native proof must not use SQLite")):
                    return compare_files(*paths, scratch=self.root, rel_tol=0, abs_tol=0, **options)
            return compare_files(*paths, scratch=self.root, rel_tol=0, abs_tol=0, **options)

    def test_complete_typed_equality_across_batch_boundaries_without_sqlite(self):
        table = pa.table({
            "i": pa.array([-(2**63), 2**63-1, None, 4, 4, 0, 1], type=pa.int64()),
            "u": pa.array([2**64-1, 2**64-2, None, 4, 4, 0, 1], type=pa.uint64()),
            "d": pa.array([Decimal("123456789012345678901234567890123456.78"), None, Decimal("-0.01"), Decimal("1.23"), Decimal("1.23"), Decimal(0), Decimal(1)], type=pa.decimal128(38, 2)),
            "t": pa.array([1, 2, None, -1, -1, 1001, 1002], type=pa.timestamp("ns", "UTC")),
            "s": pa.array(["", None, "é🙂", "same", "same", "x", "y"], type=pa.string()),
        })
        result = self.compare(table, table, native_only=True)
        self.assertTrue(result["ok"], result)
        self.assertEqual(result["comparison_strategy"], "arrow_aligned_exact")
        self.assertEqual(result["actual_rows"], 7)
        self.assertLessEqual(result["aligned_exact"]["peak_segment_rows"], 2)
        self.assertGreater(result["aligned_exact"]["segments"], 3)

    def test_reordered_duplicates_fall_back_without_losing_prefix(self):
        actual = pa.table({"v": [1, 1, 2, 3, 3, 3]})
        expected = pa.table({"v": [1, 1, 3, 2, 3, 3]})
        result = self.compare(actual, expected)
        self.assertTrue(result["ok"], result)
        self.assertEqual(result["comparison_strategy"], "duckdb_parquet_exact_bag")
        self.assertEqual(result["aligned_exact"]["matched_rows"], 2)
        self.assertFalse(self.compare(actual, pa.table({"v": [1, 1, 3, 2, 3, 2]}))["ok"])

    def test_last_row_mismatch_and_different_lengths_do_not_pass(self):
        table = pa.table({"v": [1, 2, 3, 4, 5, 6]})
        for other in [pa.table({"v": [1, 2, 3, 4, 5, 7]}), table.slice(0, 5)]:
            result = self.compare(table, other)
            self.assertFalse(result["ok"], result)
            self.assertEqual(result["comparison_strategy"], "duckdb_parquet_exact_bag")

    def test_schema_mismatch_declines_but_preserves_existing_logical_rules(self):
        actual = pa.table({"v": pa.array([1, 2], type=pa.int32())})
        expected = pa.table({"v": pa.array([1, 2], type=pa.int64())})
        result = self.compare(actual, expected)
        self.assertTrue(result["ok"], result)
        self.assertFalse(result["aligned_exact"]["attempted"])
        self.assertEqual(result["comparison_strategy"], "sqlite_exact_bag")
        self.assertFalse(self.compare(actual, pa.table({"v": ["1", "2"]}))["ok"])
        unsupported = pa.table({"v": pa.array([[1], [2]], type=pa.list_(pa.int64()))})
        self.assertFalse(self.compare(unsupported, unsupported)["ok"])

    def test_dictionary_codebooks_nulls_and_slices(self):
        first = pa.DictionaryArray.from_arrays(pa.array([0, 1, None, 2], type=pa.int32()), pa.array(["a", "b", None]))
        second = pa.DictionaryArray.from_arrays(pa.array([1, 0, None, 2], type=pa.int32()), pa.array(["b", "a", None]))
        left, right = pa.table({"v": first}), pa.table({"v": second})
        self.assertTrue(self.compare(left, right)["ok"])
        self.assertFalse(self.compare(left, pa.table({"v": ["a", "b", None, ""]}))["ok"])
        backing = pa.table({"v": ["unused", "a", None, "b", "unused"]})
        self.assertTrue(self.compare(backing.slice(1, 3), pa.table({"v": ["a", None, "b"]}), native_only=True)["ok"])

    def test_nan_signed_zero_infinities_and_temporal_precision(self):
        left = pa.table({"v": [float("nan"), -0.0, float("inf"), -float("inf"), None]})
        right = pa.table({"v": [float("nan"), 0.0, float("inf"), -float("inf"), None]})
        result = self.compare(left, right)
        self.assertTrue(result["ok"], result)
        self.assertFalse(self.compare(left, pa.table({"v": [float("nan"), 0.0, float("inf"), float("inf"), None]}))["ok"])
        first = pa.table({"v": pa.array([1001], type=pa.timestamp("ns"))})
        second = pa.table({"v": pa.array([1002], type=pa.timestamp("ns"))})
        self.assertFalse(self.compare(first, second)["ok"])

    def test_empty_batches_and_zero_column_rows_are_counted(self):
        schema = pa.schema([("v", pa.int64())])
        empty = pa.record_batch([pa.array([], type=pa.int64())], schema=schema)
        full = pa.record_batch([pa.array([1, 2], type=pa.int64())], schema=schema)
        a = self.write("a.arrow", schema, [empty, full, empty])
        e = self.write("e.arrow", schema, [full])
        with patch("benchmark.compare.sqlite3.connect", side_effect=AssertionError("unexpected SQLite")):
            self.assertTrue(compare_files(a, e, scratch=self.root, rel_tol=0, abs_tol=0)["ok"])
        for count in [0, 3]:
            batch = pa.RecordBatch.from_struct_array(pa.array([{}] * count, type=pa.struct([])))
            a = self.write("a.arrow", batch.schema, [batch])
            e = self.write("e.arrow", batch.schema, [batch])
            result = compare_files(a, e, scratch=self.root, rel_tol=0, abs_tol=0)
            self.assertTrue(result["ok"], result)
            self.assertEqual(result["actual_rows"], count)

    def test_tolerant_and_ordered_comparisons_never_take_fast_path(self):
        table = pa.table({"v": [1.0, 2.0]})
        paths = [self.write(name, table.schema, table.to_batches()) for name in ["a.arrow", "e.arrow"]]
        with patch("benchmark.compare.aligned_exact_streams", side_effect=AssertionError("not eligible")):
            self.assertTrue(compare_files(*paths, scratch=self.root)["ok"])
            self.assertTrue(compare_files(*paths, scratch=self.root, rel_tol=0, abs_tol=0,
                policy="ordered", order_by=[{"column": 0, "direction": "asc", "nulls": "last"}])["ok"])

    def test_exact_backend_resource_refusal_does_not_restart_sqlite(self):
        actual = pa.table({"v": [1, 2]})
        expected = pa.table({"v": [2, 1]})
        refusal = {"attempted": True, "ok": False, "status": "validation_error",
                   "reason": "exact bag memory budget exhausted"}
        with patch("benchmark.exact_bag.try_exact_bag", return_value=refusal), \
             patch("benchmark.compare.sqlite3.connect", side_effect=AssertionError("must preserve refusal")):
            result = self.compare(actual, expected)
        self.assertFalse(result["ok"])
        self.assertEqual(result["status"], "validation_error")
        self.assertEqual(result["reason"], refusal["reason"])
        self.assertEqual(result["comparison_strategy"], "duckdb_parquet_exact_bag")
