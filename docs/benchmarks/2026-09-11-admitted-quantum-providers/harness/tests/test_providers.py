import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

from benchmark.__main__ import parser
from benchmark.contract import ContractError, digest
from benchmark.prepare import DUCKDB_VERSION, write_json
from benchmark.providers import configure_reference, iceberg_data_file, inventory, provider_setup, require_supported


class ProviderTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(dir=os.environ["TMPDIR"])
        self.root = Path(self.directory.name)

    def tearDown(self):
        self.directory.cleanup()

    def test_tracks_parse_and_unknown_modes_fail_closed(self):
        for track in ("raw_parquet", "native", "iceberg", "lance", "decoded_ipc", "gpu", "gpu_control"):
            args = parser().parse_args(["run", "--dataset", "dataset", "--engine-binary", "engine", "--output", "output", "--track", track])
            self.assertEqual(args.track, track)
        for track in ("pretend_gpu", "unmatched_cache"):
            with self.assertRaisesRegex(ContractError, "unsupported"):
                require_supported(track)

    def conversion(self, track="native"):
        dataset = {"tables": [{"name": "t", "path": "source.parquet"}]}
        dataset_path = self.root / "dataset.json"
        write_json(dataset_path, dataset)
        destination = self.root / "t"
        destination.mkdir()
        (destination / "segment").write_bytes(b"immutable provider bytes")
        manifest = {"version": 1, "status": "validated", "track": track, "dataset_manifest_sha256": digest(dataset_path), "duckdb_version": DUCKDB_VERSION,
                    "tables": [{"name": "t", "path": "t", "files": inventory(destination), "validation": {"ok": True}, "snapshot_id": 7, "metadata_path": "t/segment", "version": 1}]}
        path = self.root / "provider.json"
        write_json(path, manifest)
        return dataset_path, dataset, path, manifest

    def test_native_reference_is_source_loaded_as_native_table(self):
        dataset_path, dataset, path, _ = self.conversion()
        tables, provenance = provider_setup(dataset_path, dataset, "native", path)
        self.assertEqual(tables[0]["reference_path"], str(self.root / "source.parquet"))
        self.assertEqual(provenance["sha256"], digest(path))
        con = Mock()
        configure_reference(con, {"track": "native", "tables": tables})
        sql = con.execute.call_args[0][0]
        self.assertIn('CREATE TABLE "t"', sql)
        self.assertIn("source.parquet", sql)
        self.assertNotIn("CREATE VIEW", sql)

    def test_changed_or_added_provider_file_invalidates_conversion(self):
        dataset_path, dataset, path, _ = self.conversion()
        (self.root / "t" / "new-version").write_text("new snapshot")
        with self.assertRaisesRegex(ContractError, "provider files changed"):
            provider_setup(dataset_path, dataset, "native", path)

    def test_wrong_track_source_or_incomplete_validation_fails(self):
        dataset_path, dataset, path, manifest = self.conversion()
        for key, value in (("track", "lance"), ("status", "converting"), ("dataset_manifest_sha256", "wrong")):
            changed = dict(manifest, **{key: value})
            write_json(path, changed)
            with self.assertRaises(ContractError):
                provider_setup(dataset_path, dataset, "native", path)
        with self.assertRaises(ContractError):
            provider_setup(dataset_path, dataset, "native")

    def test_iceberg_setup_pins_snapshot_and_metadata(self):
        dataset_path, dataset, path, _ = self.conversion("iceberg")
        tables, _ = provider_setup(dataset_path, dataset, "iceberg", path)
        self.assertEqual(tables[0]["snapshot_id"], 7)
        self.assertEqual(tables[0]["metadata_path"], str(self.root / "t" / "segment"))

    def test_missing_extension_never_falls_back_to_parquet(self):
        con = Mock()
        con.execute.side_effect = RuntimeError("extension unavailable")
        with self.assertRaisesRegex(RuntimeError, "extension unavailable"):
            configure_reference(con, {"track": "lance", "tables": [{"name": "t", "path": "t.lance"}]})
        self.assertEqual(con.execute.call_args[0][0], "LOAD lance")

    def test_decoded_ipc_uses_preloaded_arrow_with_exact_decimal_types(self):
        import duckdb
        import pyarrow as pa
        from decimal import Decimal
        table = pa.table({"d": pa.array([Decimal("1.25"), None, Decimal("-0.50")], type=pa.decimal128(15, 2))})
        path = self.root / "data.arrow"
        with pa.OSFile(str(path), "wb") as sink:
            with pa.ipc.new_stream(sink, table.schema) as writer:
                writer.write_table(table)
        with duckdb.connect() as con:
            provenance = configure_reference(con, {"track": "decoded_ipc", "tables": [{"name": "t", "path": str(path)}]})
            result = con.execute("SELECT * FROM t").fetch_arrow_table()
        self.assertEqual(provenance["reader"], "decoded_ipc")
        self.assertEqual(result.schema.field("d").type, pa.decimal128(15,2))
        self.assertEqual(result.to_pydict(), table.to_pydict())

    def test_gpu_reference_is_identical_raw_parquet(self):
        for track in ("gpu", "gpu_control"):
            con = Mock()
            provenance = configure_reference(con, {"track": track, "tables": [{"name": "t", "path": "canonical.parquet"}]})
            self.assertEqual(provenance["reader"], "raw_parquet")
            self.assertIn("read_parquet('canonical.parquet')", con.execute.call_args[0][0])


    def test_iceberg_decimal_reencoding_preserves_source_schema_and_every_value(self):
        import pyarrow as pa
        import pyarrow.parquet as pq
        from decimal import Decimal
        source = self.root / "canonical.parquet"
        table = pa.table({
            "small": pa.array([Decimal("9999999.99"), None, Decimal("-9999999.99")], type=pa.decimal128(9, 2)),
            "medium": pa.array([Decimal("1234567890123.45"), None, Decimal("-1234567890123.45")], type=pa.decimal128(15, 2)),
            "wide": pa.array([Decimal("123456789012345678901234567890123456.78"), None, Decimal("-0.01")], type=pa.decimal128(38, 2)),
            "text": ["duplicate", None, "duplicate"],
        })
        pq.write_table(table, source)
        original_hash = digest(source)
        original = pq.ParquetFile(source)
        destination = self.root / "converted"
        target, provenance = iceberg_data_file(original, source, destination)
        self.assertNotEqual(target, source)
        self.assertEqual(digest(source), original_hash)
        self.assertTrue(provenance["reencoded"])
        self.assertEqual(provenance["registered_file_sha256"], digest(target))
        self.assertEqual([change["column"] for change in provenance["changes"]], ["small", "medium"])
        converted = pq.ParquetFile(target)
        self.assertEqual([converted.schema.column(i).physical_type for i in range(3)], ["INT32", "INT64", "FIXED_LEN_BYTE_ARRAY"])
        self.assertEqual(converted.schema_arrow, original.schema_arrow)
        self.assertTrue(converted.read().equals(original.read()))
        self.assertIn("data/compatible-decimals.parquet", inventory(destination))
        with self.assertRaisesRegex(ContractError, "must not overwrite"):
            iceberg_data_file(original, source, destination)

    def test_iceberg_compatible_decimal_source_is_registered_without_copy(self):
        import pyarrow as pa
        import pyarrow.parquet as pq
        from decimal import Decimal
        source = self.root / "canonical.parquet"
        table = pa.table({"d": pa.array([Decimal("123.45"), None], type=pa.decimal128(15, 2))})
        pq.write_table(table, source, store_decimal_as_integer=True)
        target, provenance = iceberg_data_file(pq.ParquetFile(source), source, self.root / "unused")
        self.assertEqual(target, source)
        self.assertFalse(provenance["reencoded"])
        self.assertEqual(provenance["registered_file_sha256"], digest(source))
        self.assertFalse((self.root / "unused").exists())

    def test_lance_reference_records_targeted_optimizer_control(self):
        extension_path = self.root / "lance.extension"
        extension_path.write_bytes(b"pinned extension")
        con = Mock()
        con.execute.return_value.fetchone.side_effect = [
            ("892b224", str(extension_path)), ("join_order",), ("join_order,extension",),
        ]
        provenance = configure_reference(con, {"track": "lance", "tables": []})
        self.assertEqual(provenance["settings"], {"disabled_optimizers": "join_order,extension"})
        self.assertEqual(provenance["extension"]["sha256"], digest(extension_path))
        commands = [call.args[0] for call in con.execute.call_args_list]
        self.assertIn("SET disabled_optimizers='join_order,extension'", commands)
        self.assertNotIn("PRAGMA disable_optimizer", commands)

    @unittest.skipUnless(os.environ.get("BENCHMARK_TEST_LANCE") == "1",
                         "requires pinned local Lance extension and pylance; set BENCHMARK_TEST_LANCE=1")
    def test_lance_decimal_average_matches_independent_parquet_reference(self):
        import duckdb
        import lance
        import pyarrow as pa
        import pyarrow.parquet as pq
        from decimal import Decimal
        self.assertEqual(duckdb.__version__, DUCKDB_VERSION)
        table = pa.table({"g": [1, 1, 1, 1, 2],
                          "d": pa.array([Decimal("0.00"), Decimal("0.00"), Decimal("0.01"), None, None],
                                        type=pa.decimal128(15, 2))})
        source = self.root / "canonical.parquet"
        pq.write_table(table, source)
        destination = self.root / "t.lance"
        version = lance.write_dataset(table, str(destination), mode="create").version
        sql = "SELECT g, AVG(d), SUM(d), COUNT(d) FROM t GROUP BY g ORDER BY g"
        with duckdb.connect() as reference:
            reference.execute("SET autoinstall_known_extensions=false")
            configure_reference(reference, {"track": "raw_parquet", "tables": [{"name": "t", "path": str(source)}]})
            expected = reference.execute(sql).fetchall()
        self.assertEqual(expected, [(1, 0.01 / 3, Decimal("0.01"), 3), (2, None, None, 0)])
        with duckdb.connect() as con:
            con.execute("SET autoinstall_known_extensions=false")
            provenance = configure_reference(con, {"track": "lance", "tables": [{"name": "t", "path": str(destination), "version": version}]})
            self.assertEqual(provenance["settings"], {"disabled_optimizers": "extension"})
            self.assertEqual(con.execute(sql).fetchall(), expected)
            plan = con.execute("EXPLAIN " + sql).fetchone()[1]
            self.assertIn("__LANCE_SCAN", plan)
            self.assertNotIn("__LANCE_EXEC", plan)
            con.execute("CREATE TEMP TABLE materialized AS SELECT * FROM t")
            self.assertEqual(con.execute(sql.replace("FROM t ", "FROM materialized ")).fetchall(), expected)
