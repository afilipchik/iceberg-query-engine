"""Bounded exact bag comparison for a narrow, losslessly staged Arrow domain.

Only strict-schema, zero-tolerance callers may use this backend. Full typed
GROUP BY keys and signed multiplicities prove equality; hashes alone do not.
Parquet staging avoids pinned DuckDB/PyArrow reader-finalization crashes on OOM.
"""
import json
import tempfile
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from .prepare import DUCKDB_VERSION

MEMORY_LIMIT_BYTES = 512 * 1024**2
TEMP_LIMIT_BYTES = 64 * 1024**3
STAGE_BATCH_ROWS = 65536


class UnsupportedDomain(ValueError):
    pass


class InvalidInput(ValueError):
    pass


def _duckdb_type(data_type):
    names = {
        pa.bool_(): "BOOLEAN", pa.int8(): "TINYINT", pa.int16(): "SMALLINT",
        pa.int32(): "INTEGER", pa.int64(): "BIGINT", pa.uint8(): "UTINYINT",
        pa.uint16(): "USMALLINT", pa.uint32(): "UINTEGER", pa.uint64(): "UBIGINT",
        pa.float32(): "FLOAT", pa.float64(): "DOUBLE", pa.string(): "VARCHAR",
        pa.date32(): "DATE",
    }
    if data_type in names:
        return names[data_type]
    if pa.types.is_decimal128(data_type) and 0 <= data_type.scale <= data_type.precision <= 38:
        return f"DECIMAL({data_type.precision},{data_type.scale})"
    raise UnsupportedDomain(f"unsupported exact-bag domain: {data_type}")


def _schema(path):
    with pa.OSFile(str(path), "rb") as source:
        return pa.ipc.open_stream(source).schema


def _preflight(path, schema):
    count = 0
    with pa.OSFile(str(path), "rb") as source:
        reader = pa.ipc.open_stream(source)
        if not reader.schema.equals(schema, check_metadata=True):
            raise InvalidInput("input schema changed during preflight")
        for batch in reader:
            count += batch.num_rows
            for index, field in enumerate(schema):
                array = batch.column(index)
                if not field.nullable and array.null_count:
                    raise InvalidInput("NULL values contradict nonnullable Arrow schema")
                if pa.types.is_date32(field.type):
                    bounds = pc.min_max(pc.cast(array, pa.int32())).as_py()
                    # Prove a finite Gregorian DATE range without Python datetime
                    # conversion; also exclude DuckDB's infinity sentinels.
                    if bounds["min"] is not None and not (
                        -719162 <= bounds["min"] <= bounds["max"] <= 2932896
                    ):
                        raise UnsupportedDomain("Date32 outside proven finite range 0001..9999")
    return count


def _stage(path, destination, source_schema, ordinal_schema):
    with pa.OSFile(str(path), "rb") as source, pq.ParquetWriter(
        destination, ordinal_schema, compression="NONE", use_dictionary=False
    ) as writer:
        reader = pa.ipc.open_stream(source)
        if not reader.schema.equals(source_schema, check_metadata=True):
            raise InvalidInput("input schema changed during staging")
        for batch in reader:
            for offset in range(0, batch.num_rows, STAGE_BATCH_ROWS):
                piece = batch.slice(offset, STAGE_BATCH_ROWS)
                # Rename physical fields by ordinal (including duplicate source
                # names), and never let importer metadata hide actual NULLs.
                writer.write_batch(pa.RecordBatch.from_arrays(piece.columns, schema=ordinal_schema))
    if not pq.read_schema(destination).equals(ordinal_schema, check_metadata=True):
        raise InvalidInput("exact-bag staging changed Arrow schema")


def _literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def try_exact_bag(actual_path, expected_path, *, scratch, memory_limit_bytes=MEMORY_LIMIT_BYTES):
    """Return an attempted result, or an explicit unsupported-domain decline.

    Only ``attempted=False`` permits another backend. Resource errors, invalid
    input, import changes and unequal values are attempted validation errors.
    Memory overrides are bounded and intended for clean-refusal tests.
    """
    detail = {"attempted": True, "ok": False, "status": "validation_error",
              "comparison_strategy": "duckdb_exact_bag"}
    resources = {"memory_limit_bytes": memory_limit_bytes, "threads": 1,
                 "max_temp_directory_bytes": TEMP_LIMIT_BYTES, "default_collation": "",
                 "staging": "uncompressed Parquet with ordinal nullable fields",
                 "stage_batch_rows": STAGE_BATCH_ROWS}
    detail["resources"] = resources
    started = time.perf_counter()
    try:
        if type(memory_limit_bytes) is not int or not 0 < memory_limit_bytes <= MEMORY_LIMIT_BYTES:
            raise InvalidInput("exact-bag memory limit must be positive and at most 512 MiB")
        try:
            import duckdb
        except ImportError as error:
            raise UnsupportedDomain("pinned DuckDB unavailable") from error
        if duckdb.__version__ != DUCKDB_VERSION:
            raise UnsupportedDomain("DuckDB version differs from pinned exact-bag backend")
        resources["duckdb_version"] = duckdb.__version__
        actual_path, expected_path = Path(actual_path), Path(expected_path)
        actual_schema, expected_schema = _schema(actual_path), _schema(expected_path)
        if not actual_schema.equals(expected_schema, check_metadata=True):
            raise UnsupportedDomain("exact-bag backend requires identical complete Arrow schemas")
        if not len(actual_schema):
            raise UnsupportedDomain("zero-column Arrow schema")
        types = [_duckdb_type(field.type) for field in actual_schema]
        detail["actual_rows"] = _preflight(actual_path, actual_schema)
        detail["expected_rows"] = _preflight(expected_path, expected_schema)
        resources["preflight_seconds"] = time.perf_counter() - started
        names = [f"c{index}" for index in range(len(actual_schema))]
        ordinal_schema = pa.schema([pa.field(name, field.type, nullable=True)
                                    for name, field in zip(names, actual_schema)])
        with tempfile.TemporaryDirectory(prefix="exact-bag-", dir=scratch) as temporary:
            root = Path(temporary)
            stage_start = time.perf_counter()
            for name, source in (("actual", actual_path), ("expected", expected_path)):
                _stage(source, root / (name + ".parquet"), actual_schema, ordinal_schema)
            resources["staging_seconds"] = time.perf_counter() - stage_start
            resources["staging_bytes"] = sum((root / (name + ".parquet")).stat().st_size
                                              for name in ("actual", "expected"))
            con = duckdb.connect()
            try:
                con.execute("SET threads=1")
                con.execute("SET memory_limit=" + _literal(f"{memory_limit_bytes}B"))
                con.execute("SET max_temp_directory_size=" + _literal(f"{TEMP_LIMIT_BYTES}B"))
                con.execute("SET temp_directory=" + _literal(root / "spill"))
                con.execute("SET default_collation=''")
                con.execute("SET autoinstall_known_extensions=false")
                if con.execute("SELECT current_setting('default_collation')").fetchone()[0] != "":
                    raise InvalidInput("exact-bag collation is not binary/default")
                for name in ("actual", "expected"):
                    con.execute(f"CREATE VIEW {name} AS SELECT * FROM read_parquet(" +
                                _literal(root / (name + ".parquet")) + ")")
                    description = con.execute("DESCRIBE " + name).fetchall()
                    if [field[0] for field in description] != names or [field[1] for field in description] != types:
                        raise InvalidInput("exact-bag import changed ordinal names or logical types")
                detail["import_types"] = types
                columns = ",".join('"' + name + '"' for name in names)
                # SUM(BIGINT) is HUGEINT; every complete row contributes +1 or
                # -1. One full-key aggregate tests exact duplicate multiplicity.
                sql = f"""SELECT COUNT(*) FROM (
                    SELECT {columns}, SUM(n) AS delta FROM (
                        SELECT {columns}, CAST(1 AS BIGINT) AS n FROM actual
                        UNION ALL
                        SELECT {columns}, CAST(-1 AS BIGINT) AS n FROM expected
                    ) GROUP BY {columns} HAVING SUM(n) <> 0
                )"""
                detail["query_plan"] = con.execute("EXPLAIN " + sql).fetchone()[1]
                profile_path = root / "profile.json"
                con.execute("SET profiling_output=" + _literal(profile_path))
                con.execute("SET enable_profiling='json'")
                mismatches = con.execute(sql).fetchall()[0][0]
                con.execute("PRAGMA disable_profiling")
                profile = json.loads(profile_path.read_text())
                resources.update({key: profile[key] for key in (
                    "system_peak_buffer_memory", "system_peak_temp_dir_size", "latency", "cpu_time")})
                detail["mismatching_groups"] = mismatches
                detail["ok"] = mismatches == 0 and detail["actual_rows"] == detail["expected_rows"]
                detail["status"] = "validated" if detail["ok"] else "validation_error"
                if not detail["ok"]:
                    detail["reason"] = "exact value/multiplicity mismatch"
            finally:
                con.close()
    except UnsupportedDomain as error:
        detail.update(attempted=False, decline_reason=str(error))
    except (InvalidInput, ValueError, OSError, pa.ArrowException, MemoryError) as error:
        detail.update(error_kind=type(error).__name__, reason=str(error))
    except Exception as error:
        # DuckDB exceptions are optional-dependency classes. Never turn one into
        # a fallback: OOM/disk/query errors mean validation did not complete.
        detail.update(error_kind=type(error).__name__, reason=str(error))
    detail["wall_seconds"] = time.perf_counter() - started
    return detail
