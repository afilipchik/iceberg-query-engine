"""Matched provider preparation with exact full-row conversion validation.

No extension downloads, fallback readers or decimal coercions. Physical Parquet
re-encoding needed by the pinned Iceberg writer is recorded explicitly and must
pass the same exact schema/value gate. Conversion is separate from timed runs
and never overwrites a dataset or prior conversion.
"""
import json
import os
import subprocess
import shutil
import time
from pathlib import Path

from .contract import ContractError, digest, require
from .prepare import DUCKDB_VERSION, literal, write_json

SUPPORTED = ("raw_parquet", "native", "iceberg", "lance", "decoded_ipc", "gpu", "gpu_control")
REQUESTABLE = SUPPORTED


def identifier(name):
    return '"' + name.replace('"', '""') + '"'


def require_supported(track):
    require(track in SUPPORTED,
            f"{track}: unsupported; equivalent IPC residency or GPU upload/device controls are not implemented")


def inventory(path):
    root = Path(path)
    return {str(file.relative_to(root)): digest(file) for file in sorted(root.rglob("*")) if file.is_file()}


def provider_setup(dataset_path, dataset, track, provider_manifest=None):
    require_supported(track)
    if track in ("raw_parquet", "gpu", "gpu_control"):
        require(not provider_manifest, "raw_parquet does not use a conversion manifest")
        return [{"name": table["name"], "path": str(dataset_path.parent / table["path"])}
                for table in dataset["tables"]], None
    require(provider_manifest, f"{track} requires --provider-manifest from benchmark convert")
    path = Path(provider_manifest).resolve()
    manifest = json.loads(path.read_text())
    require(manifest.get("version") == 1 and manifest.get("status") == "validated", "provider conversion not validated")
    require(manifest.get("track") == track, "provider track mismatch")
    require(manifest.get("dataset_manifest_sha256") == digest(dataset_path), "conversion belongs to a different dataset")
    require(manifest.get("duckdb_version") == DUCKDB_VERSION, "conversion DuckDB version mismatch")
    require([table["name"] for table in manifest["tables"]] == [table["name"] for table in dataset["tables"]],
            "conversion table membership mismatch")
    tables = []
    for table in manifest["tables"]:
        require(table.get("validation", {}).get("ok") is True, "unvalidated provider table")
        directory = path.parent / table["path"]
        require(inventory(directory) == table["files"], f"provider files changed: {table['name']}")
        item = {"name": table["name"], "path": str(directory)}
        if track == "decoded_ipc":
            item["path"] = str(directory / "data.arrow")
        if track == "iceberg":
            item.update(metadata_path=str(path.parent / table["metadata_path"]), snapshot_id=table["snapshot_id"])
        if track == "lance":
            import lance
            require(lance.dataset(str(directory)).version == table["version"], "Lance current version differs from pinned conversion")
            item["version"] = table["version"]
        if track == "native":
            source = next(source for source in dataset["tables"] if source["name"] == table["name"])
            item["reference_path"] = str(dataset_path.parent / source["path"])
        tables.append(item)
    return tables, {"path": str(path), "sha256": digest(path), "manifest": manifest}


def configure_reference(con, setup):
    """Return observed reader provenance; errors never select another reader."""
    track = setup["track"]
    require_supported(track)
    if track in ("gpu", "gpu_control"):
        track = "decoded_ipc" if setup.get("host_arrow_preloaded") is True else "raw_parquet"
    provenance = {"reader": track}
    if track in ("iceberg", "lance"):
        if track == "iceberg":
            con.execute("LOAD avro")
        con.execute("LOAD " + track)
        extension = con.execute("SELECT extension_version, install_path FROM duckdb_extensions() WHERE extension_name=?", [track]).fetchone()
        require(extension and extension[1], f"missing {track} extension provenance")
        provenance["extension"] = {"version": extension[0], "sha256": digest(extension[1])}
        if track == "lance":
            # Pinned 892b224's __LANCE_EXEC aggregate pushdown truncates
            # AVG(decimal) to six decimal places. Keep the direct Lance scan
            # and DuckDB's ordinary optimizers, but execute aggregates in
            # DuckDB. This is a recorded compatibility configuration, not
            # stock Lance-extension optimizer performance.
            previous = con.execute("SELECT current_setting('disabled_optimizers')").fetchone()[0]
            disabled = [name for name in previous.split(",") if name]
            if "extension" not in disabled:
                disabled.append("extension")
            con.execute("SET disabled_optimizers=" + literal(",".join(disabled)))
            observed = con.execute("SELECT current_setting('disabled_optimizers')").fetchone()[0]
            require("extension" in observed.split(","), "Lance reference optimizer control not applied")
            provenance["settings"] = {"disabled_optimizers": observed}
            provenance["compatibility"] = {
                "reason": "Lance extension aggregate pushdown truncates decimal AVG",
                "verified_affected_extension": "892b224",
                "execution": "direct Lance scan with DuckDB aggregation",
                "limitation": "extension optimizer disabled for every query; not stock extension pushdown performance",
            }
    for table in setup["tables"]:
        name = identifier(table["name"])
        if track == "decoded_ipc":
            import pyarrow as pa
            with pa.memory_map(table["path"], "r") as source:
                data = pa.ipc.open_stream(source).read_all()
            con.register(table["name"], data)
        elif track == "native":
            con.execute(f"CREATE TABLE {name} AS SELECT * FROM read_parquet({literal(table['reference_path'])})")
        else:
            if track == "raw_parquet":
                source = f"read_parquet({literal(table['path'])})"
            elif track == "iceberg":
                # The immutable metadata file pins current snapshot for both
                # readers; the engine additionally receives the snapshot ID.
                source = f"iceberg_scan({literal(table['metadata_path'])})"
            else:
                import lance
                require(lance.dataset(table["path"]).version == table["version"], "DuckDB Lance current version differs from engine version")
                # The extension reads current; require that current is the
                # engine's explicit version and protect the complete inventory.
                source = literal(table["path"])
            con.execute(f"CREATE VIEW {name} AS SELECT * FROM {source}")
    return provenance


def exact_schema(path):
    import pyarrow as pa
    with pa.memory_map(str(path), "r") as source:
        schema = pa.ipc.open_stream(source).schema
        return [(field.name, field.type) for field in schema]


def iceberg_data_file(parquet, source_path, destination):
    """Adapt physical decimal encoding to PyIceberg's add_files contract.

    PyIceberg 0.10 expects INT32/INT64 for <=9/<=18 digit decimals, while
    PyArrow's default Parquet writer uses FIXED_LEN_BYTE_ARRAY. Both are valid
    Parquet representations of the same logical decimal. Re-encode into a new
    provider-owned file only when needed; never alter the canonical source or
    its Arrow schema. The caller independently validates both provider readers
    against the original source with exact types and zero numeric tolerance.
    """
    import pyarrow.parquet as pq

    changes = []
    for index in range(len(parquet.schema)):
        column = parquet.schema.column(index)
        if column.converted_type != "DECIMAL":
            continue
        expected = "INT32" if column.precision <= 9 else "INT64" if column.precision <= 18 else "FIXED_LEN_BYTE_ARRAY"
        actual = column.physical_type
        if actual != expected and (actual, expected) != ("INT32", "INT64"):
            changes.append({"column": column.path, "precision": column.precision,
                            "scale": column.scale, "source_physical_type": actual,
                            "target_physical_type": expected})
    if not changes:
        return source_path, {"reencoded": False, "registered_file_sha256": digest(source_path)}

    data_dir = destination / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    target = data_dir / "compatible-decimals.parquet"
    require(not target.exists(), "Iceberg decimal conversion must not overwrite an existing file")
    with pq.ParquetWriter(target, parquet.schema_arrow, compression="zstd",
                          store_decimal_as_integer=True) as writer:
        for batch in parquet.iter_batches(batch_size=65536):
            writer.write_batch(batch, row_group_size=65536)
    return target, {"reencoded": True,
                    "reason": "pinned PyIceberg add_files physical decimal compatibility",
                    "changes": changes,
                    "path": str(target.relative_to(destination)),
                    "registered_file_sha256": digest(target),
                    "writer": "pyarrow.parquet.ParquetWriter",
                    "store_decimal_as_integer": True,
                    "compression": "zstd", "row_group_rows": 65536}


def convert(args):
    from .run import Worker, containment, verify_dataset
    from .compare import compare_files
    import duckdb
    import pyarrow as pa
    import pyarrow.parquet as pq

    containment()
    require(args.track in ("native", "iceberg", "lance", "decoded_ipc"), "conversion requires native, iceberg or lance")
    require(duckdb.__version__ == DUCKDB_VERSION, "conversion DuckDB version mismatch")
    dataset_path = Path(args.dataset).resolve()
    dataset = verify_dataset(dataset_path)
    binary = Path(args.engine_binary).resolve()
    require(binary.is_file() and os.access(binary, os.X_OK), "executable embedded adapter required")
    require(args.process_cap_gib >= args.memory_gib > 0, "conversion query budget must fit positive process cap")
    require(args.validation_timeout_seconds > 0, "positive conversion readback timeout required")
    root = Path(args.output).resolve()
    root.mkdir(parents=True, exist_ok=False)
    scratch = root / "validation"
    scratch.mkdir()
    provenance = root / "provenance"
    provenance.mkdir()
    validation_sources = {}
    for source_file in sorted(Path(__file__).parent.glob("*.py")):
        destination_file = provenance / source_file.name
        shutil.copyfile(source_file, destination_file)
        validation_sources[source_file.name] = digest(destination_file)
    env = dict(os.environ, TMPDIR=str(scratch), QE_MEM_CAP=f"{args.process_cap_gib}G", QE_IPC_CACHE="0", QE_GPU="0", RAYON_NUM_THREADS="4")
    manifest = {"version": 1, "status": "converting", "track": args.track,
                "dataset_manifest_sha256": digest(dataset_path), "dataset_path": str(dataset_path),
                "duckdb_version": duckdb.__version__, "pyarrow_version": pa.__version__,
                "validation_sources": validation_sources,
                "engine_binary_sha256": digest(binary),
                "tables": [], "conversion_boundary": "setup outside measured queries",
                "settings": {"threads": 4, "memory_limit": f"{args.memory_gib * 1024**3}B", "process_cap_bytes": args.process_cap_gib * 1024**3,
                             "validation_timeout_seconds": args.validation_timeout_seconds,
                             "disk_admission": "8x current table uncompressed Parquet bytes, minimum 1 GiB; estimate not a quota"}}
    write_json(root / "provider.json", manifest)
    try:
        if args.track == "iceberg":
            from pyiceberg.catalog.sql import SqlCatalog
            import pyiceberg
            manifest["writer_version"] = pyiceberg.__version__
            catalog = SqlCatalog("benchmark", uri=f"sqlite:///{root / 'catalog.db'}", warehouse=root.as_uri())
            catalog.create_namespace("benchmark")
        if args.track == "lance":
            import lance
            manifest["writer_version"] = lance.__version__
        for source in dataset["tables"]:
            started = time.perf_counter()
            name = source["name"]
            source_path = dataset_path.parent / source["path"]
            parquet = pq.ParquetFile(source_path)
            uncompressed = sum(parquet.metadata.row_group(index).total_byte_size for index in range(parquet.metadata.num_row_groups))
            required_disk = max(1024**3, 8 * uncompressed)
            require(shutil.disk_usage(root).free >= required_disk,
                    f"{name}: insufficient disk for exact readback validation (estimated {required_disk} bytes required)")
            relative = name + (".lance" if args.track == "lance" else "")
            destination = root / relative
            table = {"name": name, "path": relative, "source_sha256": source["sha256"]}
            if args.track == "decoded_ipc":
                destination.mkdir()
                with pa.OSFile(str(destination / "data.arrow"), "wb") as sink:
                    with pa.ipc.new_stream(sink, parquet.schema_arrow) as writer:
                        for batch in parquet.iter_batches(batch_size=65536):
                            writer.write_batch(batch)
            elif args.track == "native":
                command = [str(binary), "--write-native", str(source_path), str(destination)]
                with (scratch / f"{name}-writer.log").open("wb") as log:
                    subprocess.run(command, env=env, stdout=log, stderr=subprocess.STDOUT, check=True, timeout=3600)
                table["writer_command"] = command
            elif args.track == "iceberg":
                iceberg = catalog.create_table(f"benchmark.{name}", schema=parquet.schema_arrow, location=destination.as_uri())
                registered_path, encoding = iceberg_data_file(parquet, source_path, destination)
                table["parquet_encoding"] = encoding
                # Both readers scan this same snapshot/file, even when the
                # pinned writer requires a new physical decimal representation.
                iceberg.add_files([str(registered_path)])
                metadata = Path(iceberg.metadata_location.removeprefix("file://"))
                table.update(metadata_path=str(metadata.relative_to(root)), snapshot_id=iceberg.current_snapshot().snapshot_id)
            else:
                reader = pa.RecordBatchReader.from_batches(parquet.schema_arrow, parquet.iter_batches(batch_size=65536))
                converted = lance.write_dataset(reader, str(destination), mode="create", max_rows_per_file=1048576)
                table["version"] = converted.version
            table["conversion_ms"] = (time.perf_counter() - started) * 1000
            item = {"name": name, "path": str(destination), "reference_path": str(source_path)}
            if args.track == "decoded_ipc":
                item["path"] = str(destination / "data.arrow")
            if args.track == "lance":
                item["version"] = table["version"]
            if args.track == "iceberg":
                item.update(metadata_path=str(root / table["metadata_path"]), snapshot_id=table["snapshot_id"])
            expected = scratch / f"{name}-source.arrow"
            with pa.OSFile(str(expected), "wb") as sink:
                with pa.ipc.new_stream(sink, parquet.schema_arrow) as writer:
                    for batch in parquet.iter_batches(batch_size=65536):
                        writer.write_batch(batch)
            setup = {"track": args.track, "threads": 4, "memory_limit": manifest["settings"]["memory_limit"], "tables": [item]}
            setup_path = scratch / f"{name}-setup.json"
            write_json(setup_path, setup)
            actual = scratch / f"{name}-engine.arrow"
            columns = ", ".join(f"{identifier(field.name)} AS {identifier(field.name)}" for field in parquet.schema_arrow)
            sql = f"SELECT {columns} FROM {identifier(name)}"
            worker = Worker([str(binary), str(setup_path)], env, scratch / f"{name}-engine.log", [], startup_seconds=300)
            try:
                require(worker.ready.get("status") == "ready", f"provider engine setup failed: {worker.ready}")
                result = worker.query({"id": name, "sql": sql, "output": str(actual)}, ceiling_ms=args.validation_timeout_seconds * 1000)
                require(result.get("status") == "completed", f"provider read failed: {result}")
                table["engine_ready"] = worker.ready
            finally:
                worker.close()
            con = duckdb.connect()
            try:
                con.execute("SET threads=4")
                con.execute("SET memory_limit=" + literal(manifest["settings"]["memory_limit"]))
                con.execute("SET autoinstall_known_extensions=false")
                con.execute("SET temp_directory=" + literal(scratch / "duckdb-temp"))
                table["reference"] = configure_reference(con, setup)
                reference = scratch / f"{name}-reference.arrow"
                reader = con.execute(sql).fetch_record_batch(65536)
                with pa.OSFile(str(reference), "wb") as sink:
                    with pa.ipc.new_stream(sink, reader.schema) as writer:
                        for batch in reader:
                            writer.write_batch(batch)
            finally:
                con.close()
            checks = {}
            for side, path in (("engine", actual), ("reference", reference)):
                require(exact_schema(path) == exact_schema(expected), f"{name}/{side}: conversion changed logical column types")
                comparison_scratch = scratch / f"{name}-{side}-compare"
                comparison_scratch.mkdir()
                checks[side] = compare_files(path, expected, scratch=comparison_scratch, policy="bag", rel_tol=0, abs_tol=0)
                require(checks[side]["ok"], f"{name}/{side}: conversion values differ: {checks[side]}")
            table["validation"] = {"ok": True, "checks": checks, "schema": str(parquet.schema_arrow), "rows": source["rows"]}
            table["files"] = inventory(destination)
            manifest["tables"].append(table)
            write_json(root / "provider.json", manifest)
        manifest["status"] = "validated"
    except Exception as error:
        manifest.update(status="failed", error=f"{type(error).__name__}: {error}")
        raise
    finally:
        write_json(root / "provider.json", manifest)
    return manifest
