"""Canonical TPC-H development input from the pinned DuckDB tpch extension.

This is a reproducible engineering workload, not an audited TPC-H submission.
Provider conversion and the other public workloads are separate work packages.
"""
import base64
import hashlib
import json
import re
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from .contract import ContractError, digest, require

DUCKDB_VERSION = "1.4.4"
TABLES = ("region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem")
# Output ordinals, checked against tpch_queries() from the pinned extension.
ORDER = {
    1: [(0, "asc"), (1, "asc")],
    2: [(0, "desc"), (2, "asc"), (1, "asc"), (3, "asc")],
    3: [(1, "desc"), (2, "asc")], 4: [(0, "asc")], 5: [(1, "desc")],
    7: [(0, "asc"), (1, "asc"), (2, "asc")], 8: [(0, "asc")],
    9: [(0, "asc"), (1, "desc")], 10: [(2, "desc")], 11: [(1, "desc")],
    12: [(0, "asc")], 13: [(1, "desc"), (0, "desc")], 15: [(0, "asc")],
    16: [(3, "desc"), (0, "asc"), (1, "asc"), (2, "asc")],
    18: [(4, "desc"), (3, "asc")], 20: [(0, "asc")],
    21: [(1, "desc"), (0, "asc")], 22: [(0, "asc")],
}
LIMITS = {2: 100, 3: 10, 10: 20, 18: 100, 21: 100}


def literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def write_json(path, value):
    path = Path(path)
    temporary = path.with_suffix(path.suffix + ".pending")
    temporary.write_text(json.dumps(value, indent=2, allow_nan=False) + "\n")
    temporary.replace(path)


def query_metadata(number, original, scale):
    require(number in range(1, 23), "unexpected TPC-H query")
    sql = original
    adaptations = []
    if number == 11:
        require(sql.count("0.0001000000") == 1, "Q11 source changed; review scale adaptation")
        constant = format(Decimal("0.0001") / Decimal(str(scale)), "f")
        sql = sql.replace("0.0001000000", constant)
        adaptations.append({"reason": "TPC-H Q11 threshold scales as 0.0001 / SF",
                            "from": "0.0001000000", "to": constant})
    limit = re.search(r"\bLIMIT\s+(\d+)\s*;?\s*$", sql, re.I)
    require((int(limit[1]) if limit else None) == LIMITS.get(number),
            f"Q{number} LIMIT changed; review result contract")
    oracle = sql[:limit.start()].rstrip() + ";\n" if limit else sql
    order = [{"column": c, "direction": d, "nulls": "last"} for c, d in ORDER.get(number, [])]
    require(bool(re.search(r"\bORDER\s+BY\b", sql, re.I)) == bool(order),
            f"Q{number} ordering changed; review result contract")
    return sql, oracle, {"id": f"q{number:02}",
                         "result_policy": "limit_ties" if limit else "ordered" if order else "bag",
                         "order_by": order, "limit_rows": LIMITS.get(number),
                         "adaptations": adaptations}


def prepare_tpch(output, scale):
    import duckdb
    import pyarrow.parquet as pq

    require(scale in (1, 10, 100), "canonical profiles require SF1, SF10 or SF100")
    require(duckdb.__version__ == DUCKDB_VERSION,
            f"requires DuckDB {DUCKDB_VERSION}; revalidate SQL metadata before upgrading")
    root = Path(output).resolve()
    root.mkdir(parents=True, exist_ok=False)
    for directory in ("parquet", "sql", "original-sql", "oracle-sql", "duckdb-temp"):
        (root / directory).mkdir()
    manifest = {"version": 1, "id": f"tpch_sf{scale}", "status": "preparing",
                "created_at": datetime.now(timezone.utc).isoformat(), "scale_factor": scale,
                "duckdb_version": duckdb.__version__,
                "source": "https://github.com/duckdb/duckdb/tree/v1.4.4/extension/tpch",
                "license_source": "https://github.com/duckdb/duckdb/blob/v1.4.4/extension/tpch/dbgen/LICENSE",
                "claim": "TPC-H engineering workload; not an audited TPC-H result",
                "tables": [], "queries": [], "tracks_prepared": ["raw_parquet"],
                "conversion_validation": "not performed; only original Parquet export prepared",
                "generator": {"threads": 4, "memory_limit": "2GB", "row_group_size": 131072,
                              "compression": "zstd", "command": f"CALL dbgen(sf={scale})"}}
    write_json(root / "dataset.json", manifest)
    con = None
    try:
        con = duckdb.connect(str(root / "source.duckdb"))
        con.execute("SET autoinstall_known_extensions=false")
        con.execute("SET threads=4")
        con.execute("SET memory_limit='2GB'")
        con.execute("SET temp_directory=" + literal(root / "duckdb-temp"))
        con.execute("LOAD tpch")
        extension = con.execute("SELECT extension_version, install_path FROM duckdb_extensions() "
                                "WHERE extension_name='tpch'").fetchone()
        require(extension and extension[0] == "v" + DUCKDB_VERSION, "unexpected tpch extension")
        manifest["extension"] = {"version": extension[0], "sha256": digest(extension[1])}
        queries = con.execute("SELECT query_nr, query FROM tpch_queries() ORDER BY query_nr").fetchall()
        require([q[0] for q in queries] == list(range(1, 23)), "incomplete TPC-H SQL")
        pinned_sql = json.loads(Path(__file__).with_name("tpch_sql_sha256.json").read_text())
        for number, original in queries:
            require(hashlib.sha256(original.encode()).hexdigest() == pinned_sql[f"q{number:02}"],
                    f"Q{number} source hash changed; review SQL and output ordering before accepting")
            sql, oracle, metadata = query_metadata(number, original, scale)
            for directory, text, field in (("sql", sql, "sql"), ("original-sql", original, "original_sql"),
                                            ("oracle-sql", oracle, "oracle_sql")):
                relative = f"{directory}/q{number:02}.sql"
                (root / relative).write_text(text)
                metadata[field + "_path"] = relative
                metadata[field + "_sha256"] = digest(root / relative)
            manifest["queries"].append(metadata)
        license_path = Path(__file__).with_name("licenses") / "duckdb-tpch-v1.4.4-LICENSE"
        (root / "generator-LICENSE").write_bytes(license_path.read_bytes())
        manifest["license_sha256"] = digest(root / "generator-LICENSE")
        write_json(root / "dataset.json", manifest)
        con.execute(f"CALL dbgen(sf={scale})")
        for table in TABLES:
            relative = f"parquet/{table}.parquet"
            path = root / relative
            con.execute(f"COPY {table} TO {literal(path)} "
                        "(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 131072)")
            parquet = pq.ParquetFile(path)
            manifest["tables"].append({"name": table, "path": relative, "sha256": digest(path),
                                       "bytes": path.stat().st_size, "rows": parquet.metadata.num_rows,
                                       "schema": str(parquet.schema_arrow),
                                       "schema_ipc_base64": base64.b64encode(
                                           parquet.schema_arrow.serialize()).decode()})
            write_json(root / "dataset.json", manifest)
        con.execute("CHECKPOINT")
        manifest["status"] = "prepared"
    except Exception as error:
        manifest.update(status="failed", error=f"{type(error).__name__}: {error}")
        raise
    finally:
        if con is not None:
            con.close()
        write_json(root / "dataset.json", manifest)
    return manifest
