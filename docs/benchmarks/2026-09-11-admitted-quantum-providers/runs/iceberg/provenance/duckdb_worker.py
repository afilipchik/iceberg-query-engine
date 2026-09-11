"""Private persistent worker; launched only by the contained benchmark runner."""
import json
import resource
import sys
import time
from pathlib import Path

from .prepare import DUCKDB_VERSION, literal
from .providers import configure_reference


def emit(value):
    print(json.dumps(value, allow_nan=False), flush=True)


def main():
    setup = json.loads(Path(sys.argv[1]).read_text())
    cap = setup["process_cap_bytes"]
    resource.setrlimit(resource.RLIMIT_DATA, (cap, cap))
    import duckdb
    import pyarrow as pa

    if duckdb.__version__ != DUCKDB_VERSION:
        raise ValueError("DuckDB version changed; revalidate pinned workload")
    started = time.perf_counter()
    con = duckdb.connect()
    con.execute("SET threads=" + str(setup["threads"]))
    con.execute("SET memory_limit=" + literal(setup["memory_limit"]))
    con.execute("SET temp_directory=" + literal(setup["temp_directory"]))
    con.execute("SET default_null_order='NULLS_LAST'")
    con.execute("SET autoinstall_known_extensions=false")
    reference = configure_reference(con, setup)
    emit({"status": "ready", "setup_ms": (time.perf_counter() - started) * 1000,
          "version": duckdb.__version__, "threads": setup["threads"], "track": setup["track"],
          "reference": reference})
    for line in sys.stdin:
        request = json.loads(line)
        emit({"id": request["id"], "event": "started"})
        started = time.perf_counter()
        try:
            result = con.execute(request["sql"]).fetch_arrow_table()
            elapsed = (time.perf_counter() - started) * 1000
            emit({"id": request["id"], "event": "query_finished", "ms": elapsed})
            serialization = time.perf_counter()
            with pa.OSFile(request["output"], "wb") as sink:
                with pa.ipc.new_stream(sink, result.schema) as writer:
                    writer.write_table(result, max_chunksize=65536)
            emit({"id": request["id"], "status": "completed", "ms": elapsed,
                  "rows": result.num_rows, "output": request["output"],
                  "serialization_ms": (time.perf_counter() - serialization) * 1000})
            del result
        except duckdb.Error as error:
            emit({"id": request["id"], "status": "refused" if isinstance(error, duckdb.OutOfMemoryException)
                  else "query_error", "ms": (time.perf_counter() - started) * 1000,
                  "error_kind": type(error).__name__, "error": str(error)})
    con.close()


if __name__ == "__main__":
    main()
