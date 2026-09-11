"""Prepare real public development extracts without synthesizing benchmark data.

Run as `python -m benchmark.prepare_public` through claude-safe-build.sh.
Acquisition is bounded and source/selection/SQL provenance is explicit. This
module does not modify the active runner or its evidence contract.
"""
import argparse
import base64
import hashlib
import io
import json
import re
import shutil
import tarfile
import tempfile
import os
import urllib.request
from pathlib import Path

from .contract import ContractError, digest, require
from .prepare import DUCKDB_VERSION, write_json

SOURCES = Path(__file__).with_name("public_sources")
JOB_URL = "https://event.cwi.nl/da/job/imdb.tgz"
HITS_URL = "https://clickhouse-public-datasets.s3.amazonaws.com/hits_compatible/hits.parquet"
HITS_BYTES = 14779976446
HITS_ETAG = '"6b028bb94eecf0ff4e6cde62a0f8fa48-829"'
HITS_MIRROR_SOURCE = "https://github.com/ClickHouse/ClickHouse/issues/46703"
JOB_BYTES = 1263193115
JOB_SHA256 = "25f9d893c54f903366e0c263f88db0d429dbc2b159d4987ebc1e203242a7e988"
DIMENSIONS = {"name": "person_id", "char_name": "person_role_id", "company_name": "company_id", "keyword": "keyword_id"}
SMALL = {"comp_cast_type", "company_type", "info_type", "kind_type", "link_type", "role_type"}
PERSON = {"aka_name", "person_info"}
CLICK_ORDER = {8: 1, 9: 1, 10: 2, 11: 1, 12: 2, 13: 1, 14: 1, 15: 2, 16: 1, 17: 2,
               19: 3, 22: 2, 23: 3, 26: 0, 28: 1, 29: 1, 31: 2, 32: 2, 33: 2,
               34: 1, 35: 2, 36: 4, 37: 1, 38: 1, 39: 1, 40: 5, 41: 2, 42: 2, 43: 0}


def schema_from_sql(sql):
    import pyarrow as pa
    types = {"integer": pa.int32(), "bigint": pa.int64(), "smallint": pa.int16(),
             "text": pa.string(), "character": pa.string(), "char": pa.string(), "varchar": pa.string(),
             "date": pa.date32(), "timestamp": pa.timestamp("us")}
    schemas = {}
    for name, body in re.findall(r"CREATE TABLE\s+(\w+)\s*\((.*?)\);", sql, re.I | re.S):
        fields = []
        for line in body.splitlines():
            match = re.match(r"\s*(\w+)\s+(\w+)", line)
            if not match:
                continue
            column, kind = match.groups()
            require(kind.lower() in types, f"unsupported source DDL type: {kind}")
            fields.append(pa.field(column, types[kind.lower()], nullable="NOT NULL" not in line.upper()))
        schemas[name] = pa.schema(fields)
    require(schemas, "pinned schema has no tables")
    return schemas


def verify_sources(workload):
    manifest = json.loads((SOURCES / "manifest.json").read_text())[workload]
    for name, entry in manifest["files"].items():
        require(digest(SOURCES / workload / name) == entry["sha256"], f"pinned source changed: {name}")
    return manifest


class RangeFile(io.RawIOBase):
    """Seekable HTTP reader: rejects non-range responses and changed objects."""
    def __init__(self, url, byte_budget):
        self.url, self.byte_budget, self.position, self.fetched = url, byte_budget, 0, 0
        self.reads = []
        with urllib.request.urlopen(urllib.request.Request(url, method="HEAD"), timeout=30) as response:
            self.length = int(response.headers["Content-Length"])
            self.etag = response.headers.get("ETag")
        require(self.etag and not self.etag.startswith("W/"), "remote extract requires a strong ETag")
    def readable(self): return True
    def seekable(self): return True
    def tell(self): return self.position
    def seek(self, offset, whence=0):
        position = offset if whence == 0 else self.position + offset if whence == 1 else self.length + offset
        require(0 <= position <= self.length, "range seek out of bounds")
        self.position = position
        return position
    def read(self, size=-1):
        size = self.length - self.position if size < 0 else min(size, self.length - self.position)
        if size == 0: return b""
        require(self.fetched + size <= self.byte_budget, "HTTP range acquisition exceeds --max-download-bytes")
        start, end = self.position, self.position + size - 1
        request = urllib.request.Request(self.url, headers={"Range": f"bytes={start}-{end}", "If-Match": self.etag})
        with urllib.request.urlopen(request, timeout=60) as response:
            require(response.status == 206 and response.headers.get("Content-Range") == f"bytes {start}-{end}/{self.length}", "server did not honor exact byte range")
            require(response.headers.get("ETag") == self.etag, "remote Parquet changed during extraction")
            data = response.read(size + 1)
        require(len(data) == size, "short or oversized range response")
        self.fetched += size
        self.position += size
        self.reads.append({"offset": start, "length": size, "sha256": hashlib.sha256(data).hexdigest()})
        return data


def acquire_job(args, root):
    if args.source_file:
        path = Path(args.source_file).resolve()
        require(args.source_sha256, "local original JOB archive requires --source-sha256")
        require(args.source_sha256 == JOB_SHA256 and digest(path) == JOB_SHA256, "local source differs from pinned original JOB archive")
        return path, {"url": JOB_URL, "local_path": str(path), "sha256": args.source_sha256, "origin": "user-supplied original archive"}
    require(args.max_download_bytes >= JOB_BYTES, f"original JOB archive needs {JOB_BYTES} download bytes")
    destination = root / "imdb.tgz"
    sha, size = hashlib.sha256(), 0
    with urllib.request.urlopen(JOB_URL, timeout=60) as response:
        length = int(response.headers.get("Content-Length", 0))
        require(length == JOB_BYTES, "JOB upstream object size changed; review source before acquisition")
        headers = {key: response.headers.get(key) for key in ("ETag", "Last-Modified", "Content-Length")}
        with destination.open("xb") as sink:
            while block := response.read(1024 * 1024):
                size += len(block)
                require(size <= args.max_download_bytes, "JOB download byte limit exceeded")
                sink.write(block); sha.update(block)
    require(size == length and sha.hexdigest() == JOB_SHA256, "JOB archive differs from pinned source hash")
    return destination, {"url": JOB_URL, "sha256": sha.hexdigest(), "bytes": size, "headers": headers}


def csv_batches(archive, member, schema):
    # JOB escapes quotes inside quoted fields, but backslashes in unquoted
    # names are literal. Arrow's global escape_char changes those values.
    # DuckDB's strict CSV parser implements the source dialect directly.
    import duckdb
    import pyarrow as pa
    require(duckdb.__version__ == DUCKDB_VERSION, "JOB parser version differs from pin")
    require(shutil.disk_usage(os.environ["TMPDIR"]).free > member.size + 1024**3,
            "insufficient scratch space for one JOB CSV member")
    with tempfile.TemporaryDirectory(prefix="job-csv-", dir=os.environ["TMPDIR"]) as temporary:
        path = Path(temporary) / "member.csv"
        with path.open("xb") as destination:
            shutil.copyfileobj(archive.extractfile(member), destination, 1024 * 1024)
        require(path.stat().st_size == member.size, "truncated archive member")
        types = {pa.int32(): "INTEGER", pa.string(): "VARCHAR"}
        columns = "{" + ",".join("'" + field.name + "':'" + types[field.type] + "'" for field in schema) + "}"
        con = duckdb.connect()
        try:
            con.execute("SET threads=1")
            con.execute("SET memory_limit='512MB'")
            query = "SELECT * FROM read_csv(?, columns=" + columns + ", header=false, delim=',', quote='\"', escape='\\', nullstr='', allow_quoted_nulls=false, strict_mode=true, parallel=false)"
            for batch in con.execute(query, [str(path)]).fetch_record_batch(65536):
                yield batch
        finally:
            con.close()


def add_rows(hasher, batch):
    for row in batch.to_pylist():
        normalized = [value.isoformat() if hasattr(value, "isoformat") else value for value in row.values()]
        hasher.update(json.dumps(normalized, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode() + b"\n")


def validate_table(root, name, schema, expected_hash, rows):
    import pyarrow.parquet as pq
    relative = f"parquet/{name}.parquet"
    path = root / relative
    parquet = pq.ParquetFile(path)
    require(parquet.schema_arrow.equals(schema), f"{name}: extraction changed schema")
    actual = hashlib.sha256()
    for batch in parquet.iter_batches(batch_size=65536):
        add_rows(actual, batch)
    require(parquet.metadata.num_rows == rows and actual.hexdigest() == expected_hash, f"{name}: extraction readback differs from selected source rows")
    return {"name": name, "path": relative, "sha256": digest(path), "bytes": path.stat().st_size,
            "rows": rows, "schema": str(schema), "schema_ipc_base64": base64.b64encode(schema.serialize()).decode(),
            "selected_source_row_sha256": expected_hash, "readback_verified": True}


def extract_job(args, root, manifest):
    import pyarrow as pa
    import pyarrow.parquet as pq
    source, provenance = acquire_job(args, root)
    manifest["data_source"] = provenance
    schemas = schema_from_sql((SOURCES / "job/schema.sql").read_text())
    require(len(schemas) == 21, "JOB schema table membership changed")
    selected = {name: set() for name in DIMENSIONS}
    hashes = {name: hashlib.sha256() for name in schemas}
    counts = {name: 0 for name in schemas}
    writers = {name: pq.ParquetWriter(root / "parquet" / f"{name}.parquet", schema, compression="zstd") for name, schema in schemas.items()}
    seen = set()
    try:
        for pass_number in (1, 2):
            with tarfile.open(source, "r|gz") as archive:
                for member in archive:
                    if not member.isfile() or not member.name.endswith(".csv"): continue
                    name = Path(member.name).stem
                    require(name in schemas, f"unexpected JOB CSV: {name}")
                    dimension = name in DIMENSIONS or name in PERSON
                    if dimension != (pass_number == 2): continue
                    require(name not in seen, f"duplicate archive table: {name}")
                    seen.add(name)
                    for batch in csv_batches(archive, member, schemas[name]):
                        if name in SMALL:
                            chosen = batch
                        else:
                            if name in DIMENSIONS:
                                values = batch.column(batch.schema.get_field_index("id")).to_pylist()
                                keep = [value in selected[name] for value in values]
                            elif name in PERSON:
                                values = batch.column(batch.schema.get_field_index("person_id")).to_pylist()
                                keep = [value in selected["name"] for value in values]
                            else:
                                key = "id" if name == "title" else "movie_id"
                                values = batch.column(batch.schema.get_field_index(key)).to_pylist()
                                keep = [value is not None and value % args.movie_modulus == 0 for value in values]
                                if name == "movie_link":
                                    linked = batch.column(batch.schema.get_field_index("linked_movie_id")).to_pylist()
                                    keep = [match and value is not None and value % args.movie_modulus == 0 for match, value in zip(keep, linked)]
                            chosen = batch.filter(pa.array(keep))
                        # Arrow CSV supplies nullable schema even for NOT NULL
                        # DDL; verify the actual values before restoring metadata.
                        for field, array in zip(schemas[name], chosen.columns):
                            require(field.nullable or array.null_count == 0, f"{name}.{field.name}: NULL in required source field")
                        chosen = pa.RecordBatch.from_arrays(chosen.columns, schema=schemas[name])
                        if pass_number == 1:
                            for dimension_name, field in DIMENSIONS.items():
                                if field in chosen.schema.names:
                                    selected[dimension_name].update(value for value in chosen.column(chosen.schema.get_field_index(field)).to_pylist() if value is not None)
                        add_rows(hashes[name], chosen)
                        counts[name] += chosen.num_rows
                        writers[name].write_batch(chosen, row_group_size=131072)
        require(seen == set(schemas), f"missing JOB tables: {set(schemas) - seen}")
    finally:
        for writer in writers.values(): writer.close()
    manifest["selection"] = {"method": "movie ID modulo", "modulus": args.movie_modulus, "remainder": 0,
        "dimensions": "retain IDs referenced by selected movie facts; retain enum dimensions entirely",
        "movie_link": "both endpoints must be selected", "passes": 2,
        "limitations": "episode-parent closure is not added; this is a development extract, not full JOB"}
    manifest["tables"] = [validate_table(root, name, schema, hashes[name].hexdigest(), counts[name]) for name, schema in schemas.items()]


def normalize_clickbench(batch, schema):
    import pyarrow as pa
    import pyarrow.compute as pc
    arrays = []
    for field, array in zip(schema, batch.columns):
        if pa.types.is_date32(field.type) and pa.types.is_integer(array.type):
            # Upstream unannotated ClickHouse Date stores epoch days.
            array = pc.cast(array, pa.int32(), safe=True)
        elif pa.types.is_timestamp(field.type) and pa.types.is_integer(array.type):
            # Upstream unannotated DateTime stores epoch seconds, not us.
            array = pc.cast(pc.cast(array, pa.int64(), safe=True), pa.timestamp("s"), safe=True)
        array = pc.cast(array, field.type, safe=True)
        require(field.nullable or array.null_count == 0, f"hits.{field.name}: NULL in required source field")
        arrays.append(array)
    return pa.RecordBatch.from_arrays(arrays, schema=schema)


def extract_clickbench(args, root, manifest):
    import pyarrow as pa
    import pyarrow.parquet as pq
    schema = schema_from_sql((SOURCES / "clickbench/duckdb/create.sql").read_text())["hits"]
    remote = None
    if args.source_file:
        source = Path(args.source_file).resolve()
        require(args.source_sha256 and digest(source) == args.source_sha256, "local canonical ClickBench Parquet requires matching --source-sha256")
        manifest["data_source"] = {"url": HITS_URL, "local_path": str(source), "sha256": args.source_sha256, "origin": "user-supplied canonical mirror"}
    else:
        remote = RangeFile(HITS_URL, args.max_download_bytes)
        require(remote.length == HITS_BYTES and remote.etag == HITS_ETAG, "ClickBench remote object changed; review new source before extraction")
        source = remote
        manifest["data_source"] = {"url": HITS_URL, "bytes": remote.length, "etag": remote.etag,
                                   "range_hashes": remote.reads, "mirror_source": HITS_MIRROR_SOURCE}
    parquet = pq.ParquetFile(source)
    manifest["source_arrow_schema"] = str(parquet.schema_arrow)
    require(parquet.schema_arrow.names == schema.names, "ClickBench source columns differ from pinned DDL")
    count, row_hash = 0, hashlib.sha256()
    with pq.ParquetWriter(root / "parquet/hits.parquet", schema, compression="zstd") as writer:
        for batch in parquet.iter_batches(batch_size=65536):
            chosen = batch.slice(0, min(batch.num_rows, args.rows - count))
            # Normalize physical Parquet representations using upstream's
            # DuckDB DDL, with checked casts and no decimal/float substitutions.
            chosen = normalize_clickbench(chosen, schema)
            add_rows(row_hash, chosen)
            writer.write_batch(chosen, row_group_size=131072)
            count += chosen.num_rows
            if count == args.rows: break
    require(count == args.rows, "ClickBench source has fewer rows than requested")
    if remote:
        manifest["data_source"] = {"url": HITS_URL, "bytes": remote.length, "etag": remote.etag,
                                   "fetched_bytes": remote.fetched, "range_hashes": remote.reads, "mirror_source": HITS_MIRROR_SOURCE}
    manifest["selection"] = {"method": "first physical rows", "rows": args.rows,
        "schema_adaptation": "checked normalization to pinned upstream duckdb/create.sql; physical Date=epoch days and DateTime=epoch seconds",
        "limitations": "prefix-biased development extract; not full ClickBench"}
    manifest["tables"] = [validate_table(root, "hits", schema, row_hash.hexdigest(), count)]


def sql_tokens(sql):
    """Lex SQL while preserving quoted values, identifiers and comments."""
    tokens, index = [], 0
    pattern = re.compile(r"(?i:E)'(?:[^'\\]|\\.|'')*'|'(?:[^']|'')*'|\"(?:[^\"]|\"\")*\"|[A-Za-z_][A-Za-z_0-9$]*|\s+|.", re.S)
    while index < len(sql):
        start = index
        if sql.startswith('--', index):
            end = sql.find('\n', index)
            index = len(sql) if end < 0 else end
            kind = 'skip'
        elif sql.startswith('/*', index):
            index += 2
            depth = 1
            while depth and index < len(sql):
                if sql.startswith('/*', index): depth += 1; index += 2
                elif sql.startswith('*/', index): depth -= 1; index += 2
                else: index += 1
            require(depth == 0, 'unterminated SQL block comment')
            kind = 'skip'
        elif match := re.match(r'\$(?:[A-Za-z_][A-Za-z_0-9]*)?\$', sql[index:]):
            delimiter = match[0]
            end = sql.find(delimiter, index + len(delimiter))
            require(end >= 0, 'unterminated dollar-quoted SQL string')
            index = end + len(delimiter)
            kind = 'protected'
        else:
            match = pattern.match(sql, index)
            value = match[0]
            index = match.end()
            require(value not in ("'", '"'), 'unterminated SQL quoted token')
            kind = 'skip' if value.isspace() else 'identifier' if re.fullmatch(r'[A-Za-z_][A-Za-z_0-9$]*', value) else 'protected' if value[0] in "'\"" or value[:2].lower() == "e'" else 'symbol'
        tokens.append([kind, sql[start:index]])
    return tokens


def quote_alias_identifiers(sql, names):
    """Quote selected AS aliases and qualified references; fail on other uses."""
    tokens = sql_tokens(sql)
    significant = [index for index, (kind, _) in enumerate(tokens) if kind != 'skip']
    names = {name.lower() for name in names}
    changed = 0
    for position, index in enumerate(significant):
        kind, value = tokens[index]
        if kind != 'identifier' or value.lower() not in names:
            continue
        previous = tokens[significant[position - 1]][1].upper() if position else None
        following = tokens[significant[position + 1]][1] if position + 1 < len(significant) else None
        require(previous == 'AS' or following == '.', f'unsupported dialect identifier context: {value}')
        tokens[index][1] = '"' + value.lower() + '"'
        changed += 1
    return ''.join(value for _, value in tokens), changed


def explicit_regexp_replace_options(sql):
    """Select DuckDB-compatible replacement semantics explicitly, by arity.

    Only unquoted builtin calls with exactly three arguments are changed.
    Balanced nested parentheses/brackets protect commas inside expressions.
    Comments and quoted content are never inspected as SQL syntax.
    """
    tokens = sql_tokens(sql)
    significant = [index for index, (kind, _) in enumerate(tokens) if kind != 'skip']
    stack, additions = [], []
    for position, index in enumerate(significant):
        kind, value = tokens[index]
        if kind != 'symbol':
            continue
        if value in ('(', '['):
            previous = tokens[significant[position - 1]] if position else None
            builtin = value == '(' and previous == ['identifier', 'REGEXP_REPLACE']
            if previous and previous[0] == 'identifier':
                builtin = value == '(' and previous[1].lower() == 'regexp_replace'
            # Qualified names can designate user functions with other semantics.
            if builtin and position > 1 and tokens[significant[position - 2]][1] == '.':
                builtin = False
            stack.append({'opening':value, 'builtin':builtin, 'commas':0})
        elif value in (')', ']'):
            require(stack and stack[-1]['opening'] == ('(' if value == ')' else '['), 'unbalanced SQL expression delimiters')
            frame = stack.pop()
            if frame['builtin'] and frame['commas'] == 2:
                additions.append(index)
        elif value == ',' and stack:
            stack[-1]['commas'] += 1
    require(not stack, 'unbalanced SQL expression delimiters')
    for index in additions:
        tokens[index][1] = ", ''" + tokens[index][1]
    return ''.join(value for _, value in tokens), len(additions)


def prepare_queries(root, workload, schema=None):
    if workload == "job":
        queries = [(path.stem, path.read_text()) for path in sorted((SOURCES / "job").glob("*.sql")) if re.fullmatch(r"\d+[a-z]", path.stem)]
        require(len(queries) == 113, "JOB query membership changed")
    else:
        lines = (SOURCES / "clickbench/duckdb/queries.sql").read_text().splitlines()
        queries = [(f"q{index:02}", sql) for index, sql in enumerate(lines, 1) if sql.strip()]
        require(len(queries) == 43, "ClickBench query membership changed")
    result = []
    for identifier, sql in queries:
        original = sql
        metadata = {"id": identifier, "result_policy": "bag", "order_by": [], "limit_rows": None, "adaptations": []}
        oracle = sql
        if workload == "job":
            require(not re.search(r"\b(?:ORDER BY|LIMIT|OFFSET)\b", sql, re.I), "JOB result ordering contract changed")
            sql, quoted = quote_alias_identifiers(sql, {'at'})
            oracle = sql
            if quoted:
                metadata['adaptations'].append({'reason': 'PostgreSQL unquoted alias at conflicts with DuckDB 1.4.4 reserved AT; quote identifier declarations and qualified references identically for both engines and oracle',
                    'adapter': 'quote_alias_identifiers_v1', 'identifiers': ['at'], 'quoted_occurrences': quoted})
        else:
            sql, replacements = explicit_regexp_replace_options(sql)
            oracle = sql
            if replacements:
                metadata['adaptations'].append({'reason': 'Explicit empty options preserve DuckDB first-match and backslash-capture replacement semantics; applied identically to both engines and oracle',
                    'adapter': 'explicit_regexp_replace_options_v1', 'calls': replacements,
                    'reference': 'https://duckdb.org/docs/current/sql/functions/regular_expressions'})
            number = int(identifier[1:])
            limit = re.search(r"\bLIMIT\s+(\d+)(?:\s+OFFSET\s+(\d+))?\s*;?\s*$", sql, re.I)
            if limit:
                metadata["limit_rows"] = int(limit[1])
                oracle = sql[:limit.start()].rstrip() + ";\n"
            if number == 24:
                ordinal = schema.get_field_index("EventTime")
            else:
                ordinal = CLICK_ORDER.get(number)
            if ordinal is not None:
                metadata["order_by"] = [{"column": ordinal, "direction": "asc" if number in (24,26,43) else "desc", "nulls": "last"}]
            metadata["offset_rows"] = int(limit[2]) if limit and limit[2] else 0
            if number in (25, 27):
                # Timed SQL remains byte-identical to upstream. Only the
                # complete untimed oracle exposes the hidden ordering value.
                projection = "SELECT SearchPhrase FROM hits"
                require(oracle.count(projection) == 1, "hidden-order oracle SQL shape changed")
                oracle = oracle.replace(projection, "SELECT SearchPhrase, EventTime AS __benchmark_hidden_order_0 FROM hits", 1)
                metadata["oracle_output_columns"] = [0]
                metadata["order_by"] = [{"column":1,"direction":"asc","nulls":"last"}]
                if number == 27:
                    metadata["order_by"].append({"column":0,"direction":"asc","nulls":"last"})
                metadata["oracle_adaptations"] = [{"reason":"Expose existing ORDER BY key for untimed eligibility validation; timed projection unchanged", "added_column":"EventTime AS __benchmark_hidden_order_0"}]
            require(bool(re.search(r"\bORDER BY\b", sql, re.I)) == bool(metadata["order_by"]), f"ClickBench {identifier} ordering mapping incomplete")
            metadata["result_policy"] = ("limit_ties" if metadata["order_by"] else "unordered_limit") if limit else "ordered" if metadata["order_by"] else "bag"
        for directory, content, field in (("sql", sql, "sql"), ("original-sql", original, "original_sql"), ("oracle-sql", oracle, "oracle_sql")):
            relative = f"{directory}/{identifier}.sql"
            (root / relative).write_text(content)
            metadata[field + "_path"] = relative
            metadata[field + "_sha256"] = digest(root / relative)
        result.append(metadata)
    return result


def refresh_public_manifest(args):
    """Publish new SQL/oracle metadata over verified immutable extracted data."""
    source_path = Path(args.reuse_data_manifest).resolve()
    manifest = json.loads(source_path.read_text())
    require(manifest.get("status") in ("prepared", "prepared_incomplete"), "source extraction did not complete")
    require(manifest.get("id") == f"{args.workload}_dev", "source workload differs")
    require(manifest.get("duckdb_version") == DUCKDB_VERSION, "source parser version differs")
    source_pin = verify_sources(args.workload)
    require(manifest.get("query_source") == source_pin, "source SQL revision differs")
    for table in manifest["tables"]:
        relative = Path(table["path"])
        require(not relative.is_absolute() and ".." not in relative.parts and relative.parts[0] == "parquet", "invalid extracted table path")
        require(table.get("readback_verified") is True and digest(source_path.parent / relative) == table["sha256"], "extracted table changed or was not validated")
    root = Path(args.output).resolve()
    root.mkdir(parents=True, exist_ok=False)
    for directory in ("parquet", "sql", "original-sql", "oracle-sql", "source-provenance"):
        (root / directory).mkdir()
    manifest.update(status="preparing", queries=[], regenerated_from={"path": str(source_path), "sha256": digest(source_path)}, result_contract_revision="sorted_group_slice_v1")
    write_json(root / "dataset.json", manifest)
    try:
        for table in manifest["tables"]:
            os.link(source_path.parent / table["path"], root / table["path"])
        for name in source_pin["files"]:
            if name in ("README.md", "LICENSE", "schema.sql", "duckdb/create.sql"):
                shutil.copyfile(SOURCES / args.workload / name, root / "source-provenance" / name.replace("/", "-"))
        schema = schema_from_sql((SOURCES / "clickbench/duckdb/create.sql").read_text())["hits"] if args.workload == "clickbench" else None
        manifest["queries"] = prepare_queries(root, args.workload, schema)
        manifest["blockers"] = []
        manifest["status"] = "prepared"
    except Exception as error:
        manifest.update(status="failed", error=f"{type(error).__name__}: {error}")
        raise
    finally:
        write_json(root / "dataset.json", manifest)
    return manifest


def prepare(args):
    from .run import containment
    import pyarrow as pa
    containment()
    if getattr(args, "reuse_data_manifest", None):
        return refresh_public_manifest(args)
    require(args.rows > 0 and args.movie_modulus > 0 and args.max_download_bytes > 0, "positive extraction limits required")
    root = Path(args.output).resolve()
    root.mkdir(parents=True, exist_ok=False)
    for directory in ("parquet", "sql", "original-sql", "oracle-sql", "source-provenance"):
        (root / directory).mkdir()
    provenance = verify_sources(args.workload)
    manifest = {"version": 1, "id": f"{args.workload}_dev", "status": "preparing", "duckdb_version": DUCKDB_VERSION,
        "tables": [], "queries": [], "tracks_prepared": ["raw_parquet"], "query_source": provenance,
        "pyarrow_version": pa.__version__, "claim": f"Deterministic real-data {args.workload} development extract; not full benchmark",
        "conversion_validation": "pending complete typed selected-source row readback",
        "license_provenance": {"sql": "ClickBench LICENSE" if args.workload == "clickbench" else "No license file at pinned JOB revision; README preserved as provenance",
            "data": "upstream source terms; not relicensed by this repository",
            "data_source_url": HITS_URL if args.workload == "clickbench" else JOB_URL,
            "job_data_terms_url": "https://www.imdb.com/interfaces" if args.workload == "job" else None}}
    write_json(root / "dataset.json", manifest)
    try:
        for name in provenance["files"]:
            if name in ("README.md", "LICENSE", "schema.sql", "duckdb/create.sql"):
                shutil.copyfile(SOURCES / args.workload / name, root / "source-provenance" / name.replace("/", "-"))
        schema = schema_from_sql((SOURCES / "clickbench/duckdb/create.sql").read_text())["hits"] if args.workload == "clickbench" else None
        manifest["queries"] = prepare_queries(root, args.workload, schema)
        write_json(root / "dataset.json", manifest)
        if args.workload == "job":
            extract_job(args, root, manifest)
        else:
            extract_clickbench(args, root, manifest)
        manifest["conversion_validation"] = "Every selected typed source row and exact schema matched Parquet readback"
        blockers = [{"query": query["id"], "reason": query["blocker"]} for query in manifest["queries"] if "blocker" in query]
        manifest["blockers"] = blockers
        manifest["status"] = "prepared_incomplete" if blockers else "prepared"
    except Exception as error:
        manifest.update(status="failed", error=f"{type(error).__name__}: {error}")
        raise
    finally:
        write_json(root / "dataset.json", manifest)
    return manifest


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workload", choices=["job", "clickbench"], required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--reuse-data-manifest", help="refresh SQL/oracle metadata over a validated extract in a new output directory; data files are hard-linked")
    parser.add_argument("--source-file", help="local original imdb.tgz or canonical hits.parquet; requires its SHA256")
    parser.add_argument("--source-sha256")
    parser.add_argument("--rows", type=int, default=1000000, help="ClickBench prefix row count")
    parser.add_argument("--movie-modulus", type=int, default=1000, help="JOB selects movie IDs divisible by this")
    parser.add_argument("--max-download-bytes", type=int, default=2 * 1024**3)
    args = parser.parse_args(argv)
    try:
        result = prepare(args)
        print(json.dumps({"status": result["status"], "dataset": str(Path(args.output) / "dataset.json"), "blockers": result["blockers"]}))
        return 0 if result["status"] == "prepared" else 1
    except Exception as error:
        print(json.dumps({"status": "failed", "error": f"{type(error).__name__}: {error}"}))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
