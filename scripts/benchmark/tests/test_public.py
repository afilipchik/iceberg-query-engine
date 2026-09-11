"""Small synthetic parser fixtures only; never advertised as public benchmark data."""
import hashlib
import io
import os
import tarfile
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
from benchmark.contract import ContractError
from benchmark.prepare_public import (SOURCES, RangeFile, add_rows, csv_batches,
                                      prepare_queries, schema_from_sql, verify_sources, normalize_clickbench,
                                      quote_alias_identifiers, explicit_regexp_replace_options)


class Response(io.BytesIO):
    def __init__(self, data=b"", headers=None, status=200):
        super().__init__(data)
        self.headers, self.status = headers or {}, status


class PublicTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(dir=os.environ["TMPDIR"])
        self.root = Path(self.directory.name)
        for name in ("sql", "original-sql", "oracle-sql"):
            (self.root / name).mkdir()

    def tearDown(self): self.directory.cleanup()

    def test_upstream_source_pins_and_job_membership(self):
        self.assertEqual(len(verify_sources("job")["files"]), 115)
        self.assertEqual(len(schema_from_sql((SOURCES / "job/schema.sql").read_text())), 21)
        queries = prepare_queries(self.root, "job")
        self.assertEqual(len(queries), 113)
        self.assertTrue(all(q["result_policy"] == "bag" and "blocker" not in q for q in queries))
        self.assertEqual([q['id'] for q in queries if q['adaptations']], ['15a', '15b', '15c', '15d'])
        for query in queries:
            self.assertEqual((self.root / query['original_sql_path']).read_bytes(), (SOURCES / 'job' / (query['id'] + '.sql')).read_bytes())
            self.assertEqual(query['sql_sha256'], query['oracle_sql_sha256'])

    def test_alias_quoting_preserves_literals_comments_and_token_boundaries(self):
        source = "SELECT AT.x, atlas.x, 'at.x ''AS at''', E'at\\\' AS at', $$at.x$$, $tag$AS at$tag$, \"at\".x /* AS at /* at.x */ */ FROM t AS /* at */ AT -- at.x\n"
        translated, count = quote_alias_identifiers(source, {'at'})
        self.assertEqual(count, 2)
        self.assertEqual(translated, source.replace('SELECT AT.x', 'SELECT "at".x').replace('*/ AT --', '*/ "at" --'))
        self.assertEqual(quote_alias_identifiers(translated, {'at'}), (translated, 0))
        for invalid in ('SELECT at FROM t', 'SELECT 1 /* unfinished', "SELECT 'unfinished", 'SELECT $$unfinished'):
            with self.assertRaises(ContractError):
                quote_alias_identifiers(invalid, {'at'})

    def test_alias_translation_matches_independent_reference_and_parses_all_job(self):
        import duckdb
        con = duckdb.connect()
        try:
            con.execute('SET threads=1')
            con.execute("SET memory_limit='256MB'")
            original = 'SELECT at.x, count(*) FROM (VALUES (1),(1),(NULL),(2)) AS at(x) GROUP BY at.x ORDER BY at.x'
            translated, _ = quote_alias_identifiers(original, {'at'})
            reference = original.replace('at.', 'safe_alias.').replace('AS at(', 'AS safe_alias(')
            self.assertEqual(con.execute(translated).fetchall(), con.execute(reference).fetchall())
            for query in prepare_queries(self.root, 'job'):
                self.assertEqual(len(con.extract_statements((self.root / query['sql_path']).read_text())), 1)
        finally:
            con.close()

    def test_clickbench_retains_all_timed_queries_with_complete_oracles(self):
        verify_sources("clickbench")
        schema = schema_from_sql((SOURCES / "clickbench/duckdb/create.sql").read_text())["hits"]
        queries = prepare_queries(self.root, "clickbench", schema)
        self.assertEqual(len(queries), 43)
        self.assertEqual([q["id"] for q in queries if "blocker" in q], [])
        original = (SOURCES / "clickbench/duckdb/queries.sql").read_text().splitlines()
        for query, upstream_sql in zip(queries, original):
            self.assertEqual((self.root / query["original_sql_path"]).read_text(), upstream_sql)
            self.assertEqual((self.root / query["sql_path"]).read_text(), explicit_regexp_replace_options(upstream_sql)[0])
        self.assertEqual(queries[17]["result_policy"], "unordered_limit")
        self.assertEqual(queries[24]["oracle_output_columns"], [0])
        self.assertIn("EventTime AS __benchmark_hidden_order_0", (self.root / queries[24]["oracle_sql_path"]).read_text())
        self.assertEqual(queries[26]["order_by"], [{"column":1,"direction":"asc","nulls":"last"},{"column":0,"direction":"asc","nulls":"last"}])
        self.assertEqual([q["offset_rows"] for q in queries[38:]], [1000,1000,100,10000,1000])
        self.assertEqual(queries[23]["order_by"][0]["column"], schema.get_field_index("EventTime"))
        self.assertEqual(queries[25]["order_by"][0]["direction"], "asc")

    def test_regex_options_adapter_handles_nested_calls_and_protected_tokens(self):
        original = "SELECT regexp_replace(regexp_replace(concat('a,b', 'x'), '(a)', '\\1'), 'x', 'y'), REGEXP_REPLACE('aa','a','b','g'), 'regexp_replace(a,b,c)', $$regexp_replace(a,b,c)$$ /* regexp_replace(a,b,c) */ -- regexp_replace(a,b,c)\n"
        expected = original.replace("'(a)', '\\1')", "'(a)', '\\1', '')").replace("'x', 'y')", "'x', 'y', '')")
        self.assertEqual(explicit_regexp_replace_options(original), (expected, 2))
        self.assertEqual(explicit_regexp_replace_options(expected), (expected, 0))
        for original in ('SELECT custom.regexp_replace(a,b,c)', 'SELECT "regexp_replace"(a,b,c)', 'SELECT regexp_replace(a,b)'):
            self.assertEqual(explicit_regexp_replace_options(original), (original, 0))
        self.assertEqual(explicit_regexp_replace_options("SELECT regexp_replace(CAST(['a','b'] AS VARCHAR), 'a', 'x')")[1], 1)
        with self.assertRaises(ContractError):
            explicit_regexp_replace_options('SELECT regexp_replace(a,b,c')

    def test_regex_options_equivalent_to_pinned_duckdb_default(self):
        import duckdb
        from benchmark.prepare import DUCKDB_VERSION
        self.assertEqual(duckdb.__version__, DUCKDB_VERSION)
        connection = duckdb.connect()
        try:
            connection.execute('SET threads=1')
            connection.execute("SET memory_limit='256MB'")
            sql = r"SELECT regexp_replace(v, '(a)', '\1X'), regexp_replace(v, 'a', '$1'), regexp_replace(v, 'a', 'z', 'g') FROM (VALUES ('aa'), ('baab'), (''), (NULL)) AS t(v)"
            translated, count = explicit_regexp_replace_options(sql)
            self.assertEqual(count, 2)
            self.assertEqual(connection.execute(sql).fetchall(), connection.execute(translated).fetchall())
            self.assertEqual(connection.execute(translated).fetchall()[0], ('aXa', '$1a', 'zz'))
            for line in (SOURCES / 'clickbench/duckdb/queries.sql').read_text().splitlines():
                self.assertEqual(len(connection.extract_statements(explicit_regexp_replace_options(line)[0])), 1)
        finally:
            connection.close()

    def test_csv_preserves_null_versus_quoted_empty_string(self):
        data = b'1,\n2,""\n3,"hello"\n4,ends\\\n5,"quoted\\\"value"\n'
        archive_data = io.BytesIO()
        with tarfile.open(fileobj=archive_data, mode="w") as archive:
            member = tarfile.TarInfo("fixture.csv"); member.size = len(data)
            archive.addfile(member, io.BytesIO(data))
        archive_data.seek(0)
        schema = pa.schema([("id", pa.int32()), ("v", pa.string())])
        with tarfile.open(fileobj=archive_data, mode="r") as archive:
            rows = [row for batch in csv_batches(archive, archive.getmember("fixture.csv"), schema) for row in batch.to_pylist()]
        self.assertEqual(rows, [{"id":1,"v":None},{"id":2,"v":""},{"id":3,"v":"hello"},{"id":4,"v":"ends\\"},{"id":5,"v":'quoted"value'}])

    def test_row_fingerprint_distinguishes_null_and_empty(self):
        hashes = []
        for value in (None, ""):
            hasher = hashlib.sha256()
            add_rows(hasher, pa.record_batch([pa.array([value], type=pa.string())], names=["v"]))
            hashes.append(hasher.hexdigest())
        self.assertNotEqual(*hashes)

    def test_http_range_requires_exact_response_and_stable_etag(self):
        head = Response(headers={"Content-Length":"4", "ETag":'"pinned"'})
        data = Response(b"ab", {"Content-Range":"bytes 0-1/4", "ETag":'"pinned"'}, 206)
        with patch("urllib.request.urlopen", side_effect=[head, data]):
            source = RangeFile("https://example.test/data", 2)
            self.assertEqual(source.read(2), b"ab")
            with self.assertRaisesRegex(ContractError, "acquisition exceeds"):
                source.read(1)
        for status, etag in [(200,'"pinned"'),(206,'"changed"')]:
            with patch("urllib.request.urlopen", side_effect=[Response(headers={"Content-Length":"4","ETag":'"pinned"'}), Response(b"ab",{"Content-Range":"bytes 0-1/4","ETag":etag},status)]):
                with self.assertRaises(ContractError):
                    RangeFile("https://example.test/data", 10).read(2)

    def test_clickbench_physical_dates_use_correct_epoch_units(self):
        from datetime import date, datetime
        schema = pa.schema([("d", pa.date32()), ("t", pa.timestamp("us")), ("s", pa.string())])
        batch = pa.record_batch([pa.array([1], type=pa.uint16()), pa.array([1], type=pa.uint32()), pa.array([b"ok"])], names=["d","t","s"])
        result = normalize_clickbench(batch, schema)
        self.assertEqual(result.to_pylist(), [{"d": date(1970,1,2), "t": datetime(1970,1,1,0,0,1), "s":"ok"}])
