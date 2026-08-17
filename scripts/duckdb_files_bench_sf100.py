"""DuckDB 1.4.4 over the engine's OWN files at SF=100: parquet via
read_parquet views, lance via the community `lance` extension (INSTALL lance
FROM community). Two passes each, best per query — the like-for-like
opponents for benchmark-parquet / benchmark-lance.

Measured 2026-08-17 (same machine as the engine numbers, warm):
  duckdb-parquet 39.4s | duckdb-lance 75.7s | duckdb-native(in-mem) 65.8s
The native number is dominated by Q9 (36.4s native vs 9.2s over views); the
stored data/sf100_duckdb_results baseline (67.1s) matches native, so engine
"vs native" ratios remain valid — but the honest like-for-like on identical
parquet is the 39.4s figure.
"""
import sys, time
sys.path.insert(0, 'scripts')
import duckdb
from duckdb_rebaseline import tpch_queries

QS = tpch_queries(100.0)
TABLES = ["nation","region","part","supplier","partsupp","customer","orders","lineitem"]

def bench(make_con, label):
    con = make_con()
    best = {}
    for it in range(2):
        for q in range(1, 23):
            t0 = time.perf_counter()
            con.execute(QS[q]).fetchall()
            ms = (time.perf_counter() - t0) * 1000
            best[q] = min(best.get(q, 1e18), ms)
    total = sum(best.values())
    print(f"--- {label} ---")
    for q in range(1, 23):
        print(f"Q{q:02d} {best[q]:10.1f}ms")
    print(f"TOTAL {label}: {total/1000:.2f}s")
    return best

def parquet_con():
    con = duckdb.connect()
    for t in TABLES:
        con.execute(f"CREATE VIEW {t} AS SELECT * FROM read_parquet('data/tpch-100gb/{t}.parquet')")
    return con

def lance_con():
    con = duckdb.connect()
    con.execute("LOAD lance;")
    for t in TABLES:
        con.execute(f"CREATE VIEW {t} AS SELECT * FROM 'data/tpch-100gb-lance/{t}.lance'")
    return con

bench(parquet_con, "duckdb-parquet")
bench(lance_con, "duckdb-lance")
