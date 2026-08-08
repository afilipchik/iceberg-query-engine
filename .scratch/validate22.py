import duckdb, csv, sys, math, re, os
con = duckdb.connect()
for t in ["customer","lineitem","nation","orders","part","partsupp","region","supplier"]:
    con.execute(f"CREATE VIEW {t} AS SELECT * FROM read_parquet('data/tpch-10gb/{t}.parquet')")
sys.path.insert(0, 'scripts')
# extract engine queries from rust source
src = open('src/tpch/queries.rs').read()
queries = {}
for m in re.finditer(r'pub const Q(\d+): &str = r#"(.*?)"#;', src, re.S):
    queries[int(m.group(1))] = m.group(2)
assert len(queries) >= 22, len(queries)
# Q11 uses SF-adjusted threshold (0.0001/SF) at SF=10
queries[11] = queries[11].replace('0.0001', '0.00001')
import datetime
def norm(v):
    if v is None or v == '': return ''
    if isinstance(v, (datetime.date, datetime.datetime)): return str(v)[:10]
    try:
        f = float(v)
        if math.isnan(f): return "NaN"
        return round(f, 2)
    except (ValueError, TypeError):
        return v
bad = 0
for q in range(1, 23):
    duck = con.execute(queries[q]).fetchall()
    path = f'.scratch/engine_csv/q{q:02d}.csv'
    with open(path) as f:
        rows = list(csv.reader(f))[1:]
    if len(duck) != len(rows):
        print(f"Q{q:02d}: ROWCOUNT MISMATCH duck={len(duck)} engine={len(rows)}"); bad += 1; continue
    mism = 0
    for dr, er in zip(duck, rows):
        for dv, ev in zip(dr, er):
            if norm(dv) != norm(ev):
                if isinstance(norm(dv), float) and isinstance(norm(ev), float) and abs(norm(dv)-norm(ev)) < 0.02:
                    continue
                mism += 1
                if mism <= 2: print(f"  Q{q:02d} cell: duck={dv!r} engine={ev!r}")
    if mism: print(f"Q{q:02d}: {mism} CELL MISMATCHES"); bad += 1
    else: print(f"Q{q:02d}: OK ({len(duck)} rows)")
print("FAILED" if bad else "ALL 22 CELL-EXACT")
