"""Summarize complete per-query pairs only; never invent a full-suite score."""
from pathlib import Path
import json,statistics,math
from benchmark.query_gate import within_ceiling
assert json.loads(Path('.scratch/parallel-aggregate-input/admitted-coalesce-full-screens.json').read_text())['status']=='terminal', 'All timing stages must finish before audit'
root=Path('.scratch/public-bench/admitted-coalesce-sf10-providers-01')
result={}
for track in ['raw_parquet','native','iceberg','lance']:
 rows=[json.loads(l) for l in (root/track/'samples.jsonl').read_text().splitlines()]
 queries={}
 for q in sorted({r['query'] for r in rows}):
  samples=[r for r in rows if r['query']==q]
  valid=[r for r in samples if r['comparison']['ok'] and r['engine']['status']=='completed' and r['duckdb']['status']=='completed' and all(within_ceiling(r[side],r['query_ceiling_ms']) for side in ['engine','duckdb'])]
  complete=len(valid)==3 and len(samples)==3
  engine=statistics.median(r['engine']['ms'] for r in valid) if complete else None
  duck=statistics.median(r['duckdb']['ms'] for r in valid) if complete else None
  queries[q]={'valid_pairs':len(valid),'requested_pairs':len(samples),'complete':complete,'engine_median_ms':engine,'duckdb_median_ms':duck,'ratio':engine/duck if complete else None}
 complete=len(queries)==22 and all(q['complete'] for q in queries.values())
 result[track]={'queries':queries,'complete':complete,'geometric_mean_ratio':math.exp(statistics.mean(math.log(q['ratio']) for q in queries.values())) if complete else None,'suite_total_ratio':sum(q['engine_median_ms'] for q in queries.values())/sum(q['duckdb_median_ms'] for q in queries.values()) if complete else None,'completed_queries_slower_than_2x':[q for q,v in queries.items() if v['complete'] and v['ratio']>2]}
output=root/'per-query-screen.json';assert not output.exists();output.write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(result,indent=2))
