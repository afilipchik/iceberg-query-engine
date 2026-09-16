"""Read-only supplemental classification: typed completion is separate from latency."""
import json,math,hashlib
from pathlib import Path
root=Path('.scratch/public-bench/ipc-dictionary-sf10-providers-01');out={}
def valid_time(x):return type(x) in (int,float) and math.isfinite(x) and x>0
for track in ['raw_parquet','native','iceberg','lance']:
 p=root/track/'samples.jsonl';rows=[json.loads(l) for l in p.read_text().splitlines()];late={}
 for row in rows:
  ceiling=row.get('query_ceiling_ms')
  for side,key in [('engine_warmup',row['query']+'-warmup'),('engine',row['query']+'-'+str(row['iteration'])+'-engine'),('duckdb',row['query']+'-'+str(row['iteration'])+'-duckdb')]:
   result=row.get(side) or {}
   if result.get('status')=='completed' and not (valid_time(ceiling) and valid_time(result.get('ms')) and result['ms']<=ceiling):
    late[key]={'query':row['query'],'phase':side,'ms':result.get('ms'),'ceiling_ms':ceiling,'result_id':result.get('id')}
 out[track]={'samples_sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'completed_outside_gate':list(late.values())}
b=Path('.scratch/parallel-aggregate-input');p=b/'ipc-dictionary-gate-outcomes.json';assert not p.exists();p.write_text(json.dumps(out,indent=2)+'\n');print(json.dumps(out,indent=2))
