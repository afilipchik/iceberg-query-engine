"""Read completed provider screens; no engine queries or timing reclassification."""
import json
from collections import Counter
from pathlib import Path
assert json.loads(Path('.scratch/parallel-aggregate-input/admitted-coalesce-full-screens.json').read_text())['status']=='terminal', 'All timing stages must finish before audit'
from benchmark.compare import compare_files
from benchmark.query_gate import within_ceiling
root=Path('.scratch/public-bench/admitted-coalesce-sf10-providers-01')
queries={q['id']:q for q in json.loads(Path('.scratch/public-bench/tpch-sf10/dataset.json').read_text())['queries']}
summary={}
state=json.loads((root/'state.json').read_text())
assert set(state)=={'raw_parquet','native','iceberg','lance'} and all(r['status']=='terminal' for r in state.values())
for track in ['raw_parquet','native','iceberg','lance']:
 folder=root/track;rows=[json.loads(l) for l in (folder/'samples.jsonl').read_text().splitlines()]
 counts=Counter();failures={};seen=set();warmups=[];completed=[]
 for r in rows:
  counts[r['engine']['status']]+=1
  q=r['query']
  if not r['comparison']['ok']:
   failures[q]={'engine_status':r['engine']['status'],'reason':r['engine'].get('error'),'warmup_status':(r.get('engine_warmup') or {}).get('status'),'calibration_ms':r['calibration_ms']}
  if r['engine']['status']=='completed':
   query=queries[q];e=r['engine']
   validation=compare_files(e['output'],folder/f"{r['session']}-{q}-oracle.arrow",scratch=folder,policy=query['result_policy'],order_by=query['order_by'],limit_rows=query['limit_rows'],expected_is_complete=True)
   completed.append({'query':q,'iteration':r['iteration'],'comparison':validation,'elapsed_ms':e['ms'],'time_gate_pass':e['ms']<=r['query_ceiling_ms'],'pair_valid':r['comparison']['ok']})
  if q in seen:continue
  seen.add(q);w=r.get('engine_warmup');query=queries[q]
  if w and w.get('status')=='completed':
   result=compare_files(w['output'],folder/f"{r['session']}-{q}-oracle.arrow",scratch=folder,policy=query['result_policy'],order_by=query['order_by'],limit_rows=query['limit_rows'],expected_is_complete=True)
   warmups.append({'query':q,'comparison':result,'elapsed_ms':w['ms'],'ceiling_ms':r['query_ceiling_ms'],'time_gate_pass':w['ms']<=r['query_ceiling_ms']})
 path=folder/'supplemental-completed-correctness.json';assert not path.exists();path.write_text(json.dumps(completed,indent=2)+'\n')
 path=folder/'supplemental-warmup-correctness.json';assert not path.exists();path.write_text(json.dumps(warmups,indent=2)+'\n')
 summary[track]={'requested_pairs':len(rows),'valid_pairs':sum(r['comparison']['ok'] and r['engine']['status']=='completed' and all(within_ceiling(r[side],r['query_ceiling_ms']) for side in ['engine','duckdb']) for r in rows),'engine_statuses':dict(counts),'failures':failures,'completed_measured':len(completed),'completed_measured_valid':all(e['comparison']['ok'] for e in completed),'completed_warmups':len(warmups),'warmups_valid':all(w['comparison']['ok'] for w in warmups),'scope_after':json.loads((folder/'scope-after.json').read_text())}
path=root/'supplemental-summary.json';assert not path.exists();path.write_text(json.dumps(summary,indent=2)+'\n')
print(json.dumps(summary,indent=2));assert all(r['warmups_valid'] and r['completed_measured_valid'] for r in summary.values())
