"""Post-timing typed audit; run only after release/provider pipeline is terminal."""
import hashlib,json,statistics,math
from pathlib import Path
from benchmark.run import compare_execution
b=Path('.scratch/parallel-aggregate-input');root=Path('.scratch/public-bench/lance-refinement-sf10-providers-01')
state=json.loads((b/'lance-refinement-release-screen.json').read_text());assert state['providers']['status']=='terminal'
assert (root/'verified-after.json').exists()
out=b/'lance-refinement-sf10-audit';out.mkdir(exist_ok=False)
queries={q['id']:q for q in json.loads(Path('.scratch/public-bench/tpch-sf10/dataset.json').read_text())['queries']}
summary={}
for track in ('raw_parquet','native','iceberg','lance'):
 run=root/track;validations=[];failures=[]
 for line in (run/'execution.jsonl').read_text().splitlines():
  x=json.loads(line);result=x['result']
  if result.get('status')!='completed':
   failures.append({'side':x['side'],'id':x['request']['id'],'result':result});continue
  if x['side']!='engine':continue
  q=x['request']['id'].split('-')[1];scratch=out/track/x['request']['id'];scratch.mkdir(parents=True)
  oracle=run/f's1-{q}-oracle.arrow'
  comparison=compare_execution(result,{'status':'completed','output':str(oracle)},queries[q],scratch)
  validations.append({'id':x['request']['id'],'comparison':comparison});assert comparison['ok'],validations[-1]
 samples=[json.loads(l) for l in (run/'samples.jsonl').read_text().splitlines()]
 valid=[s for s in samples if s.get('comparison',{}).get('ok') and s.get('engine',{}).get('status')=='completed' and s.get('duckdb',{}).get('status')=='completed']
 per_query=[]
 for q in queries:
  rows=[s for s in valid if s['query']==q]
  if len(rows)!=3:continue
  eng=statistics.median(s['engine']['ms'] for s in rows);ref=statistics.median(s['duckdb']['ms'] for s in rows)
  per_query.append({'query':q,'engine_median_ms':eng,'duckdb_median_ms':ref,'ratio':eng/ref})
 complete=len(per_query)==22
 result={'typed_engine_outputs':len(validations),'valid_pairs':len(valid),'planned_pairs':66,'complete':complete,'per_query':per_query,'failures':failures}
 if complete:
  result.update(geomean=math.exp(statistics.mean(math.log(x['ratio']) for x in per_query)),suite_ratio=sum(x['engine_median_ms'] for x in per_query)/sum(x['duckdb_median_ms'] for x in per_query),wins=sum(x['ratio']<1 for x in per_query))
 (out/(track+'-validations.json')).write_text(json.dumps(validations,indent=2))
 summary[track]=result;print(track,len(validations),len(valid),'complete',complete,flush=True)
(out/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
experiment=json.loads((root/'experiment.json').read_text())
sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
assert all(sha(p)==v for p,v in experiment['source_inputs'].items())
assert all(sha(p)==v for p,v in experiment['harness_sha256'].items())
assert sha(b/'lance_refinement_benchmark_embedded')==experiment['candidate']['binary_sha256']
(out/'verified.json').write_text(json.dumps({'source_inputs':len(experiment['source_inputs']),'binary':True,'harness':True}))
