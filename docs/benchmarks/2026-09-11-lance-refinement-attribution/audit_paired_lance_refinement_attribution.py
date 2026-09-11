import hashlib,json,re,statistics
from pathlib import Path
from benchmark.run import compare_execution
b=Path('.scratch/parallel-aggregate-input');root=b/'paired-lance-refinement-attribution-01'
assert (root/'verified.json').exists()
queries={q['id']:q for q in json.loads(Path('.scratch/public-bench/tpch-sf10/dataset.json').read_text())['queries']}
state=json.loads((root/'state.json').read_text());validation=[];cases={}
for key in state:
 run=root/key;q=key.split('-')[-1];rows=[json.loads(x) for x in (run/'execution.jsonl').read_text().splitlines()]
 for x in rows:
  if x['result'].get('status')!='completed':continue
  scratch=root/'validation-scratch'/x['request']['id'];scratch.mkdir(parents=True)
  oracle=Path('.scratch/public-bench/ordered-scanner-sf10-providers-01/lance')/f's1-{q}-oracle.arrow'
  comparison=compare_execution(x['result'],{'status':'completed','output':str(oracle)},queries[q],scratch)
  validation.append({'id':x['request']['id'],'comparison':comparison});assert comparison['ok'],validation[-1]
 measured=[x['result'] for x in rows[1:] if x['result'].get('status')=='completed'];assert len(measured)==3,key
 scans={}
 for line in (run/'engine.stderr').read_text().splitlines():
  m=re.match(r'\[lance-scan\]\s+([\d.]+)ms\s+(\d+) rows (\d+) cols (.*)',line)
  if m:
   signature=m[2]+' rows '+m[3]+' cols '+m[4];scans.setdefault(signature,[]).append(float(m[1]))
 cases[key]={'median_ms':statistics.median(x['ms'] for x in measured),'median_plan_ms':statistics.median(x['metrics']['plan_ms'] for x in measured),'median_execute_ms':statistics.median(x['metrics']['execute_ms'] for x in measured),'scans':scans,'repeated_scan_medians_ms':{k:statistics.median(v[-3:]) for k,v in scans.items() if len(v)>=4},'optimized_plans':sorted(set(x['optimized_plan'] for x in measured)),'physical_plans':sorted(set(x['physical_plan'] for x in measured))}
pairs=[]
for block in (1,2):
 for q in ['q01','q06','q12','q19','q09']:
  a=cases[f'b{block}-parent-{q}'];c=cases[f'b{block}-candidate-{q}'];pairs.append({'block':block,'query':q,'query_ratio':c['median_ms']/a['median_ms'],'parent_ms':a['median_ms'],'candidate_ms':c['median_ms'],'parent_plan_ms':a['median_plan_ms'],'candidate_plan_ms':c['median_plan_ms'],'parent_execute_ms':a['median_execute_ms'],'candidate_execute_ms':c['median_execute_ms'],'optimized_plan_equal':a['optimized_plans']==c['optimized_plans'],'physical_plan_equal':a['physical_plans']==c['physical_plans']})
(root/'validation.json').write_text(json.dumps(validation,indent=2)+'\n');(root/'analysis.json').write_text(json.dumps({'qualification':'Two reversed blocks, scan instrumentation enabled, diagnostic ceiling; not performance acceptance. Repeated scan medians use the last three occurrences per signature, excluding one-shot setup scans.','typed_outputs':len(validation),'cases':cases,'pairs':pairs},indent=2)+'\n')
print('typed outputs',len(validation));print(json.dumps(pairs,indent=2))
