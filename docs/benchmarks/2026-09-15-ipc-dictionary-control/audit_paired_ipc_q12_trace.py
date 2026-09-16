import hashlib,json,re,statistics
from pathlib import Path
from benchmark.run import compare_execution
b=Path('.scratch/parallel-aggregate-input');root=b/'paired-ipc-q12-trace-01'
assert (root/'verified.json').exists()
queries={q['id']:q for q in json.loads(Path('.scratch/public-bench/tpch-sf10/dataset.json').read_text())['queries']}
state=json.loads((root/'state.json').read_text());validation=[];cases={}
for key in state:
 run=root/key;q=key.split('-')[-1];rows=[json.loads(x) for x in (run/'execution.jsonl').read_text().splitlines()]
 for x in rows:
  if x['result'].get('status')!='completed':continue
  scratch=root/'validation-scratch'/x['request']['id'];scratch.mkdir(parents=True)
  oracle=Path('.scratch/public-bench/ipc-dictionary-sf10-providers-01/native')/f's1-{q}-oracle.arrow'
  comparison=compare_execution(x['result'],{'status':'completed','output':str(oracle)},queries[q],scratch)
  validation.append({'id':x['request']['id'],'comparison':comparison});assert comparison['ok'],validation[-1]
 measured=[x['result'] for x in rows[1:] if x['result'].get('status')=='completed'];assert len(measured)==3,key
 cases[key]={'median_ms':statistics.median(x['ms'] for x in measured),'median_plan_ms':statistics.median(x['metrics']['plan_ms'] for x in measured),'median_execute_ms':statistics.median(x['metrics']['execute_ms'] for x in measured),'optimized_plans':sorted(set(x['optimized_plan'] for x in measured)),'physical_plans':sorted(set(x['physical_plan'] for x in measured))}
for key,case in cases.items():
 case['routing_profiles']=[{k:float(v) for k,v in re.findall(r"(\w+)=([\d.]+)",line)} for line in (root/key/'engine.stderr').read_text().splitlines() if line.startswith('live_aggregate_routing ')]
for key,case in cases.items():
 if key.endswith(('-q01','-q18')):
  profiles=[{k:float(v) for k,v in re.findall(r'(\w+)=([\d.]+)',line)} for line in (root/key/'engine.stderr').read_text().splitlines() if line.startswith('live_aggregate_profile ')]
  large=[p for p in profiles if p.get('input_rows',0)>1_000_000]
  case['aggregate_profiles']=profiles
  case['large_aggregate_sample_count']=len(large)
  if len(large)==4:
   case['measured_large_aggregate_median']={k:statistics.median(p[k] for p in large[1:]) for k in ['evaluation_ms','ingestion_ms','finish_ms','output_ms']}
pairs=[]
for block in (1,2):
 for q in ['q12']:
  a=cases[f'b{block}-parent-{q}'];c=cases[f'b{block}-candidate-{q}'];pairs.append({'block':block,'query':q,'query_ratio':c['median_ms']/a['median_ms'],'parent_ms':a['median_ms'],'candidate_ms':c['median_ms'],'parent_plan_ms':a['median_plan_ms'],'candidate_plan_ms':c['median_plan_ms'],'parent_execute_ms':a['median_execute_ms'],'candidate_execute_ms':c['median_execute_ms'],'optimized_plan_equal':a['optimized_plans']==c['optimized_plans'],'physical_plan_equal':a['physical_plans']==c['physical_plans']})
(root/'validation.json').write_text(json.dumps(validation,indent=2)+'\n');(root/'analysis.json').write_text(json.dumps({'qualification':'Two reversed blocks, aggregate/input-queue/join instrumentation enabled, diagnostic ceiling; not performance acceptance. Native provider boundary matches the canonical screen; extended diagnostic deadline is separate from the recorded DuckDB ceiling.','typed_outputs':len(validation),'cases':cases,'pairs':pairs},indent=2)+'\n')
print('typed outputs',len(validation));print(json.dumps(pairs,indent=2))

phases={}
for key in state:
 run=root/key;log=(run/'engine.stderr').read_bytes();records=[]
 for event in [json.loads(line) for line in (run/'execution.jsonl').read_text().splitlines()]:
  start,end=event['stderr_range'];lines=log[start:end].decode(errors='replace').splitlines()
  traces=[json.loads(line[len('[input-queue] '):]) for line in lines if line.startswith('[input-queue] ')]
  before=event['process_before'];after=event['process_after']
  delta={k:after[k]-before[k] for k in ['minor_faults','major_faults','user_ticks','system_ticks'] if k in before and k in after}
  io={k:after['io'][k]-before['io'][k] for k in before.get('io',{}) if k in after.get('io',{})}
  records.append({'request':event['request']['id'],'process_delta':delta,'io_delta':io,'traces':traces,'qualification':'Concurrent producer durations overlap; do not sum as wall time. Process counters include all work between the request boundaries.'})
 phases[key]=records
(root/'phase-analysis.json').write_text(json.dumps(phases,indent=2)+'\n')
