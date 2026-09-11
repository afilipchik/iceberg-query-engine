"""Contained, instrumentation-enabled attribution; not performance acceptance."""
import hashlib,json,os,time,statistics
from pathlib import Path
from benchmark.run import Worker,compare_execution
b=Path('.scratch/parallel-aggregate-input');root=b/'routing-phase-profile-01';root.mkdir(exist_ok=False)
binary=(b/'aggregate_routing_profile_benchmark_embedded').resolve();sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest();expected=json.loads((b/'aggregate-routing-profile-release.json').read_text())['binary_sha256'];assert sha(binary)==expected
assert sorted(os.sched_getaffinity(0))==list(range(16))
prior=Path('.scratch/public-bench/lance-refinement-sf10-providers-01');queries={q['id']:q for q in json.loads(Path('.scratch/public-bench/tpch-sf10/dataset.json').read_text())['queries']}
(root/'manifest.json').write_text(json.dumps({'binary_sha256':expected,'driver_sha256':sha(__file__),'parent_commit':'bb38784aba074c288bedf17722e32d2f8c8cc500','qualification':'Aggregate instrumentation, fresh worker per shape, one warmup/two samples, 4/16 threads within the same 16 CPU affinity. Not performance acceptance; query/process budgets unchanged.'},indent=2))
state={};validation=[]
for track,threads in [('lance',16),('lance',4)]:
 requests={}
 for line in (prior/track/'execution.jsonl').read_text().splitlines():
  x=json.loads(line)
  if x['side']=='engine' and x['request']['id'].endswith('-warmup'):requests[x['request']['id'].split('-')[1]]=x['request']
 for q in ['q01','q18']:
  key=f'{track}-t{threads}-{q}';run=root/key;run.mkdir();(run/'temp').mkdir()
  setup=json.loads((prior/track/'setup.json').read_text());setup.update(threads=threads,temp_directory=str((run/'temp').resolve()));(run/'setup.json').write_text(json.dumps(setup,indent=2))
  env={k:v for k,v in os.environ.items() if not k.startswith(('QE_','MIMALLOC_')) and k not in ('RT_DISABLE','QUERY_ENGINE_ALLOW_THP','LANCE_PROCESS_IO_THREADS_LIMIT')}
  env.update(QE_AGG_OWNERSHIP='disjoint',QE_MEM_CAP=str(setup['process_cap_bytes']),QE_GPU='0',QE_IPC_CACHE='0',QE_LANCE_TIMING='1',QE_AGG_PROF='1',TMPDIR=str((run/'temp').resolve()),RAYON_NUM_THREADS=str(threads))
  events=[];worker=Worker([str(binary),str((run/'setup.json').resolve())],env,run/'engine.stderr',events);state[key]={'status':'running','ready':worker.ready};(root/'state.json').write_text(json.dumps(state,indent=2))
  records=[]
  try:
   with (run/'execution.jsonl').open('w') as out:
    for n in range(3):
     request=dict(requests[q],id=f'{key}-{n}',output=str((run/f'{n}.arrow').resolve()));result=worker.query(request,180000);records.append(result);out.write(json.dumps({'request':request,'result':result})+'\n');out.flush()
     if result.get('status')!='completed':break
  finally:
   worker.close();(run/'workers.json').write_text(json.dumps(events,indent=2))
  state[key]={'status':'terminal','completed':sum(x.get('status')=='completed' for x in records),'measurements':[{'ms':x.get('ms'),'metrics':x.get('metrics'),'physical_plan':x.get('physical_plan')} for x in records]};(root/'state.json').write_text(json.dumps(state,indent=2));print(key,state[key]['completed'],flush=True)
  for n,result in enumerate(records):
   if result.get('status')!='completed':continue
   scratch=run/'validation-scratch'/str(n);scratch.mkdir(parents=True);c=compare_execution(result,{'status':'completed','output':str(prior/track/f's1-{q}-oracle.arrow')},queries[q],scratch);validation.append({'case':key,'sample':n,'comparison':c});assert c['ok']
  (root/'validation.json').write_text(json.dumps(validation,indent=2))
assert sha(binary)==expected
cg=Path('/sys/fs/cgroup')/next(x.split('::',1)[1] for x in Path('/proc/self/cgroup').read_text().splitlines() if '::' in x).lstrip('/')
(root/'resources.json').write_text(json.dumps({n:(cg/n).read_text() for n in ['memory.max','memory.swap.max','memory.peak','memory.events']},indent=2))
(root/'verified.json').write_text(json.dumps({'binary_unchanged':True,'typed_outputs':len(validation),'cases':len(state)},indent=2))
