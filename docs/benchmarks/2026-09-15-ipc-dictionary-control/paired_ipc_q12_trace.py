"""Diagnostic only: run inside the capped wrapper after the matched provider timing."""
import hashlib,json,os,time
from pathlib import Path
from benchmark.run import Worker
b=Path('.scratch/parallel-aggregate-input');root=b/'paired-ipc-q12-trace-01';root.mkdir(exist_ok=False)
sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
assert not any(k.startswith('MIMALLOC_') for k in os.environ)
assert sorted(os.sched_getaffinity(0))==list(range(16))
prior=Path('.scratch/public-bench/decimal-scale-sf10-providers-01/native')
setup_template=json.loads((prior/'setup.json').read_text())
queries={}
for line in (prior/'execution.jsonl').read_text().splitlines():
 x=json.loads(line)
 if x['side']=='engine' and x['request']['id'].endswith('-warmup'):queries[x['request']['id'].split('-')[1]]=x['request']
binaries={'parent':b/'decimal_scale_benchmark_embedded','candidate':b/'ipc_dictionary_benchmark_embedded'}
expected={label:json.loads((b/name).read_text())['binary_sha256'] for label,name in [('parent','decimal-scale-release.json'),('candidate','ipc-dictionary-release.json')]}
assert all(sha(p)==expected[k] for k,p in binaries.items())
manifest={'binary_sha256':expected,'driver_sha256':sha(__file__),'setup_source_sha256':sha(prior/'setup.json'),'request_source_sha256':sha(prior/'execution.jsonl'),'queries':['q12'],'blocks':[['parent','candidate'],['candidate','parent']],'samples':3,'warmups':1,'fresh_worker_per_query':True,'qualification':'Two reversed blocks with aggregate/input-queue/join traces, fresh worker per query, 180s diagnostic ceiling; not normal performance acceptance. Dataset/setup work excluded equally; Native provider/setup and query timing boundaries match the provider screen.'}
(root/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
def process_counters(pid):
 try:
  proc=Path('/proc')/str(pid)
  status={k:v.strip() for line in (proc/'status').read_text().splitlines() if ':' in line for k,v in [line.split(':',1)]}
  fields=(proc/'stat').read_text().rsplit(') ',1)[1].split()
  io={k:int(v) for line in (proc/'io').read_text().splitlines() for k,v in [line.split(':',1)]}
  return {'clock_ticks_per_second':os.sysconf('SC_CLK_TCK'),'minor_faults':int(fields[7]),'major_faults':int(fields[9]),'user_ticks':int(fields[11]),'system_ticks':int(fields[12]),'io':io,'status':{k:status.get(k) for k in ['VmRSS','VmHWM','VmData','RssAnon','RssFile','VmSwap','Threads']}}
 except (OSError,ValueError,IndexError) as error:return {'unavailable':str(error)}
state={}
for block,order in enumerate(manifest['blocks'],1):
 for label in order:
  for q in manifest['queries']:
   key=f'b{block}-{label}-{q}';run=root/key;run.mkdir();(run/'temp').mkdir()
   setup=dict(setup_template,temp_directory=str((run/'temp').resolve()));(run/'setup.json').write_text(json.dumps(setup,indent=2))
   env={k:v for k,v in os.environ.items() if not k.startswith('QE_') and k not in ('RT_DISABLE','QUERY_ENGINE_ALLOW_THP','LANCE_PROCESS_IO_THREADS_LIMIT')}
   env.update(QE_AGG_OWNERSHIP='disjoint',QE_MEM_CAP=str(setup['process_cap_bytes']),QE_GPU='0',QE_IPC_CACHE='0',QE_AGG_PROF='1',QE_INPUT_QUEUE_TRACE='1',HJ_TIMING='1',TMPDIR=str((run/'temp').resolve()),RAYON_NUM_THREADS='16')
   events=[];worker=Worker([str(binaries[label].resolve()),str((run/'setup.json').resolve())],env,run/'engine.stderr',events)
   state[key]={'status':'running','started':time.time(),'ready':worker.ready};(root/'state.json').write_text(json.dumps(state,indent=2))
   completed=0
   try:
    with (run/'execution.jsonl').open('w') as out:
     for sample in range(4):
      request=dict(queries[q]);request['id']=key+('-warmup' if sample==0 else f'-{sample}-engine');request['output']=str((run/(request['id']+'.arrow')).resolve())
      stderr_start=(run/'engine.stderr').stat().st_size;before=process_counters(worker.process.pid);start=time.time();result=worker.query(request,180000);after=process_counters(worker.process.pid)
      out.write(json.dumps({'request':request,'result':result,'started':start,'finished':time.time(),'process_before':before,'process_after':after,'stderr_range':[stderr_start,(run/'engine.stderr').stat().st_size]})+'\n');out.flush()
      if result.get('status')!='completed':break
      completed+=1
   finally:
    worker.close();(run/'workers.json').write_text(json.dumps(events,indent=2))
   state[key].update(status='terminal',completed=completed,finished=time.time());(root/'state.json').write_text(json.dumps(state,indent=2))
   print(key,completed,flush=True)
assert all(sha(p)==expected[k] for k,p in binaries.items())
rel=next(x.split('::',1)[1] for x in Path('/proc/self/cgroup').read_text().splitlines() if '::' in x);cg=Path('/sys/fs/cgroup')/rel.lstrip('/')
(root/'resources.json').write_text(json.dumps({n:(cg/n).read_text() for n in ['memory.max','memory.swap.max','memory.peak','memory.events']},indent=2))
(root/'verified.json').write_text(json.dumps({'binary_hashes_unchanged':True,'cases':len(state),'completed_outputs':sum(v['completed'] for v in state.values())},indent=2))
