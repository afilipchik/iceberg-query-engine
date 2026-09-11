"""Run after timing, inside the wrapper: observe an owned frozen engine worker."""
import hashlib,json,os,sys,time
from collections import Counter
from pathlib import Path
from benchmark.run import Worker
root=Path(sys.argv[1]).resolve();root.mkdir(exist_ok=False)
binary=Path(sys.argv[2]).resolve();sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
prior=Path('.scratch/public-bench/reference-io-quota-sf10-01/lance')
setup=json.loads((prior/'setup.json').read_text());setup['temp_directory']=str(root/'temp');(root/'temp').mkdir()
(root/'setup.json').write_text(json.dumps(setup,indent=2))
env={k:v for k,v in os.environ.items() if not k.startswith('QE_') and k not in ('RT_DISABLE','QUERY_ENGINE_ALLOW_THP','LANCE_PROCESS_IO_THREADS_LIMIT')}
env.update(QE_AGG_OWNERSHIP='disjoint',QE_MEM_CAP=str(setup['process_cap_bytes']),QE_GPU='0',QE_IPC_CACHE='0',TMPDIR=str(root/'temp'),RAYON_NUM_THREADS='16')
assert sorted(os.sched_getaffinity(0))==list(range(16))
events=[];worker=Worker([str(binary),str(root/'setup.json')],env,root/'engine.stderr',events)
def snapshot():
 p=Path('/proc')/str(worker.process.pid);result={}
 try:
  for line in (p/'status').read_text().splitlines():
   key,_,value=line.partition(':')
   if key in ('VmData','VmRSS','RssAnon','RssFile','Threads','VmSize'):result[key]=value.strip()
  names=Counter()
  for task in (p/'task').iterdir():
   try:names[(task/'comm').read_text().strip()]+=1
   except FileNotFoundError:pass
  result['thread_names']=dict(names)
 except FileNotFoundError:result['process_gone']=True
 return result
manifest={'binary_sha256':sha(binary),'setup_source_sha256':sha(prior/'setup.json'),'request_source_sha256':sha(prior/'execution.jsonl'),'driver_sha256':sha(__file__),'ready':worker.ready,'ready_process':snapshot(),'qualification':'Fresh Q9 warmup only, same setup/caps;180s diagnostic ceiling, not performance acceptance.'}
(root/'manifest.json').write_text(json.dumps(manifest,indent=2))
try:
 with (root/'execution.jsonl').open('w') as out:
  for line in (prior/'execution.jsonl').read_text().splitlines():
   x=json.loads(line)
   if x['side']!='engine' or x['request']['id']!='s1-q09-warmup':continue
   request=dict(x['request']);request['output']=str(root/Path(request['output']).name)
   before=snapshot();result=worker.query(request,180000);after=snapshot()
   out.write(json.dumps(dict(request=request,result=result,before=before,after=after))+'\n');out.flush()
   print(request['id'],result.get('status'),after.get('VmData'),flush=True)
   if result.get('status')!='completed':break
finally:
 worker.close();(root/'workers.json').write_text(json.dumps(events,indent=2))
assert sha(binary)==manifest['binary_sha256']
rel=next(x.split('::',1)[1] for x in Path('/proc/self/cgroup').read_text().splitlines() if '::' in x)
cgroup=Path('/sys/fs/cgroup')/rel.lstrip('/')
(root/'resources.json').write_text(json.dumps({n:(cgroup/n).read_text() for n in ['memory.max','memory.swap.max','memory.peak','memory.events']},indent=2))
