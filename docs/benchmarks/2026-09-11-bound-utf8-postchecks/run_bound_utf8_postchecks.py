"""Wait for this cycle's existing provider screen, then audit and compare sequentially."""
import hashlib,json,subprocess,sys,time
from pathlib import Path
b=Path('.scratch/parallel-aggregate-input');out=b/'bound-utf8-postchecks.json';assert not out.exists()
state={'status':'waiting_existing_cycle','cycle_session':42123,'started':time.time(),'driver_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'stages':{},'qualification':'One sequential16GiB postcheck scope; cumulative peak, not per-query RSS.'};out.write_text(json.dumps(state,indent=2))
while True:
 cycle=json.loads((b/'bound-utf8-cycle.json').read_text())
 if cycle.get('release_screen',{}).get('status')=='terminal':break
 for name in ['comparison','archive_validation']:
  if cycle.get(name,{}).get('status')=='terminal':assert cycle[name]['exit_code']==0,cycle[name]
 if time.time()-state['started']>7200:raise RuntimeError('Existing cycle wait exceeded2hours; inspect original job, do not restart')
 time.sleep(10)
prior=json.loads((b/'bound-utf8-release-screen.json').read_text());assert prior['release']['exit_code']==0 and prior['providers']['status']=='terminal'
def resources():
 cg=Path('/sys/fs/cgroup')/next(x.split('::',1)[1] for x in Path('/proc/self/cgroup').read_text().splitlines() if '::' in x).lstrip('/')
 return {n:(cg/n).read_text() for n in ['memory.max','memory.swap.max','memory.peak','memory.events']}
for stage,script in [('sf10_audit','audit_bound_utf8_sf10.py'),('paired','paired_bound_utf8_attribution.py'),('paired_audit','audit_paired_bound_utf8_attribution.py'),('sf10_archive','archive_bound_utf8_sf10.py'),('paired_archive','archive_bound_utf8_attribution.py')]:
 command=[sys.executable,str(b/script)];state['status']='running';state['stages'][stage]={'status':'running','started':time.time(),'command':command,'driver_sha256':hashlib.sha256((b/script).read_bytes()).hexdigest(),'before':resources()};out.write_text(json.dumps(state,indent=2))
 with (b/f'bound-utf8-post-{stage}.log').open('x') as log:result=subprocess.run(command,stdout=log,stderr=subprocess.STDOUT)
 state['stages'][stage].update(status='terminal',exit_code=result.returncode,finished=time.time(),after=resources());out.write_text(json.dumps(state,indent=2));print(stage,result.returncode,flush=True)
 if result.returncode:raise SystemExit(result.returncode)
source=json.loads((b/'bound-utf8-release-source-hashes.json').read_text());assert all(hashlib.sha256(Path(p).read_bytes()).hexdigest()==h for p,h in source.items());state.update(status='terminal',exit_code=0,source_inputs_verified=len(source));out.write_text(json.dumps(state,indent=2))
