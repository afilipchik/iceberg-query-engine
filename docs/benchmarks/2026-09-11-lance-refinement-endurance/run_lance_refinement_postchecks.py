"""Run inside a 16 GiB scope only after the timed provider pipeline is terminal."""
import hashlib,json,subprocess,sys,time
from pathlib import Path
b=Path('.scratch/parallel-aggregate-input')
prior=json.loads((b/'lance-refinement-release-screen.json').read_text())
assert prior['providers']['status']=='terminal'
assert (b/'lance-refinement-release.json').exists()
state_path=b/'lance-refinement-postchecks.json';assert not state_path.exists()
def resources():
 rel=next(x.split('::',1)[1] for x in Path('/proc/self/cgroup').read_text().splitlines() if '::' in x)
 root=Path('/sys/fs/cgroup')/rel.lstrip('/')
 return {n:(root/n).read_text() for n in ['memory.max','memory.swap.max','memory.peak','memory.events']}
stages=[('sf10_audit',['audit_lance_refinement_sf10.py']),('paired',['paired_lance_refinement_attribution.py']),('paired_audit',['audit_paired_lance_refinement_attribution.py']),('endurance',['engine_lance_refinement_endurance.py',str(b/'lance-refinement-endurance-01'),str(b/'lance_refinement_benchmark_embedded')]),('endurance_audit',['audit_lance_refinement_endurance.py'])]
state={'driver_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'qualification':'Sequential post-timing diagnostics and audits; cgroup peak is cumulative across stages, not per-query RSS.','stages':{}}
for name,args in stages:
 command=[sys.executable,str(b/args[0]),*args[1:]]
 state['stages'][name]={'status':'running','command':command,'started':time.time(),'before':resources()}
 state_path.write_text(json.dumps(state,indent=2)+'\n')
 with (b/('lance-refinement-post-'+name+'.log')).open('x') as log:
  result=subprocess.run(command,stdout=log,stderr=subprocess.STDOUT)
 state['stages'][name].update(status='terminal',exit_code=result.returncode,finished=time.time(),after=resources())
 state_path.write_text(json.dumps(state,indent=2)+'\n');print(name,result.returncode,flush=True)
 if result.returncode:raise SystemExit(result.returncode)
release=json.loads((b/'lance-refinement-release.json').read_text())
assert hashlib.sha256((b/'lance_refinement_benchmark_embedded').read_bytes()).hexdigest()==release['binary_sha256']
source=json.loads((b/'lance-refinement-release-source-hashes.json').read_text())
assert all(hashlib.sha256(Path(p).read_bytes()).hexdigest()==v for p,v in source.items())
state['verified_source_inputs_after']=len(source);state_path.write_text(json.dumps(state,indent=2)+'\n')
