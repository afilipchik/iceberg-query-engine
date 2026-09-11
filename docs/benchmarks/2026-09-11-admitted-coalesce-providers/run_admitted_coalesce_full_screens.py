"""Matched provider/residency screens; one heavy stage at a time in a capped scope."""
import hashlib,json,subprocess,sys,time
from pathlib import Path
b=Path('.scratch/parallel-aggregate-input');out=b/'admitted-coalesce-full-screens.json';assert not out.exists()
h=json.loads((b/'admitted-coalesce-release-source-hashes.json').read_text())
def sha(p):
 with Path(p).open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
assert all(sha(p)==v for p,v in h.items())
def resources():
 rel=next(x.split(':',2)[2] for x in Path('/proc/self/cgroup').read_text().splitlines() if x.startswith('0::'))
 p=Path('/sys/fs/cgroup')/rel.lstrip('/')
 return {'path':str(p),**{n:(p/n).read_text() for n in ['memory.max','memory.swap.max','memory.peak','memory.events']}}
state={'status':'running','steps':[],'resources_before':resources()}
def save():out.write_text(json.dumps(state,indent=2)+'\n')
save()
for name,script,root in [('providers','run_admitted_coalesce_providers.py','.scratch/public-bench/admitted-coalesce-sf10-providers-01'),('residency','run_admitted_coalesce_residency.py','.scratch/public-bench/admitted-coalesce-residency-32g-01')]:
 cmd=[sys.executable,str(b/script)];step={'name':name,'command':cmd,'status':'running','started_unix':time.time(),'resources_before':resources()};state['steps'].append(step);save()
 with (b/f'admitted-coalesce-{name}.log').open('x') as log:r=subprocess.run(cmd,stdout=log,stderr=subprocess.STDOUT)
 step.update(status='terminal',exit_code=r.returncode,finished_unix=time.time(),resources_after=resources());save();print(name,r.returncode,flush=True)
 assert (Path(root)/'verified-after.json').exists(), 'Stage failed before after-verification'
 assert all(sha(p)==v for p,v in h.items())
state.update(status='terminal',resources_after=resources());save()
raise SystemExit(int(any(s['exit_code'] for s in state['steps'])))
