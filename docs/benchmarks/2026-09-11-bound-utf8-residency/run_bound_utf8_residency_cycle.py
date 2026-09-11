"""After the intermediary push, run residency then audit inside one capped scope."""
import hashlib,json,subprocess,sys,time
from pathlib import Path
b=Path('.scratch/parallel-aggregate-input');out=b/'bound-utf8-residency-cycle.json';assert not out.exists()
assert json.loads((b/'bound-utf8-postchecks.json').read_text())['status']=='terminal'
assert json.loads((b/'native-q12-bound-utf8-controller.json').read_text())['status']=='terminal'
assert subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip()=='8c8989965d242b0a9728993b16f177392bab5ad3'
def resources():
 cg=Path('/sys/fs/cgroup')/next(x.split('::',1)[1] for x in Path('/proc/self/cgroup').read_text().splitlines() if '::' in x).lstrip('/')
 return {n:(cg/n).read_text() for n in ['memory.max','memory.swap.max','memory.peak','memory.events']}
state={'pushed_checkpoint':'8c8989965d242b0a9728993b16f177392bab5ad3','stages':{},'qualification':'Sequential64GiB scope. Canonical32/48GiB capacity experiment does not clear16GiB preload; custom required GPU smoke is not SF10.'}
for stage,script in [('screen','run_bound_utf8_residency.py'),('audit','summarize_bound_utf8_residency.py')]:
 command=[sys.executable,str(b/script)];state['stages'][stage]={'status':'running','command':command,'started':time.time(),'before':resources(),'driver_sha256':hashlib.sha256((b/script).read_bytes()).hexdigest()};out.write_text(json.dumps(state,indent=2))
 with (b/f'bound-utf8-residency-{stage}.log').open('x') as log:r=subprocess.run(command,stdout=log,stderr=subprocess.STDOUT)
 state['stages'][stage].update(status='terminal',exit_code=r.returncode,finished=time.time(),after=resources());out.write_text(json.dumps(state,indent=2));print(stage,r.returncode,flush=True)
 if stage=='screen':assert (Path('.scratch/public-bench/bound-utf8-residency-32g-01')/'verified-after.json').exists(),'Screen did not complete all cases/source verification'
 elif r.returncode:raise SystemExit(r.returncode)
state['status']='terminal';state['exit_code']=state['stages']['screen']['exit_code'];out.write_text(json.dumps(state,indent=2));raise SystemExit(state['exit_code'])
