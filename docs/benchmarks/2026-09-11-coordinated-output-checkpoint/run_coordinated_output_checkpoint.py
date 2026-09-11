"""Contained serial checkpoint measurements; never commit or push automatically."""
import json, subprocess, sys, time, hashlib
from pathlib import Path
b=Path('.scratch/parallel-aggregate-input');out=b/'coordinated-output-checkpoint.json';assert not out.exists()
state={'status':'waiting_for_release','steps':[]}
def save():out.write_text(json.dumps(state,indent=2)+'\n')
save();deadline=time.monotonic()+3600
while not (b/'coordinated-output-release.json').exists():
 job=json.loads((b/'coordinated-output-release-job.json').read_text())
 if job['status']=='failed' or time.monotonic()>deadline:
  state['status']='release_unavailable';save();raise SystemExit(1)
 time.sleep(5)
h=json.loads((b/'coordinated-output-release-source-hashes.json').read_text())
def verify():
 assert len(h)==531
 assert all(hashlib.sha256(Path(p).read_bytes()).hexdigest()==v for p,v in h.items())
verify();state['status']='running';save()
for name,script in [('triage','run_coordinated_output_triage.py'),('full_screens','run_coordinated_output_full_screens.py')]:
 verify();cmd=[sys.executable,str(b/script)]
 step={'name':name,'command':cmd,'status':'running','started_unix':time.time()};state['steps'].append(step);save()
 with (b/f'coordinated-output-{name}.log').open('x') as log:r=subprocess.run(cmd,stdout=log,stderr=subprocess.STDOUT)
 step.update(status='terminal',exit_code=r.returncode,finished_unix=time.time());save();print(name,r.returncode,flush=True)
 verify()
state['status']='terminal';save()
raise SystemExit(int(any(s['exit_code'] for s in state['steps'])))
