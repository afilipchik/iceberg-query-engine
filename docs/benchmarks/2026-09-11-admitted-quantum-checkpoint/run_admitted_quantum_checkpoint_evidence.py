"""Contain terminal evidence audits; no source edits, commits or pushes."""
import json,subprocess,sys,time
from pathlib import Path
b=Path('.scratch/parallel-aggregate-input');out=b/'admitted-quantum-checkpoint-evidence.json';assert not out.exists()
state={'status':'waiting_for_timing','steps':[]}
def save():out.write_text(json.dumps(state,indent=2)+'\n')
save();deadline=time.monotonic()+7200
while True:
 checkpoint=json.loads((b/'admitted-quantum-checkpoint.json').read_text())
 if checkpoint['status']=='terminal':break
 if time.monotonic()>deadline:
  state['status']='observation_deadline';save();raise SystemExit(1)
 time.sleep(5)
assert json.loads((b/'admitted-quantum-full-screens.json').read_text())['status']=='terminal'
state['status']='auditing';save()
for name,script in [('routes','audit_admitted_quantum_routes.py'),('triage','finish_admitted_quantum_triage.py'),('full','finish_admitted_quantum_full_evidence.py')]:
 cmd=[sys.executable,str(b/script)];step={'name':name,'command':cmd,'status':'running'};state['steps'].append(step);save()
 with (b/f'admitted-quantum-evidence-{name}.log').open('x') as log:r=subprocess.run(cmd,stdout=log,stderr=subprocess.STDOUT)
 step.update(status='terminal',exit_code=r.returncode);save();print(name,r.returncode,flush=True)
state['status']='terminal';save()
raise SystemExit(int(any(s['exit_code'] for s in state['steps'])))
