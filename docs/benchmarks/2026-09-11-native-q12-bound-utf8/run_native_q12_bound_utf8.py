import hashlib,json,subprocess,sys,time,shutil
from pathlib import Path
b=Path('.scratch/parallel-aggregate-input');out=b/'native-q12-bound-utf8-controller.json';assert not out.exists()
state={'status':'waiting_postchecks','postcheck_session':50559,'started':time.time(),'stages':{}};out.write_text(json.dumps(state,indent=2))
while True:
 prior=json.loads((b/'bound-utf8-postchecks.json').read_text())
 for v in prior['stages'].values():
  if v['status']=='terminal':assert v['exit_code']==0,v
 if prior['status']=='terminal':break
 assert time.time()-state['started']<1800,'Inspect existing postcheck, never restart automatically'
 time.sleep(10)
for stage,script in [('paired','paired_native_q12_bound_utf8.py'),('audit','audit_native_q12_bound_utf8.py')]:
 command=[sys.executable,str(b/script)];state['status']='running';state['stages'][stage]={'status':'running','command':command,'started':time.time(),'driver_sha256':hashlib.sha256((b/script).read_bytes()).hexdigest()};out.write_text(json.dumps(state,indent=2))
 with (b/f'native-q12-bound-utf8-{stage}.log').open('x') as log:r=subprocess.run(command,stdout=log,stderr=subprocess.STDOUT)
 state['stages'][stage].update(status='terminal',exit_code=r.returncode,finished=time.time());out.write_text(json.dumps(state,indent=2));assert r.returncode==0,(stage,r.returncode)
source=json.loads((b/'bound-utf8-release-source-hashes.json').read_text());assert all(hashlib.sha256(Path(p).read_bytes()).hexdigest()==h for p,h in source.items());state.update(status='terminal',source_inputs_verified=len(source));out.write_text(json.dumps(state,indent=2))
root=Path('docs/benchmarks/2026-09-11-native-q12-bound-utf8');root.mkdir(exist_ok=False);run=b/'native-q12-bound-utf8-01'
for p in run.rglob('*'):
 if p.is_file() and not set(p.relative_to(run).parts)&{'temp','validation-scratch'}:
  q=root/'run'/p.relative_to(run);q.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(p,q)
for name in ['run_native_q12_bound_utf8.py','paired_native_q12_bound_utf8.py','audit_native_q12_bound_utf8.py','native-q12-bound-utf8-controller.json','native-q12-bound-utf8-paired.log','native-q12-bound-utf8-audit.log','lance-refinement-release.json','bound-utf8-release.json']:
 shutil.copyfile(b/name,root/name)
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest();files={str(p.relative_to(root)):sha(p) for p in root.rglob('*') if p.is_file()};(root/'manifest.json').write_text(json.dumps({'files':files,'qualification':'Fresh native workers,2reversed blocks, original SF10 timeout retained. Source snapshot in adjacent bound-utf8-validation archive.'},indent=2)+'\n');assert all(sha(root/p)==h for p,h in files.items());print('verified native Q12 archive',len(files))
