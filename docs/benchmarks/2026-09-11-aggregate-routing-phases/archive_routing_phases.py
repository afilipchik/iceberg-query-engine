import hashlib,json,shutil,tarfile
from pathlib import Path
b=Path('.scratch/parallel-aggregate-input');run=b/'routing-phase-profile-01'
controller=json.loads((b/'routing-phase-controller.json').read_text());assert controller['status']=='terminal' and controller['exit_code']==0
assert json.loads((run/'verified.json').read_text())['typed_outputs']==12
source=json.loads((b/'aggregate-routing-profile-release-source-hashes.json').read_text())
sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
assert all(sha(p)==h for p,h in source.items())
root=Path('docs/benchmarks/2026-09-11-aggregate-routing-phases');root.mkdir(exist_ok=False)
for p in run.rglob('*'):
 if p.is_file() and not set(p.relative_to(run).parts)&{'temp','validation-scratch'}:
  q=root/'run'/p.relative_to(run);q.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(p,q)
for name in ['archive_routing_phases.py','profile_routing_phases.py','build_aggregate_routing_profile.py','run_routing_profile_after_build.py','routing-phase-controller.json','routing-phase-controller.log','routing-phase-profile-driver.log','aggregate-routing-profile-build.log','aggregate-routing-profile-tests.log','aggregate-routing-profile-release.json','aggregate-routing-profile-release-job.json','aggregate-routing-profile-release-source-hashes.json']:
 shutil.copyfile(b/name,root/name)
with tarfile.open(root/'source.tar.gz','w:gz') as out:
 for p in source:out.add(p,arcname=p,recursive=False)
with tarfile.open(root/'source.tar.gz') as src:
 assert all(hashlib.sha256(src.extractfile(p).read()).hexdigest()==h for p,h in source.items())
files={str(p.relative_to(root)):sha(p) for p in root.rglob('*') if p.is_file()}
(root/'manifest.json').write_text(json.dumps({'files':files,'source_inputs':len(source),'qualification':'Instrumented frozen diagnostic, one warmup/two samples per shape, not performance acceptance.'},indent=2)+'\n')
assert all(sha(root/p)==h for p,h in files.items());print('verified routing phase archive',len(files),'files',len(source),'inputs')
