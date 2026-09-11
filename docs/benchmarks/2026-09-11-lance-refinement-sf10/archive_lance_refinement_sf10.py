import hashlib,json,shutil
from pathlib import Path
b=Path('.scratch/parallel-aggregate-input');run=Path('.scratch/public-bench/lance-refinement-sf10-providers-01')
state=json.loads((b/'lance-refinement-release-screen.json').read_text());assert state['providers']['status']=='terminal'
audit=b/'lance-refinement-sf10-audit';assert (audit/'verified.json').exists()
root=Path('docs/benchmarks/2026-09-11-lance-refinement-sf10');root.mkdir(exist_ok=False)
for p in run.rglob('*'):
 if p.is_file() and 'scratch' not in p.relative_to(run).parts:
  q=root/'screen'/p.relative_to(run);q.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(p,q)
for p in audit.glob('*.json'):shutil.copyfile(p,root/p.name)
for name in ['lance-refinement-release.json','lance-refinement-release-source-hashes.json','lance-refinement-release-job.json','lance-refinement-release-screen.json','run_lance_refinement_providers.py','run_lance_refinement_release_screen.py','build_lance_refinement.py','audit_lance_refinement_sf10.py','archive_lance_refinement_sf10.py']:
 shutil.copyfile(b/name,root/name)
def sha(p):
 with p.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
files={str(p.relative_to(root)):sha(p) for p in root.rglob('*') if p.is_file()}
(root/'manifest.json').write_text(json.dumps({'files':files,'qualification':'Four provider tracks; failures retained, no new residency run. Validation source snapshot is in adjacent lance-refinement-validation archive.'},indent=2)+'\n')
assert all(sha(root/p)==v for p,v in files.items());print('archive verified',len(files))
