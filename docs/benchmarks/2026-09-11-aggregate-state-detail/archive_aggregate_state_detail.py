import hashlib,json,shutil
from pathlib import Path
b=Path('.scratch/parallel-aggregate-input');src=b/'aggregate-state-detail-01'
assert json.loads((src/'verified.json').read_text())=={'binary_unchanged':True,'typed_outputs':6,'cases':2}
root=Path('docs/benchmarks/2026-09-11-aggregate-state-detail');root.mkdir(exist_ok=False)
for p in src.rglob('*'):
 if p.is_file() and not set(p.relative_to(src).parts)&{'temp','validation-scratch','__pycache__'}:
  q=root/'runs'/p.relative_to(src);q.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(p,q)
for name in ['profile_aggregate_state_detail.py','aggregate-state-detail-driver.log','bound-utf8-release.json','bound-utf8-release-source-hashes.json','archive_aggregate_state_detail.py']:
 shutil.copyfile(b/name,root/name)
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
files={str(p.relative_to(root)):sha(p) for p in root.rglob('*') if p.is_file()}
(root/'manifest.json').write_text(json.dumps({'files':files,'qualification':'Unchanged59619bde diagnostic,6typedoutputs,800input snapshot retained in bound-utf8-validation archive. Subsequent decimal tests are not part of this frozen binary.'},indent=2)+'\n')
assert all(sha(root/p)==h for p,h in files.items());print(len(files),'files verified')
