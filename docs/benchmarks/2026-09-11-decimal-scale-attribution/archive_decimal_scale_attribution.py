import hashlib,json,shutil
from pathlib import Path
b=Path('.scratch/parallel-aggregate-input');run=b/'paired-decimal-scale-attribution-01'
assert (run/'verified.json').exists() and (run/'analysis.json').exists()
root=Path('docs/benchmarks/2026-09-11-decimal-scale-attribution');root.mkdir(exist_ok=False)
for p in run.rglob('*'):
 if p.is_file() and not set(p.relative_to(run).parts)&{'temp','validation-scratch'}:
  q=root/'run'/p.relative_to(run);q.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(p,q)
for name in ['paired_decimal_scale_attribution.py','audit_paired_decimal_scale_attribution.py','archive_decimal_scale_attribution.py','bound-utf8-release.json','bound-utf8-release-source-hashes.json','decimal-scale-release.json','decimal-scale-release-source-hashes.json']:
 shutil.copyfile(b/name,root/name)
sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
files={str(p.relative_to(root)):sha(p) for p in root.rglob('*') if p.is_file()}
(root/'manifest.json').write_text(json.dumps({'files':files,'qualification':'Two reversed diagnostic blocks with scan timing enabled; independent typed outputs; not normal performance acceptance.'},indent=2)+'\n')
assert all(sha(root/p)==v for p,v in files.items());print('verified attribution archive',len(files))
