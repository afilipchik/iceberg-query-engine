"""Archive completed residency evidence without transient scratch or cache data."""
import hashlib,json,shutil
from pathlib import Path
b=Path('.scratch/parallel-aggregate-input');run=Path('.scratch/public-bench/bound-utf8-residency-32g-01')
state=json.loads((b/'bound-utf8-residency-cycle.json').read_text());assert all(v['status']=='terminal' for v in state['stages'].values())
assert (run/'verified-after.json').exists() and (run/'supplemental-validation.json').exists()
source=json.loads((b/'bound-utf8-release-source-hashes.json').read_text())
def sha(p):
 with Path(p).open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
assert all(sha(p)==h for p,h in source.items())
root=Path('docs/benchmarks/2026-09-11-bound-utf8-residency');root.mkdir(exist_ok=False)
for p in run.rglob('*'):
 if p.is_file() and not set(p.relative_to(run).parts)&{'scratch','temp','__pycache__'}:
  q=root/'runs'/p.relative_to(run);q.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(p,q)
for name in ['run_bound_utf8_residency.py','summarize_bound_utf8_residency.py','run_bound_utf8_residency_cycle.py','archive_bound_utf8_residency.py','bound-utf8-residency-cycle.json','bound-utf8-residency-screen.log','bound-utf8-residency-audit.log','bound-utf8-residency-driver.log','bound-utf8-release.json','bound-utf8-release-source-hashes.json','bound-utf8-decoded-excess.json']:
 shutil.copyfile(b/name,root/name)
files={str(p.relative_to(root)):sha(p) for p in root.rglob('*') if p.is_file()}
(root/'manifest.json').write_text(json.dumps({'files':files,'source_inputs':len(source),'qualification':'Canonical32/48GiB capacity screens, custom required-device smoke separate; preserve failures. Source snapshot in committed bound-utf8-validation archive.'},indent=2)+'\n')
assert all(sha(root/p)==h for p,h in files.items());print('verified residency archive',len(files),'files',len(source),'source inputs')
