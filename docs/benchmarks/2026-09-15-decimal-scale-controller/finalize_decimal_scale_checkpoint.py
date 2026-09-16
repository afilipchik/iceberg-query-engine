"""Archive terminal evidence and prepare an exact commit allowlist; never stage here."""
import hashlib,json,shutil
from pathlib import Path
b=Path('.scratch/parallel-aggregate-input')
state=json.loads((b/'decimal-scale-postchecks.json').read_text());assert state['status']=='terminal'
source=json.loads((b/'decimal-scale-release-source-hashes.json').read_text())
def sha(p):
 with Path(p).open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
assert all(sha(p)==h for p,h in source.items())
assert sha(b/'decimal_scale_benchmark_embedded')==json.loads((b/'decimal-scale-release.json').read_text())['binary_sha256']
def manifest(root,qualification):
 files={str(p.relative_to(root)):sha(p) for p in root.rglob('*') if p.is_file()}
 (root/'manifest.json').write_text(json.dumps({'files':files,'source_inputs':len(source),'qualification':qualification},indent=2)+'\n')
 return len(files)
root=Path('docs/benchmarks/2026-09-15-decimal-scale-residency');root.mkdir(exist_ok=False)
run=Path('.scratch/public-bench/decimal-scale-residency-32g-01');assert (run/'verified-after.json').exists() and (run/'supplemental-validation.json').exists()
for p in run.rglob('*'):
 if p.is_file() and not set(p.relative_to(run).parts)&{'scratch','temp','__pycache__'}:
  q=root/'runs'/p.relative_to(run);q.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(p,q)
for name in ['run_decimal_scale_residency.py','summarize_decimal_scale_residency.py','decimal-scale-release.json','decimal-scale-release-source-hashes.json']:
 shutil.copyfile(b/name,root/name)
print(root,manifest(root,'Canonical32/48GiB capacity screens; custom required GPU smoke is separate. Source snapshot in decimal-scale-validation archive.'))
root=Path('docs/benchmarks/2026-09-15-decimal-scale-controller');root.mkdir(exist_ok=False)
for name in ['run_decimal_scale_cycle.py','resume_decimal_scale_cycle.py','resume_decimal_scale_validation.py','run_decimal_scale_postchecks.py','audit_decimal_scale_gate_outcomes.py','finalize_decimal_scale_checkpoint.py','decimal-scale-before.rs','decimal-scale-global-candidate.rs','decimal-scale-original.rs','decimal-scale-binding-red-tests.rs']:
 shutil.copyfile(b/name,root/name)
for p in b.glob('decimal-scale-*'):
 if p.is_file() and p.suffix in ('.json','.log'):shutil.copyfile(p,root/p.name)
print(root,manifest(root,'Cycle provenance, interrupted/resumed scopes, rejected global cache and final batch-bound candidate. Preserve all failures; source verification is not full acceptance.'))
roots=[Path('docs/benchmarks')/name for name in ['2026-09-11-aggregate-state-detail','2026-09-11-decimal-scale-validation','2026-09-11-decimal-scale-sf10','2026-09-11-decimal-scale-attribution','2026-09-15-decimal-scale-residency','2026-09-15-decimal-scale-controller']]
paths=[Path(p) for p in ['AGENTS.md','.claude/epics/realistic-benchmarks-duckdb-leadership/005.md','docs/architecture.md','docs/README.md','docs/native-admission-follow-up-2026-09-11.md','docs/bound-utf8-residency-2026-09-11.md','docs/decimal-scale-cache-2026-09-11.md','src/physical/morsel_agg/state_rows.rs','src/physical/morsel_agg/state_rows/array_view.rs','src/physical/morsel_agg/state_rows/batch_binding_tests.rs']]
for root in roots:
 m=json.loads((root/'manifest.json').read_text())
 for name,h in m['files'].items():
  p=root/name;assert p.resolve().is_relative_to(root.resolve()) and sha(p)==h,p;paths.append(p)
 paths.append(root/'manifest.json')
paths=sorted(set(paths));assert all(p.is_file() and p.stat().st_size<100*1024*1024 for p in paths)
(b/'decimal-scale-commit-paths.bin').write_bytes(b''.join(str(p).encode()+b'\0' for p in paths))
(b/'decimal-scale-commit-payload.json').write_text(json.dumps({'paths':len(paths),'bytes':sum(p.stat().st_size for p in paths),'files':{str(p):sha(p) for p in paths}},indent=2)+'\n')
print('verified',len(paths),'paths')
