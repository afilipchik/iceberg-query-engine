"""Archive terminal evidence and prepare an exact commit allowlist; never stage here."""
import hashlib,json,shutil
from pathlib import Path
b=Path('.scratch/parallel-aggregate-input')
state=json.loads((b/'scalar-decimal-postchecks.json').read_text());assert state['status']=='terminal' and state['exit_code']==0
source=json.loads((b/'scalar-decimal-release-source-hashes.json').read_text())
def sha(p):
 with Path(p).open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
assert all(sha(p)==h for p,h in source.items())
assert sha(b/'scalar_decimal_benchmark_embedded')==json.loads((b/'scalar-decimal-release.json').read_text())['binary_sha256']
def manifest(root,qualification):
 files={str(p.relative_to(root)):sha(p) for p in root.rglob('*') if p.is_file()}
 (root/'manifest.json').write_text(json.dumps({'files':files,'source_inputs':len(source),'qualification':qualification},indent=2)+'\n')
 return len(files)
root=Path('docs/benchmarks/2026-09-16-scalar-decimal-residency');root.mkdir(exist_ok=False)
run=Path('.scratch/public-bench/scalar-decimal-residency-32g-01');assert (run/'verified-after.json').exists() and (run/'supplemental-validation.json').exists()
for p in run.rglob('*'):
 if p.is_file() and not set(p.relative_to(run).parts)&{'scratch','temp','__pycache__'}:
  q=root/'runs'/p.relative_to(run);q.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(p,q)
for name in ['run_scalar_decimal_residency.py','summarize_scalar_decimal_residency.py','scalar-decimal-release.json','scalar-decimal-release-source-hashes.json']:
 shutil.copyfile(b/name,root/name)
print(root,manifest(root,'Canonical32/48GiB capacity screens; custom required GPU smoke is separate. Source snapshot in scalar-decimal-validation archive.'))
root=Path('docs/benchmarks/2026-09-16-scalar-decimal-controller');root.mkdir(exist_ok=False)
for name in ['run_scalar_decimal_cycle.py','run_scalar_decimal_postchecks.py','audit_scalar_decimal_gate_outcomes.py','finalize_scalar_decimal_checkpoint.py','wait_scalar_decimal_postchecks.py','summarize_scalar_decimal_checkpoint.py']:
 shutil.copyfile(b/name,root/name)
for p in b.glob('scalar-decimal-*'):
 if p.is_file() and p.suffix in ('.json','.log'):shutil.copyfile(p,root/p.name)
print(root,manifest(root,'Cycle provenance for scalar exact arithmetic and admitted singleton coercion; preserve the initial allocation-target refusal and native stack attribution. Preserve all failures; source verification is not full acceptance.'))
roots=[Path('docs/benchmarks')/name for name in ['2026-09-16-scalar-decimal-validation','2026-09-16-scalar-decimal-sf10','2026-09-16-scalar-decimal-attribution','2026-09-16-scalar-decimal-residency','2026-09-16-scalar-decimal-controller','2026-09-15-serial-frontier-native-profile','2026-09-15-scalar-decimal-allocation-red']]
paths=[Path(p) for p in ['AGENTS.md', '.claude/epics/realistic-benchmarks-duckdb-leadership/005.md', 'docs/architecture.md', 'docs/README.md', 'docs/scalar-decimal-arithmetic-2026-09-15.md', 'docs/native-aggregate-profile-2026-09-15.md', 'src/physical/operators/filter.rs', 'src/physical/operators/filter/scalar_arithmetic.rs', 'src/physical/operators/filter/scalar_arithmetic_tests.rs', 'src/planner/numeric.rs', 'src/planner/reserved_decimal.rs']]
for root in roots:
 m=json.loads((root/'manifest.json').read_text())
 for name,h in m['files'].items():
  p=root/name;assert p.resolve().is_relative_to(root.resolve()) and sha(p)==h,p;paths.append(p)
 paths.append(root/'manifest.json')
paths=sorted(set(paths));assert all(p.is_file() and p.stat().st_size<100*1024*1024 for p in paths)
(b/'scalar-decimal-commit-paths.bin').write_bytes(b''.join(str(p).encode()+b'\0' for p in paths))
(b/'scalar-decimal-commit-payload.json').write_text(json.dumps({'paths':len(paths),'bytes':sum(p.stat().st_size for p in paths),'files':{str(p):sha(p) for p in paths}},indent=2)+'\n')
print('verified',len(paths),'paths')
