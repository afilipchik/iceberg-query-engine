"""Archive terminal evidence and prepare an exact commit allowlist; never stage here."""
import hashlib,json,shutil
from pathlib import Path
b=Path('.scratch/parallel-aggregate-input')
state=json.loads((b/'serial-frontier-v2-postchecks.json').read_text());assert state['status']=='terminal'
source=json.loads((b/'serial-frontier-v2-release-source-hashes.json').read_text())
def sha(p):
 with Path(p).open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
assert all(sha(p)==h for p,h in source.items())
assert sha(b/'serial_frontier_v2_benchmark_embedded')==json.loads((b/'serial-frontier-v2-release.json').read_text())['binary_sha256']
def manifest(root,qualification):
 files={str(p.relative_to(root)):sha(p) for p in root.rglob('*') if p.is_file()}
 (root/'manifest.json').write_text(json.dumps({'files':files,'source_inputs':len(source),'qualification':qualification},indent=2)+'\n')
 return len(files)
root=Path('docs/benchmarks/2026-09-15-serial-frontier-v2-residency');root.mkdir(exist_ok=False)
run=Path('.scratch/public-bench/serial-frontier-v2-residency-32g-01');assert (run/'verified-after.json').exists() and (run/'supplemental-validation.json').exists()
for p in run.rglob('*'):
 if p.is_file() and not set(p.relative_to(run).parts)&{'scratch','temp','__pycache__'}:
  q=root/'runs'/p.relative_to(run);q.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(p,q)
for name in ['run_serial_frontier_v2_residency.py','summarize_serial_frontier_v2_residency.py','serial-frontier-v2-release.json','serial-frontier-v2-release-source-hashes.json']:
 shutil.copyfile(b/name,root/name)
print(root,manifest(root,'Canonical32/48GiB capacity screens; custom required GPU smoke is separate. Source snapshot in serial-frontier-v2-validation archive.'))
root=Path('docs/benchmarks/2026-09-15-serial-frontier-v2-controller');root.mkdir(exist_ok=False)
for name in ['run_serial_frontier_v2_cycle.py','run_serial_frontier_v2_postchecks.py','audit_serial_frontier_v2_gate_outcomes.py','finalize_serial_frontier_v2_checkpoint.py','wait_serial_frontier_v2_postchecks.py','summarize_serial_frontier_v2_checkpoint.py']:
 shutil.copyfile(b/name,root/name)
for p in b.glob('serial-frontier-v2-*'):
 if p.is_file() and p.suffix in ('.json','.log'):shutil.copyfile(p,root/p.name)
print(root,manifest(root,'Cycle provenance and single-slot frontier resource-contract repair. Failed initial lifecycle expectations remain in the first validation archive. Preserve all failures; source verification is not full acceptance.'))
roots=[Path('docs/benchmarks')/name for name in ['2026-09-15-serial-frontier-v2-validation','2026-09-15-serial-frontier-v2-sf10','2026-09-15-serial-frontier-v2-attribution','2026-09-15-serial-frontier-v2-residency','2026-09-15-serial-frontier-v2-controller','2026-09-15-serial-frontier-validation','2026-09-15-ipc-dictionary-control']]
paths=[Path(p) for p in ['AGENTS.md', '.claude/epics/realistic-benchmarks-duckdb-leadership/005.md', 'docs/architecture.md', 'docs/README.md', 'docs/serial-frontier-contract-2026-09-15.md', 'docs/ipc-q12-phase-follow-up-2026-09-15.md', 'docs/native-admitted-ipc-design-2026-09-15.md', 'src/storage/ipc_cache.rs', 'src/storage/ipc_cache/projection_tests.rs', 'src/physical/morsel_agg/input_frontier.rs', 'src/physical/morsel_agg/input_frontier/tests.rs', 'tests/fused_aggregate_input_errors.rs']]
for root in roots:
 m=json.loads((root/'manifest.json').read_text())
 for name,h in m['files'].items():
  p=root/name;assert p.resolve().is_relative_to(root.resolve()) and sha(p)==h,p;paths.append(p)
 paths.append(root/'manifest.json')
paths=sorted(set(paths));assert all(p.is_file() and p.stat().st_size<100*1024*1024 for p in paths)
(b/'serial-frontier-v2-commit-paths.bin').write_bytes(b''.join(str(p).encode()+b'\0' for p in paths))
(b/'serial-frontier-v2-commit-payload.json').write_text(json.dumps({'paths':len(paths),'bytes':sum(p.stat().st_size for p in paths),'files':{str(p):sha(p) for p in paths}},indent=2)+'\n')
print('verified',len(paths),'paths')
