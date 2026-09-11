"""Prepare an exact verified allowlist only after all cycle stages are terminal."""
import hashlib,json,shutil
from pathlib import Path
b=Path('.scratch/parallel-aggregate-input')
state=json.loads((b/'bound-utf8-postchecks.json').read_text());assert state['status']=='terminal' and state['exit_code']==0 and state['source_inputs_verified']==800
root=Path('docs/benchmarks/2026-09-11-bound-utf8-postchecks');root.mkdir(exist_ok=False)
for name in ['bound-utf8-postchecks.json','run_bound_utf8_postchecks.py','bound-utf8-postchecks-driver.log','bound-utf8-cycle.json','run_bound_utf8_cycle.py','bound-utf8-cycle-driver.log','bound-utf8-cycle-validation.log','bound-utf8-cycle-comparison.log','bound-utf8-cycle-archive_validation.log','bound-utf8-cycle-release_screen.log','prepare_bound_utf8_checkpoint.py']:
 shutil.copyfile(b/name,root/name)
for p in b.glob('bound-utf8-post-*.log'):shutil.copyfile(p,root/p.name)
def sha(p):
 with Path(p).open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
files={str(p.relative_to(root)):sha(p) for p in root.rglob('*') if p.is_file()}
(root/'manifest.json').write_text(json.dumps({'files':files,'qualification':'Terminal cycle/postcheck controller provenance; cumulative cgroup peaks, not per-query memory.'},indent=2)+'\n')
roots=[Path('docs/benchmarks')/('2026-09-11-'+name) for name in ['shared-aggregate-profile','aggregate-routing-phases','bound-utf8-validation','bound-utf8-sf10','bound-utf8-attribution','bound-utf8-postchecks','native-q12-bound-utf8']]
paths=[Path(p) for p in ['AGENTS.md','.claude/epics/realistic-benchmarks-duckdb-leadership/005.md','docs/architecture.md','docs/shared-aggregate-profile-2026-09-11.md','docs/bound-utf8-aggregate-keys-2026-09-11.md','src/physical/morsel_agg/key_rows/bound_arrays.rs','src/physical/morsel_agg/parallel_controllers.rs']]
for root in roots:
 manifest=json.loads((root/'manifest.json').read_text())
 for name,digest in manifest['files'].items():
  p=root/name;assert p.resolve().is_relative_to(root.resolve()) and sha(p)==digest,p
  paths.append(p)
 paths.append(root/'manifest.json')
paths=sorted(set(paths));assert all(p.is_file() and p.stat().st_size<100*1024*1024 for p in paths)
(b/'bound-utf8-commit-paths.bin').write_bytes(b''.join(str(p).encode()+b'\0' for p in paths))
(b/'bound-utf8-commit-payload.json').write_text(json.dumps({'paths':len(paths),'bytes':sum(p.stat().st_size for p in paths),'files':{str(p):sha(p) for p in paths}},indent=2)+'\n')
print('verified checkpoint payload',len(paths),'paths')
