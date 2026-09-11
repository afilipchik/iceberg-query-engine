import hashlib,json,shutil
from pathlib import Path
b=Path('.scratch/parallel-aggregate-input');run=b/'lance-refinement-endurance-01'
assert (run/'workers.json').exists() and (run/'resources.json').exists() and (run/'validation.json').exists()
root=Path('docs/benchmarks/2026-09-11-lance-refinement-endurance');root.mkdir(exist_ok=False)
for p in run.iterdir():
 if p.is_file():shutil.copyfile(p,root/('worker-manifest.json' if p.name=='manifest.json' else p.name))
for name in ['engine_lance_refinement_endurance.py','audit_lance_refinement_endurance.py','archive_lance_refinement_endurance.py','lance-refinement-release.json','lance-refinement-release-source-hashes.json','run_lance_refinement_postchecks.py','lance-refinement-postchecks.json','lance-refinement-postchecks-driver.log','lance-refinement-post-sf10_audit.log','lance-refinement-post-paired.log','lance-refinement-post-paired_audit.log','lance-refinement-post-endurance.log','lance-refinement-post-endurance_audit.log']:
 shutil.copyfile(b/name,root/name)
sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
files={str(p.relative_to(root)):sha(p) for p in root.rglob('*') if p.is_file()}
(root/'manifest.json').write_text(json.dumps({'files':files,'qualification':'Default allocator, two recorded SF10 engine sequences in one worker; no reference interleaving, 180s diagnostic ceiling; not a matched performance comparison.'},indent=2)+'\n')
assert all(sha(root/p)==v for p,v in files.items())
print('verified endurance archive',len(files),flush=True)
