import hashlib,json,shutil,tarfile
from pathlib import Path
b=Path('.scratch/parallel-aggregate-input')
root=Path('docs/benchmarks/2026-09-15-serial-frontier-v2-validation');root.mkdir(exist_ok=False)
hashes=json.loads((b/'serial-frontier-v2-validation-source-hashes.json').read_text())
sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
assert all(sha(p)==v for p,v in hashes.items())
assert (b/'serial-frontier-v2-validation-verified-after.json').exists()
with tarfile.open(root/'source.tar.gz','w:gz') as tar:
 for p in hashes:tar.add(p,arcname=p)
with tarfile.open(root/'source.tar.gz') as archive:
 assert all(hashlib.sha256(archive.extractfile(p).read()).hexdigest()==v for p,v in hashes.items())
for p in b.glob('serial-frontier-v2-*'):
 if p.is_file() and (p.suffix in ('.log','.json')) and 'release' not in p.name:shutil.copyfile(p,root/p.name)
for name in ['run_serial_frontier_v2_validation.py','compare_serial_frontier_v2_failures.py','archive_serial_frontier_v2_validation.py']:
 shutil.copyfile(b/name,root/name)
for pattern in ['unknown-frontier-*', 'fused_aggregate_input_errors_*.rs', 'serial-frontier-error-lifecycle-green.log']:
 for p in b.glob(pattern):
  if p.is_file():shutil.copyfile(p,root/p.name)
files={str(p.relative_to(root)):sha(p) for p in root.rglob('*') if p.is_file()}
(root/'manifest.json').write_text(json.dumps({'files':files,'source_inputs':len(hashes)},indent=2)+'\n')
assert all(sha(root/p)==v for p,v in files.items())
print('verified validation archive',len(files),flush=True)
