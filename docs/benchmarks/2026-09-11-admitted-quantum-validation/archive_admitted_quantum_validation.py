import hashlib,json,shutil,re
from pathlib import Path
b=Path('.scratch/parallel-aggregate-input');o=Path('docs/benchmarks/2026-09-11-admitted-quantum-validation')
sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
h=json.loads((b/'admitted-quantum-validation-source-hashes.json').read_text());assert all(sha(p)==v for p,v in h.items())
assert (b/'admitted-quantum-validation-verified-after.json').exists()
c=json.loads((b/'admitted-quantum-failure-comparison.json').read_text());assert len(c)==8 and not any(v['added'] or v['removed'] for v in c.values())
names=['admitted-quantum-validation.json','admitted-quantum-validation-source-hashes.json','admitted-quantum-validation-verified-after.json','admitted-quantum-validation-resources-before.json','admitted-quantum-validation-resources-after.json','run_admitted_quantum_validation.py','compare_admitted_quantum_failures.py','admitted-quantum-failure-comparison.json','admitted-quantum-green.log','archive_admitted_quantum_validation.py']
for key in c:names+=['admitted-quantum-'+key+'.log','float-dispatch-'+key+'.log']
o.mkdir(exist_ok=False)
for name in names:shutil.copyfile(b/name,o/name)
f={str(p.relative_to(o)):sha(p) for p in o.rglob('*') if p.is_file()}
(o/'manifest.json').write_text(json.dumps({'files':f,'source_inputs':h,'source_archive':'../2026-09-11-admitted-planned-quantum/source.tar.gz'},indent=2)+'\n')
assert all(sha(o/p)==v for p,v in f.items());print({'files_verified':len(f),'source_inputs':len(h)})
