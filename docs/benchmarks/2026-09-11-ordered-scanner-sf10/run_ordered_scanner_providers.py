"""Run sequentially inside the required capped wrapper; preserve all failures."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

base = Path('.scratch/parallel-aggregate-input')
root = Path('.scratch/public-bench/ordered-scanner-sf10-providers-01')
root.mkdir(exist_ok=False)
binary = base / 'ordered_scanner_benchmark_embedded'
release = json.loads((base / 'ordered-scanner-release.json').read_text())
assert hashlib.sha256(binary.read_bytes()).hexdigest() == release['binary_sha256']
hashes = json.loads((base / 'ordered-scanner-release-source-hashes.json').read_text())
assert all(hashlib.sha256(Path(p).read_bytes()).hexdigest() == h for p,h in hashes.items())
env = {k:v for k,v in os.environ.items() if not k.startswith('QE_') and not k.endswith(('_PROF','_DEBUG')) and k not in ['RT_DISABLE','QUERY_ENGINE_ALLOW_THP']}
env.update(QE_AGG_OWNERSHIP='disjoint', QE_GPU='0', QE_GPU_DEBUG='0')
assert sorted(os.sched_getaffinity(0)) == list(range(16))
harness = {str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in Path('scripts/benchmark').glob('*.py')}
driver_hash = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
(root/'experiment.json').write_text(json.dumps({'candidate':release,'source_inputs':hashes,'driver_sha256':driver_hash,'harness_sha256':harness,'ownership':'disjoint','gpu':False,'affinity':sorted(os.sched_getaffinity(0)),'qualification':'one-session provider screen; explicit reference-only Lance I/O quota16; not complete leadership certification','prespecified_tracks':['raw_parquet','native','iceberg','lance'],'samples':3,'sessions':1,'threads':16,'query_gib':4,'process_gib':12},indent=2)+'\n')
records = {}
for track in ['raw_parquet', 'native', 'iceberg', 'lance']:
    command = [sys.executable, '-m', 'benchmark', 'run', '--dataset', '.scratch/public-bench/tpch-sf10/dataset.json',
               '--engine-binary', str(binary), '--output', str(root / track), '--track', track,
               '--samples', '3', '--sessions', '1', '--threads', '16', '--memory-gib', '4', '--process-cap-gib', '12']
    if track != 'raw_parquet':
        command += ['--provider-manifest', f'.scratch/public-bench/canonical-sf10-providers-02/{track}/provider.json']
    if track == 'lance':
        command += ['--reference-lance-io-limit', '16']
    record = {'command': command, 'started_unix': time.time(), 'status': 'running'}
    records[track] = record
    (root / 'state.json').write_text(json.dumps(records, indent=2) + '\n')
    with (root / f'{track}.log').open('w') as log:
        result = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, env=env)
    record.update(status='terminal', exit_code=result.returncode, finished_unix=time.time())
    report = root / track / 'report.json'
    if report.exists():
        record['report'] = json.loads(report.read_text())
    (root / 'state.json').write_text(json.dumps(records, indent=2) + '\n')
    print(json.dumps({'track': track, 'exit_code': result.returncode, 'report': record.get('report')}), flush=True)
assert all(hashlib.sha256(Path(p).read_bytes()).hexdigest() == h for p,h in hashes.items())
assert hashlib.sha256(binary.read_bytes()).hexdigest() == release['binary_sha256']
assert hashlib.sha256(Path(__file__).read_bytes()).hexdigest()==driver_hash
assert all(hashlib.sha256(Path(p).read_bytes()).hexdigest()==v for p,v in harness.items())
(root/'verified-after.json').write_text(json.dumps({'source_inputs':len(hashes),'binary_sha256':release['binary_sha256']})+'\n')
raise SystemExit(0 if all(r['exit_code'] == 0 for r in records.values()) else 1)
