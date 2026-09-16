"""Run only inside claude-safe-build; freeze a source-verified release candidate."""
import datetime
import hashlib
import json
from pathlib import Path
import shutil
import subprocess

base = Path('.scratch/parallel-aggregate-input')
prefix = base / 'scalar-decimal-release'
manifest = prefix.with_suffix('.json')
assert not manifest.exists(), 'Never overwrite a frozen candidate'
def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()
def sources():
    paths = set()
    for directory in ['src', 'tests', 'examples', 'benches', 'vendor']:
        paths.update(p for p in Path(directory).rglob('*') if p.is_file())
    paths.update(Path(p) for p in ['Cargo.toml', 'Cargo.lock', 'scripts/claude-safe-build.sh', 'build.rs', '.cargo/config.toml'] if Path(p).exists())
    return {str(p): digest(p) for p in sorted(paths)}
assert (base/'scalar-decimal-validation-verified-after.json').exists()
comparison=json.loads((base/'scalar-decimal-failure-comparison.json').read_text())
assert len(comparison)==8 and not any(v['added'] for v in comparison.values())
before = sources()
assert before == json.loads(Path('.scratch/parallel-aggregate-input/scalar-decimal-validation-source-hashes.json').read_text())
hashes = Path(str(prefix) + '-source-hashes.json')
hashes.write_text(json.dumps(before, indent=2) + '\n')
command = ['cargo', 'build', '--locked', '--offline', '--release', '--features', 'lance,gpu', '--example', 'benchmark_embedded']
started = datetime.datetime.now(datetime.timezone.utc).isoformat()
job = Path(str(prefix) + '-job.json')
job.write_text(json.dumps({'status': 'running', 'command': command, 'started': started}, indent=2) + '\n')
result = subprocess.run(command)
if result.returncode:
    job.write_text(json.dumps({'status': 'failed', 'exit': result.returncode, 'command': command, 'started': started}, indent=2) + '\n')
    raise SystemExit(result.returncode)
assert before == sources(), 'Source changed while compiling; do not certify binary'
binary = base / 'scalar_decimal_benchmark_embedded'
assert not binary.exists()
shutil.copy2('target/release/examples/benchmark_embedded', binary)
record = {'binary_sha256': digest(binary), 'source_hashes_sha256': digest(hashes), 'source_count': len(before), 'build_exit': 0, 'features': ['lance', 'gpu'], 'SAFE_BUILD_MEM': '48G', 'SAFE_BUILD_JOBS': 1, 'locked': True, 'offline': True, 'revision': subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(), 'dirty_tree': True, 'frozen_at': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'command': command}
record['toolchain'] = json.loads((base/'lance-refinement-toolchain.json').read_text())
assert digest(Path(record['toolchain']['protoc_path'])) == record['toolchain']['protoc_sha256']
manifest.write_text(json.dumps(record, indent=2) + '\n')
job.write_text(json.dumps({'status': 'completed', 'exit': 0, 'command': command, 'started': started, 'binary_sha256': record['binary_sha256']}, indent=2) + '\n')
print(json.dumps(record), flush=True)
