"""Run only inside claude-safe-build: extended-deadline diagnostics, not acceptance."""
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
from benchmark.compare import compare_files
from benchmark.run import verify_dataset
from benchmark.providers import provider_setup

root = Path('.scratch/parallel-aggregate-input').resolve()
output = root / 'admitted-coalesce-paired-01'
output.mkdir(exist_ok=False)
binary = root / 'admitted_coalesce_benchmark_embedded'
release = json.loads((root / 'admitted-coalesce-release.json').read_text())
def digest(p):
    return hashlib.sha256(p.read_bytes()).hexdigest()
assert digest(binary) == release['binary_sha256']
hashes = json.loads((root / 'admitted-coalesce-release-source-hashes.json').read_text())
assert all(digest(Path(p)) == h for p, h in hashes.items())
dataset_path = Path('.scratch/public-bench/tpch-sf10/dataset.json').resolve()
dataset = verify_dataset(dataset_path)
assert sorted(os.sched_getaffinity(0)) == list(range(16))
cases = [
    ('raw_parquet_generic', Path('.scratch/public-bench/dictionary-chunks-sf10-providers-01/raw_parquet')),
    ('raw_parquet', Path('.scratch/public-bench/dictionary-chunks-sf10-providers-01/raw_parquet')),
    ('native', Path('.scratch/public-bench/dictionary-chunks-sf10-providers-01/native')),
    ('cpu_resident_4t_32g', Path('.scratch/public-bench/dictionary-chunks-residency-32g-01/canonical_gpu_control')),
]
manifest = {'purpose': 'Shared compiled numeric comparison candidate; default disjoint ownership; not acceptance or performance comparison',
            'release': release, 'dataset_sha256': digest(dataset_path),
            'query_ids': ['q06', 'q09'], 'requests_per_query': 1,
            'affinity': sorted(os.sched_getaffinity(0)), 'watchdog_seconds': 180,
            'source_hashes': hashes, 'driver_sha256':digest(Path(__file__)), 'harness_sha256':{str(p):digest(p) for p in Path('scripts/benchmark').glob('*.py')}, 'ownership':'disjoint', 'cases': {}}
providers = [('native', Path('.scratch/public-bench/canonical-sf10-providers-02/native/provider.json')),
             ('decoded_ipc', Path('.scratch/public-bench/canonical-sf10-providers-01/decoded_ipc/provider.json'))]
verified_tables = {}
manifest['providers'] = {}
for track, path in providers:
    tables, provenance = provider_setup(dataset_path, dataset, track, path)
    verified_tables[track] = tables
    manifest['providers'][track] = provenance
cgroup = Path('/sys/fs/cgroup') / next(line.split('::', 1)[1].lstrip('/') for line in Path('/proc/self/cgroup').read_text().splitlines() if '::' in line)
def resources():
    return {'path': str(cgroup), **{name: (cgroup / name).read_text() for name in ['memory.max', 'memory.swap.max', 'memory.peak', 'memory.events']}}
assert int(resources()['memory.max']) == 48 * 1024**3
assert int(resources()['memory.swap.max']) == 0
(output / 'resources-before.json').write_text(json.dumps(resources(), indent=2) + '\n')
summary = []
control = root / 'admitted_quantum_benchmark_embedded'
control_release = json.loads((root / 'admitted-quantum-release.json').read_text())
assert digest(control) == control_release['binary_sha256']
manifest['control_release'] = control_release
baseline = root / 'incremental_header_benchmark_embedded'
baseline_release = json.loads((root/'incremental-header-release.json').read_text())
assert digest(baseline)==baseline_release['binary_sha256']
manifest['baseline_release']=baseline_release
manifest['purpose']='Three frozen binaries: baseline13210a20, control beb0cdfd, bounded filtered output candidate; reverse-order Q6/Q9 raw/native/resident4 diagnostic; no acceptance claim'
trials = [(block,label,mode,previous) for block,order in [(1,['baseline','control','candidate']),(2,['candidate','control','baseline'])] for mode,previous in cases for label in order]
for block, label, mode, previous in trials:
    setup = json.loads((previous / 'setup.json').read_text())
    setup['threads'] = 4 if mode == 'cpu_resident_4t_32g' else 16
    if not mode.startswith('raw_parquet'):
        assert setup['tables'] == verified_tables['native' if mode == 'native' else 'decoded_ipc']
    case = output / f'b{block}-{label}-{mode}'
    case.mkdir()
    setup['temp_directory'] = str(case / 'temp')
    (case / 'temp').mkdir()
    setup_path = case / 'setup.json'
    setup_path.write_text(json.dumps(setup, indent=2) + '\n')
    manifest['cases'][case.name] = {'setup': setup, 'oracle_parent': str(previous), 'setup_sha256': digest(setup_path)}
    (output / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    env = {k: v for k, v in os.environ.items() if not k.startswith('QE_') and not k.endswith(('_PROF', '_DEBUG')) and k not in ['RT_DISABLE', 'QUERY_ENGINE_ALLOW_THP']}
    env.update(QE_GPU='0', QE_GPU_DEBUG='0', QE_AGG_OWNERSHIP='disjoint')
    if mode == 'raw_parquet_generic': env['QE_MORSEL']='0'
    for key in ['QE_AGG_PROF', 'QE_AGG_DETAIL_PROF', 'HJ_PROF', 'HJ_TIMING', 'QE_INPUT_QUEUE_TRACE']:
        env.pop(key, None)
    env['QE_JOIN_STREAM_PROF'] = '1'
    env['QE_AGG_PROF'] = '1'
    env['QE_INPUT_QUEUE_TRACE'] = '1'
    env['TMPDIR'] = str(case / 'temp')
    env['QE_MEM_CAP'] = str(setup['process_cap_bytes'])
    for query_id in (['q06'] if mode == 'raw_parquet_generic' else manifest['query_ids']):
        query = next(q for q in dataset['queries'] if q['id'] == query_id)
        sql = (dataset_path.parent / query['sql_path']).read_text()
        requests = [{'id': f'{query_id}-{i}', 'sql': sql, 'output': str(case / f'{query_id}-{i}.arrow')} for i in range(1)]
        payload = ''.join(json.dumps(r) + '\n' for r in requests)
        (case / f'{query_id}-requests.jsonl').write_text(payload)
        command = ['/usr/bin/time', '-v', '-o', str(case / f'{query_id}.time'), 'taskset', '-c', '0-3' if mode == 'cpu_resident_4t_32g' else '0-15', str({'baseline':baseline,'control':control,'candidate':binary}[label]), str(setup_path)]
        process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                   text=True, env=env, start_new_session=True)
        timed_out = False
        try:
            stdout, stderr = process.communicate(payload, timeout=180)
        except subprocess.TimeoutExpired:
            timed_out = True
            os.killpg(process.pid, signal.SIGKILL)
            stdout, stderr = process.communicate()
        (case / f'{query_id}-responses.jsonl').write_text(stdout)
        (case / f'{query_id}.stderr').write_text(stderr)
        responses = [json.loads(line) for line in stdout.splitlines()]
        completed = [r for r in responses if r.get('status') == 'completed']
        oracle = previous / f's1-{query_id}-oracle.arrow'
        comparisons = [compare_files(r['output'], oracle, scratch=case, policy=query['result_policy'],
                                    order_by=query['order_by'], limit_rows=query.get('limit_rows'),
                                    expected_is_complete=True) for r in completed]
        traces = [json.loads(line.removeprefix('[join-stream] ')) for line in stderr.splitlines() if line.startswith('[join-stream] ')]
        record = {'mode': mode, 'block': block, 'label': label, 'case': case.name, 'query': query_id, 'command': command, 'exit': process.returncode,
                  'timeout': timed_out, 'sql_sha256': hashlib.sha256(sql.encode()).hexdigest(),
                  'oracle_sha256': digest(oracle), 'comparisons': comparisons, 'traces': traces,
                  'instrumented_ms': [r['ms'] for r in completed], 'responses': responses}
        summary.append(record)
        (output / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
        print(block, label, mode, query_id, 'exit', process.returncode, 'typed', [c['ok'] for c in comparisons], 'ms', record['instrumented_ms'], flush=True)
        assert all(c['ok'] for c in comparisons), 'Typed output mismatch'
        if mode in ['raw_parquet', 'raw_parquet_generic'] and query_id in ['q01','q06']:
            assert all(('MorselAggregate' in r['physical_plan']) == (mode == 'raw_parquet') for r in completed), 'Unexpected aggregate route'
        assert 'panicked at' not in stderr, 'Unexpected worker panic'
        assert all(r.get('gpu_before') is None and r.get('gpu_after') is None for r in completed), 'Unexpected GPU runtime'
        assert not timed_out and process.returncode == 0 and len(completed) == 1, 'Diagnostic incomplete'
assert len(summary) == 42, 'Incomplete diagnostic matrix'
assert digest(control) == control_release['binary_sha256']
assert digest(baseline) == baseline_release['binary_sha256']
verify_dataset(dataset_path)
for track, path in providers:
    provider_setup(dataset_path, dataset, track, path)
assert digest(binary) == release['binary_sha256']
assert all(digest(Path(p)) == h for p, h in hashes.items())
assert digest(Path(__file__))==manifest['driver_sha256']
assert all(digest(Path(p))==h for p,h in manifest['harness_sha256'].items())
(output / 'verified-after.json').write_text(json.dumps({'dataset': True, 'binary': True, 'source_inputs': len(hashes)}) + '\n')

(output / 'resources-after.json').write_text(json.dumps(resources(), indent=2) + '\n')
