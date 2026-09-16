"""Paired canonical CPU development screen; run through the memory-capped wrapper."""
import argparse
import json
import math
import os
from pathlib import Path
import resource
import statistics
import sys
import time

from benchmark.contract import TIMING, digest, require
from benchmark.providers import provider_setup
from benchmark.run import Worker, compare_execution, containment, memory_events, verify_dataset

TRACKS = ('raw_parquet', 'decoded_ipc', 'native', 'iceberg', 'lance')
GIB = 1024**3


def write(path, value):
    path.write_text(json.dumps(value, indent=2, allow_nan=False) + '\n')


class BoundaryWorker(Worker):
    """Keep protocol boundaries without sampling/profiling during latency."""
    def read(self, seconds):
        value = super().read(seconds)
        self.events.append({'received_monotonic': time.monotonic(),
                            'command': self.command, 'response': value})
        return value


def valid_ms(response):
    ms = response.get('ms')
    return (response.get('status') == 'completed' and type(ms) in (int, float)
            and math.isfinite(ms) and ms > 0)


def pair_order(iteration, ordinal, execution_offset):
    """Execution order is independent of binary identity and startup order."""
    return ('before', 'after') if (iteration + ordinal + execution_offset) % 2 == 0 else ('after', 'before')


def summarize_case(result, count):
    rows = result['attempts']
    expected = {(side, i) for side in ('before', 'after') for i in range(count + 1)}
    observed = [(r['side'], r['iteration']) for r in rows]
    complete = (count > 0 and not result.get('error') and len(observed) == len(expected)
                and set(observed) == expected
                and all(type(r['iteration']) is int and r['ok'] and type(r['warmup']) is bool and r['warmup'] == (r['iteration'] == 0)
                        and valid_ms(r['response']) for r in rows))
    summary = {'complete': complete, 'requested_pairs': count,
               'valid_steady_pairs': sum(all(any(r['side'] == side and r['iteration'] == i and r['ok']
                                                for r in rows) for side in ('before', 'after'))
                                         for i in range(1, count+1)),
               'before_median_ms': None, 'after_median_ms': None, 'after_over_before': None}
    if complete:
        for side in ('before', 'after'):
            summary[side+'_median_ms'] = statistics.median(r['response']['ms'] for r in rows
                                                         if r['side'] == side and not r['warmup'])
        for side in ('before', 'after'):
            values = [r['response']['ms'] for r in rows if r['side'] == side and not r['warmup']]
            summary[side+'_mean_ms'] = statistics.mean(values)
            summary[side+'_total_ms'] = sum(values)
            summary[side+'_max_ms'] = max(values)
        summary['mean_after_over_before'] = summary['after_mean_ms']/summary['before_mean_ms']
        summary['after_over_before'] = summary['after_median_ms']/summary['before_median_ms']
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--track', choices=TRACKS, required=True)
    parser.add_argument('--queries', nargs='+', help='explicit canonical IDs; default all 22 in manifest order')
    parser.add_argument('--samples', type=int, default=3, help='steady pairs, plus one warmup per side')
    parser.add_argument('--dataset', default='.scratch/public-bench/tpch-sf10/dataset.json')
    parser.add_argument('--provider-manifest', help='validated matching conversion; required except raw')
    parser.add_argument('--before-binary', required=True)
    parser.add_argument('--after-binary', required=True)
    parser.add_argument('--expected-before-sha256', required=True)
    parser.add_argument('--expected-after-sha256', required=True)
    parser.add_argument('--execution-offset', type=int, choices=(0, 1), default=0,
                        help='independently reverse pair execution order; balance both offsets across sessions')
    parser.add_argument('--startup-order', choices=('before-first', 'after-first'), default='before-first')
    parser.add_argument('--output', required=True, help='fresh directory under repository .scratch')
    args = parser.parse_args()
    require(args.samples > 0, 'positive sample count required')
    require(len(args.expected_after_sha256) == 64 and all(c in '0123456789abcdef' for c in args.expected_after_sha256),
            'explicit lowercase candidate SHA256 required')
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    capped = containment()
    require(int(capped['memory.max']) == 96*GIB, 'launch in the requested 96 GiB scope')
    require(sorted(os.sched_getaffinity(0)) == list(range(16)), 'launch with taskset -c 0-15')
    root = Path(args.output).resolve()
    require(root.is_relative_to(Path('.scratch').resolve()), 'output must be inside repository .scratch')
    root.mkdir(parents=True, exist_ok=False)
    binaries = {'before': Path(args.before_binary).resolve(), 'after': Path(args.after_binary).resolve()}
    expected = {'before': args.expected_before_sha256, 'after': args.expected_after_sha256}
    provenance = {'purpose': 'paired development screen, not full multi-session acceptance',
                  'command': sys.argv, 'python': sys.executable, 'cwd': str(Path.cwd()),
                  'timing_boundary': TIMING, 'track': args.track,
                  'threads': 16, 'affinity': sorted(os.sched_getaffinity(0)),
                  'query_memory_bytes': 40*GIB, 'process_cap_bytes': 48*GIB,
                  'containment': capped, 'scope_before': memory_events(capped),
                  'binary_paths': {k: str(v) for k,v in binaries.items()}, 'expected_binary_sha256': expected,
                  'driver_sha256': digest(__file__),
                  'harness_sha256': {p.name: digest(p) for p in sorted(Path('scripts/benchmark').glob('*.py'))},
                  'profiling_enabled': False, 'created_unix': time.time(),
                  'cache_policy': 'warm host; one gated warmup excluded from steady summaries per fresh per-query engine worker',
                  'startup_order': args.startup_order,
                  'execution_offset': args.execution_offset,
                  'ordering': 'alternate before/after per pair, reverse starting side on alternating query ordinal',
                  'failure_policy': 'stop failed side for query; retain explicit not_run failures; continue other side; no restarts',
                  'watchdog': 'Worker dispatch 30s/startup 300s/serialization 120s; query watchdog ceiling+100ms; exact elapsed gate has no grace',
                  'source_provenance': 'binary digests pinned; no current production source attributed to either binary'}
    write(root/'provenance.json', provenance)
    results = {}
    try:
        for side, binary in binaries.items():
            require(binary.is_file() and os.access(binary, os.X_OK), f'{side}: executable binary required')
            require(digest(binary) == expected[side], f'{side}: binary SHA256 mismatch')
        dataset_path = Path(args.dataset).resolve()
        dataset = verify_dataset(dataset_path)
        ids = [q['id'] for q in dataset['queries']]
        require(ids == [f'q{i:02}' for i in range(1,23)], 'complete canonical 22-query membership required')
        selected = args.queries or ids
        require(len(set(selected)) == len(selected) and all(q in ids for q in selected), 'unique canonical query IDs required')
        tables, provider = provider_setup(dataset_path, dataset, args.track, args.provider_manifest)
        provenance.update(dataset_path=str(dataset_path), dataset_sha256=digest(dataset_path), queries=selected,
                          steady_pairs_per_query=args.samples, warmup_pairs_per_query=1,
                          max_engine_requests=len(selected)*2*(args.samples+1), provider=provider)
        write(root/'dataset.json', dataset)
        write(root/'provenance.json', provenance)
        # Clear inherited engine debug/profiling/tuning switches, then explicitly
        # configure identical CPU settings. Do not persist unrelated credentials.
        env = {k:v for k,v in os.environ.items() if not k.startswith('QE_') and not k.endswith(('_PROF', '_DEBUG'))
               and k not in ('RT_DISABLE', 'QUERY_ENGINE_ALLOW_THP')}
        env.update(QE_MEM_CAP='48G', QE_IPC_CACHE='0', QE_GPU='0', QE_GPU_DEBUG='0', RAYON_NUM_THREADS='16')
        for ordinal, query_id in enumerate(selected):
            case = root/query_id
            case.mkdir()
            scratch = case/'comparison-scratch'
            scratch.mkdir()
            query = next(q for q in dataset['queries'] if q['id'] == query_id)
            sql = (dataset_path.parent/query['sql_path']).read_text()
            oracle_sql = (dataset_path.parent/query['oracle_sql_path']).read_text()
            (case/'timed.sql').write_text(sql)
            (case/'oracle.sql').write_text(oracle_sql)
            write(case/'query-contract.json', query)
            events, workers = [], {}
            result = {'query': query_id, 'track': args.track, 'calibrations': [], 'attempts': [], 'pairs': []}
            def persist():
                write(case/'result.json', result)
                write(case/'worker-events.json', events)
            def start(side, command):
                local = case/side
                local.mkdir()
                setup = {'track': args.track, 'threads': 16, 'memory_limit': f'{40*GIB}B',
                         'process_cap_bytes': 48*GIB, 'temp_directory': str(local/'duckdb-temp'), 'tables': tables}
                write(local/'setup.json', setup)
                side_env = dict(env, TMPDIR=str(local))
                write(local/'environment.json', {k:v for k,v in side_env.items()
                                                if k.startswith(('QE_', 'RAYON_', 'SAFE_BUILD_')) or k == 'TMPDIR'})
                worker = BoundaryWorker(command+[str(local/'setup.json')], side_env, local/'worker.log', events, startup_seconds=300)
                workers[side] = worker
                return worker
            try:
                reference = start('reference', [sys.executable, '-m', 'benchmark.duckdb_worker'])
                require(reference.ready.get('status') == 'ready', 'reference startup failed')
                request = {'id': 'oracle', 'sql': oracle_sql, 'output': str(case/'oracle.arrow')}
                oracle = reference.query(request, ceiling_ms=120000)
                result['oracle'] = {'request': request, 'response': oracle}
                if oracle.get('status') == 'completed': result['oracle']['output_sha256'] = digest(oracle['output'])
                persist()
                require(oracle.get('status') == 'completed', 'full typed oracle did not complete')
                for i in range(3):
                    request = {'id': f'calibration-{i}', 'sql': sql, 'output': str(case/f'calibration-{i}.arrow')}
                    response = reference.query(request, ceiling_ms=120000)
                    comparison = compare_execution(response, oracle, query, scratch)
                    result['calibrations'].append({'request': request, 'response': response, 'comparison': comparison,
                                                   'output_sha256': digest(response['output']) if response.get('status') == 'completed' else None})
                    persist()
                    require(valid_ms(response) and comparison['ok'], 'fresh reference calibration failed')
                ceiling = 10*statistics.median(r['response']['ms'] for r in result['calibrations'])
                require(math.isfinite(ceiling) and ceiling > 0, 'invalid exact query ceiling')
                result['ceiling_ms'] = ceiling
                reference.close()
                stopped = {}
                startup_order = ('before', 'after') if args.startup_order == 'before-first' else ('after', 'before')
                for side in startup_order:
                    try:
                        worker = start(side, [str(binaries[side])])
                        if worker.ready.get('status') != 'ready': stopped[side] = 'startup_failed'
                    except Exception as error:
                        stopped[side] = f'startup_exception: {type(error).__name__}: {error}'
                for i in range(args.samples+1):
                    order = pair_order(i, ordinal, args.execution_offset)
                    pair = {'iteration': i, 'warmup': i == 0, 'order': list(order)}
                    for side in order:
                        request = {'id': f'{side}-{i}', 'sql': sql, 'output': str(case/f'{side}-{i}.arrow')}
                        row = {'query': query_id, 'side': side, 'iteration': i, 'warmup': i == 0,
                               'request': request, 'ok': False}
                        if side in stopped:
                            row.update(response={'status': 'not_run', 'reason': stopped[side]}, comparison=None, time_gate_ok=False)
                        else:
                            try:
                                response = workers[side].query(request, ceiling_ms=ceiling)
                                row['response'] = response
                                row['time_gate_ok'] = valid_ms(response) and response['ms'] <= ceiling
                                row['comparison'] = compare_execution(response, oracle, query, scratch)
                                if response.get('status') == 'completed': row['output_sha256'] = digest(response['output'])
                                row['ok'] = row['time_gate_ok'] and row['comparison']['ok']
                            except Exception as error:
                                row.update(error=f'{type(error).__name__}: {error}', time_gate_ok=False)
                            if not row['ok']:
                                stopped[side] = f'failed_iteration_{i}; no restart'
                                workers[side].close()
                        result['attempts'].append(row)
                        with (root/'attempts.jsonl').open('a') as output: output.write(json.dumps(row, allow_nan=False)+'\n')
                        pair[side] = {'ok': row['ok'], 'ms': row.get('response', {}).get('ms'),
                                      'status': row.get('response', {}).get('status', 'exception')}
                        persist()
                    pair['ok'] = all(pair[s]['ok'] for s in ('before','after'))
                    pair['after_over_before'] = pair['after']['ms']/pair['before']['ms'] if pair['ok'] else None
                    result['pairs'].append(pair)
                    persist()
                    print(json.dumps({'query':query_id, **pair}), flush=True)
            except Exception as error:
                result['error'] = f'{type(error).__name__}: {error}'
            finally:
                for worker in workers.values(): worker.close()
                result['summary'] = summarize_case(result, args.samples)
                result['scope_after'] = memory_events(capped)
                persist()
                results[query_id] = result['summary']
                write(root/'query-summary.json', results)
        # Outside every timed worker: independently recheck all source inputs,
        # conversions and binary bytes, retaining mutation as a failed screen.
        require(digest(dataset_path) == provenance['dataset_sha256'], 'dataset manifest changed during screen')
        verify_dataset(dataset_path)
        _, final_provider = provider_setup(dataset_path, dataset, args.track, args.provider_manifest)
        require(final_provider == provider, 'provider provenance changed during screen')
        for side,binary in binaries.items(): require(digest(binary) == expected[side], f'{side}: binary changed during screen')
        require(digest(__file__) == provenance['driver_sha256'], 'driver changed during screen')
        require({p.name:digest(p) for p in sorted(Path('scripts/benchmark').glob('*.py'))} == provenance['harness_sha256'], 'harness changed during screen')
        provenance['postrun_input_verification'] = 'passed'
    except Exception as error:
        provenance['error'] = f'{type(error).__name__}: {error}'
    provenance.update(completed_unix=time.time(), scope_after=memory_events(capped))
    before_events = dict(line.split() for line in provenance['scope_before']['memory.events'].splitlines())
    after_events = dict(line.split() for line in provenance['scope_after']['memory.events'].splitlines())
    provenance['oom_event_delta'] = {k:int(after_events[k])-int(before_events[k]) for k in ('oom','oom_kill')}
    complete = (not provenance.get('error') and not any(provenance['oom_event_delta'].values())
                and len(results) == len(provenance.get('queries', [])) and bool(results)
                and all(r['complete'] for r in results.values()))
    summary = {'complete': complete, 'queries': results, 'suite_after_over_before': None,
               'geomean_after_over_before': None, 'aggregation_policy': 'no successful-only suite; any missing/failed row prevents suite ratios'}
    if complete:
        summary['suite_after_over_before'] = sum(r['after_median_ms'] for r in results.values())/sum(r['before_median_ms'] for r in results.values())
        summary['geomean_after_over_before'] = math.exp(statistics.mean(math.log(r['after_over_before']) for r in results.values()))
    write(root/'provenance.json', provenance)
    write(root/'summary.json', summary)
    return 0 if complete else 1


if __name__ == '__main__':
    raise SystemExit(main())
