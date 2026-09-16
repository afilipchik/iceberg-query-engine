"""Single-query resident/preloaded control orchestration; mixed runner unchanged."""
import json
import statistics
from .contract import TIMING, positive
from .gpu_residency import resident_sample_issues
from .query_gate import calibrate_reference, within_ceiling


def manifest_residency(required, preloaded, preparation_timeout):
    return {'cache_state': 'resident' if required else 'warm_host',
            'gpu_residency_policy': 'required' if required else 'mixed',
            'gpu_preparation_timeout_seconds': preparation_timeout if required else None,
            'host_arrow_preloaded': preloaded}


def skipped(reason):
    return {'status': 'not_run', 'error': reason}


def run_query(*, workers, query, sql, oracle_sql, prefix, worker_id, required,
              preparation_timeout_seconds, samples, rng, execute, compare,
              trace, workload, track, session, record_preparation=None):
    """No retries/restarts. Every requested iteration produces a preserved row."""
    oracle, ceiling, times = calibrate_reference(worker=workers['duckdb'], execute=execute,
        compare=compare, query=query, sql=sql, oracle_sql=oracle_sql, prefix=prefix, trace=trace)
    calibration_valid = ceiling is not None
    preparation = None
    token = None
    reference_blocked = None if calibration_valid else 'invalid reference calibration; dependent requests not run'
    blocked = reference_blocked
    ready = workers['engine'].ready
    if blocked is None and ready.get('status') != 'ready':
        blocked = 'engine startup failed: ' + json.dumps(ready, sort_keys=True)
    if blocked is None and ready.get('residency') != 'host_arrow_preloaded':
        blocked = 'engine did not acknowledge host Arrow preload'
    if blocked is None and workers['duckdb'].ready.get('reference', {}).get('reader') != 'decoded_ipc':
        blocked = 'reference did not acknowledge matching host Arrow preload'
    if blocked is None and required and ready.get('gpu_residency_required') is not True:
        blocked = 'engine did not acknowledge required residency policy'
    if required:
        request = {'id': prefix + '-prepare', 'operation': 'prepare_gpu_resident',
                   'sql': sql, 'output': '',
                   'preparation_timeout_ms': preparation_timeout_seconds * 1000}
        if blocked:
            preparation = skipped(blocked)
        else:
            preparation = workers['engine'].query(request, preparation_timeout_seconds * 1000)
        preparation = {**preparation, 'sql_sha256': query['sql_sha256'], 'worker_id': worker_id}
        trace.write(json.dumps({'side': 'engine', 'request': request, 'result': preparation}) + '\n')
        trace.flush()
        if record_preparation is not None:
            record_preparation(preparation)
        if preparation.get('status') != 'prepared':
            blocked = blocked or 'resident preparation failed'
        else:
            token = preparation.get('preparation', {}).get('session_id')
            if type(token) is not int or token <= 0:
                blocked = 'resident preparation returned invalid session'
    if ceiling is None:
        blocked = blocked or 'reference calibration did not establish validated exact ceiling'
    if not blocked:
        warmup = execute('engine', sql, prefix + '-warmup', ceiling, token)
        if not within_ceiling(warmup, ceiling):
            blocked = 'engine warmup failed query-time gate; no measured retries'
        elif required:
            probe = {'sql_sha256': query['sql_sha256'], 'gpu_preparation': preparation,
                     'gpu_worker_id': worker_id, 'engine': warmup}
            if resident_sample_issues(probe):
                blocked = 'engine warmup did not execute required resident operator'
        if not compare(warmup, oracle, query)['ok']:
            blocked = blocked or 'engine warmup failed typed oracle'
    if blocked:
        workers['engine'].close()
    for iteration in range(1, samples + 1):
        sides = ['engine', 'duckdb']
        rng.shuffle(sides)
        pair = {}
        for side in sides:
            label = prefix + f'-{iteration}-{side}'
            reason = reference_blocked or (blocked if side == 'engine' else None)
            if reason:
                pair[side] = skipped(reason)
                trace.write(json.dumps({'side': side, 'request': {'id': label, 'sql': sql}, 'result': pair[side]}) + '\n')
                trace.flush()
            else:
                pair[side] = execute(side, sql, label, ceiling, token if side == 'engine' else None)
                if side == 'duckdb' and (not within_ceiling(pair[side], ceiling)
                                        or not compare(pair[side], oracle, query)['ok']):
                    reference_blocked = 'measured reference failed; dependent requests not run'
                    blocked = blocked or reference_blocked
                    workers['duckdb'].close()
                    workers['engine'].close()
        comparison = compare(pair['engine'], oracle, query)
        duck_comparison = compare(pair['duckdb'], oracle, query)
        if not calibration_valid or reference_blocked or not duck_comparison['ok']:
            comparison.update(ok=False, reference_validation=duck_comparison,
                              calibration_valid=calibration_valid)
        row = {'workload': workload, 'query': query['id'], 'track': track,
               'session': session, 'iteration': iteration, 'order': sides,
               'sql_sha256': query['sql_sha256'], 'timing_boundary': TIMING,
               'calibration_ms': times, **pair, 'comparison': comparison}
        if required:
            row.update(gpu_preparation=preparation, gpu_worker_id=worker_id)
        yield row
        if not within_ceiling(pair['engine'], ceiling) or not comparison['ok']:
            blocked = blocked or 'engine failed; remaining requested samples not run'
        elif required and resident_sample_issues(row):
            blocked = 'resident evidence failed; remaining samples not run'
        if blocked and workers['engine'].process.poll() is None:
            workers['engine'].close()
    if required and token is not None and workers['engine'].process.poll() is None:
        request = {'id': prefix + '-release', 'operation': 'release_gpu_resident',
                   'sql': '', 'output': '', 'session_id': token}
        result = workers['engine'].query(request, 30_000)
        trace.write(json.dumps({'side': 'engine', 'request': request, 'result': result}) + '\n')
        trace.flush()
