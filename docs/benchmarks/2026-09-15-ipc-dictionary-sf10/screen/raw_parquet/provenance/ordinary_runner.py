"""Ordinary embedded queries with validated calibration and no failed retries."""
import json
from .contract import TIMING
from .query_gate import calibrate_reference, within_ceiling


def run_query(*, workers, query, sql, oracle_sql, prefix, samples, rng,
              execute, compare, trace, workload, track, session):
    oracle, ceiling, times = calibrate_reference(worker=workers['duckdb'], execute=execute,
        compare=compare, query=query, sql=sql, oracle_sql=oracle_sql, prefix=prefix, trace=trace)
    reference_blocked = None if ceiling is not None else 'invalid reference calibration; no validated ceiling'
    blocked = reference_blocked
    warmup = None
    if not blocked:
        warmup = execute('engine', sql, prefix + '-warmup', ceiling)
        if not within_ceiling(warmup, ceiling):
            blocked = 'engine warmup failed query-time gate; no measured retries'
        elif not compare(warmup, oracle, query)['ok']:
            blocked = 'engine warmup failed typed oracle; no measured retries'
    closed = False

    def close_engine():
        nonlocal closed
        if not closed:
            workers['engine'].close()
            closed = True

    def skipped(side, label, reason):
        result = {'status': 'not_run', 'executed': False, 'error': reason}
        trace.write(json.dumps({'side': side, 'request': {'id': label, 'sql': sql}, 'result': result}) + '\n')
        trace.flush()
        return result

    if blocked:
        close_engine()
    if reference_blocked:
        workers['duckdb'].close()
    for iteration in range(1, samples + 1):
        sides = ['engine', 'duckdb']
        rng.shuffle(sides)
        pair = {}
        for side in sides:
            label = prefix + f'-{iteration}-{side}'
            reason = reference_blocked or (blocked if side == 'engine' else None)
            if reason:
                pair[side] = skipped(side, label, reason)
                continue
            pair[side] = execute(side, sql, label, ceiling)
            if side == 'duckdb' and (not within_ceiling(pair[side], ceiling)
                                    or not compare(pair[side], oracle, query)['ok']):
                reference_blocked = 'measured reference failed; dependent requests not run'
                blocked = blocked or reference_blocked
                close_engine()
                # A live process can still be unusable after an allocation or
                # execution failure. Existing next-query restart logic sees this
                # closed, previously-ready worker and creates a fresh reference.
                workers['duckdb'].close()
        comparison = compare(pair['engine'], oracle, query)
        duck_comparison = compare(pair['duckdb'], oracle, query)
        if ceiling is None or reference_blocked or not duck_comparison['ok']:
            comparison.update(ok=False, reference_validation=duck_comparison,
                              calibration_valid=ceiling is not None,
                              reference_failure=reference_blocked)
        yield {'workload': workload, 'query': query['id'], 'track': track,
               'session': session, 'iteration': iteration, 'order': sides,
               'sql_sha256': query['sql_sha256'], 'timing_boundary': TIMING,
               'calibration_ms': times, 'query_ceiling_ms': ceiling,
               'engine_warmup': warmup, **pair, 'comparison': comparison}
        if not within_ceiling(pair['engine'], ceiling) or not comparison['ok']:
            blocked = blocked or 'engine sample failed; remaining requests not run'
            close_engine()
