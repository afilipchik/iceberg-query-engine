"""A completed transport response is not proof of a valid query-time gate."""
import statistics
import json
from .contract import positive


def calibrate_reference(*, worker, execute, compare, query, sql, oracle_sql, prefix, trace):
    """Stop at the first failed reference; preserve every planned request as evidence."""
    blocked = None

    def request(statement, suffix, expected=None, timed=False):
        nonlocal blocked
        label = prefix + suffix
        if blocked:
            result = {'status': 'not_run', 'executed': False, 'error': blocked}
            trace.write(json.dumps({'side': 'duckdb', 'request': {'id': label, 'sql': statement},
                                    'result': result}) + '\n')
            trace.flush()
            return result
        result = execute('duckdb', statement, label)
        if (result.get('status') != 'completed'
                or (timed and not positive(result.get('ms')))
                or (expected is not None and not compare(result, expected, query)['ok'])):
            blocked = 'reference failed at ' + label + '; dependent requests not run'
            worker.close()
        return result

    request('EXPLAIN ' + sql, '-duckdb-plan')
    oracle = request(oracle_sql, '-oracle')
    request(sql, '-duckdb-warmup', oracle, timed=True)
    calibration = [request(sql, f'-cal{i}', oracle, timed=True) for i in range(3)]
    ceiling, times = calibrated_ceiling(calibration, oracle, query, compare)
    if ceiling is None:
        worker.close()
    return oracle, ceiling, times


def calibrated_ceiling(calibration, oracle, query, compare):
    times = [r['ms'] for r in calibration
             if r.get('status') == 'completed' and positive(r.get('ms'))]
    valid = (oracle.get('status') == 'completed' and len(calibration) == 3
             and len(times) == 3
             and all(compare(r, oracle, query)['ok'] for r in calibration))
    ceiling = 10 * statistics.median(times) if valid else None
    # Multiplication can overflow even when each sample is finite.
    return (ceiling if positive(ceiling) else None), times


def within_ceiling(result, ceiling):
    return (positive(ceiling) and result.get('status') == 'completed'
            and positive(result.get('ms')) and result['ms'] <= ceiling)
