"""Fixed, process-isolated query windows. Setup and validation are not query time."""
import math
import os
import statistics
from pathlib import Path

SIDES = ('before', 'after')


class IsolationError(RuntimeError):
    def __init__(self, result):
        self.result = result
        super().__init__('benchmark isolation failure: ' + result['isolation_error'])


def host_cpu_context():
    """Condition metadata only; never change the host to obtain a passing run."""
    affinity = sorted(os.sched_getaffinity(0))
    result = {'affinity': affinity, 'cpus': {}}
    try:
        result['loadavg'] = Path('/proc/loadavg').read_text().strip()
    except OSError as error:
        result['loadavg_unavailable'] = str(error)
    for cpu in affinity:
        root = Path(f'/sys/devices/system/cpu/cpu{cpu}')
        fields = {}
        for name in ['topology/core_id', 'topology/physical_package_id',
                     'topology/thread_siblings_list', 'cpufreq/scaling_governor',
                     'cpufreq/scaling_min_freq', 'cpufreq/scaling_max_freq',
                     'cpufreq/scaling_cur_freq']:
            try:
                fields[name] = (root / name).read_text().strip()
            except OSError:
                fields[name] = None
        result['cpus'][str(cpu)] = fields
    return result


def process_snapshot(worker):
    """Read-only CPU/placement evidence; absent fields are unavailable, never zero."""
    pid = worker.process.pid
    result = {'pid': pid}
    try:
        fields = Path(f'/proc/{pid}/stat').read_text().rsplit(') ', 1)[1].split()
        result.update(user_ticks=int(fields[11]), system_ticks=int(fields[12]),
                      ticks_per_second=os.sysconf('SC_CLK_TCK'),
                      affinity=sorted(os.sched_getaffinity(pid)))
        tasks = {}
        for path in Path(f'/proc/{pid}/task').glob('*/status'):
            try:
                values = dict(line.split(':', 1) for line in path.read_text().splitlines() if ':' in line)
                tasks[path.parent.name] = {key: int(values[key]) for key in
                    ('voluntary_ctxt_switches', 'nonvoluntary_ctxt_switches') if key in values}
            except (OSError, ValueError):
                continue
        result['live_thread_context_switches'] = tasks
        result['context_scope'] = 'snapshot of live threads; exited threads can disappear'
    except (OSError, ValueError, IndexError) as error:
        result['unavailable'] = str(error)
    return result


def valid_attempt(row):
    if not isinstance(row, dict) or not isinstance(row.get('response'), dict) or not isinstance(row.get('comparison'), dict):
        return False
    response = row['response']
    ms = response.get('ms')
    return (response.get('status') == 'completed' and type(ms) in (int, float)
            and math.isfinite(ms) and ms > 0 and row.get('comparison', {}).get('ok') is True
            and row.get('time_gate_pass') is True)


def run_isolated_pair(order, count, minimum_ms, start, execute, *, snapshot=process_snapshot, persist=lambda _: None):
    """Callbacks retain typed outputs; execute returns response/comparison/time_gate_pass.

    One gated warmup precedes exactly count measured slots per side. Never extend
    a window in response to timings. A callback startup exception stops both sides:
    without a returned handle, isolation cannot be established for another start.
    """
    if tuple(sorted(order)) != tuple(sorted(SIDES)) or len(order) != 2:
        raise ValueError('order must contain each side exactly once')
    if type(count) is not int or not 0 < count <= 65536:
        raise ValueError('fixed count must be between 1 and 65536')
    if type(minimum_ms) not in (int, float) or not math.isfinite(minimum_ms) or minimum_ms <= 0:
        raise ValueError('positive finite minimum measured duration required')
    result = {'order': list(order), 'count': count, 'minimum_measured_ms': minimum_ms,
              'windows': [], 'complete': False, 'precision_sufficient': False}
    unsafe_to_start = None
    for side in order:
        window = {'side': side, 'attempts': [], 'measured_ms': 0.0, 'complete': False}
        result['windows'].append(window)
        worker = None
        stopped = unsafe_to_start
        try:
            if stopped is None:
                try:
                    worker = start(side)
                except Exception as error:
                    stopped = unsafe_to_start = f'startup exception; ownership unverified: {type(error).__name__}: {error}'
                if worker is not None:
                    window['ready'] = worker.ready
                    if worker.ready.get('status') != 'ready':
                        stopped = 'startup failed'
            for iteration in range(count + 1):
                if stopped:
                    row = {'response': {'status': 'not_run', 'executed': False, 'reason': stopped},
                           'comparison': {'ok': False}, 'time_gate_pass': False}
                else:
                    try:
                        if iteration == 1:
                            window['resources_before'] = snapshot(worker)
                        row = execute(worker, side, iteration)
                        if not isinstance(row, dict):
                            raise TypeError('execute must return an attempt object')
                    except Exception as error:
                        row = {'response': {'status': 'error', 'error': f'{type(error).__name__}: {error}'},
                               'comparison': {'ok': False}, 'time_gate_pass': False}
                    if not valid_attempt(row):
                        stopped = f'failed iteration {iteration}; no restart'
                row = dict(row, side=side, iteration=iteration, warmup=iteration == 0)
                window['attempts'].append(row)
                if iteration and valid_attempt(row):
                    window['measured_ms'] += row['response']['ms']
                persist(result)
            if worker is not None:
                window['resources_after'] = snapshot(worker)
        finally:
            if worker is not None:
                try:
                    worker.close(force=bool(stopped))
                    terminal = worker.process.poll() is not None
                    window['teardown'] = getattr(worker, 'teardown', {})
                    if not terminal:
                        unsafe_to_start = 'previous worker remains live after close'
                    window['cleanup_ok'] = (terminal and worker.process.returncode == 0
                                            and not window['teardown'].get('forced', False))
                except Exception as error:
                    unsafe_to_start = f'cleanup exception: {type(error).__name__}: {error}'
                    window['cleanup_error'] = unsafe_to_start
                    window['cleanup_ok'] = False
            window['complete'] = (len(window['attempts']) == count + 1
                                  and all(valid_attempt(r) for r in window['attempts'])
                                  and window.get('cleanup_ok', False))
            window['precision_sufficient'] = window['complete'] and window['measured_ms'] >= minimum_ms
            persist(result)
    result['complete'] = all(w['complete'] for w in result['windows'])
    result['precision_sufficient'] = result['complete'] and all(w['precision_sufficient'] for w in result['windows'])
    result['primary_metric'] = 'mean query time across each complete fixed window'
    if result['precision_sufficient']:
        windows = {w['side']: w for w in result['windows']}
        result['after_over_before'] = windows['after']['measured_ms'] / windows['before']['measured_ms']
        medians = {side: statistics.median(r['response']['ms'] for r in w['attempts'][1:]) for side, w in windows.items()}
        result['median_after_over_before'] = medians['after'] / medians['before']
    if unsafe_to_start:
        result['isolation_error'] = unsafe_to_start
    persist(result)
    if unsafe_to_start:
        raise IsolationError(result)
    return result
