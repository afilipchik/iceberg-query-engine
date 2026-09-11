"""Contained diagnostic replay of recorded reference requests, preserving SQL."""
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parent


def main():
    output = Path(sys.argv[1]).resolve()
    output.mkdir(exist_ok=False)
    original = Path('scripts/benchmark/duckdb_worker.py').read_text()
    instrumentation = '''
def process_snapshot():
    from pathlib import Path
    result = {}
    for line in Path('/proc/self/status').read_text().splitlines():
        key, _, value = line.partition(':')
        if key in ('Threads', 'VmData', 'VmRSS', 'RssAnon', 'RssFile', 'VmSize'):
            result[key] = value.strip()
    return result

'''
    source = original.replace('from .prepare import', 'from benchmark.prepare import').replace('from .providers import', 'from benchmark.providers import')
    source = source.replace('def emit(value):', instrumentation + 'def emit(value):\n    value["process"] = process_snapshot()')
    source = source.replace('            del result', '            del result\n            emit({"id": request["id"], "event": "released"})')
    worker = output / 'worker.py'
    worker.write_text(source)
    cgroup = Path('/sys/fs/cgroup') / Path('/proc/self/cgroup').read_text().strip().split('::')[-1].lstrip('/')
    def memory():
        return {name: (cgroup / name).read_text().strip() for name in ('memory.max', 'memory.peak', 'memory.events', 'memory.swap.max')}
    manifest = dict(original_sha256=hashlib.sha256(original.encode()).hexdigest(),
                    worker_sha256=hashlib.sha256(source.encode()).hexdigest(),
                    qualification='Exact reference request prefix; no engine interleaving or original idle delays. Diagnostic only.',
                    before=memory(), runs=[])
    for track in ('iceberg', 'lance'):
        run = output / track
        run.mkdir()
        prior = Path('.scratch/public-bench/coordinated-output-sf10-providers-01') / track
        setup = json.loads((prior / 'setup.json').read_text())
        setup['temp_directory'] = str(run / 'duckdb-temp')
        (run / 'setup.json').write_text(json.dumps(setup, indent=2))
        requests = []
        for line in (prior / 'execution.jsonl').read_text().splitlines():
            record = json.loads(line)
            if record['side'] != 'duckdb':
                continue
            request = dict(record['request'])
            request['output'] = str(run / Path(request['output']).name)
            requests.append(request)
            if record['result'].get('status') != 'completed':
                break
        (run / 'requests.json').write_text(json.dumps(requests, indent=2))
        started = time.monotonic()
        with (run / 'stdout.jsonl').open('w') as stdout, (run / 'stderr.txt').open('w') as stderr:
            proc = subprocess.Popen([sys.executable, str(worker), str(run / 'setup.json')],
                                    stdin=subprocess.PIPE, stdout=stdout, stderr=stderr, start_new_session=True)
            timeout = False
            try:
                proc.communicate((''.join(json.dumps(r)+'\n' for r in requests)).encode(), timeout=900)
            except subprocess.TimeoutExpired:
                timeout = True
                os.killpg(proc.pid, signal.SIGKILL)
                proc.communicate()
        record = dict(track=track, requests=len(requests), exit_code=proc.returncode, timeout=timeout,
                      seconds=time.monotonic()-started, memory=memory())
        manifest['runs'].append(record)
        (output / 'manifest.json').write_text(json.dumps(manifest, indent=2)+'\n')
        print(json.dumps(record), flush=True)


if __name__ == '__main__':
    main()
