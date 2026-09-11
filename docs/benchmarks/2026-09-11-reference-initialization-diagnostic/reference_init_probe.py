"""Scratch diagnostic: exact provider setup, fresh workers, unchanged caps."""
import json
import os
from pathlib import Path
import resource
import subprocess
import sys
import time


def snapshot(stage, **extra):
    status = {}
    for line in Path('/proc/self/status').read_text().splitlines():
        key, _, value = line.partition(':')
        if key in ('Threads', 'VmData', 'VmRSS', 'VmSize', 'RssAnon', 'RssFile'):
            status[key] = value.strip()
    print(json.dumps(dict(stage=stage, monotonic=time.monotonic(), status=status, **extra)), flush=True)


def worker(setup_path, mode, output, query=''):
    setup = json.loads(Path(setup_path).read_text())
    setup['temp_directory'] = str(Path(output).resolve() / 'duckdb-temp')
    cap = setup['process_cap_bytes']
    resource.setrlimit(resource.RLIMIT_DATA, (cap, cap))
    snapshot('before_imports', cap=cap, affinity=sorted(os.sched_getaffinity(0)))
    import duckdb
    snapshot('after_duckdb_import', version=duckdb.__version__)
    import pyarrow
    snapshot('after_pyarrow_import', version=pyarrow.__version__)
    from benchmark.providers import configure_reference
    from benchmark.prepare import DUCKDB_VERSION, literal
    assert duckdb.__version__ == DUCKDB_VERSION
    config = dict(threads=str(setup['threads']), memory_limit=setup['memory_limit'],
                  temp_directory=setup['temp_directory'], default_null_order='NULLS_LAST',
                  autoinstall_known_extensions='false')
    con = duckdb.connect(config=config) if mode == 'construction' else duckdb.connect()
    snapshot('after_connection', mode=mode)
    for key, value in config.items():
        con.execute('SET ' + key + '=' + literal(value))
    snapshot('after_settings', actual=con.execute("SELECT current_setting('threads'), current_setting('memory_limit'), current_setting('temp_directory')").fetchone())
    class ObservedConnection:
        def execute(self, sql, *args):
            snapshot('before_provider_sql', sql=sql)
            result = con.execute(sql, *args)
            snapshot('after_provider_sql', sql=sql)
            return result
    provenance = configure_reference(ObservedConnection(), setup)
    snapshot('provider_ready', provenance=provenance)
    if query:
        sql = Path(query).read_text()
        snapshot('query_started', query=query)
        started = time.perf_counter()
        result = con.execute(sql).fetch_arrow_table()
        elapsed = (time.perf_counter() - started) * 1000
        snapshot('query_finished', ms=elapsed, rows=result.num_rows)
        with pyarrow.OSFile(str(Path(output) / 'result.arrow'), 'wb') as sink:
            with pyarrow.ipc.new_stream(sink, result.schema) as writer:
                writer.write_table(result, max_chunksize=65536)
        del result
        snapshot('serialized')
    con.close()
    snapshot('closed')


def parent(root, queries=False):
    root = Path(root).resolve()
    root.mkdir(exist_ok=False)
    records = []
    for repeat in range(2):
        for track in ('iceberg', 'lance'):
            for mode in (('default', 'construction') if repeat == 0 else ('construction', 'default')):
                run = root / f'{track}-{mode}-{repeat}'
                run.mkdir()
                setup = Path('.scratch/public-bench/coordinated-output-sf10-providers-01') / track / 'setup.json'
                command = [sys.executable, __file__, 'worker', str(setup), mode, str(run)]
                if queries:
                    number = '13' if track == 'iceberg' else '09'
                    command.append(str(Path('.scratch/public-bench/tpch-sf10/sql') / ('q'+number+'.sql')))
                started = time.monotonic()
                with (run / 'stdout.jsonl').open('w') as out, (run / 'stderr.txt').open('w') as err:
                    proc = subprocess.Popen(command, stdout=out, stderr=err, start_new_session=True)
                    timed_out = False
                    try:
                        code = proc.wait(timeout=120)
                    except subprocess.TimeoutExpired:
                        timed_out = True
                        import signal
                        os.killpg(proc.pid, signal.SIGKILL)
                        code = proc.wait()
                record = dict(track=track, mode=mode, repeat=repeat, exit_code=code,
                              timeout=timed_out, seconds=time.monotonic()-started)
                records.append(record)
                (root / 'results.json').write_text(json.dumps(records, indent=2)+'\n')
                print(json.dumps(record), flush=True)


if __name__ == '__main__':
    if sys.argv[1] == 'worker':
        try:
            worker(*sys.argv[2:])
        except Exception as error:
            snapshot('error', error_kind=type(error).__name__, error=str(error))
            raise
    else:
        parent(sys.argv[1], len(sys.argv) > 2)
