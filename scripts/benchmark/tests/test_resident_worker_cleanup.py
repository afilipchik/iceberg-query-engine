"""Real Worker lifecycle with tiny protocol processes, never an engine benchmark."""
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from benchmark.run import Worker


class RealWorkerCleanupTests(unittest.TestCase):
    def test_duckdb_spill_files_are_removed_by_normal_eof_cleanup(self):
        import pyarrow.ipc as ipc
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            temporary = root / 'duckdb-temp'
            temporary.mkdir()
            setup = root / 'setup.json'
            setup.write_text(json.dumps({'track': 'raw_parquet', 'tables': [],
                'threads': 2, 'memory_limit': '32MB', 'process_cap_bytes': 2 * 1024**3,
                'temp_directory': str(temporary)}))
            worker = Worker([sys.executable, '-m', 'benchmark.duckdb_worker', str(setup)],
                            dict(os.environ, OPENBLAS_NUM_THREADS='1'), root / 'duckdb.stderr', [], startup_seconds=10)
            try:
                self.assertEqual(worker.ready['status'], 'ready', worker.ready)
                created = worker.query({'id': 'create', 'sql':
                    "CREATE TEMP TABLE spill_data AS SELECT i, repeat(md5(i::VARCHAR), 8) AS payload FROM range(500000) t(i)",
                    'output': str(root / 'create.arrow')}, ceiling_ms=10000)
                self.assertEqual(created['status'], 'completed', created)
                self.assertTrue(any(p.is_file() and p.stat().st_size > 0 for p in temporary.rglob('*')),
                                'the cleanup regression must exercise actual spill files')
                checked = worker.query({'id': 'check', 'sql': 'SELECT count(*), sum(i) FROM spill_data',
                                        'output': str(root / 'check.arrow')}, ceiling_ms=10000)
                self.assertEqual(checked['status'], 'completed', checked)
                with (root / 'check.arrow').open('rb') as source:
                    result = ipc.open_stream(source).read_all()
                self.assertEqual(int(result.column(0)[0].as_py()), 500000)
                self.assertEqual(int(result.column(1)[0].as_py()), 499999 * 500000 // 2)
            finally:
                worker.close()
            self.assertEqual(worker.process.returncode, 0)
            self.assertFalse(worker.teardown['forced'])
            self.assertEqual([p for p in temporary.rglob('*') if p.is_file()], [])

    def test_startup_deadline_retains_abort_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            events = []
            worker = Worker([sys.executable, '-c', 'import threading; threading.Event().wait()'],
                            dict(os.environ), Path(directory) / 'startup-timeout.stderr',
                            events, startup_seconds=0.02)
            self.assert_closed(worker)
            self.assertEqual(worker.ready['status'], 'timeout')
            self.assertEqual(events[0]['teardown']['requested'], 'abort')
            self.assertTrue(events[0]['teardown']['forced'])

    def test_normal_eof_completes_cleanup_and_records_clean_exit(self):
        with tempfile.TemporaryDirectory() as directory:
            marker = Path(directory) / 'cleanup'
            program = ('import sys; from pathlib import Path; '
                       'print(\'{"status":"ready"}\',flush=True); sys.stdin.read(); '
                       f'Path({str(marker)!r}).write_text("done")')
            events = []
            worker = Worker([sys.executable, '-c', program], dict(os.environ),
                            Path(directory) / 'normal.stderr', events, startup_seconds=5)
            worker.close()
            self.assert_closed(worker)
            self.assertEqual(marker.read_text(), 'done')
            self.assertEqual(worker.process.returncode, 0)
            self.assertEqual(events[0]['teardown'], {'requested': 'graceful', 'forced': False, 'exit_code': 0})
            worker.close()
            self.assertEqual(len(events), 1)

    def test_timeout_aborts_without_running_normal_eof_cleanup(self):
        with tempfile.TemporaryDirectory() as directory:
            marker = Path(directory) / 'cleanup'
            program = ('import sys; from pathlib import Path; '
                       'print(\'{"status":"ready"}\',flush=True); sys.stdin.read(); '
                       f'Path({str(marker)!r}).write_text("unexpected")')
            worker = Worker([sys.executable, '-c', program], dict(os.environ),
                            Path(directory) / 'timeout.stderr', [], startup_seconds=5)
            self.assertEqual(worker.read(0.02)['status'], 'timeout')
            self.assert_closed(worker)
            self.assertFalse(marker.exists())
            self.assertEqual(worker.teardown['requested'], 'abort')
            self.assertTrue(worker.teardown['forced'])

    def test_stuck_eof_cleanup_is_bounded_and_records_forced_exit(self):
        with tempfile.TemporaryDirectory() as directory:
            program = ('import sys,threading; print(\'{"status":"ready"}\',flush=True); '
                       'sys.stdin.read(); threading.Event().wait()')
            worker = Worker([sys.executable, '-c', program], dict(os.environ),
                            Path(directory) / 'stuck.stderr', [], startup_seconds=5)
            worker.close(grace_seconds=0.02)
            self.assert_closed(worker)
            self.assertEqual(worker.teardown['requested'], 'graceful')
            self.assertTrue(worker.teardown['grace_expired'])
            self.assertTrue(worker.teardown['forced'])

    def worker(self, directory, status):
        # No sleeps: ready response, then block on stdin until killed/closed.
        program = "import sys; print(" + repr(json.dumps({'status': status})) + ",flush=True); sys.stdin.read()"
        return Worker([sys.executable, '-c', program],
                      dict(os.environ, QE_GPU='0', QE_GPU_DEBUG='0'),
                      Path(directory) / 'worker.stderr', [], startup_seconds=5)

    def assert_closed(self, worker):
        self.assertIsNotNone(worker.process.poll())
        self.assertTrue(worker.process.stdin.closed)
        self.assertTrue(worker.process.stdout.closed)
        self.assertTrue(worker.log.closed)
        self.assertTrue(worker.selector.get_map() is None)

    def test_startup_refusal_then_blocked_and_finally_close_are_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            worker = self.worker(directory, 'startup_error')
            try:
                self.assertEqual(worker.ready['status'], 'startup_error')
                self.assert_closed(worker)  # __init__ already owns the first close
                worker.close()             # resident blocked branch
                self.assert_closed(worker)
            finally:
                worker.close()             # per-query finally
            worker.close()                 # outer teardown
            self.assert_closed(worker)

    def test_ready_worker_then_failed_query_teardown_can_close_repeatedly(self):
        with tempfile.TemporaryDirectory() as directory:
            worker = self.worker(directory, 'ready')
            try:
                self.assertEqual(worker.ready['status'], 'ready')
                worker.close()             # failure/cancellation branch
                worker.close()
                self.assert_closed(worker)
                self.assertTrue(worker.restart_for_next_query())
                response = worker.query({'id': 'after-close', 'sql': 'SELECT 1', 'output': ''})
                self.assertEqual(response['status'], 'startup_error')
            finally:
                worker.close()
            self.assert_closed(worker)

    def test_setup_refusal_is_preserved_without_sql_or_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            response = {'status': 'refused', 'phase': 'setup',
                        'error_kind': 'preload_admission', 'error': 'named pool refusal'}
            program = ('import sys; print(' + repr(json.dumps(response)) +
                       ',flush=True); sys.stdin.read()')
            events = []
            worker = Worker([sys.executable, '-c', program], dict(os.environ),
                            Path(directory) / 'refused.stderr', events, startup_seconds=5)
            try:
                self.assert_closed(worker)
                for query_id in ('first', 'second'):
                    result = worker.query({'id': query_id, 'sql': 'SELECT 1'})
                    self.assertEqual(result['status'], 'refused')
                    self.assertEqual(result['error_kind'], 'preload_admission')
                    self.assertEqual(result['error'], 'named pool refusal')
                    self.assertEqual(result['id'], query_id)
                    self.assertEqual(result['phase'], 'setup')
                    self.assertFalse(result['executed'])
                    self.assertTrue(result['setup_failure_reused'])
                    self.assertFalse(worker.restart_for_next_query())
                self.assertEqual(worker.ready, response)
                self.assertEqual(len(events), 1)
            finally:
                worker.close()


if __name__ == '__main__':
    unittest.main()
