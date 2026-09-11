import copy
import unittest

from benchmark.gpu_residency import execution_telemetry
from test_gpu_residency import row


class ExecutionTelemetry(unittest.TestCase):
    def test_resident_device_run_needs_no_legacy_trace(self):
        engine = row()['engine']
        original = copy.deepcopy(engine)
        telemetry = execution_telemetry(engine, '')
        self.assertEqual(telemetry['residency'], 'device_executed')
        self.assertEqual(telemetry['successful_device_runs'], 1)
        self.assertEqual(telemetry['trace_successful_device_runs'], 0)
        self.assertEqual(engine, original)

    def test_absent_trace_is_unobserved_not_cpu_proof(self):
        self.assertEqual(execution_telemetry({}, '')['residency'], 'unobserved')
        telemetry = execution_telemetry({}, '[gpu-trace] run OK query\n')
        self.assertEqual(telemetry['successful_device_runs'], 1)
        self.assertEqual(telemetry['evidence_source'], 'legacy_trace')

    def test_malformed_resident_evidence_cannot_be_rescued_by_trace(self):
        for evidence in [[], {}, 'invalid',
                         dict(row()['engine']['gpu_resident_evidence'], failures=1),
                         dict(row()['engine']['gpu_resident_evidence'], completed_device_runs=True),
                         dict(row()['engine']['gpu_resident_evidence'], session_id=0)]:
            with self.subTest(evidence=evidence):
                telemetry = execution_telemetry({'gpu_resident_evidence': evidence},
                                                '[gpu-trace] run OK query\n')
                self.assertEqual(telemetry['residency'], 'invalid_device_evidence')
                self.assertEqual(telemetry['successful_device_runs'], 0)
                self.assertEqual(telemetry['trace_successful_device_runs'], 1)


if __name__ == '__main__':
    unittest.main()
