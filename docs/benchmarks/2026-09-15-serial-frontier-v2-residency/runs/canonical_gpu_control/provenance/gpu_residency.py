"""Fail-closed resident GPU evidence validation; mixed runs remain separate."""
import math


def execution_telemetry(engine, trace):
    """Describe device execution; resident certification still needs preparation.

    Resident execution does not emit the legacy trace marker. Preserve both
    observations, prefer request-scoped evidence, and never interpret silence
    in stderr as proof of CPU execution.
    """
    runs = trace.count('[gpu-trace] run OK ')
    result = {'successful_device_runs': runs,
              'trace_successful_device_runs': runs,
              'upload_requests': trace.count('[gpu-trace] not ready '),
              'evidence_source': 'legacy_trace',
              'residency': 'device_executed' if runs else 'unobserved'}
    evidence = engine.get('gpu_resident_evidence')
    if evidence is None:
        return result
    result.update(evidence_source='request_scoped', successful_device_runs=0,
                  residency='invalid_device_evidence')
    if not isinstance(evidence, dict):
        return result
    expected = {'matched_operators': 1, 'attempted_device_runs': 1,
                'completed_device_runs': 1, 'failures': 0}
    valid = (type(evidence.get('session_id')) is int and evidence['session_id'] > 0
             and all(type(evidence.get(k)) is int and evidence[k] == v
                     for k, v in expected.items())
             and evidence.get('failure_reason') is None)
    if valid:
        result.update(successful_device_runs=1, residency='device_executed')
    return result


def resident_sample_issues(row):
    """Return issues without mutating correctness, timing or the preserved sample."""
    issues = []
    preparation = row.get('gpu_preparation')
    engine = row.get('engine', {})
    if not isinstance(preparation, dict) or preparation.get('status') != 'prepared':
        return ['resident preparation missing or failed']
    if preparation.get('sql_sha256') != row.get('sql_sha256'):
        issues.append('resident preparation SQL mismatch')
    worker = row.get('gpu_worker_id')
    if not isinstance(worker, str) or not worker or preparation.get('worker_id') != worker:
        issues.append('resident preparation worker mismatch')
    metadata = preparation.get('preparation')
    if not isinstance(metadata, dict):
        return issues + ['resident preparation metadata missing']
    session = metadata.get('session_id')
    if type(session) is not int or session <= 0:
        issues.append('invalid resident session')
    elapsed = preparation.get('preparation_ms')
    if type(elapsed) not in (int, float) or not math.isfinite(elapsed) or elapsed < 0:
        issues.append('invalid preparation elapsed time')
    for field in ('rows', 'groups', 'column_bytes', 'codes_bytes'):
        value = metadata.get(field)
        if type(value) is not int or value < 0:
            issues.append('invalid preparation ' + field)
    columns = metadata.get('columns')
    if not isinstance(columns, list) or not columns or not all(isinstance(c, str) and c for c in columns) or len(set(columns)) != len(columns):
        issues.append('invalid prepared columns')
    evidence = engine.get('gpu_resident_evidence')
    if not isinstance(evidence, dict):
        return issues + ['request-scoped resident device evidence missing']
    if evidence.get('session_id') != session or type(evidence.get('session_id')) is not int:
        issues.append('resident request session mismatch')
    expected = {'matched_operators': 1, 'attempted_device_runs': 1,
                'completed_device_runs': 1, 'failures': 0}
    for field, count in expected.items():
        if type(evidence.get(field)) is not int or evidence[field] != count:
            issues.append('resident ' + field + ' contract failed')
    if evidence.get('failure_reason') is not None:
        issues.append('resident request reported failure')
    return issues
