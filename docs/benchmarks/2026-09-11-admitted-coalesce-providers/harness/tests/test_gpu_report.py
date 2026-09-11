import copy
import unittest
from benchmark.contract import report,ContractError
from test_gpu_residency import row


def manifest():
    return {'version':1,'tracks':['gpu'],'sessions':['s1'],'samples_per_query':1,
        'timing_boundary':'embedded_parse_to_arrow_consumed','cache_state':'resident',
        'gpu_residency_policy':'required','environment':{'setup':{'gpu_residency_required':True,'host_arrow_preloaded':True}},'profile':'development','source_revision':'frozen',
        'engine_binary_sha256':'b'*64,'duckdb_version':'1.4.4','dataset_manifest_sha256':'c'*64,
        'workloads':[{'id':'fixture','queries':[{'id':'q1','sql_sha256':'a'*64,'result_policy':'bag'}]}]}


def sample():
    s=row();s.update(workload='fixture',query='q1',track='gpu',session='s1',iteration=1,
        timing_boundary='embedded_parse_to_arrow_consumed',calibration_ms=[1,1,1],
        duckdb={'status':'completed','ms':1},comparison={'ok':True,'policy':'bag'})
    s['engine']['ms']=1
    return s

class ReportResidency(unittest.TestCase):
    def test_required_report_checks_each_device_event(self):
        self.assertTrue(report(manifest(),[sample()],repetitions=2)['complete'])
        s=sample();s['engine']['gpu_resident_evidence']['completed_device_runs']=0
        r=report(manifest(),[s],repetitions=2)
        self.assertFalse(r['complete']);self.assertTrue(any('completed_device_runs' in e for e in r['issues']))
    def test_correct_cpu_result_cannot_pass_required_report(self):
        s=sample();s['engine'].pop('gpu_resident_evidence')
        self.assertFalse(report(manifest(),[s],repetitions=2)['complete'])
    def test_mixed_legacy_policy_is_unchanged(self):
        m=manifest();m.pop('gpu_residency_policy');m['cache_state']='warm_host';m['environment']['setup']['gpu_residency_required']=False
        s=sample();s.pop('gpu_preparation');s.pop('gpu_worker_id');s['engine'].pop('gpu_resident_evidence')
        self.assertTrue(report(m,[s],repetitions=2)['complete'])
    def test_policy_requires_resident_gpu_manifest(self):
        for edit in [lambda m:m.update(gpu_residency_policy='guess'),
                lambda m:m.update(cache_state='warm_host'),lambda m:m.update(tracks=['gpu_control']),lambda m:m['environment']['setup'].update(host_arrow_preloaded=False),lambda m:m['environment']['setup'].update(gpu_residency_required=False)]:
            m=manifest();edit(m)
            with self.assertRaises(ContractError):report(m,[sample()],repetitions=2)
    def test_malformed_environment_is_an_explicit_contract_error(self):
        for value in [None, [], "invalid"]:
            m=manifest();m['environment']=value
            with self.assertRaises(ContractError):report(m,[sample()],repetitions=2)
    def test_residency_never_overrides_correctness_or_time_failure(self):
        for edit in [lambda s:s['comparison'].update(ok=False),lambda s:s['engine'].update(ms=11),
                lambda s:s['engine'].update(status='query_error')]:
            s=sample();edit(s);before=copy.deepcopy(s)
            self.assertFalse(report(manifest(),[s],repetitions=2)['complete']);self.assertEqual(s,before)

if __name__=='__main__':unittest.main()
