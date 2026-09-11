import copy
import unittest
from benchmark.gpu_residency import resident_sample_issues


def row():
    return {'sql_sha256':'a'*64,'gpu_worker_id':'session/query/engine',
        'gpu_preparation':{'status':'prepared','sql_sha256':'a'*64,'worker_id':'session/query/engine',
            'preparation_ms':12.3,'preparation':{'session_id':1,'rows':600000,'groups':2,
                'column_bytes':24000000,'codes_bytes':600000,'columns':['v','x'],'codes_key':'groups'}},
        'engine':{'status':'completed','gpu_resident_evidence':{'session_id':1,'matched_operators':1,
            'attempted_device_runs':1,'completed_device_runs':1,'failures':0,'failure_reason':None}}}

class ResidencyEvidence(unittest.TestCase):
    def test_exact_device_completion_is_required(self):
        self.assertEqual(resident_sample_issues(row()), [])
        for field, values in {'matched_operators':[0,2,True,None], 'attempted_device_runs':[0,2],
                'completed_device_runs':[0,2], 'failures':[1,True]}.items():
            for value in values:
                with self.subTest(field=field,value=value):
                    sample=row(); sample['engine']['gpu_resident_evidence'][field]=value
                    self.assertTrue(resident_sample_issues(sample))
    def test_wrong_worker_query_or_session_refuses(self):
        for where,field,value in [('gpu_preparation','worker_id','different'),
                ('gpu_preparation','sql_sha256','b'*64),('evidence','session_id',2)]:
            sample=row();obj=sample['engine']['gpu_resident_evidence'] if where=='evidence' else sample[where]
            obj[field]=value;self.assertTrue(resident_sample_issues(sample))
    def test_failures_missing_ack_and_missing_evidence_are_preserved(self):
        for edit in [lambda s:s.pop('gpu_preparation'),lambda s:s['gpu_preparation'].update(status='preparation_error'),
                lambda s:s['engine'].pop('gpu_resident_evidence'),
                lambda s:s['engine']['gpu_resident_evidence'].update(failure_reason='device lost')]:
            sample=row();edit(sample);before=copy.deepcopy(sample)
            self.assertTrue(resident_sample_issues(sample));self.assertEqual(sample,before)
    def test_invalid_preparation_and_boolean_counts_refuse(self):
        for value in [-1,float('nan'),float('inf'),True,None]:
            sample=row();sample['gpu_preparation']['preparation_ms']=value
            self.assertTrue(resident_sample_issues(sample))
        sample=row();sample['gpu_preparation']['preparation']['rows']=True
        self.assertTrue(resident_sample_issues(sample))
    def test_empty_result_is_not_a_missing_device_run(self):
        sample=row();sample['gpu_preparation']['preparation'].update(rows=0,groups=0,column_bytes=0,codes_bytes=0)
        self.assertEqual(resident_sample_issues(sample),[])

if __name__=='__main__':unittest.main()
