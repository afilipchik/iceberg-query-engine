"""Independent completed-output and request-scoped GPU audit; does not reclassify failed pairs."""
import json,statistics
from collections import Counter
from pathlib import Path
from benchmark.compare import compare_files
from benchmark.query_gate import within_ceiling
from benchmark.gpu_residency import resident_sample_issues
root=Path('.scratch/public-bench/decimal-scale-residency-32g-01')
assert (root/'verified-after.json').exists()
state=json.loads((root/'state.json').read_text())
assert len(state)==5 and all(v['status']=='terminal' for v in state.values())
summary={}
def strict_pair(row,required):
 times=row.get('calibration_ms') or []
 ceiling=row.get('query_ceiling_ms') or (statistics.median(times)*10 if times else None)
 return ceiling is not None and row['comparison']['ok'] and all(within_ceiling(row[side],ceiling) for side in ['engine','duckdb']) and (not required or not resident_sample_issues(row))
for name,job in state.items():
 folder=root/name
 ds='tpch-sf10' if name.startswith('canonical') else 'custom-gpu-float-smoke'
 queries={q['id']:q for q in json.loads((Path('.scratch/public-bench')/ds/'dataset.json').read_text())['queries']}
 checks=[];preparations={};device=[];mixed=[]
 log=folder/'execution.jsonl'
 for line in log.read_text().splitlines() if log.exists() else []:
  event=json.loads(line);request=event['request'];response=event['result']
  if event['side']!='engine':continue
  parts=request['id'].split('-');prefix='-'.join(parts[:2]);q=parts[1]
  if response.get('status')=='prepared':
   preparations[prefix]=response;continue
  if response.get('status')!='completed' or not response.get('output'):continue
  query=queries[q]
  comp=compare_files(response['output'],folder/(prefix+'-oracle.arrow'),scratch=folder,policy=query['result_policy'],order_by=query['order_by'],limit_rows=query['limit_rows'],expected_is_complete=True)
  warmup=request['id'].endswith('-warmup')
  checks.append({'request_id':request['id'],'warmup':warmup,'comparison':comp,'ms':response['ms']})
  if name=='canonical_gpu_mixed':
   mixed.append({'request_id':request['id'],'warmup':warmup,'evidence':response.get('gpu_resident_evidence'),'telemetry':response.get('gpu_execution'),'before':response.get('gpu_before'),'after':response.get('gpu_after')})
  if name=='smoke_gpu_required':
   evidence=response.get('gpu_resident_evidence') or {};prepared=preparations.get(prefix,{}).get('preparation',{})
   before=response.get('gpu_before') or {};after=response.get('gpu_after') or {}
   ok=evidence.get('attempted_device_runs')==1 and evidence.get('completed_device_runs')==1 and evidence.get('failures')==0 and evidence.get('session_id')==prepared.get('session_id') and prepared.get('session_id') is not None
   telemetry=response.get('gpu_execution') or {}
   ok=ok and all(after.get(k)==before.get(k) for k in ['resident_bytes','resident_columns','run_fallbacks','upload_failures']) and telemetry.get('upload_requests')==0
   device.append({'request_id':request['id'],'warmup':warmup,'ok':ok,'evidence':evidence,'gpu_before':before,'gpu_after':after,'upload_requests':telemetry.get('upload_requests')})
 samples=folder/'samples.jsonl';rows=[json.loads(l) for l in samples.read_text().splitlines()] if samples.exists() else []
 summary[name]={'case_exit_code':job['exit_code'],'completed_outputs':len(checks),'completed_warmups':sum(c['warmup'] for c in checks),'outputs_valid':all(c['comparison']['ok'] for c in checks),'engine_statuses':dict(Counter(r['engine']['status'] for r in rows)),'valid_pairs':sum(strict_pair(r,name=='smoke_gpu_required') for r in rows),'requested_pairs':len(rows),'execution_absent':not log.exists(),'checks':checks,'device_checks':device,'preparations':preparations,'mixed_device_records':mixed}
 if name=='smoke_gpu_required':summary[name]['required_device_measured_valid']=sum(not d['warmup'] for d in device)==40 and all(d['ok'] for d in device)
p=root/'supplemental-validation.json';assert not p.exists();p.write_text(json.dumps(summary,indent=2)+'\n')
print(json.dumps({k:{a:b for a,b in v.items() if a not in ['checks','device_checks','preparations','mixed_device_records']} for k,v in summary.items()},indent=2))
assert all(v['outputs_valid'] for v in summary.values())
assert summary['smoke_gpu_required']['required_device_measured_valid']
