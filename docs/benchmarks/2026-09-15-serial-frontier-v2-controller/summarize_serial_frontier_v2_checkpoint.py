"""Read completed evidence only; no engines, modifications or reclassification."""
import json
from pathlib import Path
b=Path('.scratch/parallel-aggregate-input')
read=lambda p:json.loads(Path(p).read_text())
post=read(b/'serial-frontier-v2-postchecks.json');assert post['status']=='terminal' and post['exit_code']==0
providers=read(b/'serial-frontier-v2-sf10-audit/summary.json')
paired=read(b/'paired-serial-frontier-v2-attribution-01/analysis.json')
root=Path('.scratch/public-bench/serial-frontier-v2-residency-32g-01')
residency=read(root/'supplemental-validation.json')
summary={'release':read(b/'serial-frontier-v2-release.json'),'providers':{},'paired':{'typed_outputs':paired['typed_outputs'],'pairs':paired['pairs']},'residency':{},'provider_scope':read(b/'serial-frontier-v2-release-screen.json')['providers']['after'],'post_scope':list(post['stages'].values())[-1]['after']}
for name,v in providers.items():
 summary['providers'][name]={k:v[k] for k in ['typed_engine_outputs','valid_pairs','planned_pairs','complete','failures','geomean','suite_ratio','wins'] if k in v}
for name,v in residency.items():
 summary['residency'][name]={k:v[k] for k in ['case_exit_code','completed_outputs','completed_warmups','outputs_valid','valid_pairs','requested_pairs','engine_statuses','required_device_measured_valid'] if k in v}
 summary['residency'][name]['report']=read(root/name/'report.json')
p=b/'serial-frontier-v2-checkpoint-summary.json';assert not p.exists();p.write_text(json.dumps(summary,indent=2)+'\n')
print(json.dumps(summary,indent=2))
