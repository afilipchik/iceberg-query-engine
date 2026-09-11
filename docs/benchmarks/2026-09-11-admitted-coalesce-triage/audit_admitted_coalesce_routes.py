"""Extract completed diagnostic queue records; no engine execution."""
import json
from pathlib import Path
b=Path('.scratch/parallel-aggregate-input');r=b/'admitted-coalesce-paired-01'
assert (r/'verified-after.json').exists()
a=json.loads((r/'summary.json').read_text());assert len(a)==42
out=[]
for x in a:
 if x['query']!='q06':continue
 events=[json.loads(l[len('[input-queue] '):]) for l in (r/x['case']/'q06.stderr').read_text().splitlines() if l.startswith('[input-queue] ')]
 producers=[e for e in events if e['phase']=='queue_producer'];preps=[e['detail'] for e in events if e['phase']=='queue_prepare']
 out.append({'block':x['block'],'mode':x['mode'],'label':x['label'],'preparations':preps,'producers':len(producers),'producer_batches':sum(e['detail']['batches'] for e in producers),'producer_rows':sum(e['detail']['rows'] for e in producers)})
p=b/'admitted-coalesce-runtime-route-audit.json'
if p.exists():assert json.loads(p.read_text())==out
else:p.write_text(json.dumps(out,indent=2)+'\n')
print('Verified',len(out),'records; absent queue traces do not prove zero input work')
