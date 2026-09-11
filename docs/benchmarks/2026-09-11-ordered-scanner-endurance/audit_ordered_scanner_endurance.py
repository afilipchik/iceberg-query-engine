import hashlib,json
from pathlib import Path
from benchmark.run import compare_execution
b=Path('.scratch/parallel-aggregate-input')
root=b/'ordered-scanner-endurance-01'
queries={q['id']:q for q in json.loads(Path('.scratch/public-bench/tpch-sf10/dataset.json').read_text())['queries']}
records=[]
for line in (root/'execution.jsonl').read_text().splitlines():
 x=json.loads(line)
 if x['result'].get('status')!='completed': continue
 q=x['request']['id'].split('-')[1]
 scratch=root/'validation-scratch'/x['request']['id'];scratch.mkdir(parents=True)
 oracle=Path('.scratch/public-bench/lance-fragment-sf10-providers-01/lance')/f's1-{q}-oracle.arrow'
 comparison=compare_execution(x['result'],{'status':'completed','output':str(oracle)},queries[q],scratch)
 records.append({'id':x['request']['id'],'comparison':comparison})
 assert comparison['ok'],records[-1]
(root/'validation.json').write_text(json.dumps(records,indent=2)+'\n')
print('independently typed outputs',len(records),flush=True)
