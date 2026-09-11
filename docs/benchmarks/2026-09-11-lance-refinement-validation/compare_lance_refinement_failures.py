import json,re
from pathlib import Path
b=Path('.scratch/parallel-aggregate-input')
assert (b/'lance-refinement-validation-verified-after.json').exists()
def failures(path):return sorted(set(re.findall(r"thread '([^']+)' .*panicked at",path.read_text())))
result={}
for mode in ['disjoint','partial']:
 for suite in ['library','contracts','native_ipc','spill']:
  key=mode+'-'+suite
  before=failures(b/('ordered-scanner-'+key+'.log'))
  after=failures(b/('lance-refinement-'+key+'.log'))
  result[key]={'before':before,'after':after,'removed':sorted(set(before)-set(after)),'added':sorted(set(after)-set(before)),'results':re.findall(r'test result: .*',(b/('lance-refinement-'+key+'.log')).read_text())}
p=b/'lance-refinement-failure-comparison.json';assert not p.exists();p.write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(result,indent=2));assert not any(v['added'] for v in result.values())

# Failure-name matching alone could miss a compiler failure with no test results.
state=json.loads((b/'lance-refinement-validation.json').read_text());assert len(state)==8
for key,v in result.items():
 before_log=(b/('ordered-scanner-'+key+'.log')).read_text()
 old=re.findall(r'test result: .*',before_log)
 assert len(old)+(1 if key.endswith('-contracts') else 0)==len(v['results']) and old, (key,'missing test executables')
 def totals(lines):
  nums=[tuple(map(int,re.search(r'(\d+) passed; (\d+) failed; (\d+) ignored',line).groups())) for line in lines]
  return tuple(sum(row[i] for row in nums) for i in range(3))
 prior=totals(old);now=totals(v['results'])
 assert now[0]>=prior[0]+(3 if key.endswith('-contracts') else 0) and now[1]==len(v['after']) and now[2]==prior[2], (key,prior,now)
 assert state[key]['exit']==(101 if v['after'] else 0), (key,state[key]['exit'])

for key in result:
 old=(b/('ordered-scanner-'+key+'.log')).read_text()
 new=(b/('lance-refinement-'+key+'.log')).read_text()
 names=lambda log:set(re.findall(r'^test (\S+) \.\.\. ', log, re.M))
 missing=names(old)-names(new)
 assert not missing,(key,'missing test names',sorted(missing))
print('executable counts, exits, failure names and retained test inventory verified')
