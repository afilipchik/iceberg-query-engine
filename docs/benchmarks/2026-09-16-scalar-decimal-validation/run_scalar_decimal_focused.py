import json,os,subprocess,time
from pathlib import Path
b=Path('.scratch/parallel-aggregate-input');out=b/'scalar-decimal-focused.json';assert not out.exists()
def resources():
 rel=next(s.split('::',1)[1] for s in Path('/proc/self/cgroup').read_text().splitlines() if '::' in s)
 p=Path('/sys/fs/cgroup')/rel.lstrip('/')
 return {'path':str(p),**{n:(p/n).read_text() for n in ['memory.max','memory.swap.max','memory.peak','memory.events']}}
state={'before':resources(),'stages':{}}
for key,args in [('scalar',['--lib','physical::operators::filter::scalar_arithmetic_tests']),('filter',['--lib','physical::operators::filter::']),('decimal',['--lib','planner::reserved_decimal::']),('integration',['--test','decimal_expression_admission','--test','decimal_root_reuse_aggregate_contract','--test','systemic_numeric_tests'])]:
 cmd=['cargo','test','--locked','--offline','--features','lance,gpu']+args
 with (b/('scalar-decimal-'+key+'-green.log')).open('x') as log:r=subprocess.run(cmd,stdout=log,stderr=subprocess.STDOUT)
 state['stages'][key]={'command':cmd,'exit':r.returncode};state['after']=resources();out.write_text(json.dumps(state,indent=2)+'\n');print(key,r.returncode,flush=True)
 if r.returncode:raise SystemExit(r.returncode)
