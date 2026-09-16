import json,subprocess,sys,time
from pathlib import Path
b=Path('.scratch/parallel-aggregate-input');state={}
def resources():
 rel=next(x.split('::',1)[1] for x in Path('/proc/self/cgroup').read_text().splitlines() if '::' in x)
 p=Path('/sys/fs/cgroup')/rel.lstrip('/')
 return {n:(p/n).read_text() for n in ['memory.max','memory.swap.max','memory.peak','memory.events']}
for name,script in [('release','build_serial_frontier_v2.py'),('providers','run_serial_frontier_v2_providers.py')]:
 state[name]={'status':'running','started':time.time(),'before':resources()}
 (b/'serial-frontier-v2-release-screen.json').write_text(json.dumps(state,indent=2))
 with (b/('serial-frontier-v2-'+name+'-pipeline.log')).open('x') as log:
  r=subprocess.run([sys.executable,str(b/script)],stdout=log,stderr=subprocess.STDOUT)
 state[name].update(status='terminal',exit_code=r.returncode,finished=time.time(),after=resources())
 (b/'serial-frontier-v2-release-screen.json').write_text(json.dumps(state,indent=2))
 print(name,r.returncode,flush=True)
 if name=='release' and r.returncode:sys.exit(r.returncode)
sys.exit(state['providers']['exit_code'])
