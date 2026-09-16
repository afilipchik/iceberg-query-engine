"""Run after the first cycle is terminal, inside a sequential64GiB scope."""
import hashlib,json,subprocess,sys,time
from pathlib import Path
b=Path('.scratch/parallel-aggregate-input');out=b/'serial-frontier-v2-postchecks.json';assert not out.exists()
prior=json.loads((b/'serial-frontier-v2-release-screen.json').read_text());assert prior['release']['exit_code']==0 and prior['providers']['status']=='terminal'
def resources():
 cg=Path('/sys/fs/cgroup')/next(x.split('::',1)[1] for x in Path('/proc/self/cgroup').read_text().splitlines() if '::' in x).lstrip('/')
 return {n:(cg/n).read_text() for n in ['memory.max','memory.swap.max','memory.peak','memory.events']}
state={'status':'running','stages':{},'qualification':'Sequential64GiB scope; cumulative peak not RSS. Residency32/48GiB remains capacity experiment, not16GiB gate.'}
for stage,script in [('sf10_audit','audit_serial_frontier_v2_sf10.py'),('gate_outcomes','audit_serial_frontier_v2_gate_outcomes.py'),('paired','paired_serial_frontier_v2_attribution.py'),('paired_audit','audit_paired_serial_frontier_v2_attribution.py'),('sf10_archive','archive_serial_frontier_v2_sf10.py'),('paired_archive','archive_serial_frontier_v2_attribution.py'),('residency','run_serial_frontier_v2_residency.py'),('residency_audit','summarize_serial_frontier_v2_residency.py')]:
 command=[sys.executable,str(b/script)];state['stages'][stage]={'status':'running','started':time.time(),'command':command,'driver_sha256':hashlib.sha256((b/script).read_bytes()).hexdigest(),'before':resources()};out.write_text(json.dumps(state,indent=2))
 with (b/f'serial-frontier-v2-post-{stage}.log').open('x') as log:r=subprocess.run(command,stdout=log,stderr=subprocess.STDOUT)
 state['stages'][stage].update(status='terminal',exit_code=r.returncode,finished=time.time(),after=resources());out.write_text(json.dumps(state,indent=2));print(stage,r.returncode,flush=True)
 if stage=='residency':assert Path('.scratch/public-bench/serial-frontier-v2-residency-32g-01/verified-after.json').exists()
 elif r.returncode:raise SystemExit(r.returncode)
state.update(status='terminal',exit_code=0);out.write_text(json.dumps(state,indent=2))
