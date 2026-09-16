"""Contained sequential continuation; no benchmark overlaps the provider cycle."""
import json,os,sys,time
from pathlib import Path
b=Path('.scratch/parallel-aggregate-input')
end=time.monotonic()+14400
while True:
 state=json.loads((b/'serial-frontier-v2-cycle.json').read_text())
 for stage in ('comparison','archive_validation'):
  if state.get(stage,{}).get('status')=='terminal' and state[stage]['exit_code']!=0:
   raise SystemExit('Prior gate failed: '+stage)
 last=state.get('release_screen',{})
 if last.get('status')=='terminal':
  prior=json.loads((b/'serial-frontier-v2-release-screen.json').read_text())
  assert prior['release']['exit_code']==0 and prior['providers']['status']=='terminal'
  break
 if time.monotonic()>end:raise SystemExit('Prior cycle did not complete within four hours')
 time.sleep(5)
os.execv(sys.executable,[sys.executable,str(b/'run_serial_frontier_v2_postchecks.py')])
