import json,subprocess,sys,time
from pathlib import Path
b=Path('.scratch/parallel-aggregate-input');out=b/'ipc-dictionary-control-diagnostic.json';assert not out.exists();state={}
for stage,script in [('build','build_ipc_dictionary_control.py'),('paired','paired_ipc_dictionary_control.py'),('audit','audit_paired_ipc_dictionary_control.py')]:
 state[stage]={'status':'running','started':time.time()};out.write_text(json.dumps(state,indent=2))
 with (b/('ipc-dictionary-control-'+stage+'.log')).open('x') as log:r=subprocess.run([sys.executable,str(b/script)],stdout=log,stderr=subprocess.STDOUT)
 state[stage].update(status='terminal',exit_code=r.returncode,finished=time.time());out.write_text(json.dumps(state,indent=2));print(stage,r.returncode,flush=True)
 if r.returncode:raise SystemExit(r.returncode)
