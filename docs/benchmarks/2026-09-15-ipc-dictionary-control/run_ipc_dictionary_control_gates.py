import os,subprocess,json
from pathlib import Path
b=Path('.scratch/parallel-aggregate-input');out=b/'ipc-dictionary-control-gates.json';assert not out.exists();state={}
for policy in ['prune','decode-all']:
 command=['cargo','test','--locked','--offline','--features','lance,gpu','--test','ipc_extent_contract','--test','native_streaming_scan_tests','--test','native_delete_tests','--test','native_dictionary_semi_anti']
 with (b/('ipc-dictionary-control-'+policy+'.log')).open('x') as log:r=subprocess.run(command,env=dict(os.environ,QE_IPC_DICTIONARY_CONTROL=policy),stdout=log,stderr=subprocess.STDOUT)
 state[policy]={'command':command,'exit_code':r.returncode};out.write_text(json.dumps(state,indent=2)+'\n')
 if r.returncode:raise SystemExit(r.returncode)
print(json.dumps(state),flush=True)
