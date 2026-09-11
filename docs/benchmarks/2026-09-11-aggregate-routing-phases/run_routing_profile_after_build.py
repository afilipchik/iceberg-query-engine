"""Wait for the existing frozen diagnostic build; never launch a replacement build."""
import json,subprocess,sys,time,hashlib
from pathlib import Path
b=Path('.scratch/parallel-aggregate-input');state_path=b/'routing-phase-controller.json';assert not state_path.exists()
state={'status':'waiting_existing_build','build_session':74760,'started':time.time(),'driver_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest()};state_path.write_text(json.dumps(state,indent=2))
while True:
 job=json.loads((b/'aggregate-routing-profile-release-job.json').read_text())
 if job['status']!='running':break
 if time.time()-state['started']>1800:raise RuntimeError('Existing build wait exceeded 30 minutes; inspect original session, do not restart')
 time.sleep(10)
assert job['status']=='completed',job
state.update(status='profiling',build=job);state_path.write_text(json.dumps(state,indent=2))
command=[sys.executable,str(b/'profile_routing_phases.py')]
with (b/'routing-phase-profile-driver.log').open('x') as log:result=subprocess.run(command,stdout=log,stderr=subprocess.STDOUT)
state.update(status='terminal',exit_code=result.returncode,finished=time.time(),command=command)
source=json.loads((b/'aggregate-routing-profile-release-source-hashes.json').read_text());state['source_inputs_verified']=len(source) if all(hashlib.sha256(Path(p).read_bytes()).hexdigest()==v for p,v in source.items()) else False
state_path.write_text(json.dumps(state,indent=2));raise SystemExit(result.returncode)
