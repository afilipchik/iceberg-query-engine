"""Sequential contained gates, frozen release and canonical provider screen."""
import hashlib,json,subprocess,sys,time
from pathlib import Path
b=Path('.scratch/parallel-aggregate-input');p=b/'decimal-scale-cycle.json';assert not p.exists();state={}
for stage,script in [('validation','run_decimal_scale_validation.py'),('comparison','compare_decimal_scale_failures.py'),('archive_validation','archive_decimal_scale_validation.py'),('release_screen','run_decimal_scale_release_screen.py')]:
 command=[sys.executable,str(b/script)];state[stage]={'status':'running','started':time.time(),'command':command,'driver_sha256':hashlib.sha256((b/script).read_bytes()).hexdigest()};p.write_text(json.dumps(state,indent=2))
 with (b/f'decimal-scale-cycle-{stage}.log').open('x') as log:result=subprocess.run(command,stdout=log,stderr=subprocess.STDOUT)
 state[stage].update(status='terminal',exit_code=result.returncode,finished=time.time());p.write_text(json.dumps(state,indent=2));print(stage,result.returncode,flush=True)
 if stage=='validation':
  # Existing resource/numeric failures must match by executable/count/name;
  # the next stage enforces that before any optimized build is allowed.
  assert result.returncode in (0,1) and (b/'decimal-scale-validation-verified-after.json').exists()
 elif result.returncode:raise SystemExit(result.returncode)
