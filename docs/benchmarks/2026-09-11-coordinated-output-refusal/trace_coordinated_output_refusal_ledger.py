"""Owned test-child allocation-denial stacks, after all timing and archival."""
import hashlib,json,os,re,signal,subprocess,time
from pathlib import Path
b=Path('.scratch/parallel-aggregate-input');job=b/'coordinated-output-ledger-refusal-trace-job.json';assert not job.exists()
state={'status':'waiting_for_evidence','tests':[]}
def save():job.write_text(json.dumps(state,indent=2)+'\n')
def sha(p):
 with Path(p).open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
def resources():
 rel=next(line.split(':',2)[2] for line in Path('/proc/self/cgroup').read_text().splitlines() if line.startswith('0::'))
 p=Path('/sys/fs/cgroup')/rel.lstrip('/')
 return {'path':str(p),**{n:(p/n).read_text() for n in ['memory.max','memory.swap.max','memory.peak','memory.events']}}
save()
assert (b/'coordinated-output-validation-verified-after.json').exists()
assert not (b/'coordinated-output-checkpoint.json').exists(), 'Run before measurement drivers start'
hashes=json.loads((b/'coordinated-output-validation-source-hashes.json').read_text());assert all(sha(p)==v for p,v in hashes.items())
log=(b/'coordinated-output-disjoint-spill.log').read_text();match=re.search(r'Running tests/spill_tests.rs \(([^)]+)\)',log);assert match
binary=Path(match[1]).resolve();out=b/'coordinated-output-ledger-refusal-traces-01';out.mkdir(exist_ok=False)
fixture={str(p):sha(p) for p in sorted(Path('data/tpch-10mb').glob('*.parquet'))};assert len(fixture)==8
provenance={'binary':str(binary),'binary_sha256':sha(binary),'source_inputs':hashes,'fixture_inputs':fixture,'driver_sha256':sha(__file__),'purpose':'Allocation-denial attribution only; expected test failures, no performance or passing-resource-gate claim'}
(out/'manifest.json').write_text(json.dumps(provenance,indent=2)+'\n');(out/'resources-before.json').write_text(json.dumps(resources(),indent=2)+'\n')
source=Path('src/execution/memory.rs').resolve();lines=source.read_text().splitlines();line=next(i for i,x in enumerate(lines,1) if 'return Err(crate::error::QueryError::MemoryLimit {' in x)
commands=out/'refusal.gdb';commands.write_text('source '+str((b/'refusal_ownership_ledger_gdb.py').resolve())+'\nset pagination off\nset confirm off\nset debuginfod enabled off\nset print thread-events off\nset print frame-arguments none\nbreak '+str(source)+':'+str(line)+'\ncommands\nsilent\nprintf "MEMORY_REFUSAL_STACK\\n"\nbt 24\nrefusal-ledger\ncontinue\nend\nrun\nprintf "INFERIOR_EXIT=%d\\n", $_exitcode\n')
state['status']='tracing';save()
for name in ['count_distinct_spill_matches_in_memory']:
 cmd=['/usr/bin/gdb','--batch','--nx','-x',str(commands),'--args',str(binary),name,'--exact','--nocapture','--test-threads=1']
 env=dict(os.environ,QE_AGG_OWNERSHIP='disjoint',RAYON_NUM_THREADS='16',TMPDIR=str(Path('.scratch').resolve()))
 record={'name':name,'command':cmd,'status':'running'};state['tests'].append(record);save()
 with (out/(name+'.log')).open('x') as output:
  process=subprocess.Popen(cmd,stdout=output,stderr=subprocess.STDOUT,env=env,start_new_session=True)
  timeout=False
  try:code=process.wait(timeout=30)
  except subprocess.TimeoutExpired:
   timeout=True;os.killpg(process.pid,signal.SIGKILL);code=process.wait()
 text=(out/(name+'.log')).read_text();expected=(not timeout and code==0 and 'INFERIOR_EXIT=101' in text and 'Memory limit exceeded' in text and 'MEMORY_REFUSAL_STACK' in text)
 record.update(status='terminal',gdb_exit=code,timeout=timeout,expected_refusal=expected,stack_count=text.count('MEMORY_REFUSAL_STACK'));save();print(name,record,flush=True)
 if not expected:
  state['status']='diagnostic_failed';save();raise SystemExit(1)
assert sha(binary)==provenance['binary_sha256'] and all(sha(p)==v for p,v in hashes.items()) and all(sha(p)==v for p,v in fixture.items()) and sha(__file__)==provenance['driver_sha256']
(out/'resources-after.json').write_text(json.dumps(resources(),indent=2)+'\n');(out/'verified-after.json').write_text(json.dumps({'binary':True,'source_inputs':len(hashes),'fixtures':len(fixture),'tests':len(state['tests'])})+'\n')
assert not (b/'coordinated-output-checkpoint.json').exists(), 'No measurement may overlap this probe'
state['status']='terminal';state['completed_before_measurements']=True;save()
