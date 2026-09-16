"""Launch only the frozen local query child under GDB; sampled stacks, not timing."""
import hashlib,json,os,selectors,shlex,signal,subprocess,time
from pathlib import Path
from benchmark.compare import compare_files
b=Path('.scratch/parallel-aggregate-input').resolve();out=b/'serial-frontier-native-q1-stack-profile-01';out.mkdir(exist_ok=False)
binary=b/'serial_frontier_v2_benchmark_embedded';release=json.loads((b/'serial-frontier-v2-release.json').read_text())
sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
assert sha(binary)==release['binary_sha256']
hashes=json.loads((b/'serial-frontier-v2-release-source-hashes.json').read_text());assert all(sha(p)==h for p,h in hashes.items())
previous=Path('.scratch/public-bench/serial-frontier-v2-sf10-providers-01/native').resolve()
setup=json.loads((previous/'setup.json').read_text());setup['temp_directory']=str(out/'temp');(out/'temp').mkdir();(out/'setup.json').write_text(json.dumps(setup))
request=next(x['request'] for x in (json.loads(line) for line in (previous/'execution.jsonl').read_text().splitlines()) if x['side']=='engine' and x['request']['id']=='s1-q01-warmup');request['output']=str(out/'answer.arrow');(out/'request.jsonl').write_text(json.dumps(request)+'\n')
env={k:v for k,v in os.environ.items() if not k.startswith('QE_') and not k.endswith(('_PROF','_DEBUG')) and k not in ['RT_DISABLE','QUERY_ENGINE_ALLOW_THP']}
env.update(RAYON_NUM_THREADS='16',QE_GPU='0',QE_AGG_OWNERSHIP='disjoint',QE_MEM_CAP=str(setup['process_cap_bytes']),TMPDIR=str(out/'temp'))
assert sorted(os.sched_getaffinity(0))==list(range(16))
(out/'manifest.json').write_text(json.dumps({'release':release,'source_inputs':hashes,'driver_sha256':sha(__file__),'affinity':sorted(os.sched_getaffinity(0)),'sampling_seconds':0.2,'maximum_samples':40,'source_commit':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),'perf_event_paranoid':Path('/proc/sys/kernel/perf_event_paranoid').read_text().strip(),'purpose':'Owned local query child GDB stopped-thread attribution; not a CPU percentage or performance comparison. No host profiling permissions changed.'},indent=2))
proc=subprocess.Popen(['gdb','--nx','--quiet','--interpreter=mi2',str(binary)],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,env=env,start_new_session=True,bufsize=0)
selector=selectors.DefaultSelector();selector.register(proc.stdout,selectors.EVENT_READ)
pending=b'';lines=[];token=0;deadline=time.monotonic()+180
log=(out/'gdb-mi.log').open('w')
def read_until(predicate):
 global pending
 captured=[]
 while time.monotonic()<deadline:
  while b'\n' in pending:
   raw,pending=pending.split(b'\n',1);line=raw.decode(errors='replace');log.write(line+'\n');log.flush();lines.append(line);captured.append(line)
   if predicate(line):return captured
  if proc.poll() is not None:raise RuntimeError('gdb terminated before expected response')
  if selector.select(1):pending+=os.read(proc.stdout.fileno(),65536)
 raise TimeoutError('owned debugger watchdog')
def command(text):
 global token
 token+=1;prefix=str(token);proc.stdin.write((prefix+text+'\n').encode());proc.stdin.flush()
 response=read_until(lambda l:l.startswith(prefix+'^'))
 if response[-1].startswith(prefix+'^error'):raise RuntimeError(response[-1])
 return response
samples=[]
try:
 for setting in ['mi-async on','pagination off','confirm off','debuginfod enabled off','print thread-events off']:
  command('-gdb-set '+setting)
 run='run '+shlex.quote(str(out/'setup.json'))+' < '+shlex.quote(str(out/'request.jsonl'))+' > '+shlex.quote(str(out/'responses.jsonl'))+' 2> '+shlex.quote(str(out/'engine.stderr'))
 command('-interpreter-exec console '+json.dumps(run+' &'))
 exited=False
 for i in range(40):
  time.sleep(0.2)
  response=command('-exec-interrupt --all')
  stopped=[l for l in response if l.startswith('*stopped')]
  if not stopped: stopped=read_until(lambda l:l.startswith('*stopped'))
  if 'exited' in stopped[-1]:exited=True;break
  result=command('-interpreter-exec console '+json.dumps('thread apply all bt 8'))
  samples.append({'index':i,'mi':result})
  (out/'samples.json').write_text(json.dumps(samples,indent=2))
  command('-exec-continue')
 if not exited:read_until(lambda l:l.startswith('*stopped') and 'exited' in l)
 command('-gdb-exit');proc.wait(timeout=10)
finally:
 if proc.poll() is None:os.killpg(proc.pid,signal.SIGKILL);proc.wait()
 log.close();selector.close()
responses=[json.loads(x) for x in (out/'responses.jsonl').read_text().splitlines()];completed=[x for x in responses if x.get('status')=='completed'];assert len(completed)==1,responses
policy=json.loads(Path('.scratch/public-bench/tpch-sf10/dataset.json').read_text());q=next(x for x in policy['queries'] if x['id']=='q01')
oracle=Path('.scratch/public-bench/serial-frontier-v2-sf10-providers-01/native/s1-q01-oracle.arrow')
valid=compare_files(out/'answer.arrow',oracle,scratch=out,policy=q['result_policy'],order_by=q['order_by'],limit_rows=q.get('limit_rows'),expected_is_complete=True)
assert valid['ok'];assert sha(binary)==release['binary_sha256'];assert all(sha(p)==h for p,h in hashes.items())
(out/'verified-after.json').write_text(json.dumps({'samples':len(samples),'comparison':valid,'binary':True,'source_inputs':len(hashes),'oracle_sha256':sha(oracle)},indent=2))
print('samples',len(samples),'typed_correct',valid['ok'])
