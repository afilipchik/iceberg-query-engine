"""Sequential residency screens. Run inside the required memory-capped wrapper."""
import hashlib,json,subprocess,sys,time,os
from pathlib import Path
base=Path('.scratch/parallel-aggregate-input');root=Path('.scratch/public-bench/admitted-quantum-residency-32g-01')
release=json.loads((base/'admitted-quantum-release.json').read_text());binary=base/'admitted_quantum_benchmark_embedded'
assert hashlib.sha256(binary.read_bytes()).hexdigest()==release['binary_sha256']
hashes=json.loads((base/'admitted-quantum-release-source-hashes.json').read_text())
assert all(hashlib.sha256(Path(p).read_bytes()).hexdigest()==h for p,h in hashes.items())
root.mkdir(exist_ok=False);records={}
for key in list(os.environ):
 if key.startswith('QE_') or key.endswith(('_PROF','_DEBUG')) or key in ['RT_DISABLE','QUERY_ENGINE_ALLOW_THP']:os.environ.pop(key,None)
os.environ['QE_AGG_OWNERSHIP']='disjoint'
assert sorted(os.sched_getaffinity(0))==list(range(16))
harness={str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in Path('scripts/benchmark').glob('*.py')}
driver_hash=hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
(root/'experiment.json').write_text(json.dumps({'release':release,'source_inputs':hashes,'ownership':'disjoint','driver_sha256':driver_hash,'harness_sha256':harness,'affinity':sorted(os.sched_getaffinity(0)),'qualification':'canonical32GiB capacity experiment; default disjoint ownership; does not clear16GiB preload; custom float GPU smoke is not canonicalSF10'},indent=2)+'\n')
lib=Path('.venv/lib/python3.12/site-packages/nvidia/cuda_nvrtc/lib').resolve()
assert (lib/'libnvrtc.so.12').is_file()
os.environ['LD_LIBRARY_PATH']=str(lib)+(':'+os.environ['LD_LIBRARY_PATH'] if os.environ.get('LD_LIBRARY_PATH') else '')
(root/'runtime.json').write_text(json.dumps({'LD_LIBRARY_PATH':os.environ['LD_LIBRARY_PATH'],'libraries':{str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in lib.glob('*.so*')},'reason':'use the existing verified NVRTC runtime through a process-local loader path'},indent=2)+'\n')
canonical='.scratch/public-bench/tpch-sf10/dataset.json'
canonical_provider='.scratch/public-bench/canonical-sf10-providers-01/decoded_ipc/provider.json'
smoke='.scratch/public-bench/custom-gpu-float-smoke/dataset.json'
smoke_provider='.scratch/public-bench/custom-gpu-float-ipc-01/provider.json'
cases=[
 ('canonical_decoded_ipc','decoded_ipc',canonical,canonical_provider,16,32,48,3,False,None),
 ('canonical_gpu_control','gpu_control',canonical,canonical_provider,16,32,48,3,False,None),
 ('canonical_gpu_mixed','gpu',canonical,canonical_provider,16,32,48,3,False,'canonical_gpu_control'),
 ('smoke_gpu_control','gpu_control',smoke,smoke_provider,4,4,8,20,False,None),
 ('smoke_gpu_required','gpu',smoke,smoke_provider,4,4,8,20,True,'smoke_gpu_control'),
]
for name,track,dataset,provider,threads,memory,process,samples,required,control in cases:
 command=[sys.executable,'-m','benchmark','run','--dataset',dataset,'--engine-binary',str(binary),'--output',str(root/name),'--track',track,'--provider-manifest',provider,'--threads',str(threads),'--memory-gib',str(memory),'--process-cap-gib',str(process),'--samples',str(samples),'--sessions','1','--gpu-residency','required' if required else 'mixed']
 if track in ('gpu','gpu_control'):command+=['--host-arrow-preloaded']
 if control:command+=['--gpu-control-manifest',str(root/control/'manifest.json')]
 record={'command':command,'started_unix':time.time(),'status':'running','capacity_experiment_not_16g_gate':dataset==canonical,'workload_kind':'canonical_sf10' if dataset==canonical else 'custom_float_smoke_not_sf10'};records[name]=record
 (root/'state.json').write_text(json.dumps(records,indent=2)+'\n')
 with (root/f'{name}.log').open('w') as log:result=subprocess.run(command,stdout=log,stderr=subprocess.STDOUT)
 record.update(status='terminal',exit_code=result.returncode,finished_unix=time.time())
 report=root/name/'report.json'
 if report.exists():record['report']=json.loads(report.read_text())
 (root/'state.json').write_text(json.dumps(records,indent=2)+'\n')
 print(json.dumps({'case':name,'exit_code':result.returncode,'report':record.get('report')}),flush=True)
assert all(hashlib.sha256(Path(p).read_bytes()).hexdigest()==h for p,h in hashes.items())
assert hashlib.sha256(binary.read_bytes()).hexdigest()==release['binary_sha256']
assert hashlib.sha256(Path(__file__).read_bytes()).hexdigest()==driver_hash
assert all(hashlib.sha256(Path(p).read_bytes()).hexdigest()==v for p,v in harness.items())
(root/'verified-after.json').write_text(json.dumps({'source_inputs':len(hashes),'binary_sha256':release['binary_sha256']})+'\n')
raise SystemExit(0 if all(r['exit_code']==0 for r in records.values()) else 1)
