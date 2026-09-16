import hashlib,json,os,subprocess
from pathlib import Path
b=Path('.scratch/parallel-aggregate-input')
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
paths=set()
for directory in ['src','tests','examples','benches','vendor']:paths.update(p for p in Path(directory).rglob('*') if p.is_file())
paths.update(Path(p) for p in ['Cargo.toml','Cargo.lock','scripts/claude-safe-build.sh','build.rs','.cargo/config.toml'] if Path(p).exists())
hashes={str(p):sha(p) for p in sorted(paths)}
assert len(hashes) >= 799
assert not (b/'serial-frontier-v2-validation.json').exists(), 'Do not overwrite a validation run'
(b/'serial-frontier-v2-validation-source-hashes.json').write_text(json.dumps(hashes,indent=2)+'\n')
def resources():
 rel=next(x.split(':',2)[2] for x in Path('/proc/self/cgroup').read_text().splitlines() if x.startswith('0::'))
 p=Path('/sys/fs/cgroup')/rel.lstrip('/')
 return {'path':str(p),**{n:(p/n).read_text() for n in ['memory.max','memory.swap.max','memory.peak','memory.events']}}
(b/'serial-frontier-v2-validation-resources-before.json').write_text(json.dumps(resources(),indent=2)+'\n')
state={}
suites=[('library',None),('contracts',['lance_filter_concurrency','aggregate_startup_headroom','memory_reservation_contract','reserved_buffer_builder','result_buffer_ownership','typed_memory_pressure','prepared_join_contract','streaming_prepared_join_contract','fused_aggregate_input_errors','fused_input_cpu_parallelism','prepared_wrapper_unknown_contract','shared_prescan_errors','partition_contract','semantic_proof_tests','qualified_column_identity','optimizer_convergence_contract','outer_on_pushdown','outer_stream_contract','runtime_filter_lineage_contract','runtime_filter_domain_contract']),('native_ipc',['native_streaming_scan_tests','ipc_extent_contract','native_table_validation','native_delete_tests','native_insert_tests','native_update_tests','native_dictionary_semi_anti']),('spill',['spill_tests','systemic_numeric_tests','decimal_expression_admission','decimal_root_reuse_aggregate_contract','parallel_input_spill_contract'])]
for mode in ['disjoint','partial']:
 env=dict(os.environ,QE_AGG_OWNERSHIP=mode,RAYON_NUM_THREADS='16')
 for suite,targets in suites:
  cmd=['cargo','test','--locked','--offline','--features','lance,gpu','--no-fail-fast']
  if targets is None:cmd+=['--lib']
  else:
   for target in targets:cmd+=['--test',target]
  key=mode+'-'+suite
  with (b/('serial-frontier-v2-'+key+'.log')).open('w') as log:r=subprocess.run(cmd,env=env,stdout=log,stderr=subprocess.STDOUT)
  state[key]={'command':cmd,'exit':r.returncode,'environment':{'QE_AGG_OWNERSHIP':mode,'RAYON_NUM_THREADS':'16','TMPDIR':env.get('TMPDIR'),'SAFE_BUILD_MEM':env.get('SAFE_BUILD_MEM'),'SAFE_BUILD_JOBS':env.get('SAFE_BUILD_JOBS')}}
  (b/'serial-frontier-v2-validation.json').write_text(json.dumps(state,indent=2)+'\n')
  print(key,r.returncode,flush=True)
assert all(sha(Path(p))==v for p,v in hashes.items())
(b/'serial-frontier-v2-validation-verified-after.json').write_text(json.dumps({'source_inputs':len(hashes)})+'\n')
(b/'serial-frontier-v2-validation-resources-after.json').write_text(json.dumps(resources(),indent=2)+'\n')
raise SystemExit(0 if all(x['exit']==0 for x in state.values()) else 1)
