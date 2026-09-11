"""Audit only terminal frozen screens, then archive every non-temporary artifact."""
import hashlib,json,shutil,subprocess,sys,time
from pathlib import Path
b=Path('.scratch/parallel-aggregate-input');job=b/'admitted-quantum-full-evidence.json'
assert not job.exists()
def save(status,**kw):job.write_text(json.dumps(dict(status=status,**kw),indent=2)+'\n')
def digest(p):
 with Path(p).open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
save('waiting_for_screens')
start=time.monotonic()
while True:
 state=json.loads((b/'admitted-quantum-full-screens.json').read_text())
 if state['status']=='terminal':break
 if time.monotonic()-start>7200:save('observation_deadline');raise SystemExit(1)
 time.sleep(5)
assert len(state['steps'])==2 and all(s['status']=='terminal' for s in state['steps'])
h=json.loads((b/'admitted-quantum-release-source-hashes.json').read_text())
assert all(digest(p)==v for p,v in h.items())
save('auditing')
audit_exits={}
for kind in ['providers','provider_ratios','residency']:
 with (b/f'admitted-quantum-{kind}-summary.log').open('x') as log:
  result=subprocess.run([sys.executable,str(b/f'summarize_admitted_quantum_{kind}.py')],stdout=log,stderr=subprocess.STDOUT)
  audit_exits[kind]=result.returncode
  save('auditing',audit_exits=audit_exits)
roots={'providers':Path('.scratch/public-bench/admitted-quantum-sf10-providers-01'),'residency':Path('.scratch/public-bench/admitted-quantum-residency-32g-01')}
archives={}
for kind,root in roots.items():
 exp=json.loads((root/'experiment.json').read_text())
 assert (root/'verified-after.json').exists()
 assert digest(b/f'run_admitted_quantum_{kind}.py')==exp['driver_sha256']
 for folder in root.iterdir():
  m=folder/'manifest.json'
  if m.is_file():
   m=json.loads(m.read_text());assert all(digest(p)==v for p,v in m['implementation_sha256'].items())
 summary=json.loads((root/('supplemental-summary.json' if kind=='providers' else 'supplemental-validation.json')).read_text())
 lines=[f'# Planned scan quantum: {kind} screen — 2026-09-11','',
 'Frozen candidate recorded in the archived release manifest, 527 verified source inputs, default disjoint ownership. One session with three samples per canonical SF10 query. This is a screening run, not multi-session, resource, concurrency or DuckDB leadership certification.', '',
 'Raw/native/Iceberg/Lance use16threads and4/12GiB query/process caps. Canonical decoded IPC and GPU controls use16threads and32/48GiB, with preload excluded: a capacity experiment that does not clear16GiB preload admission. Custom float GPU smoke uses4threads and4/8GiB,20samples per query; its device proof does not establish canonical SF10 device execution.', '',
 '| Track | Valid measured pairs | Typed-correct outputs including warmup | Completion |','|---|---:|---:|---|']
 for name,v in summary.items():
  if kind=='providers':
   checks=json.loads((root/name/'supplemental-completed-correctness.json').read_text())+json.loads((root/name/'supplemental-warmup-correctness.json').read_text())
  else:checks=v['checks']
  outputs=sum(c['comparison']['ok'] for c in checks)
  complete=v['valid_pairs']==v['requested_pairs'] and v['requested_pairs']>0
  lines.append(f"| {name} | {v['valid_pairs']}/{v['requested_pairs']} | {outputs} | {'complete screen' if complete else 'incomplete'} |")
 if kind=='providers':
  ratios=json.loads((root/'per-query-screen.json').read_text())
  lines+=['','| Track | Geometric mean ratio | Suite total ratio |','|---|---:|---:|']
  for name,v in ratios.items():lines.append(f"| {name} | {v['geometric_mean_ratio'] if v['complete'] else 'incomplete'} | {v['suite_total_ratio'] if v['complete'] else 'incomplete'} |")
  lines+=['','Ratios are engine/DuckDB; lower is better. Incomplete tracks receive no full-suite score. All query ratios and failures remain in the archive.']
 else:
  gpu=summary['canonical_gpu_mixed'];smoke=summary['smoke_gpu_required']
  lines+=['',f"Canonical mixed-GPU requests with reported successful device execution: {gpu['reported_successful_device_requests']}; case exit {gpu['case_exit_code']}. Custom required-GPU request-scoped device validation: {smoke['required_device_measured_valid']}. Failed CPU control gates are preserved; no missing execution is counted as GPU coverage."]
 lines+=['','Cumulative build-free screen scope resource evidence:', '', '```json',json.dumps(state['resources_after'],indent=2),'```','',
 f'Independent audit exit codes: {audit_exits}. A nonzero audit remains a failed gate and is preserved in this archive.', '',
 'Every completed output was independently compared with its typed oracle after all timed stages ended. Strict valid-pair counts require both engines within the matched query ceiling. Timeouts, refusals, reference crashes and not-run samples remain failed outcomes; typed correctness alone does not clear performance gates.', '',
 f'[Complete archive](benchmarks/2026-09-11-admitted-quantum-{kind}/manifest.json). See runs/state.json, each report.json, samples.jsonl and execution.jsonl for exact failure provenance.']
 doc=Path(f'docs/admitted-quantum-{kind}-screen-2026-09-11.md');assert not doc.exists();doc.write_text('\n'.join(lines)+'\n')
 out=Path(f'docs/benchmarks/2026-09-11-admitted-quantum-{kind}');out.mkdir(exist_ok=False);omitted=[]
 for p in sorted(root.rglob('*')):
  if not p.is_file():continue
  rel=p.relative_to(root)
  if 'engine-spill' in p.parts or 'duckdb-temp' in p.parts:
   omitted.append({'path':str(rel),'bytes':p.stat().st_size});continue
  dest=out/'runs'/rel;dest.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(p,dest)
 shutil.copytree('scripts/benchmark',out/'harness',ignore=shutil.ignore_patterns('__pycache__','*.pyc'))
 names=[f'run_admitted_quantum_{kind}.py',f'summarize_admitted_quantum_{kind}.py',f'admitted-quantum-{kind}.log',f'admitted-quantum-{kind}-summary.log','admitted-quantum-release.json','admitted-quantum-release-source-hashes.json','admitted-quantum-full-screens.json','run_admitted_quantum_full_screens.py','finish_admitted_quantum_full_evidence.py']
 if kind=='providers':names+=['summarize_admitted_quantum_provider_ratios.py','admitted-quantum-provider_ratios-summary.log']
 for name in names:shutil.copy2(b/name,out/name)
 shutil.copy2(doc,out/'analysis.md')
 (out/'omitted-temporary-payloads.json').write_text(json.dumps(omitted,indent=2)+'\n')
 files={str(p.relative_to(out)):digest(p) for p in sorted(out.rglob('*')) if p.is_file()}
 (out/'manifest.json').write_text(json.dumps({'source_count':len(h),'source_inputs_verified':True,'engine_source_archive':'../2026-09-11-admitted-planned-quantum/source.tar.gz','files':files},indent=2)+'\n')
 assert all(digest(out/p)==v for p,v in files.items())
 archives[kind]={'files_verified':len(files),'omitted_payloads':len(omitted),'omitted_bytes':sum(x['bytes'] for x in omitted)}
 print(kind,json.dumps(archives[kind]),flush=True)
assert all(digest(p)==v for p,v in h.items())
save('terminal',archives=archives,source_inputs=len(h),audit_exits=audit_exits)
raise SystemExit(int(any(audit_exits.values())))
