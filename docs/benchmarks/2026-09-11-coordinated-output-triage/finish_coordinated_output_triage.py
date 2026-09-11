"""Audit and archive terminal three-binary diagnostics; no engine execution."""
import json,statistics,hashlib,shutil
from pathlib import Path
b=Path('.scratch/parallel-aggregate-input');r=b/'coordinated-output-paired-01'
assert (r/'verified-after.json').exists()
a=json.loads((r/'summary.json').read_text());assert len(a)==42 and all(x['exit']==0 and not x['timeout'] and len(x['comparisons'])==1 and x['comparisons'][0]['ok'] for x in a)
sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
h=json.loads((b/'coordinated-output-release-source-hashes.json').read_text());assert all(sha(p)==v for p,v in h.items())
m=json.loads((r/'manifest.json').read_text());assert sha(b/'run_coordinated_output_triage.py')==m['driver_sha256'];assert all(sha(p)==v for p,v in m['harness_sha256'].items())
ratios={};plans=[]
for mode,q in sorted({(x['mode'],x['query']) for x in a}):
 samples={label:[next(x['instrumented_ms'][0] for x in a if x['mode']==mode and x['query']==q and x['label']==label and x['block']==i) for i in [1,2]] for label in ['baseline','control','candidate']}
 ratios[mode+'/'+q]={'samples_ms':samples,**{label:{'ratio_of_means':statistics.mean(samples['candidate'])/statistics.mean(samples[label]),'block_ratios':[v/w for v,w in zip(samples['candidate'],samples[label])]} for label in ['baseline','control']}}
 for block in [1,2]:
  rows=[next(z for z in x['responses'] if z.get('status')=='completed') for x in a if x['mode']==mode and x['query']==q and x['block']==block];assert len(rows)==3
  plans.append({'mode':mode,'query':q,'block':block,**{key:len({x[key] for x in rows})==1 for key in ['physical_plan','optimized_plan']}})
(r/'triage-ratios.json').write_text(json.dumps(ratios,indent=2)+'\n');(r/'plan-comparison.json').write_text(json.dumps(plans,indent=2)+'\n')
res=json.loads((r/'resources-after.json').read_text());release=json.loads((b/'coordinated-output-release.json').read_text())
lines=['# Coordinated reader output: three-binary diagnostic — 2026-09-11','',f"Candidate `{release['binary_sha256']}` produces42independently typed-correct outputs and{sum(len(x['traces']) for x in a)}join traces. Source531,all three binaries,dataset/providers,harness and driver verify after execution. Of14three-way logical/physical plan groups,{sum(not x['physical_plan'] or not x['optimized_plan'] for x in plans)}differ.",'','Baseline13210a20 predates mixed numeric compilation; control 1bba20b3 precedes coordinated page preparation and reversible output trials. Two reverse-order blocks,default disjoint ownership,GPUoff. Raw/native16threads and4/12GiB;resident4threads at32/48GiB,preload excluded. Generic rawQ6 disables morsel routing,with actual plan assertions. All instrumentation matches. The180second diagnostic watchdog is not a fresh DuckDB10x gate. No confidence interval,leadership or full provider/resource/concurrency acceptance is implied.','','| Mode/query | Candidate/baseline | Block ratios | Candidate/previous checkpoint | Block ratios |','|---|---:|---|---:|---|']
for key,v in ratios.items():lines.append('| '+key+' | '+f"{v['baseline']['ratio_of_means']:.6f}"+' | '+', '.join(f'{x:.6f}' for x in v['baseline']['block_ratios'])+' | '+f"{v['control']['ratio_of_means']:.6f}"+' | '+', '.join(f'{x:.6f}' for x in v['control']['block_ratios'])+' |')
lines+=['',f"Cumulative scope peak{int(res['memory.peak'])}bytes;swap maximum{int(res['memory.swap.max'])};events `{res['memory.events'].strip().replace(chr(10),'; ')}`. This scope includes only the diagnostic requests; its peak is not query-only RSS.",'','[Attribution and tests](compiled-coercion-attribution-2026-09-11.md). [Complete evidence](benchmarks/2026-09-11-coordinated-output-triage/manifest.json). All samples,failures,oracles and plans are preserved.']
p=Path('docs/coordinated-output-triage-2026-09-11.md');assert not p.exists();p.write_text('\n'.join(lines)+'\n')
o=Path('docs/benchmarks/2026-09-11-coordinated-output-triage');o.mkdir(exist_ok=False);shutil.copytree(r,o/'runs')
for x in a:
 source=Path(m['cases'][x['case']]['oracle_parent'])/('s1-'+x['query']+'-oracle.arrow');assert sha(source)==x['oracle_sha256'];shutil.copyfile(source,o/'runs'/x['case']/(x['query']+'-independent-oracle.arrow'))
for name in ['audit_coordinated_output_routes.py','coordinated-output-runtime-route-audit.json','run_coordinated_output_triage.py','build_coordinated_output.py','finish_coordinated_output_triage.py','coordinated-output-triage.log','coordinated-output-release.log','coordinated-output-release.json','coordinated-output-release-source-hashes.json','coordinated-output-release-job.json','admitted-coalesce-release-source-hashes.json','incremental-header-release-source-hashes.json']:shutil.copyfile(b/name,o/name)
shutil.copyfile(p,o/'analysis.md');shutil.copytree('scripts/benchmark',o/'harness',ignore=shutil.ignore_patterns('__pycache__','*.pyc'))
f={str(p.relative_to(o)):sha(p) for p in o.rglob('*') if p.is_file()};(o/'manifest.json').write_text(json.dumps({'files':f,'source_inputs':h,'source_archive':'../2026-09-11-coordinated-output-source/source.tar.gz'},indent=2)+'\n');assert all(sha(o/p)==v for p,v in f.items());print({'files_verified':len(f),'source_inputs':len(h),'outputs':len(a)})
