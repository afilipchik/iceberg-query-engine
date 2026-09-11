"""Package terminal checkpoint records and independently checked residency ratios."""
import hashlib,json,math,shutil,statistics
from pathlib import Path

b=Path('.scratch/parallel-aggregate-input')
e=json.loads((b/'coordinated-output-checkpoint-evidence.json').read_text())
assert e['status']=='terminal' and all(s['exit_code']==0 for s in e['steps'])
r=Path('.scratch/public-bench/coordinated-output-residency-32g-01')
validation=json.loads((r/'supplemental-validation.json').read_text())
ratios={}
for track,v in validation.items():
 samples_path=r/track/'samples.jsonl'
 rows=[json.loads(l) for l in samples_path.read_text().splitlines()] if samples_path.exists() else []
 expected=20 if track.startswith('smoke') else 3
 queries={}
 for q in sorted({row['query'] for row in rows}):
  samples=[row for row in rows if row['query']==q]
  complete=len(samples)==expected and all(row['comparison']['ok'] and all(row[s]['status']=='completed' and row[s]['ms']<=(row.get('query_ceiling_ms') or statistics.median(row['calibration_ms'])*10) for s in ['engine','duckdb']) for row in samples)
  em=statistics.median(row['engine']['ms'] for row in samples) if complete else None
  dm=statistics.median(row['duckdb']['ms'] for row in samples) if complete else None
  queries[q]={'complete':complete,'engine_median_ms':em,'duckdb_median_ms':dm,'ratio':em/dm if complete else None}
 complete=all(q['complete'] for q in queries.values()) and len(queries)==(2 if track.startswith('smoke') else 22) and v['valid_pairs']==len(rows)
 ratios[track]={'complete':complete,'queries':queries,'geometric_mean_ratio':math.exp(statistics.mean(math.log(q['ratio']) for q in queries.values())) if complete else None,'suite_total_ratio':sum(q['engine_median_ms'] for q in queries.values())/sum(q['duckdb_median_ms'] for q in queries.values()) if complete else None,'wins':sum(q['ratio']<1 for q in queries.values()) if complete else None}
out=Path('docs/benchmarks/2026-09-11-coordinated-output-checkpoint');out.mkdir(exist_ok=False)
(out/'residency-ratios.json').write_text(json.dumps(ratios,indent=2)+'\n')
for name in ['coordinated-output-checkpoint.json','coordinated-output-checkpoint-evidence.json','coordinated-output-full-evidence.json','run_coordinated_output_checkpoint.py','run_coordinated_output_checkpoint_evidence.py','supplement_coordinated_output_checkpoint.py']:
 shutil.copy2(b/name,out/name)
files={str(p.relative_to(out)):hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(out.iterdir())}
(out/'manifest.json').write_text(json.dumps({'files':files,'source_count':531,'release_sha256':json.loads((b/'coordinated-output-release.json').read_text())['binary_sha256'],'note':'Supplement to immutable triage/provider/residency archives; all ratios use complete independently validated measured pairs.'},indent=2)+'\n')
assert all(hashlib.sha256((out/p).read_bytes()).hexdigest()==h for p,h in files.items())
p=Path('docs/coordinated-output-residency-screen-2026-09-11.md')
lines=['','## Complete measured ratios','','Ratios are engine/DuckDB using per-query medians. One session is screening evidence, not a multi-session confidence result.','','| Track | Geometric mean | Suite time | Query wins |','|---|---:|---:|---:|']
for track,v in ratios.items():
 lines.append(f"| {track} | {v['geometric_mean_ratio']:.6f} | {v['suite_total_ratio']:.6f} | {v['wins']}/{len(v['queries'])} |" if v['complete'] else f'| {track} | incomplete | incomplete | incomplete |')
lines+=['','[Per-query ratio supplement](benchmarks/2026-09-11-coordinated-output-checkpoint/residency-ratios.json). The original archived analysis remains unchanged; this table was added after its verification.']
p.write_text(p.read_text()+'\n'.join(lines)+'\n')
print(json.dumps({k:{x:y for x,y in v.items() if x!='queries'} for k,v in ratios.items()},indent=2))
