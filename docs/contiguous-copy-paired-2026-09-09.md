# Cumulative contiguous-copy candidate: balanced comparison — 2026-09-09

Frozen acdb8c51 versus a023079f (inline aggregate input control). This evaluates
the cumulative candidate, including later readers, outer pipeline, resident copy,
runtime-filter lineage and payload ownership. It does not isolate range.rs.

## Outcome

Runner40018 terminal1: custom component exits0, canonical exits1 because timing
and completion gates fail. Across44 blocks,510 engine outputs complete and all510
are typed-correct;506 pass timing,4 finish late.13 requests time out and93 are
not run after a failure. No wrong completed outputs. Neither component has scope
max/OOM/kill events; shared scope high-water marks are480,923,648 bytes after
custom and4,307,410,944 after canonical. These are not query-wide reservations.

Q13 candidate completes all24 measured requests plus4 warmups under fresh
2,866–2,951ms ceilings. The control times out in all4 warmups. This proves a
completion improvement for this component, but cannot produce a paired speed
ratio from censored control samples. Candidate requests remain about2.6s against
DuckDB calibration around287–295ms, well short of leadership.

Q9 times out in all4 warmups on both sides (ceilings4,997–5,548ms). Q12 has only
one fully paired block: control passes18 measured requests overall, candidate6.
Four late completed requests and one candidate warmup timeout prevent acceptance.
Do not hide these failures behind correct outputs or resident-mode improvements.

| Component/query | Candidate/control block ratio | Bootstrap95% interval |
|---|---:|---|
|Canonical q01|1.05094|1.00770–1.08726|
|Canonical q02|1.06946|1.04350–1.09169|
|Canonical q05|0.98758|0.96785–1.00656|
|Canonical q09|incomplete|no complete paired interval|
|Canonical q10|1.00891|0.99233–1.02067|
|Canonical q12|incomplete|no complete paired interval|
|Canonical q13|incomplete|no complete paired interval|
|Canonical q19|1.00357|0.97294–1.04743|
|Canonical q20|1.03546|1.01354–1.06328|
|Custom memory q01|1.04111|1.01695–1.06722|
|Custom memory q06|0.94094|0.86160–1.02758|

Canonical Q1,Q2,Q20 and custom Q1 have intervals entirely above1, indicating
small slowdowns in these comparisons. All complete upper bounds are below1.10;
that does not clear incomplete queries or establish suite leadership. Q5,Q10,Q19
and custom Q6 intervals include1. No aggregate suite ratio is certified.

## Reproduction and boundaries

Drivers: `.scratch/parallel-aggregate-input/run_contiguous_copy_measurements.py`,
`measure_contiguous_copy.py`, `measure_contiguous_copy_smoke.py`. The required
memory-capped wrapper runs them sequentially with TMPDIR in repository scratch,
PYTHONPATH=scripts, taskset0–15,48GiB/no swap. Both binaries verify their SHA256;
all503 current source inputs verify before/after, as do dataset/provider inputs.
The archive retains scripts, harness, samples, outputs, plans, failures and source.

Four fresh blocks per query, six measured pairs plus one warmup per side. Startup
and execution orders are balanced. Each block uses four fresh DuckDB calibration
requests; the median of the last three sets the10×query ceiling. Startup/pipe
allowances do not expand the exact query-time gate. Typed complete DuckDB oracles
validate SQL values and ordering. Timing is embedded parse-to-Arrow-consumed.

Canonical raw Parquet uses16 threads,4GiB query and12GiB process; custom memory
uses4 threads and its separately recorded setup. Custom is600,000-row float smoke,
not canonical SF10 or GPU execution. Complete ratios use geometric means of four
block ratios and an exact four-block bootstrap; this small study is not three
independent sessions or full provider/resource/concurrency certification.

Next: screen raw/native/Iceberg/Lance and separate residency on this frozen binary.
Attribute the small protected slowdowns before accumulating further changes.
The [outer-count design](outer-count-preaggregation-design-2026-09-09.md) records a
possible reduction in logical work, not an implemented or accepted optimization.

Evidence archive: [manifest](benchmarks/2026-09-09-contiguous-copy-pairs/manifest.json),
1,179 files and503 source inputs verified. Provider10708 is now running.
