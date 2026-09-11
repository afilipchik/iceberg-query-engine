# Parallel reduction SF10 provider screen — September 9, 2026

## Outcome

Frozen `bd689bf6` completes 247/264 valid measured pairs across four providers.
All331completed engine outputs are independently typed-correct:247measured and
84warmups. Four warmup timeouts, one late correct warmup, and a measured Lance
DuckDB reference refusal leave required execution incomplete. This is not full
provider/resource/concurrency certification. Default ownership stays disjoint.

| Track | Valid measured pairs | Correct measured outputs | Correct warmups | Remaining failures |
|---|---:|---:|---:|---|
| Raw Parquet |63/66|63|21|Q9 warmup timeout;3measured not run|
| Native |57/66|57|20|Q1/Q6 warmup timeouts;Q11 late correct warmup;9measured not run|
| Iceberg |66/66|66|22|None in this session|
| Lance |61/66|61|21|Q13 warmup timeout;Q9 second reference refused, dependent requests stopped;5engine samples not run|

Iceberg wins17/22queries, with engine/DuckDB geometric-mean ratio0.415091 and
suite-total ratio0.592510. Its worst query ratio is1.830853 (Q16). Every query
completes and remains below2× in this one session. Three independent sessions,
protected-regression and resource/concurrency gates are still required; the
harness correctly leaves leadership uncertified. The other tracks are incomplete
and therefore have no full-suite aggregate score.

Native Q11 completes its warmup correctly in164.292ms versus a163.802ms ceiling.
It remains a timing failure; no measured retry was used to replace it. Lance Q9
completes its first measured pair, then the second DuckDB reference refuses a
134,217,728-byte block. Both dependent engine requests remain unexecuted. This is
a reference failure, not an engine answer or timing failure.

## Matched per-query ratios

Each value is the median engine time divided by median DuckDB time, using all
three valid measured pairs for that query. A dash means incomplete required
pairs, even when an earlier result completed correctly. Do not combine the
remaining cells into a substitute full-suite score.

| Query | Raw Parquet | Native | Iceberg | Lance |
|---|---:|---:|---:|---:|
|Q1|3.138|—|0.343|7.876|
|Q2|1.119|1.978|0.213|0.953|
|Q3|1.626|3.029|0.217|1.251|
|Q4|1.655|1.466|0.234|1.296|
|Q5|2.053|7.577|0.315|1.253|
|Q6|1.902|—|0.236|1.089|
|Q7|3.859|5.243|0.530|4.437|
|Q8|1.198|4.316|0.179|2.288|
|Q9|—|4.805|1.787|—|
|Q10|4.884|5.447|0.765|2.668|
|Q11|2.868|—|0.456|2.329|
|Q12|9.835|3.323|1.282|1.323|
|Q13|8.846|8.314|1.217|—|
|Q14|1.832|1.607|0.255|0.856|
|Q15|1.722|6.041|0.238|5.162|
|Q16|7.400|7.254|1.831|4.423|
|Q17|8.308|8.839|1.006|2.181|
|Q18|3.362|12.760|0.523|8.074|
|Q19|1.699|2.891|0.241|1.107|
|Q20|1.578|3.469|0.221|1.105|
|Q21|1.223|3.869|0.192|1.239|
|Q22|1.574|1.519|0.262|0.648|

## Scope and reproducibility

Candidate SHA256:
`bd689bf675ea1977e8573d6a63587cf5a8fb882164161b26439c8793b202ab23`.
The510source inputs, candidate binary, driver and versioned harness verify after
measurement. No source or harness changes occurred during execution.

The experiment explicitly requests `QE_AGG_OWNERSHIP=partial`. This selects the
experimental strategy where supported; it does not imply every query/provider
executes the resident aggregate kernel. Unsupported layouts retain their existing
fallback. GPU is disabled, and decoded IPC/GPU residency are separate tracks.

Each track uses canonical SF10, original SQL, three measured samples in one
session, sixteen threads, CPU affinity0–15,4GiB query and12GiB process caps.
The existing harness uses matched schemas/data and provider-specific preparation
boundaries, fresh DuckDB calibration, typed comparison, and a10×query-time ceiling.
The outer scope is48GiB with zero swap. Track runs are sequential. Scope peak
is20,523,143,168bytes across the full driver lifetime, with zeroOOM/max events.
Repeated track snapshots share that cumulative peak; they are not independent
per-track peaks or proof of exact query-wide reservations.

Job81715 exits1 because the screen is incomplete: raw/native/Lance children exit1,
Iceberg exits0. Supplemental typed audit4915 exits0. The audit includes all
completed warmups and measured results, including the late nativeQ11 warmup.
The ratio summary withholds aggregate scores for incomplete tracks.

Driver: `.scratch/parallel-aggregate-input/run_parallel_reduction_providers.py`.
Data: `.scratch/public-bench/tpch-sf10/dataset.json`.
Native/Iceberg/Lance manifests: `.scratch/public-bench/canonical-sf10-providers-02`.
Output: `.scratch/public-bench/parallel-reduction-sf10-providers-01`.
All engine/audit invocations use `scripts/claude-safe-build.sh`, with repository
scratch as `TMPDIR`.

[Immutable archive](benchmarks/2026-09-09-parallel-reduction-providers/manifest.json)
preserves samples, warmups, references, plans, configuration, source provenance,
failures, driver, harness and typed audits. Temporary spill payloads are listed
separately if omitted. Source archive:
[parallel reduction correctness](benchmarks/2026-09-09-parallel-aggregate-reduction/manifest.json).

## Next work

The single-session Iceberg result is promising, while raw/native/Lance still need
completion and latency improvements. RawQ12 now passes its fresh ceiling in all
three samples, but this is not a same-condition comparison against the preceding
binary. The nativeQ1/Q6 startup/first-query cost and rawQ9/LanceQ13 timeouts need
attribution under their actual provider boundaries, not larger deadlines.

The [prior output batching audit](aggregate-output-quantum-audit-2026-09-09.md)
remains applicable after the all-valid bitmap repair: final aggregate output still
builds64-row ranges, and current Q18 profiling attributes roughly1.25–1.30s to
output construction/HAVING. Its proposed bounded construction-only range fallback
is the next isolated shared batching candidate. Preserve exact admission and
retry cursors; do not replay filtering or already-published rows. Any new source
requires a new frozen binary and protected measurements. General ownership
costing, repeat-session provider evidence, decoded IPC/GPU residency and resource/
concurrency gates remain open. No default-policy or leadership promotion follows
from this screen.
