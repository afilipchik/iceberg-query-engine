# Startup admission: canonical SF10 provider screen

Frozen008d92f7 provider session61495 is terminal1; independent audit18656 is
terminal0. All334 completed outputs are independently typed-correct:250 measured
outputs and84 warmups. Only249/264 requested measured pairs pass completion,
correctness and the10×query-time ceiling. No DuckDB leadership is established.

| Provider | Valid measured pairs | Correct measured outputs | Correct warmups |
|---|---:|---:|---:|
| Raw Parquet |66/66|66|22|
| Native |59/66|60|20|
| Iceberg |64/66|64|22|
| Lance |60/66|60|20|

## Conditions and provenance

Canonical SF10, default disjoint ownership,16threads/CPU0–15,4GiB query/12GiB
process,3samples and1session. Fresh DuckDB calibration sets each10×query-time
ceiling; startup allowance is separate. GPUoff. All engines and reference/comparator
work run under the48GiB capped wrapper, swap0. Peak scope memory19,328,647,168bytes;
zero max/OOM events. The scope includes setup/reference work, not per-query RSS.
All520 source inputs, candidate binary, driver and harness after-run guards verify.
All provider records are terminal; no source/harness edits occurred during measurement.

## Completion and failures

Raw Parquet completes all22 queries: geomean2.971047800172142,
suite ratio3.3691920903363974, zero query wins, worst ratio8.772984382835851.
The other providers are incomplete; do not assign whole-suite winning ratios.

Native Q1 warmup times out at2852.1756595000625ms query ceiling and Q17 at
1069.7273118421435ms. Their dependent measurements remain not_run. Q6 iteration3
completes correctly in1358.372472ms but exceeds its1306.562409736216ms ceiling.
This correct-but-late output reduces native from the preliminary60 to59 valid pairs.
The independent audit preserves that distinction from the harness comparison flag.

Iceberg Q18 measured reference iteration2 refuses a262144-byte block with
OutOfMemoryException after853.3378667198122ms. Engine iterations2/3 remain not_run;
engine warmup and first pair completed. This is a reference refusal, not an engine
wrong result. Lance Q1 warmup times out at7534.439656883478ms ceiling. Lance Q9
fails at the oracle request: DuckDB refuses a268435456-byte block after
122.51295987516642ms. There is no oracle or calibrated ceiling for dependent work.
Do not conflate these with the prior binary's reference failure phases or retry
calibration until successful. No cgroup OOM event accompanied these refusals.

## Path forward

Q13/Q17 account for31.87%of raw median-suite excess (total11208.418262563264ms).
This ranks query-level work, not operator CPU time. The systemic copied-input
queue/progress hypothesis now has an owned allocation trace and requires runtime
frontier attribution before a repair. Preserve generic SQL and resource contracts.

The [paired diagnostic](startup-headroom-measurement-2026-09-10.md) completed64
correct outputs against38966ae6 with small native/rawQ9 slowdowns. The
[startup repair](aggregate-startup-headroom-2026-09-10.md) clears four partial-native
memory failures; it does not imply canonical performance leadership. Multi-session,
resource/concurrency, decoded residency and canonical GPU execution remain open.

[Immutable archive](benchmarks/2026-09-10-startup-headroom-providers/manifest.json)
contains all samples, failures, typed oracles, plans, independent audits, scope
measurements and reproducible scripts. Temporary spill payload omissions are listed.

## Reference refusal attribution follow-up

A read-only review of duckdb_worker.py:18–28 confirms RLIMIT_DATA is installed
from the12GiB process cap before importing DuckDB/PyArrow, while DuckDB separately
receives the4GiB query-memory setting. providers.py:79–105 loads the direct
Iceberg/Lance extensions; the pinned Lance aggregate-pushdown compatibility
configuration disables its extension optimizer. The other DuckDB optimizers remain.
Thus a bad-allocation exception and zero cgroup OOM events do not isolate the
limiting layer or establish a query-engine defect. No reference setup was changed.

After current candidate measurements, reproduce the exact failing reference
request in an owned capped worker and capture the effective limits, process memory
and request phase. Preserve the original stopped calibration/measurement chain.
Do not silently raise limits, switch readers, enable an affected optimizer, or
substitute a successful later oracle into this frozen screen. Any explicitly
changed-boundary diagnostic is separate evidence and requires a new baseline.
