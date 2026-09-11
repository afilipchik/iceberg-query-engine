# Retained LEFT COUNT: canonical SF10 provider screen

Frozen0c867c5c provider20601 is terminal1; independent audit89129 is terminal0.
All336 completed outputs are typed-correct:252 measured outputs and84 warmups.
There are251/264 valid measured pairs. Twelve engine measurements were not run;
one completed Iceberg engine output lacks a successful measured reference.

| Provider | Valid/requested pairs | Outstanding measurements |
|---|---:|---|
| Raw Parquet |66/66|None|
| Native |60/66|Q1/Q17 warmup timeouts|
| Iceberg |65/66|Q18 measured reference iteration3 allocation refusal|
| Lance |60/66|Q1 warmup timeout; Q9 oracle allocation refusal|

Raw geomean is2.8312845311292767, suite ratio3.0597785109090903, zero wins/22,
and worst ratio8.779810270083438(Q17). Q13 ratio is2.2078880577912425.
These ratios are engine/DuckDB. The prior a4103dfa screen was2.955826/3.348167,
but separate calibrations/sessions are not a paired causal estimate. The matched
[comparison](left-count-measurement-2026-09-11.md) supplies candidate/control
Q13 evidence and preserves Q9 regression signals. No DuckDB leadership is certified.
Incomplete providers have no accepted suite result.

Native Q1 warmup exceeds3400.242426ms and Q17 exceeds1196.143604ms.
Iceberg Q18 measured reference iteration3 refuses262144bytes after540.842730ms;
the engine had already completed, and its independent oracle check passes.
Lance Q1 warmup exceeds7502.321447ms; Q9 oracle refuses268435456bytes after
119.177965ms, so there is no validated timing ceiling. Allocation exceptions
and zero cgroup OOM events do not identify the refusing resource layer.

Conditions: default disjoint ownership,16threads/CPU0–15,4GiB query/12GiB process,
three measured samples, one warmup and one session per provider. Fresh matched
DuckDB calibration supplies the10×query-time gate; startup allowance is separate.
GPUoff. The48GiB capped scope disables swap and peaks at25257869312bytes,
with zero max/OOM/kill events. Global after-run checks verify520 source inputs,
frozen binary, data/providers and harness/driver hashes. No source/harness edits
occurred during measurement. Complete resource/concurrency and multi-session
acceptance remain open; residency and GPU execution are separate tracks.

[Both-mode validation](left-count-validation-2026-09-11.md) retains historical
native/IPC and legacy spill failures with no new names. The new logical shape
also has an [independent1MiB actual-spill oracle](left-count-resource-2026-09-11.md).
Neither substitutes for the remaining acceptance gates.

Original artifacts: `.scratch/public-bench/left-count-sf10-providers-01`.
[Verified archive](benchmarks/2026-09-11-left-count-providers/manifest.json).
