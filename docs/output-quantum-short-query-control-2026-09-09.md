# Output quantum short-query precision control — September 9, 2026

## Outcome

Identical-binary null91329 exits1: ratio1.040493,95%interval0.976383–1.130588.
The interval contains1 but exceeds the prespecified1.10upper limit. All32,784
engine outputs are typed-correct and timing-gated. All16windows finish with
sufficient exposure and graceful exit0; this failure is measurement precision,
not incorrect answers, insufficient window duration, or a candidate regression.

The dependent bd689bf6→01bb077a candidate comparison is **not executed**. The
[earlier protected Q6 interval](output-quantum-protected-2026-09-09.md),
0.987050–1.136243, remains unresolved. Do not discard either result or repeat the
same protocol until a favorable interval appears.

## Prespecified protocol

Same01bb077a binary on both sides, default disjoint ownership, four query threads,
CPU mask0,2,4,6,8,10,12,14,4GiB query/8GiB process budgets and48GiB outer scope,
zero swap. This is the existing600,000-row custom float memory workload, not
canonical SF10 or GPU execution. GPU is disabled.

Eight fresh blocks alternate window order. Each window uses one warmup plus
2048fixed measured requests; the primary metric is mean query time across every
request. Minimum measured exposure is2s. Only one engine process is live at a
time, and it must close gracefully before the next. DuckDB calibrates fresh
10×ceilings per block; every completed output is compared with the independent
typed oracle. Bootstrap uses20,000resamples with fixed seed20260909. Null acceptance
requires0.90≤lower95≤1≤upper95≤1.10, not merely overlap with1.

|Block|Before mean ms|After mean ms|After/before|
|---:|---:|---:|---:|
|0|3.492574|3.523215|1.008773|
|1|2.950538|3.933963|1.333304|
|2|3.747855|3.416669|0.911633|
|3|3.353227|3.364970|1.003502|
|4|3.541238|3.743224|1.057038|
|5|3.461275|3.629007|1.048460|
|6|3.821017|3.626321|0.949046|
|7|3.584912|3.805419|1.061510|

## What the retained evidence supports

Block1's slower identical-binary window takes8056.756ms measured query time
versus6042.702ms, a33.33%difference. Its process CPU-time delta is22.46s versus
16.75s, approximately34.09%higher. These process counters also include protocol,
serialization and warmup work; they are not exclusive query-kernel samples.
The faster window has more recorded involuntary context switches, so attributing
the difference solely to descheduling is unsupported.

The host snapshot records the `powersave` governor, minimum800MHz and maximum
5.5/5.8GHz on the selected cores, with varying instantaneous readings. These are
snapshots, not effective frequencies during each window. They identify a control
variable to investigate, not proof that frequency scaling caused this result.
No host power settings or unrelated processes were changed.

Next collect effective frequency/cycles/instructions and CPU placement with the
same window boundaries, or establish a documented controlled-host protocol before
another acceptance attempt. Keep wall-clock latency as the primary product
metric. Source changes, query-specific tuning, larger DuckDB deadlines or dropping
slow windows would not resolve this measurement uncertainty. Other provider/
residency/resource work can continue while Q6's regression bound remains open.

## Provenance

Candidate/identical control SHA256:
`01bb077a6793b955f43a098e0bf271bc52ce9a7570482e43aeeeb2459d9c975f`.
All511source inputs, binary, data, driver and harness verify after execution.
Scope peak823,513,088bytes; zeroOOM/max events. Archive24759 exits0 and verifies
33,055files. The [immutable archive](benchmarks/2026-09-09-output-quantum-q6-isolated-physical-null-01/manifest.json)
preserves every request/output, window boundary, lifecycle record, host/process
snapshot, fixed protocol and summary. Source remains unchanged.

Driver: `.scratch/parallel-aggregate-input/measure_output_quantum_q6_isolated.py`.
Run through `scripts/claude-safe-build.sh` with repository scratch asTMPDIR,
`--samples 2048 --blocks 8 --minimum-ms 2000 --mode null`. The candidate mode's
prespecification remains preserved, but no candidate run directory exists.
