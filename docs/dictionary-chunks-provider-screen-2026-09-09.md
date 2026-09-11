# Frozen11d16e73 canonical provider screen — September 9, 2026

Provider94344 terminal1; independent audit79244 terminal0. All320 completed engine
outputs are typed-correct:240 measured and80 warmups. The screen requested264
measured pairs;240 are valid. Six engine warmup timeouts and two reference
failures produce24 not-run engine measured slots. No completed wrong answer.

| Mode | Valid measured pairs | Completed warmups | Incomplete queries |
|---|---:|---:|---|
| Raw Parquet |60/66|20|Q9,Q13 engine warmup timeout|
| Native |60/66|20|Q1,Q13 engine warmup timeout|
| Iceberg |63/66|21|Q9 reference warmup allocation refusal|
| Lance |57/66|19|Q1,Q13 engine warmup timeout; Q9 reference calibration crash|

Iceberg's DuckDB warmup refused a262144-byte allocation. Lance completed its
reference warmup but crashed at cal0; do not infer an identical cause from the
shared query number. Invalid reference calibration closes dependent execution;
a missing engine query is neither a wrong answer nor a performance win. All four
modes are incomplete, so no full-suite leadership score is certified.

Same canonical SF10 dataset/provider manifests and harness boundaries as the
preceding screen:22queries,16threads,4GiB query/12GiB process,3measured samples,
1session per mode. Raw/native/Iceberg/Lance execute sequentially under one48GiB
scope. Scope peak27477094400 bytes; zero max/OOM events. The cumulative scope peak
is not process RSS, query admission or a per-mode memory measurement. Frozen
candidate11d16e73975eef6055cc06330533de1c6786ef79ce42566f035004e80470168a,
495source inputs verified; full source in the paired archive.

Command: TMPDIR=$PWD/.scratch PYTHONPATH=$PWD/scripts RAYON_NUM_THREADS=4
SAFE_BUILD_MEM=48G SAFE_BUILD_JOBS=1 scripts/claude-safe-build.sh
.scratch/venv-lance/bin/python .scratch/parallel-aggregate-input/run_dictionary_chunks_providers.py.
Audit used the same wrapper at16GiB with summarize_dictionary_chunks_providers.py;
it independently compares every completed measured and warmup output with the
full typed oracle, without changing timing/pair outcomes.

[Immutable provider evidence](benchmarks/2026-09-09-dictionary-chunks-providers/manifest.json)
includes manifests, plans, outputs, logs, runtime provenance, supplemental checks
and the exact harness/driver. [Paired cumulative candidate study](dictionary-chunks-paired-2026-09-09.md)
found no established speed change and preserved Q9/Q12/Q13 failures; its expanded
Q2 confirmation bounded slowdown below10%.

Next run separate decoded-IPC and CPU/GPU resident checks. The32GiB canonical
capacity experiment does not clear the16GiB preload refusal or lower-budget gates.
Custom float GPU fixtures do not establish canonical SF10 GPU leadership. Complete
provider/resource/concurrency acceptance and collected outer-join attribution remain
open. Dictionary-preserving scan output needs both physical-schema and recursive
buffer-ownership contracts: the current admitted handoff accepts flat arrays only.
