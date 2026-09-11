# Frozen11d16e73 residency screen — September 9, 2026

Residency48225 terminal1; independent audit79688 terminal0. All244 completed
engine outputs are typed-correct (200 measured,44warmups). Required custom GPU
has40/40 measured device executions plus2warmups: one attempted/completed device
run per request, zero failures, matching prepared session ID, unchanged residency
and fallback counters, and zero upload requests. A success status alone was not
used as device proof.

| Case | Valid measured pairs | Completed warmups | Outcome |
|---|---:|---:|---|
| Canonical decoded IPC |60/66|20|Q1,Q13 warmup timeout|
| Canonical preloaded CPU control |60/66|20|Q1,Q13 warmup timeout|
| Canonical mixed GPU |Not executed|0|Incomplete CPU-control prerequisite; exit2|
| Custom float CPU control |40/40|2|Complete|
| Custom required GPU |40/40|2|Complete with device evidence|

Canonical cases are labelled capacity experiments at32GiB query/48GiB process,
16threads and3samples in one session. They do not clear16GiB preload refusal,
lower resource gates or full canonical GPU certification. Custom cases contain
600000 float rows,4threads,4GiB query/8GiB process and20samples per query in one
session. They are neither canonical SF10 nor an upload-inclusive GPU benchmark.

Custom resident median milliseconds:

| Query | CPU control | Required GPU | CPU-run DuckDB | GPU-run DuckDB |
|---|---:|---:|---:|---:|
| Q1 |70.354711|1.045604|14.161282|14.218427|
| Q6 |7.718670|0.837747|13.297342|8.518498|

Separate GPU preparation responses:Q1 55.020721ms (24Mcolumn bytes,600000code
bytes,6groups);Q6 35.153208ms (19.2Mcolumn bytes,1group). Preparation is excluded
from the resident medians. These single-session medians are not balanced
multi-session causal effects or cold end-to-end comparisons.

Existing repository NVRTC libraries were exposed only through the worker's
LD_LIBRARY_PATH; runtime.json preserves paths and hashes. No installation or
global host change. Sequential cases used a64GiB scope; cumulative peak29727342592
bytes, zero max/OOM events. This is not process RSS, query-wide admission or VRAM
certification. Binary11d16e73975eef6055cc06330533de1c6786ef79ce42566f035004e80470168a,
495 source hashes unchanged through measurement.

Command: TMPDIR=$PWD/.scratch PYTHONPATH=$PWD/scripts RAYON_NUM_THREADS=4
SAFE_BUILD_MEM=64G SAFE_BUILD_JOBS=1 scripts/claude-safe-build.sh
.scratch/venv-lance/bin/python .scratch/parallel-aggregate-input/run_dictionary_chunks_residency.py.
Audit used the same wrapper at16GiB with summarize_dictionary_chunks_residency.py.
[Immutable evidence](benchmarks/2026-09-09-dictionary-chunks-residency/manifest.json)
preserves all outputs, manifests, runtime/device records, harness and scripts.

The cumulative candidate repairs several resource contracts but the paired
study establishes no new CPU speed gain. Next CPU attribution must cover the
collected outer-join preflight/collection/probe/filter/gather phases, and compare
resident/native aggregate routing with the raw fast path. Do not implement a
query-specific shortcut or remove the outer candidate guard without a bounded
output contract. Full provider/resource/concurrency/workload acceptance remains
open in the existing epic.
