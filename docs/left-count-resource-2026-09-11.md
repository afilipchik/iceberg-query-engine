# Retained LEFT COUNT: independent low-budget execution

Frozen0c867c5c completes the generated count-preaggregation workload at1,4 and
16MiB query budgets with an independent exact Int64 oracle. Each result contains
10,002 groups. The1MiB run reports1,960,572bytes spilled;4/16MiB report no spill.
The optimized retained SUM/count-preaggregation plan is visible in every run.

The fixture contains10,000 right key groups with16 rows each, every fourth value
NULL, and every17th group entirely NULL. Left keys repeat1–3 times; two NULL left
keys and one unmatched key are included. Right NULL keys have both nullable and
nonnullable counted values. Expected counts are computed directly from fixture
arithmetic, not another execution of this engine. The user column `__ea_cnt`
forces generated-name freshness. Parquet row groups contain256 rows, uncompressed
and without dictionary encoding. This is a deliberately bounded fixed-width
resource fixture, not a whole-page decompression stress test or a benchmark.

Each fresh engine process uses4 threads/CPU0–3, default disjoint ownership,
12GiB process cap, GPUoff, repository TMPDIR and180s watchdog. Driver runs through
the48GiB wrapper with swap disabled. No source edits occur. After-run hashes
verify520 source inputs, binary, fixture data and driver. The scope peaks at
179,470,336bytes with zero max/OOM/kill events.

| Query budget | Typed output | Reported spill bytes | Reserved peak bytes |
|---|---|---:|---:|
|1MiB|10,002 correct groups|1,960,572|1,046,508|
|4MiB|10,002 correct groups|0|2,584,646|
|16MiB|10,002 correct groups|0|2,928,261|

Reservation telemetry does not certify exact process RSS or all allocation paths.
Physical plans use existing spillable aggregate/join operators and legacy scan
routing. The trace does not certify admitted aggregate parallelism. This closes
an actual-spill oracle for the new logical shape; it does not clear historical
small-budget join gates, partial ownership, canonical resource/concurrency
acceptance, or GPU execution.

Reproduction driver: `.scratch/parallel-aggregate-input/run_left_count_resource.py`,
executed with `.scratch/venv-lance/bin/python` inside the required wrapper and the
existing NVRTC loader path. The driver preserves every response and refusal;
its exit alone is not proof of successful queries. All three responses and typed
arrays were inspected for this report.
