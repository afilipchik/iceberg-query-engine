# Join-index repair: canonical SF10 provider screen — September 8, 2026

Frozen binary `476ec119ea06b591767a09e2cdb7107aec433b48a15951b8dd23032c3ff602a8`
contains checked join-index spill costing and bounded direct-index selection.
The exact 493 source inputs and release provenance are preserved with the
[paired study](join-index-admission-2026-09-08.md). No source or harness edits
occur during this screen.

Command: repository TMPDIR, `PYTHONPATH=scripts`, `SAFE_BUILD_MEM=48G`,
`SAFE_BUILD_JOBS=1`, `scripts/claude-safe-build.sh`, repository Lance Python,
`.scratch/parallel-aggregate-input/run_join_index_providers.py`.
The driver records each full command and terminal status in
`.scratch/public-bench/join-index-sf10-providers-01/state.json`.

Conditions: canonical SF10, 16 threads, 4 GiB query memory, 12 GiB process cap,
one session, three measured pairs per query, all 22 queries. Raw Parquet, native,
Iceberg and Lance execute sequentially. Each track has fresh matched DuckDB
references, independent typed validation and an unchanged 10× query-time gate.
Late/failed warmups prevent measured engine retries. Preserved output includes
failures, plans, provenance, timing samples and reference calibration outcomes.

Job53182 is terminal1; supplemental23286 is terminal0. All241 completed measured
engine outputs and81 completed warmups pass independent typed comparison with the
preserved full oracle.240 measured pairs pass all original gates. The extra
completed Iceberg Q18 engine output is correct, but its paired reference refuses
262144bytes during measurement; that pair remains failed, and its two dependent
requests are not run.

| Provider | Valid pairs | Completed measured outputs | Completed warmups |
|---|---:|---:|---:|
| Raw Parquet | 60/66 | 60 | 20 |
| Native | 60/66 | 60 | 20 |
| Iceberg | 63/66 | 64 | 22 |
| Lance | 57/66 | 57 | 19 |

Six engine warmups time out: raw Q9/Q13, native Q1/Q13 and Lance Q1/Q13.
Lance Q9 has invalid reference calibration; no engine requests execute for it.
Together with the Iceberg reference refusal,23 measured engine slots are not run.
These are failures, not skips or zero-duration completions. No measured completed
engine output exceeds its calibrated ceiling. Scope peak24314773504bytes; all
cgroup max/OOM/kill counters remain zero. This scope high-water mark is cumulative
across the sequential tracks and is not an individual query RSS or reservation
certificate.

Evidence: `benchmarks/2026-09-08-join-index-providers/`, including all commands,
plans, results, independent completed-output and warmup validation, runtime
provenance and a link to the frozen source archive. Temporary spill payloads are
listed separately if omitted. No source or harness changed during measurement.

This screen does not certify decoded IPC, GPU residency, resource/concurrency
limits, held-out workloads or latency leadership. Earlier provider screens belong
to earlier binaries. Changes in single-session completion counts cannot be
attributed to the repair without a matched comparison.
