# Bound key arrays: SF10 provider screen — 2026-09-09

Frozen `0f30c946ad79f08fba94ac8941394fee0e6c2401fe4b1c3628e4809aaf12c3e0`
completes325 typed-correct engine outputs:243 measured and82 warmups. There
are242/264 strictly valid measured pairs. This is incomplete provider acceptance,
not DuckDB leadership.

| Track | Valid measured pairs | Correct measured outputs | Correct warmups |
|---|---:|---:|---:|
| Raw Parquet | 60/66 | 60 | 20 |
| Native | 60/66 | 60 | 21 |
| Iceberg | 65/66 | 66 | 22 |
| Lance | 57/66 | 57 | 19 |

Five engine warmups time out: raw Q12/Q9, native Q1 and Lance Q1/Q18. Native
Q17 completes correctly in1308.014 ms but exceeds its1217.610 ms ceiling; its
three dependent measured requests are not run. Lance Q18's4952.179 ms ceiling
is not cleared, despite the separately observed diagnostic improvement. Native
Q18 does complete all measured pairs in this screen; the fresh calibration and
this session's conditions must remain attached to that result.

Two reference failures are distinct from engine correctness. Iceberg Q18's
third measured DuckDB request refuses a262144-byte allocation after valid
calibration; the corresponding engine output is independently correct, but the
pair remains invalid. Lance Q9's first DuckDB calibration refuses134217728 bytes;
no valid ceiling is established and dependent engine requests are not run.
Do not classify either as an engine wrong answer or as a successful paired result.

All four tracks use canonical SF10 data, three samples in one session,16 threads
on CPUs0–15,4 GiB query memory and12 GiB process caps. The sequential48 GiB
scope peaks at20,955,594,752 bytes with zero OOM/max events. These are separate
provider tracks with matched schemas, data and embedded timing boundaries. The
source-verified release includes Lance/GPU; this screen measures CPU providers.
Decoded IPC and GPU residency are separate acceptance gates.

Run5434 exits1 for incomplete acceptance. Supplemental audit70354 exits0 and
independently validates every completed measured and warmup output without
reclassifying time/reference failures. All508 source inputs and the frozen
binary verify unchanged after measurement. Reproduce with the archived
`run_bound_keys_providers.py`, using `TMPDIR="$PWD/.scratch"`,
`PYTHONPATH="$PWD/scripts"`, `SAFE_BUILD_MEM=48G`, `SAFE_BUILD_JOBS=1` and
`scripts/claude-safe-build.sh taskset -c 0-15 .scratch/venv-lance/bin/python`.
Output roots must be fresh. The [archive](benchmarks/2026-09-09-bound-keys-providers/manifest.json)
preserves raw samples, plans, failures, oracles, manifests, harness and the
independent audit. Resource/concurrency and complete leadership gates remain open.
