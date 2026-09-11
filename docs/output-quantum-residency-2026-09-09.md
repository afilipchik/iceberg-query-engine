# Output quantum: residency screen — 2026-09-09

Frozen `01bb077a` completes all five cases:348 independently typed-correct
outputs, including70 warmups; all278 measured pairs pass correctness and the
matched10× DuckDB ceiling. Run28279 and audit69638 both exit0. This screen
explicitly selects experimental partial ownership; default disjoint is unchanged.

| Track | Valid measured pairs | Correct warmups | Geomean / DuckDB | Suite / DuckDB | Wins | Worst ratio |
|---|---:|---:|---:|---:|---:|---:|
| Canonical decoded IPC | 66/66 | 22 | 0.568087 | 0.915419 | 14/22 | 3.957248 |
| Canonical preloaded CPU | 66/66 | 22 | 0.755917 | 1.270481 | 13/22 | 4.297768 |
| Canonical mixed GPU | 66/66 | 22 | 0.798089 | 1.401587 | 12/22 | 4.651904 |
| Custom float CPU | 40/40 | 2 | 1.527596 | 2.441677 | 1/2 | 3.900573 |
| Custom float required GPU | 40/40 | 2 | 0.090532 | 0.085659 | 2/2 | 0.117640 |

Canonical mixed mode records **zero successful device executions** across all66
measured requests; telemetry labels each unobserved. It is not evidence of
canonical GPU acceleration. Custom required GPU validates all40 measured device
requests and both warmups: one matched attempted/completed run, no failure,
matching preparation session, stable residency/fallback counters and no upload
requests. Median resident Q1/Q6 times are1.028160/0.867874ms; separate preparation
is57.316816/25.389229ms. These600,000-row custom float results are not canonical
SF10 or upload-inclusive latency. Preserve exact integer/decimal SUM CPU routing.

Canonical cases use16threads, CPUs0–15,32GiB query/48GiB process memory; this
capacity experiment does not clear16GiB preload refusal. Custom cases use4threads,
4/8GiB and20samples per query. There is one session per case. Existing NVRTC
libraries use a process-local loader path. The64GiB sequential scope peaks at
20,585,652,224bytes with zero OOM/max events. Per-case peaks are cumulative in
that shared scope, not independent provider peaks. Source511 inputs, binary,
driver and harness hashes verify after measurement.

Completion improved relative to earlier incomplete capacity screens, but this
is not a paired old-binary performance result. Five IPC queries exceed3× DuckDB;
CPU-control and mixed-mode suite totals are slower than their matched references.
Different reference totals and timing boundaries must not be merged into one
provider ratio. Three-session, resource/concurrency, default-ownership and
protected-regression acceptance remain open; the current Q6 precision null
failed and its dependent follow-up remains unexecuted.

The completed decoded-IPC Q15 first sample reports657.435243ms planning versus
5.087146ms execution (663.051ms total). Source `src/physical/planner.rs` calls
`materialize_shared_ctes` before returning the final plan; that method executes
and collects shared CTE output into the query cache. The final physical tree
therefore omits already-executed work. Full parse-to-consumed-output latency is
the comparison boundary. This is a source-backed profiling limitation, not a
newly measured exclusive CTE cost or proof of a specific optimization.

Reproduction driver: `run_output_quantum_residency.py` in the
[immutable archive](benchmarks/2026-09-09-output-quantum-residency/manifest.json).
Run through `scripts/claude-safe-build.sh` with repository `TMPDIR`,
`PYTHONPATH="$PWD/scripts"`, `SAFE_BUILD_MEM=64G`, `SAFE_BUILD_JOBS=1`,
`taskset -c 0-15` and `.scratch/venv-lance/bin/python`; use a fresh output root.
The archive preserves all samples, typed oracles, device records, setup, runtime,
source provenance, harness, driver and independent audit.
