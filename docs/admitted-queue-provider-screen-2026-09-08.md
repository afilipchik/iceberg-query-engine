# Frozen admitted-queue SF10 provider screen — September 8, 2026

The current frozen engine candidate, ed721285, has now run all22 canonical SF10
queries in raw Parquet, Native, Iceberg and Lance. Each screen uses one session,
three measured pairs,16 threads,4GiB query budgets and12GiB process caps. These
are development screens; none establishes full leadership or resource/concurrency
certification. Raw job42325 and sequential provider job11660 both exited1.

| Track | Valid measured pairs | Engine outputs independently validated | Remaining failure |
|---|---:|---:|---|
| Raw Parquet | 60/66 | 60/66 | Q9, Q13 engine timeouts |
| Native | 60/66 | 60/66 | Q1, Q13 engine timeouts |
| Iceberg | 63/66 | 66/66 | Q9 DuckDB calibration SIGSEGV |
| Lance | 57/66 | 60/66 | Q1, Q13 engine timeouts; Q9 DuckDB calibration SIGABRT |

For each engine timeout, the later two requests in that query group did not run:
the old harness labels the unavailable worker `startup_error`. These are dependent
failures after a timeout, not three independently observed timeouts or startup bugs.
Across the four tracks,240/264 pairs validate; all246 completed measured engine
outputs validate after supplemental Q9 checks. No mismatch was reproduced.

Both failed Q9 reference cases produced an oracle/warmup before failing during
calibration. Supplemental comparisons check all three completed engine outputs
against the preserved canonical typed oracle with identical SQL hashes. They
establish result correctness only. They do not repair reference calibration,
certify those timings or justify a suite ratio that omits failed queries. The
Iceberg supplemental checker initially lacked its scratch directory; that setup
error is preserved separately from the subsequent three successful comparisons.

Lance uses extension892b224 with its extension optimizer disabled because of the
previously reproduced decimal AVG pushdown error. This is direct Lance scanning
with DuckDB aggregation, not stock extension-pushdown performance. Source/version,
schemas, converted-input hashes, settings and commands are in the archive.

## Performance implications

Raw Q12 now validates all three warm measurements: engine median941.777ms,
DuckDB106.468ms, ratio8.846×. The separate instrumented budget-isolation failures
remain failures; their fresh ceilings and execution conditions differ. See
`admitted-queue-q12-2026-09-08.md`.

Separate-screen comparisons to the prior frozen parallel-input candidate show
possible Q2 (+13.2%) and Q19 (+11.6%) regressions. They require controlled paired
confirmation before attribution. No complete suite/geometric-mean result is
reported because every track has missing valid pairs. Raw Parquet still has
large per-query gaps, including Q10 at6.385× and Q16 at6.777× DuckDB.

The source audit found an unintegrated boundary: `morsel_agg/input_frontier.rs`
does not consume PreparedAdmittedInput, so eligible variable-width aggregate
inputs still select serial polling. The next implementation task is recorded in
`.claude/epics/realistic-benchmarks-duckdb-leadership/updates/2026-09-08-admitted-frontier.md`.
It must preserve one-slot accounting, buffer lifetime, typed refusal and spill
semantics. Dictionary expansion/filtering remains a separate cost hypothesis.

## Resource and harness findings

No scope max/OOM/OOM-kill events increased. The raw scope peak was5455650816 bytes;
the sequential provider scope peaked at25314136064 bytes. Provider scope peaks
are cumulative across its tracks and include file cache; they are not individual
query RSS or proof of complete memory accounting. The reference crash signals do
not establish their cause, and zero OOM events do not rule out a process-cap issue.

The screen exposed a harness shutdown bug: ordinary Worker.close unconditionally
sent SIGKILL. Native left13 DuckDB temporary files totaling11238014976 bytes.
Their paths/sizes were archived, then only those verified unchanged `.tmp` files
were removed after the provider job became terminal. Result/evidence files remain.
The harness now allows bounded EOF cleanup on normal close and keeps immediate
abort for deadlines/protocol errors. A real spilling DuckDB regression proves
cleanup; see `benchmark-worker-shutdown-2026-09-08.md`. The four screens above used
the archived old harness; their evidence has not been overwritten.

The archive `docs/benchmarks/2026-09-08-admitted-queue-sf10/` contains1267 verified
files: every sample/result/plan, provenance, scope records, supplemental checks,
commands, old harness source and temporary-file inventory. The engine source and
binary hashes link to the preceding admitted-queue archive. IPC, GPU residency,
resource/concurrency matrices and broader public workloads remain open gates.
