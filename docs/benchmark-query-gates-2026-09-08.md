# Validated reference and warmup gates — September 8, 2026

The ordinary embedded benchmark runner now delegates query orchestration to
ordinary_runner.py. A completed oracle and three positive, finite, typed-valid
reference calibration samples are required before an engine request is issued.
The10× median ceiling must itself remain finite. Invalid reference evidence
produces explicit not_run/executed:false records for every dependent requested
sample, preserving calibration traces without inventing engine failures or ratios.

Engine warmups now receive the fresh ceiling and must pass typed validation and
the exact elapsed-time gate. A failed warmup is retained in trace and each sample
row's engine_warmup field; measured engine retries are suppressed. During measured
runs, failure stops subsequent dependent requests. A measured reference failure
invalidates that pair and prevents later dependent retries. Every requested sample
still has a row, and ordinary lifecycle cleanup is bounded and idempotent. The
next SQL can start a fresh worker using the existing per-query restart contract.

query_gate.py shares finite calibration and elapsed-response checks with the
resident runner. Resident warmups now explicitly check elapsed ms; completed
status alone is insufficient because a response can arrive during delivery grace
while exceeding the query ceiling. Resident preparation retains its separate
timeout and residency checks. This change does not claim every resident lifecycle
policy is identical to the ordinary runner.

Six new fake-worker tests assert actual request issuance/suppression and stable
sample records: valid calibration, missing/invalid/NaN/infinite/overflow timing,
wrong reference values, oracle failure, warmup timeout/wrong answer/late completed
response, measured late/wrong output and reference failure. The resident red test
reproduces four engine calls instead of one after a21ms warmup against20ms ceiling.
No engine or device was used for these protocol tests. Existing real DuckDB
spill-cleanup and comparator tests remain in the full harness gate.

Validation under2GiB safe-build, repository TMPDIR and pinned Python:
query-gates-red.log: five passes, one reproduced resident failure;
query-gates-full-01.log:120 tests run,118 pass, two existing optional skips
(EXACT_BAG_SPILL and LANCE opt-in tests). No production engine source changed.
Frozen engine3ff868c7 remains the current candidate; older benchmark archives
retain their original harness and are not rewritten.

Canonical SF10 raw-Parquet run68263 completed with exit1 under48GiB containment,
using corrected harness gates,16 threads,4GiB query and12GiB process caps, one
session and three requested measured samples per query.57/66 measured pairs are
valid (19/22 queries complete). Q9 warmup times out at4592.46ms ceiling; Q13
warmup times out at2539.19ms. Q12 completes in998.12ms against943.08ms ceiling.
Their nine measured requests are not_run, not nine independent timeouts.

All57 completed measured outputs validate. Supplemental comparison of saved
warmup outputs validates all20 completed warmups, including late Q12; no engine
was rerun and its time-gate failure remains. Q9/Q13 have no complete warmup
output. Thus77 completed engine outputs have typed correctness evidence. Scope
peak5511491584bytes; memory.events max/oom/oom_kill zero. The run is incomplete:
no full-suite ratio or leadership claim is valid.

Strict warmup gating can leave samples unexecuted where older runs attempted
them; this is a harness-policy change, not evidence of an engine regression.
Immutable evidence: docs/benchmarks/2026-09-08-query-gates-raw/ (raw traces,
responses/plans/outputs, manifest/source hashes, scope/lifecycle, harness snapshot,
red/green tests and supplemental warmup comparisons).

Sequential Native/Iceberg/Lance driver76528 is ACTIVE with the same binary and
harness. No engine/harness edits or overlapping heavy jobs until terminal.
Saved warmup comparisons ran in a separate2GiB scope without launching queries;
this was light post-processing during the development provider screen, not
multi-session isolated performance certification.


Provider follow-up observed during the unchanged-harness run: Iceberg Q9's second
measured DuckDB request refuses with OutOfMemoryException (262144-byte block,
bad allocation). Dependent samples stop. The still-live reference process later
crashes with SIGSEGV on Q19 EXPLAIN, so Q19 calibration never becomes valid and
no engine requests run. This temporal sequence does not prove the crash cause.
After preserving this screen, retire a failed reference worker as well as the
engine at the first measured reference failure, allowing existing next-query
restart logic to create a fresh reference. Add a lifecycle regression; do not
rewrite these failures as successful Q9/Q19 timing evidence.
