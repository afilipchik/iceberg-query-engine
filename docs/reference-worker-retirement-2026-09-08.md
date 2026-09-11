# Retire failed benchmark reference workers — September 8, 2026

The completed Iceberg screen records a DuckDB allocation refusal on Q9, followed
by SIGSEGV on the next query's EXPLAIN. The original harness had stopped dependent
Q9 requests and closed the engine, but kept the still-live failed reference for
reuse. The sequence does not prove why DuckDB crashed; it shows the harness reused
a reference after its validation contract had failed.

The ordinary runner now closes the reference on invalid calibration or a failed
measured reference request (timing, execution or typed comparison). All dependent
samples remain not_run. The existing restart_for_next_query policy then creates
a fresh worker for a previously ready process before the next SQL. Failed initial
setup remains a session-wide refusal; it is not retried blindly for every query.
No within-query retries or recovered timing samples are introduced. Normal EOF
cleanup remains bounded; actual transport deadlines still abort immediately.
Resident runs already have fresh worker lifetimes per query and outer cleanup.

The fake-worker red regression fails because a reference returning refused remains
open. The fix passes both that test and invalid-calibration retirement assertions.
Full harness gate:120 tests run,118 pass,2 existing optional skips,2.943s under
2GiB safe-build and repository TMPDIR with pinned Python. Logs:
reference-retirement-red.log and reference-retirement-full.log under
.scratch/parallel-aggregate-input/. No production engine source changed.

Completed provider evidence remains immutable under
 docs/benchmarks/2026-09-08-budget-providers/; do not reinterpret Q19 as measured.
Residency driver22984 completed using the updated harness and frozen3ff868c7.
Canonical preload refused at16GiB; custom floating-point Q1 failed its warmup
gate. Dependent GPU cases rejected incomplete controls. No device performance
was established. See [terminal evidence](budget-quantum-residency-2026-09-08.md).
