# General benchmark calibration gap — September 8, 2026

Source review of scripts/benchmark/run.py, ordinary (non-resident) run branch,
confirms two gaps before the next full canonical/provider screen:

1. After three reference calibration calls compute a ceiling, the engine warmup
   is invoked without that ceiling. Its correctness and elapsed gate are not
   propagated into the measured rows' acceptance.
2. If fewer than three positive completed reference samples exist, ceiling is
   None. Engine measured calls still execute with that value; later comparison
   marks invalid reference evidence, but execution was not bounded by the required
   fresh DuckDB ceiling. Even three completed samples with invalid typed reference
   comparison can still supply a numeric ceiling, despite invalid calibration.

These are verified source-level harness gaps, not new measured engine failures.
The recent custom paired drivers explicitly validate fresh references, gate
engine warmups and measurements, stop failed sides, and mark dependent requests
not_run. Their results are not invalidated by this general-runner branch.
The resident runner is a separate implementation and needs its own contract tests;
do not infer its behavior from these lines.

Next correction before full-suite benchmarking:

- Require a completed typed oracle plus three positive, finite, typed-valid
  calibration results before executing dependent engine queries. Preserve failed
  reference traces. Emit explicit invalid-reference/not-run rows without inventing
  a timeout, correctness failure or speed ratio for unexecuted engine requests.
- Pass the fresh ceiling to engine warmup and check both exact elapsed time and
  typed output. A delivered late result must remain a gate failure. A failed
  warmup must not be hidden by successful later samples or silently replayed.
- Preserve stable output row counts for requested samples and correct executed
  flags; distinguish a failed attempted warmup from dependent unexecuted samples.
- Use fake workers to test invalid/missing/NaN timing, wrong-answer calibration,
  oracle failure, warmup wrong output/timeout/late completed response, and the
  passing path. Assert dependent engine requests were never issued when reference
  calibration was invalid. Keep immediate abort semantics for actual deadlines.
- Run harness tests under the small safe-build scope, then freeze the harness
  snapshot used for the next canonical/provider run. Do not rewrite older records.

No harness changes are part of this source review; budget-quantum release29075
remains the active heavy job. Fixing this gap is required follow-up for the full
benchmark objective, not a substitute for the currently queued paired measurement.

Follow-up resident-runner review: resident_runner.run_query already requires a
validated ceiling before engine warmup/measurement. However, warmup acceptance
checks completed status, typed output and residency telemetry without explicitly
checking elapsed ms against the ceiling. The Worker transport can deliver a
completed response during its delivery grace period (as the paired Q12 results
prove), so completed status alone does not establish this gate. Add a late-completed
warmup test for resident mode too. Resident preparation has a separate timeout
and must not be conflated with query timing. This finding does not claim resident
measured samples lack report-level gates; it concerns warmup acceptance.
