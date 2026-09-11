# Preserve resident worker startup failures — September 8, 2026

The required-GPU capacity screen exposed a benchmark diagnostic bug. The engine
worker exited1 while cudarc could not load NVRTC. The runner replaced that startup
failure first with missing preload acknowledgement, then missing required policy,
then generic preparation failure. Stderr preserved the cause, but sample-level
failure reasons obscured it.

resident_runner.py now preserves the first established blocking cause. A failed
engine readiness response produces an explicit startup failure with its status,
exit code and available error fields. Later acknowledgement/preparation checks do
not overwrite it. Invalid reference calibration still blocks dependent work.
Actual preparation failures remain distinct. No timing, oracle, workload, engine
or resource limit changes were made.

The new regression covers crash, timeout and refusal for both ordinary preloaded
and required-GPU execution. Its fake worker cannot prepare; tests verify no engine
execution, closed workers and preserved causes in all requested slots. Before the
fix: one test fails six subcases. After:126 tests run,124 pass, two existing optional
skips,2.925s. Commands use the required2GiB wrapper and repository TMPDIR.
Rust formatting and whitespace checks pass; no engine rebuild was needed.

This change follows the completed GPU runtime correction and all measurement
archives. Those archives retain their original harness hashes and failed outcomes.
The existing process-local NVRTC path correction successfully measured40 device
requests; see parallel-key-gpu-runtime-2026-09-08.md. No remeasurement is required
to validate this failure-message-only change.

Evidence: `benchmarks/2026-09-08-resident-startup-diagnostics/`.
