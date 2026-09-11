# Short-query comparison fails its identical-binary control — 2026-09-09

A prespecified custom Q6 null comparison produced a9.63% apparent difference
between two processes using the exact same binary. This exposes inadequate
measurement precision for this short-query protocol. It does not establish a
source regression or identify a specific scheduler cause.

## Evidence

The candidate/control [protected study](aggregate-threshold-paired-2026-09-09.md)
first had an upper95 ratio1.10794. Its fixed follow-up35348 retained all208
correct/gated outputs but failed with ratio1.06521,95%0.96007–1.21064.

Before running a separate null control, its prespecification fixed eight fresh
blocks, twelve measured pairs per block,20,000 bootstrap samples, seed20260909,
and the same setup, input, thread count, affinity and reference calibration.
Both labels use binary SHA256
`1efb25547486dc98a297d8824e8738b5fea2dcd15255cdded0cdaad6b74687dc`.

Null33603 terminal1: all208 outputs correct/gated, observed ratio1.09628 and
95% interval1.03061–1.17850. All507 source hashes and both binary entries verify.
The48GiB scope peaks at227,348,480 bytes with zero max/OOM/kill events.
The [460-file null archive](benchmarks/2026-09-09-aggregate-threshold-q6-null-control/manifest.json)
preserves the prespecification, identical hashes, every output, ordering, driver,
reference calibration and resource evidence. The separate candidate follow-up
remains failed; the null result cannot convert it into a pass.

The driver keeps both engine processes alive and alternates tiny requests.
Each process has its own runtime/worker pool. Request medians move between
roughly1.5 and6ms across blocks. Startup and execution order are balanced, but
that alone has not delivered a precise null result. Idle-worker interference,
CPU placement/frequency, inter-request validation work and short observation
windows are hypotheses, not established exclusive causes. A single null study
also cannot distinguish systematic bias from a statistical false positive.

## Implementation sequence in the existing benchmark epic

1. Preserve the current artifacts and failed acceptance state. Add an explicit
   identical-binary mode to the paired driver: same input, setup, affinity,
   executable hash and environment on both labels. Record that it is a precision
   diagnostic, never candidate performance evidence.
2. Add a process-isolated comparison schedule. Start only one engine at a time;
   prepare data and warm the process outside the measured query boundary, collect
   a complete fixed window, close it and verify terminal exit before starting
   the other label. Balance AB/BA blocks. Keep startup/preload time separately
   reported. Do not overlap build, provider or other benchmark jobs.
3. Use a separate recorded pilot to choose the request count before the actual
   comparison. Target at least2 seconds of measured engine time per label/window,
   using the faster pilot side, with an explicit count/storage bound. Freeze the
   count and number of blocks in a prespecification; if the fixed window is still
   too short, report insufficient precision rather than silently extend a run
   until it passes. Keep per-request10× reference ceilings and every typed
   validation. First implementation should retain the existing result protocol.
4. Record per-process CPU time, context switches and affinity before/after each
   window, plus available CPU topology/governor information. Reads suffice;
   do not modify host governor, services or affinity outside the benchmark scope.
   These fields explain conditions; they must not become post-hoc sample filters.
5. Test the scheduling contract with a fake worker that proves only one engine
   is live, EOF cleanup completes, failures prevent dependent requests, and the
   measured boundary excludes setup. Test fixed counts, missing samples and the
   distinction between insufficient precision, timeout and wrong output.
6. Run the prespecified identical-binary control first. Evaluate all complete
   blocks and report its interval relative to1. Failure leaves precision open;
   retain it rather than repeatedly retrying the same study until success.
   Only after a justified protocol correction and its null validation should a
   fresh candidate/control comparison be used for the protected Q6 decision.
7. Keep canonical SF10, provider and residency screens separate. The matched
   Lance diagnostic already ran one process at a time and concerns multi-second
   execution; its raw evidence remains available. Complete raw/native/Iceberg/
   Lance and CPU/GPU screens with unchanged correctness and ceiling outcomes,
   without treating a provider screen as a replacement for this failed gate.

Source/binary changes are frozen during active provider43384. This document
specifies the next bounded harness repair; no timing threshold has been relaxed,
no samples discarded and no source change is attributed to the null difference.
