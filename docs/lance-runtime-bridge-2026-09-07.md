# Persistent Lance runtime bridge

The synchronous Lance read and write bridges previously created and joined a
fresh OS thread for every operation, despite sharing a persistent Tokio runtime.
A fixture-independent regression demonstrates40 calls on40 distinct threads,
exceeding the32 existing runtime workers. Four value/error/runtime controls pass
on the unchanged implementation. The original failure is preserved.

The new bridge submits each future to the existing Lance runtime and receives
its result through a one-slot channel. Reads and writes share that implementation.
Synchronous callers and callers inside an unrelated current-thread Tokio runtime
can wait without entering a nested runtime. Callers already on the Lance runtime
use `block_in_place` so waiting hands their worker core back and nested I/O can
progress. A task panic/cancellation closes the reply channel and becomes a named
Lance execution error; ordinary storage errors and values are unchanged.

All five focused tests now pass. The full feature-enabled library, Lance reader/
writer/version/nested-data suite, partition/materialization, memory and optimizer
contracts are running. The required small TPC-H-derived, vectors, nested and
versioned fixtures are present; actual test output must still be checked for skips.
No optimized-source performance measurement exists yet.

This removes routine per-operation bridge threads. It does not establish a
query-wide CPU budget, bound all Tokio tasks, or make collected provider results
spillable. Same-runtime reentrancy can use Tokio's blocking-worker replacement;
the ordinary sequential reuse test is not a bound on every reentrant thread.
The one-slot reply bounds reply count, not the size of the existing collected
`Vec<RecordBatch>` result. Those broader resource gaps remain open.

The motivation is shared allocation/thread churn, not a query-specific fast path.
[Memory traces](parse-maintenance-stalls-2026-09-07.md) show page-discard bursts
near later parses. Bounded stack samples additionally identify mimalloc's
`_mi_prim_decommit` as the discard caller in both frozen binaries; unwinding stops
there, so the higher-level producer is not proven by the stack. Short-lived bridge
heaps are a plausible contributor, not yet a measured cause of those bursts.

Next finish the semantic gates, freeze/build, compare Lance against640 and612
with real typed/time gates, and repeat memory/thread diagnostics. Preserve any
regression and validate other provider/GPU/resource paths before adoption. Do not
change allocator reclamation or the benchmark timing boundary to hide cleanup.

Evidence: `.scratch/lance-runtime-bridge-repair/`. Additional bounded stacks are
under `.scratch/parse-maintenance-profile/stack-sample/`; the previous trace archive
is immutable and these later stack samples are not part of it.

## Completed source-level gate

Session55817 passes786 tests:720 feature-enabled library,31 Lance integration,
6 materialization,10 memory reservations,2 optimizer convergence and17 partition
contracts. The two preexisting library ignores remain explicit. Required Lance
fixtures are present and no SKIP message appears. Read/write/version/nested-data
cases execute successfully. The five focused bridge tests are included in the
library count, not added again. Formatting and whitespace checks pass.

## Initial optimized Lance screens

Release binary SHA
`387b9b0aeb436fffc12f339a04f60398986992726b138931668e02b849f6c07c`
passes the preserved independent optimized SQL, literal/budget and timestamp
oracles. Session18415 completes352 typed/time-gated requests across two full
22-query Lance screens. Against640, suite/geomean ratios are0.92890/0.92878;
against612 they are0.93047/0.93605. These are short development screens, not
performance acceptance.

Flags above10%: Q8/Q18/Q20 against640, Q8/Q20 against612. Q20 again places
substantial excess in the parse timer (candidate about19–25ms vs about0.1ms for
controls). The previous candidate640 now runs quickly as a control. That raises
a benchmark-position/cadence hypothesis; it does not prove bias or clear a flag.

Before attributing another regression to source, session90366 runs identical-
binary A/A checks for612/640/642 and reversed642→640 assignment, with the same
Q8/Q20 membership,10 pairs, oracles, time ceilings and resource settings. These
negative controls are under `.scratch/benchmark-position-audit/`. Protected
regression repeats and remaining provider/GPU/cap gates remain pending.

## Startup controls and resource follow-up

The benchmark-position audit completed528 typed/time-gated requests. Identical
binaries show startup/execution-cadence effects comparable to some candidate
changes. This confounds source attribution; the original flags remain open.
The versioned paired runner now supports independent startup/execution factors,
reports mean/total/max as well as median, and rejects duplicate/missing or
mislabelled samples. All108 Python harness tests pass with no skips, including
both opt-in gates. See [the full methodology investigation](benchmark-startup-order-2026-09-07.md).

Session65528 completes both aggregate cap scenarios with real spill and exact
counts over250 million input rows. The first GPU attempt61194 passed40 CPU
control requests but could not initialize device kernels because NVRTC was absent
from the session library path; its40 device requests remain explicitly not_run.
A contained NVIDIA-SMI check confirms the RTX5090/driver580.173.02 are available.
The repo `.venv` already supplies NVRTC; its library hashes and corrected path
are in `.scratch/lance-runtime-bridge-repair/gpu-environment-corrected.json`.
The fresh corrected GPU session92386 passes all80 CPU/device comparisons with
40 required-device executions and a256 MiB cache target. This is the supported
600k-row float fixture, not canonical SF10 GPU coverage or hard VRAM admission.
The cap peaks are412 MiB (1 GiB cgroup) and414 MiB (2 GiB RLIMIT_DATA). No driver installation, hardware
reset, allocator change or production source edit was used for this correction.

The longer immediate-control gate3501 completes1,224 typed/time-gated requests
across all four startup/execution configurations with50 steady pairs each.
The initial Q8/Q18/Q20 flags vs640 do not reproduce (>10% in no cell). This
clears the incremental protected gate only; the historical612 comparison and
remaining full-provider gates are open. See the methodology report for median
and mean ranges, which do not establish a source speedup.
