# Active join instrumentation

Added opt-in `QE_JOIN_STREAM_PROF=1` attribution to the current pull-driven inner
join and its shared build initialization. This closes the coverage gap identified
in the [local source audit](join-stream-source-audit-2026-09-08.md). The old
`HJ_PROF` counters remain separate and do not cover the active inner stream.

Each stream reports a process-local ID, key expressions, route, input/output
rows and batches, exact-key matches before the residual ON filter, and whether
it reached EOF. Dropping an early-limit, cancelled or failed stream reports
`completed=false`; this flag alone does not distinguish those three reasons.
An unwinding flag identifies Rust panic unwinding when applicable.

The phases are input wait, key preparation, candidate allocation/traversal,
residual filtering, output gathering/cleanup, cooperative yield wait and
downstream wait. Build initialization separates input collection from subsequent
state construction. Its time is recorded only for the actual cache initializer,
not each waiter reusing the cache. Phases are wall intervals; nested operators
and concurrent partitions overlap. They cannot be summed into exclusive CPU
or into a query critical path. Candidate work excludes the explicit cooperative
yield interval, but can still include OS preemption.

Timers and counters run only when the new variable equals1. There is no
per-row profile counter in the traversal loop. This changes telemetry, not join
selection, key equality, admission, spilling, output chunk bounds or cancellation.
The source-frozen scheduling provider binary does not contain this change.

## Validation and live checkpoint

All commands use repository `.scratch` as TMPDIR and `scripts/claude-safe-build.sh`.
The selected gate runs with `QE_JOIN_STREAM_PROF=1`, Rayon4,48GiB scope,one build
job, and `--locked --features lance,gpu`:

`cargo test --test hash_join_streaming_tests --test streaming_prepared_join_contract
--test prepared_join_contract --test prepared_row_store_contract
--test hash_join_initialization_ownership`

All32 tests pass, with no failures or ignores. Seven streaming tests were rerun
with separate stdout/stderr because the initial combined file interleaved test
harness output with JSON. The separate run passes all7 and yields22 valid trace
records:10 complete builds,10 complete probes and2 incomplete probes. The hot-key
case drops after exactly12,288 output rows in three4,096-row batches; the miss-only
cancellation emits zero rows. These records demonstrate active-path coverage
and preserved early-drop behavior. They do not measure optimized performance.
Formatting and whitespace checks pass.

Source is frozen in `.scratch/join-stream-profile/source-hashes.json` (280
Rust/Cargo inputs). The optimized release build completed with exit0 in8m43s (handle67969):
`cargo build --locked --release --features lance,gpu --example benchmark_embedded`.
Log: `.scratch/join-stream-profile/release-build.log`.

All280 source hashes were verified before copying the executable to
`.scratch/join-stream-profile/profile_benchmark_embedded`, SHA256
`0dd72323f85332a3b68e24fafbde2535d8842c4592b699b364fd88db90399ff1`.
The driver `.scratch/join-stream-profile/run_profile.py` completed with exit0
(handle1792). It runs the five unresolved raw queries in separate processes with
two requests each,4GiB query,12GiB process and32GiB scope budgets, CPUs0–15 and16
execution threads. Dataset integrity checks pass before/after. All10 outputs
pass the preserved independent typed oracles. Diagnostic requests deliberately
allow longer than the failed acceptance deadline; they do not clear those failures.

## First active-path measurements

The run records372 valid complete traces:30 build initializations and342 inner
probe streams. Each table phase value is the sum across that query's trace
records divided by its two requests. These are cumulative, possibly overlapping
wall intervals, not exclusive CPU or additive query-wall components.

| Query | Instrumented query wall ms, two requests | Build state ms/request | Candidate ms/request | Gather ms/request |
|---|---|---:|---:|---:|
| Q5 |2763.3 /2727.9|57.4|190.3|16.9|
| Q9 |6172.9 /6113.1|439.9|782.8|89.4|
| Q10 |3002.6 /3071.1|29.7|154.0|126.1|
| Q12 |1324.5 /1308.6|5.8|27.0|3.7|
| Q13 |4778.4 /4826.9|11.2|Not covered|Not covered|

Q13 uses a collected LEFT join; only its build initializer is covered by this
new instrumentation. Its zero inner-probe traces must not be interpreted as zero
probe cost. All other listed queries execute the current inner-stream path.

Q5/Q9/Q12 spend far more cumulative time in input, downstream and explicit yield
wait intervals than in candidate traversal or gathering. Nested streams and
partitions overlap heavily; the wait totals exceed query wall time and do not
prove a scheduler defect or identify its cause. They do show why old collected
join gather profiles should not choose the next optimization. Int32 keys use a
general comparator rather than the Int64 typed cache, but no Int32 change is
justified as the leading repair by these timings alone.

A second diagnostic completed with exit0 (handle89990), using the same frozen binary and
budgets, with existing `QE_INPUT_QUEUE_TRACE=1` and `QE_AGG_PROF=1` enabled as well.
`.scratch/join-stream-profile/run_queue_profile.py` preserves per-process CPU/RSS
records and queue preparation, permit, poll, copy and send traces alongside join
phases. Output: `.scratch/join-stream-profile/sf10-queue-profile/`.
All10 outputs validate and dataset checks pass;304 queue records are preserved.
Process accounting shows Q5/Q9/Q10/Q12 using only1.0–1.34 cores despite16
configured threads. A new bounded-input regression confirms that the aggregate
frontier polls synchronous source work serially. See the
[reproducer and required repair](serial-aggregate-input-2026-09-08.md).

Optimized join/queue attribution is complete. The next repair is bounded
parallel input scheduling; its new regression is currently red and production
implementation remains pending. No performance improvement is claimed for this instrumentation.

[Frozen source and validation archive](benchmarks/2026-09-08-active-join-profile/manifest.json).
