# Aggregate batch scheduling candidate

The first parallel-state candidate improves Q13 by31.6% but regresses Q20 by4.4%
and loses its existing timing gate. Its phase traces show that dispatching four
workers for small batches can cost more than the original serial state updates.
This candidate changes scheduling while retaining canonical worker ownership.

`ParallelControllers::ingest` now counts nonempty routed owners. It uses scoped
Rayon processing only when at least two owners have work and the batch averages
at least256 rows per active owner. Otherwise it visits the same owners serially.
No owner reassignment, aggregate-state migration, output merge, source replay,
budget change or query-specific rule is introduced. The threshold is a provisional
cost policy, not a semantic proof or an established optimum. Expensive variable
values and skew can have different crossover points and remain performance gates.

The measured motivating cases are Q20's98,107 rows in458 batches (about214 rows
per batch), and Q13's outer aggregate receiving23,440 batches of roughly64 rows.
Q13's inner aggregate averages over8,000 rows per batch and should retain parallel
processing. The decision is based on each actual routed batch, not SQL identity
or estimated uniqueness. Profile output adds serial/parallel batch counters to
verify which path actually ran.

## Validation

All builds/tests use `.scratch` as TMPDIR and `scripts/claude-safe-build.sh`,
with `RAYON_NUM_THREADS=4`, `SAFE_BUILD_MEM=48G`, `SAFE_BUILD_JOBS=1`.

- The four-owner test now alternates128-row and2,048-row batches. It asserts
  two serial and two parallel dispatches over the same owners, actual spill,
  unique complete groups, exact COUNT and wide-decimal SUM, weighted AVG and
  complete memory/file cleanup. This tests that changing scheduling does not
  change group ownership or confuse selection cursors after spill.
- A new test covers empty input and a4,096-row single hot NULL key. It verifies
  serial dispatch, one final group, SQL COUNT(non-NULL) semantics and cleanup.
- Final gate:896 library tests pass, zero failures,10 explicit ignores;37 selected
  integration tests pass without skips. The ignores remain eight dedicated CUDA
  cases, one dedicated IPC-cache case and the pre-existing dependent-join case.
- Command: `cargo test --locked --features lance,gpu --lib
  --test fused_aggregate_budget_transition --test fused_aggregate_input_errors
  --test live_dictionary_input --test systemic_numeric_tests
  --test aggregate_encoding_contract --test shared_prescan_errors
  --test outer_on_pushdown`. Formatting and whitespace checks pass.

## Measurement checkpoint

Source is frozen in `.scratch/live-schema-boundary/dispatch-source-hashes.json`
(280 Rust/Cargo files). The contained release build completed in8m41s with exit0:
`cargo build --locked --release --features lance,gpu --example benchmark_embedded`.
All280 source hashes were verified before freezing `dispatch_benchmark_embedded`
(SHA2561571d7211c085c29a002a7441b557c4d1019bd8d1927a293521c2b1fe4f52122).
The three-way Q20 comparison is complete: all54 typed outputs pass. Median times
are1605.710ms original serial,1675.302ms first parallel, and1610.413ms scheduling
candidate. The new candidate is0.3% above serial and3.9% below first parallel.
Every order block improves against first parallel. This recovers most of the
measured regression; it does not establish a fresh deadline or suite pass.

The balanced Q13 comparison also completes32/32 typed outputs:7311.184ms
original serial versus4776.912ms scheduling candidate,34.7% lower. Its four
block ratios are0.6554,0.6514,0.6493,0.6510, with12 steady samples and4 warmups
per binary. This remains a targeted diagnostic past the normal deadline.

A separate four-output Q20 phase check passes. All458 input batches use serial
dispatch; worker processing is32.0–32.3ms and canonical routing9.0–9.1ms, giving
41.1–41.4ms ingestion. This is below the first parallel candidate's62.0–67.2ms
ingestion while preserving the same four key owners. Phase intervals are wall
measurements, not exclusive CPU or acceptance latency.

Q10's follow-up validates32/32 outputs and measures2932.865ms serial versus
2980.284ms candidate (1.0162×); block ratios range0.9993–1.0224. This is a small
observed slowdown, not an assertion of unchanged performance.

The separate Q13 phase check validates all four outputs. Its1,832 inner batches
all run in parallel, with1.700–1.731s ingestion. Its23,440 outer batches all run
serially, with246–247ms ingestion, versus476–479ms for the first parallel
candidate. These traces verify that the intended scheduling paths execute.

Fresh full raw SF10 validation has completed unsuccessfully in
`.scratch/public-bench/dispatch-sf10-raw-01/`, with all22 queries,
three samples, one development session and unchanged4/12/32GiB resource limits.
It records66 pairs:51 complete correctly and meet their timing gates. Q20 is
back inside its fresh1649.328ms ceiling at1607.550,1610.136 and1608.089ms.
Q5/Q9/Q10/Q12/Q13 still time out once each, followed by two unavailable-worker
records each. Thus the scheduling repair removes the new Q20 failure while
preserving the measured Q13 gain, but the overall suite still fails. No full-suite
performance score, broad provider acceptance or DuckDB leadership is claimed.

Native SF10 completed with exit1 in
`.scratch/public-bench/dispatch-sf10-native-01/`:60/66 pairs pass typed validation
and timing. Q1/Q13 each time out, followed by two unavailable-worker records.
The frozen binary and4/12/32GiB budgets are unchanged.

Iceberg SF10 completed with exit0:66/66 pairs pass correctness and timing, with
zero issues. Suite ratio is0.878601 (29,794.199ms engine versus33,910.953ms
DuckDB, sum of per-query medians); geometric mean is0.576103, with12/22 wins.
Q13 is still2.18724× slower. This single development session does not satisfy
the three-session, worst-query, resource/concurrency and regression acceptance
gates. The earlier reference refusal/crash did not recur in this frozen run;
its evidence remains preserved separately. Results are in
`.scratch/public-bench/dispatch-sf10-iceberg-01/`.

Lance validation completed with exit1 in
`.scratch/public-bench/dispatch-sf10-lance-01/`:57/66 valid pairs. Engine Q1/Q13
time out, followed by two unavailable-worker records each. The DuckDB reference
refuses128MiB allocation on all three Q9 measurements; fresh calibration is also
invalid. The engine completes60 outputs; all three Q9 outputs also pass a separate
comparison with the preserved typed oracle. The pinned reference retains its documented optimizer-disable setting.
No full Lance suite score is valid.

All four scheduling-provider runs are terminal. Subsequent source adds opt-in
active join profiling; that instrumentation is absent from this frozen binary.

`compare_dispatch_q20.py` compares the
original serial, first parallel and scheduling candidates across all six binary
order permutations, with12 steady samples and6 warmups per binary. It validates
all outputs against the independent typed oracle and checks dataset integrity
before/after. Q13/Q10 paired drivers and separate Q13/Q20 phase drivers are also
prepared. All run under32GiB containment,12GiB process caps and4GiB query budgets
with CPUs0–15. Fresh full-suite deadlines remain the subsequent acceptance gate.

[Previous candidate and preserved regression](parallel-aggregate-candidate-2026-09-08.md).
[Frozen source, tests and experiment drivers](benchmarks/2026-09-08-batch-dispatch/manifest.json).

## Release IPC preparation check

The frozen scheduling executable also refuses the original oversized decoded-IPC
setup cleanly in release mode: exit1, `status=refused`, `phase=setup`,
`error_kind=preload_admission`. It requests7,762,147,216 bytes before decoding
against the4,294,967,296-byte context limit, with no retained context allocation.
The12GiB process cap is unchanged; this small setup-only verification uses a
stricter2GiB cgroup scope. `/usr/bin/time` reports16,720KiB peak RSS. It ran while
the separate contained profiler build was active, so its0.04s wall time is not a
performance measurement. No query or full-residency benchmark executed.

Command: `TMPDIR="$PWD/.scratch" QE_MEM_CAP=12G SAFE_BUILD_MEM=2G
SAFE_BUILD_JOBS=1 scripts/claude-safe-build.sh /usr/bin/time -v -o
.scratch/live-schema-boundary/dispatch-ipc-preload.time
.scratch/live-schema-boundary/dispatch_benchmark_embedded
.scratch/public-bench/outer-on-sf10-ipc-01/setup.json`. Stdout/stderr are preserved
next to the time record. This confirms the prior debug admission repair in the
frozen release binary; it does not establish full IPC or GPU residency coverage.
