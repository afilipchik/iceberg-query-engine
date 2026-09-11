# Paired benchmark startup-order investigation

The historical paired development screen confounded binary identity with process
startup order. Alternating query execution order did not balance that factor.
Identical-binary SF10 Lance controls reproduce a Q20 median difference; reversing
startup flips its direction. This prevents attributing the entire earlier Q20
regression to source. It does not establish that any candidate meets performance
acceptance or invalidate typed correctness results.

## Reproduction and observed results

All runs use the unchanged canonical SF10 data and pinned Lance provider, CPUs
0–15, 16 engine threads, 40 GiB query budget, 48 GiB process cap, and a 96 GiB
memory-capped scope. Each case runs Q8 then Q20 with fresh per-query workers,
one excluded but validated warmup and 10 measured pairs. Each case preserves a
fresh typed DuckDB oracle, three calibration samples, and the exact 10× ceiling.
No allocator setting or timing boundary changed. Sessions90366 and81482 each
completed176 correct/time-gated requests with exit0:352 total.

Ratios below are `after / before` for Q20. They are individual development
sessions; no confidence interval or leadership claim is attached.

| Before / after binary | Startup order | Ratio of medians | Ratio of means |
|---|---|---:|---:|
|612 /612|before first|1.04889|1.0102|
|640 /640|before first|1.05716|1.0384|
|642 /642|before first|1.05503|1.0447|
|642 /640|before first|1.08653|1.0710|
|612 /612|after first|0.97860|0.9687|
|640 /640|after first|0.99513|0.9834|
|642 /642|after first|0.96636|0.9928|
|640 /642|after first|0.94832|0.9911|

The before-first A/A runs place a roughly77–79ms parse burst in iteration8 of
the first worker, while the second worker has intermittent shorter bursts.
Medians and means therefore answer different questions. The earlier diagnostic
traces attribute many slow parse windows to aggregate MADV_DONTNEED cost and
bounded stacks identify mimalloc decommit; these new untraced runs do not prove
the exact allocation producer or a universal startup mechanism. Q8 shows smaller,
mixed differences. Repeated, balanced measurements remain necessary.

Binary hashes and complete commands are in
`.scratch/benchmark-position-audit/{status,startup-status}.json`; raw attempts,
plans, Arrow results and provenance are under
`.scratch/public-bench/position-audit-*-01/`. The initial per-sample analysis is
`.scratch/benchmark-position-audit/initial-analysis.json`.

## Systemic harness corrections

`python -m benchmark.paired` moves the reviewed paired development driver into
`scripts/benchmark/paired.py`. Both binary paths and hashes are mandatory.
`--startup-order before-first|after-first` and `--execution-offset 0|1` are
independent controls. Balance their four combinations across fresh sessions,
retain each session separately, and include identical-binary negative controls.
A single invocation remains a development screen, not an acceptance report.

Per-query summaries now retain steady means, totals and maxima alongside medians,
so infrequent cleanup remains visible. All original per-request timings remain
preserved; cleanup is not removed from measured query time.

A second defect was reproduced in the old scratch driver's summary: duplicating
one iteration in place of another could still return `complete=true`, and a
warmup labelled as steady was accepted. The first two-pair reproducer also
exposed an empty-side median exception; the expanded reproducer produced two
actual assertion failures. The new summary requires each expected side/iteration
exactly once, consistent warmup labels, finite positive elapsed times and every
correctness/time gate. Invalid or partial results cannot produce ratios.
This defect is in the old scratch paired summary, not the existing versioned
`contract.report`, which already tests duplicate/missing membership. No evidence
has been found that historical raw requests actually contained these corruptions.

The initial Python suite runs108 tests:106 pass and two explicit opt-in tests
skip. With `BENCHMARK_TEST_LANCE=1 BENCHMARK_TEST_EXACT_BAG_SPILL=1`, session60717
passes all108 tests with no skips, including actual comparator spill and the
independent Lance decimal reference. Five new
paired tests cover ordering balance, cleanup-burst accounting, duplicate samples,
warmup labels, and invalid/failing/missing outcomes. Session54854 passes another176 real SF10 typed/time-gated requests with exit0
using the versioned runner, execution-offset1 and both startup orders. Total
position-audit evidence is528 requests. Q20 results are:

| Before / after | Startup order | Ratio of medians | Ratio of means |
|---|---|---:|---:|
|642 /642|before first|0.99747|0.95329|
|642 /642|after first|0.92680|0.95908|
|640 /642|before first|0.99292|0.99842|
|640 /642|after first|0.94320|0.97498|

These runs validate the runner but do not establish a source speedup: identical
binaries show comparable apparent gains. Startup balancing alone is insufficient
to remove short-stream cadence effects. Preserve the full distributions and use
longer fixed streams plus balanced independent sessions before adoption.
Formatting and whitespace checks pass. Production source remains frozen642.

## Remaining acceptance work

Preserve the original642 Q8/Q18/Q20 flags and640-vs612 Q20 flag. Repeat protected
queries under balanced startup/execution assignments and longer fixed streams;
report mean/tail behavior as well as medians. Do not silently replace historical
results or dismiss every slowdown as noise. Other provider, GPU and resource
gates for642 remain open. This checkpoint changes benchmark methodology and
validation, not production engine code or dependencies.

## Archived checkpoint and next measurement

The verified archive is [evidence and provenance](benchmarks/2026-09-07-benchmark-startup-order/README.md):
1,059 files,528 requests, SHA256
`18a16a216c43c159b02c057e0ffe754cc390afa236034f7e9e79ca9d6161977e`.
Every archived member was read back and checked. The archive is immutable.

Session3501 now runs the predeclared longer comparison642 vs640 on all three
initially flagged queries, Q8/Q18/Q20. Each of the four startup/execution-factor
combinations gets50 measured pairs plus one validated warmup:1,224 maximum
engine requests. This changes stream length for both binaries equally and keeps
all cleanup within the same timer. Results are under
`.scratch/public-bench/long-640to642-*-01`; commands and completion state are in
`.scratch/benchmark-position-audit/long-status.json`. No protected flag is cleared
until this evidence and the remaining612 comparison have been assessed.

The first two longer configurations complete612 correct/time-gated requests.
Before-first/offset0 median ratios for Q8/Q18/Q20 are0.97345/1.00288/1.01470;
after-first/offset1 ratios are1.05498/0.9934/0.9786. Both show no >10% flag,
but the remaining two predeclared configurations are still running. These are
interim results and do not clear the prior flags or establish a speedup.

## Completed longer immediate-control comparison

Session3501 exits0 with all1,224 requests correct and within their fresh DuckDB
time ceilings. No query/configuration exceeds the10% regression threshold:

| Query | Range of median ratios,642/640 | Range of mean ratios,642/640 |
|---|---:|---:|
|Q8|0.92452–1.05498|0.98372–1.01191|
|Q18|0.98133–1.00288|0.97546–0.99868|
|Q20|0.97863–1.08525|0.97915–1.02980|

Thus the initial >10% flags against the immediate640 control do not reproduce
under this longer balanced check. This clears that incremental protected gate;
it does not clear the separate642-vs612 comparison, establish a speedup, or
complete provider/resource/leadership acceptance. In particular Q8 has a7.5%
median improvement in one cell while its mean is1.2% worse: median-only readings
remain inadequate for interpreting cleanup cadence. All individual distributions,
including startup/early samples, remain preserved in `long-analysis.json` and the
raw per-query result files. No sample window was selected for adoption.

The completed longer-control archive is [preserved here](benchmarks/2026-09-07-lance-bridge-long-controls/README.md):
1,474 files/1,224 requests, SHA256
`8fc0238a3e54f7e50f12ae84b0da2c92c03d492094c1ef2b1afbf01ac76f0af6`.
Every member was verified on readback; the earlier528-request archive remains
unchanged.
