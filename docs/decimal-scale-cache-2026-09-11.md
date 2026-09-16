# Decimal conversion metadata and aggregate state costs — 2026-09-11

Completed September15: binary6d0b9317 hoists decimal scale conversion into batch
binding. Q1 is1.98%/0.81% faster in two diagnostic blocks; larger CPU costs remain.
Provider SF10 validates338 outputs/252 pairs and residency validates348/278.
Native/reference failures and full acceptance remain open. No DuckDB leadership
or canonical GPU acceleration is certified.

The completed state-detail diagnostic identifies different costs in the shared
aggregate path. Frozen binary `59619bde` produced six independently typed-correct
outputs across Lance Q1 and Q18. The 16 GiB scope peaked at 5,370,228,736 bytes,
with zero OOM/max events and no swap. These are cumulative cgroup charges, not
exact process RSS or proof of query-wide reservations.

[The 25-file archive](benchmarks/2026-09-11-aggregate-state-detail/manifest.json)
retains requests, results, source references, timings, and profiler stderr.
One warmup and two measured requests per query used 16 threads, affinity 0–15,
default disjoint ownership, and 4/12 GiB query/process budgets. Profiling samples
one in 1,024 rows per owner/call; selection and timer overhead make these
attribution evidence, not exclusive CPU accounting or performance acceptance.

| Sampled phase | Q1 ns/row | Q18 inner aggregate ns/row |
|---|---:|---:|
| Prepared key access | 30.69 | 33.32 |
| Hash lookup | 31.77 | 89.64 |
| State preparation | 122.31 | 53.64 |
| Commit | 18.38 | 25.83 |

Q18 additionally spends about 0.99 seconds constructing output. State preparation
is the larger sampled component for Q1. Source inspection finds decimal-to-float
conversion recomputes `10_f64.powi(-scale)` for every scalar, including decimal
AVG inputs. Scale is representation metadata with only 256 possible values.

A process-wide OnceLock cache was tried and rejected before release: the
10-million-conversion diagnostic rose from 163.5 ms to 183.0 ms (178.8 ms with
an inline annotation). Those short, unpaired timings are not workload evidence,
but do not justify retaining the candidate. Its source and logs are preserved
in scratch for the cycle archive; the shared scalar routine is restored.

The revised candidate binds the conversion factor beside the borrowed decimal
array for floating aggregate states. It uses the original runtime powi operation
once per batch. Coefficient conversion and multiplication retain their order;
exact SUM keeps coefficient and scale. The bounded descriptor storage already
exists, and row staging/commit, admission and fallback remain unchanged.

Red5422 reproduces the missing floating conversion binding. This is a binding
contract regression, not evidence of previously incorrect SQL output. Green61078 passes
11 state-row tests. Broader two-mode validation, release and provider SF10
cycle25275 was interrupted during the partial library run. On September15, the
handle was missing and a host process check found no running build/benchmark.
All800 source hashes still match. Resume90400 preserves the interrupted log,
retains completed default-mode suites and resumes only unfinished work. Source
remains frozen during measurement. The regression covers every supported Arrow scale,
NULL, positive/negative inputs, exact SUM alongside AVG, allocation-free binding
and cleanup. The completed SF10, paired and residency evidence follows below.
The observed gain remains modest and diagnostic.


## Completed validation and release status — September15

Both modes pass 1,135 library tests with 11 existing ignores and 128 contract
checks. Native/IPC passes 63 default checks and 62 partial checks, with the
existing partial numeric-output failure. Spill/numeric passes 28 checks and
retains the same six spill refusals in each mode. Failure names, executable
counts, exit codes and the retained test inventory were checked against the
previous UTF-8 candidate. The two added library regressions pass.

The resumed validation scope peaks at 2,113,761,280 bytes under 48 GiB with no
OOM/max events and no swap. This is only the resumed scope; the interrupted
original scope has no final resource record. Its termination cause is unknown.
Release `6d0b9317e57b8669fe18d291a58e706667fa4855fbb50667d0b2f0a6f1ceae08`
completed in8m52s with800 source inputs verified. The resumed build scope peaked
at13,260,533,760 bytes with zeroOOM/max events. The provider, paired and residency stages are now terminal. Retention is
provisional; this cycle does not establish full performance acceptance.

## Next shared boundary to measure

Local DuckDB `src/execution/aggregate_hashtable.cpp`, `UpdateAggregates`, iterates
aggregate functions over a vector of resolved state addresses. The local
ClickHouse `src/Interpreters/Aggregator.cpp` uses `executeImplBatch` and
`addBatchSinglePlace`. The existing local-source comparison records checkout
revisions; neither checkout identifies the benchmark's Python DuckDB wheel.

Our `group_rows.rs` calls `StateRows::prepare_arrays_indexed` for each selected
row. `StateRows::begin` copies fixed state into staging, and `PreparedRow::publish`
copies it back after every slot succeeds. This protects late-slot error rollback,
selected-payload admission and exact cursors on spill retry. A future column-wise
or in-place update cannot simply remove this contract. It needs explicit bounded
admission before updates, an exact committed-prefix contract for memory refusal,
and tests covering duplicates, NULLs, mixed fixed/selected state, late overflow,
actual spilling and independent typed results. First measure the surviving state
cost after decimal binding; the current sampled intervals do not isolate copying
from arithmetic or dispatch.

Q18's output path is a separate hypothesis: `live_spill.rs` builds all columns
for each output quantum before applying HAVING through `AdmittedBatchFilter`.
Attribution puts about 0.99 seconds there, but no change has been implemented.
Any deferred construction must preserve final-state filtering after merging,
typed refusal, bounded output quanta and terminal predicate errors. No source
edits for these follow-ups are made during the frozen candidate's measurements.


The supplemental provider audit now explicitly checks both measured times
against the recorded ceiling before counting a valid pair. The previous UTF-8
screen was rechecked: raw66/native57/Iceberg63/Lance66 valid pairs are unchanged.
The versioned harness already rejects late engine samples independently of typed
correctness. This audit refinement changes no workload, calibration or timing.


## Completed canonical provider SF10 screen

Binary6d0b9317: 16 threads, affinity0–15, 4/12 GiB query/process budgets,
default disjoint ownership, one session and three measured samples. Independent
audit validates338 completed engine outputs and252 of264 planned measured pairs.
The provider/build scope peaks at26,865,696,768 bytes under48 GiB, zeroOOM/max,
no swap. It is cumulative scope charge, not query RSS.

| Provider | Typed outputs | Valid pairs | Complete | Geometric mean QE/DuckDB | Suite ratio | Wins |
|---|---:|---:|---|---:|---:|---:|
| Raw Parquet | 88 | 66 | yes | 2.432179 | 2.616286 | 0/22 |
| Native | 77 | 57 | no | — | — | — |
| Iceberg | 85 | 63 | no | — | — | — |
| Lance | 88 | 66 | yes | 1.986131 | 2.952229 | 1/22 |

NativeQ1/Q6 warmups time out. Q18 completes its warmup in4,234.428063ms but exceeds
the4,155.345284ms calibrated ceiling; its three measured requests are NOTRUN.
Q12 completes this screen. These phases differ from the parent checkpoint and
must not be generalized into identical benchmark failures. Iceberg's DuckDBQ9
first measured request refuses a262,144-byte allocation. The engine's Q9 warmup
is typed-correct; all measured engineQ9 requests are NOTRUN. This is not the
parent's oracle-stage256MiB refusal. Correct outputs, timely pairs and complete
suites remain separate outcomes. No successful-only native/Iceberg suite ratio
is reported. These fresh DuckDB ratios are not paired candidate/parent effects.


## Paired attribution

All80 outputs match independent DuckDB oracles; optimized and physical plans
match between parent59619bde and candidate6d0b9317. Two reversed blocks use fresh
workers per query, one warmup/three measured samples, 16 threads, 4/12 GiB budgets,
default disjoint ownership and existing scan/aggregate phase instrumentation.
The180s watchdog is diagnostic and does not clear canonical latency gates.

| Query | Candidate/parent block1 | Block2 |
|---|---:|---:|
| q01 | 0.980169 | 0.991930 |
| q06 | 0.991921 | 0.832578 |
| q12 | 1.044357 | 0.966276 |
| q18 | 0.969470 | 1.002913 |
| q09 | 1.003308 | 1.033460 |

Q1 median time is1.98%/0.81% lower. Ingestion medians move3,760.077→3,639.516ms
and3,792.211→3,758.934ms; evaluation stays around1.43–1.44s. This is modest
support for retaining the bounded metadata change, not strong performance
acceptance or a solution to the larger shared-state cost. Control-query variation
is visible, including Q12/Q9 increases in one block. Do not claim blanket
regression freedom or attribute changes in unaffected shapes to decimal binding.
The prior failed same-binary short-query precision control remains unresolved.


## Completed residency screen

All five cases complete, with348 independently typed-correct outputs and278 valid
measured pairs. Canonical cases use16 threads, 32/48 GiB query/process budgets,
one session/three samples, and default disjoint ownership. These capacity screens
exclude preload and do not clear the16 GiB preload gate. The two custom float
cases use4 threads, 4/8 GiB budgets and20 samples; they are not canonical SF10.
All use affinity0–15. The sequential postcheck scope peaks at26,090,225,664 bytes
under64 GiB, with zeroOOM/max events and no swap.

| Case | Typed outputs | Valid pairs | Geometric mean QE/DuckDB | Suite ratio | Wins |
|---|---:|---:|---:|---:|---:|
| canonical_decoded_ipc | 88 | 66 | 0.655837 | 1.011982 | 15/22 |
| canonical_gpu_control | 88 | 66 | 0.790456 | 1.247289 | 14/22 |
| canonical_gpu_mixed | 88 | 66 | 0.782244 | 1.238873 | 14/22 |
| smoke_gpu_control | 42 | 40 | 1.322902 | 1.753884 | 1/2 |
| smoke_gpu_required | 42 | 40 | 0.097584 | 0.085233 | 2/2 |

All88 canonical mixed-GPU outputs record zero successful device executions in
both telemetry counters. This does not demonstrate canonical GPU acceleration.
The custom required-GPU case has40/40 measured request-scoped device proofs,
matching preparation sessions without per-request uploads or new fallback/allocation
failures. Its two warmups are additional outputs, not part of the40 measured proofs.

The decoded suite ratio improves against its fresh DuckDB reference, but engine
suite time is17,370ms versus17,309ms in the previous screen; DuckDB changes from
14,720ms to17,165ms. Do not attribute that ratio shift to the patch. The matched
parent/candidate diagnostic above is the evidence for its modest CPU effect.

## Provenance and next cycle

Validation/source archive: `docs/benchmarks/2026-09-11-decimal-scale-validation`.
Provider and paired archives use adjacent `decimal-scale-sf10` and
`decimal-scale-attribution` directories. Residency and complete controller logs
are in `docs/benchmarks/2026-09-15-decimal-scale-residency` and
`docs/benchmarks/2026-09-15-decimal-scale-controller`. Each has a SHA-256 manifest;
the validation tar preserves all800 source inputs. Controllers record exact
commands, environment, interruption/resumption, failures and cgroup evidence.
The original interrupted scope's final resource state is unavailable.

The next concrete task is the [native IPC dictionary contract and projection
follow-up](native-admission-follow-up-2026-09-11.md), beginning with a missing-table
regression. Source remains unchanged during this completed measurement cycle.
Full provider, resource, concurrency, multi-session and short-query precision
acceptance are still required.

Formatting passes. The exact staged allowlist contains2,897 files; Git reports
18 whitespace warnings only in verbatim raw logs, which are intentionally preserved.
