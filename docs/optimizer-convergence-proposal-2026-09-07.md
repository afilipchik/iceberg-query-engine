# Optimizer convergence: systemic fix and validation

Frozen640 fixes repeated optimizer rounds and accumulating scan predicates. The
patch passes762 selected Rust tests, independent optimized semantic checks, the
five-mode canonical SF10 screen against638, protected repeats, supported GPU
execution and actual-spill cap checks. Direct comparison with the older612
performance control completed; its three flagged queries are in protected repeats. This is a scoped improvement, not DuckDB
leadership or complete query-wide memory certification.

## Reproduced defect and implementation

The measured638 Q7 plan contains ten copies of each implied nation IN predicate.
DeriveOrPredicates adds predicates above the join, PredicatePushdown moves them
into scans, and the original OR remains. Each round can add the same predicates
again. The driver also treats any intermediate change as evidence that another
round is needed, even when the complete pipeline returns to the same plan.
It allocates two complete Debug plan strings per rule to compare outputs.

The production fix changes two shared boundaries:

- Compare complete rounds using existing structural LogicalPlan equality. Preserve
  the iteration cap, named rule errors and exactly-once final PackedJoinKeys phase.
  Intermediate comparisons occur only in diagnostics and also use structural
  equality. Expr equality preserves literal types, decimal scales, float bits and
  timestamp metadata.
- At scan destinations, flatten conjunctions and retain the first occurrence of
  structurally equal safe atoms. The whitelist covers column/literal comparisons,
  column IN literal lists and column NULL tests. It excludes functions, casts,
  arithmetic and subqueries. Original OR predicates and join/pushdown barriers remain.

There is no query-text recognition or benchmark-specific production branch.

## Red/green evidence

Tests were installed before changing production behavior. Four assertions failed:
synthetic inverse rules ran [10,10,1] times instead of [1,1,1]; typed comparisons
accumulated four atoms instead of two; NULL/equality predicates accumulated three
instead of two; a real multi-batch/partition join plan had duplicate IN predicates.
Three controls passed: actual round changes need another round, volatile
occurrences remain distinct, and outer-join NULL extension is preserved.
Tests-only source and proposed patch hashes were verified before integration.

After the patch,692 library tests and70 selected integration tests pass. The two
preexisting library ignores remain explicit: the dependent-join test and the
real-sidecar IPC test requiring a dedicated environment. Integration coverage
includes expression identity, semantic proofs, partition/materialization contracts,
memory reservations, projection admission, timestamps, group-key equivalence and
scalar comparison. Formatting and whitespace checks pass. No dependencies changed.

Optimized checks:89 float/date,10 dense-float,56 coercion and43 additional timestamp
queries match DuckDB.58 primitive queries match the preserved control;50 match
DuckDB and eight retain documented integer-division differences.19 canonical
literal results plus one separately typed bare-NULL value contract pass. Three
timestamp metadata probes match; two invalid decimal metadata cases reject.
Under64KiB, borrowed input succeeds while expanded literal, Double and Decimal
construction refuse with named memory errors. The initial validation invocation
used a missing virtual-environment executable and ran no tests; its log is kept.
The successful run uses `.scratch/venv-lance/bin/python`.

## Matched canonical SF10 screen against638

All880 requests pass typed validation and the fresh10× DuckDB time limit. Each
query uses one warmup and three steady pairs, alternating binary order, fixed
16-CPU affinity,40GiB query budget,48GiB process cap and96GiB outer containment.
Raw samples, plans, hashes, provider manifests and reference calibrations are kept.
This is one development session, not the three-session leadership gate.

| Mode | Suite/638 | Geomean/638 | Suite/DuckDB | Geomean/DuckDB | Wins/DuckDB |
|---|---:|---:|---:|---:|---:|
| decoded_ipc | 0.9094 | 0.9207 | 0.4363 | 0.3692 | 19/22 |
| raw_parquet | 0.9834 | 0.9828 | 2.3819 | 2.1425 | 1/22 |
| native | 0.9657 | 0.9635 | 3.5627 | 3.1674 | 0/22 |
| iceberg | 0.9794 | 0.9846 | 0.3171 | 0.2914 | 20/22 |
| lance | 0.9361 | 0.9500 | 1.2381 | 1.0995 | 9/22 |

The only >10% flag is Lance Q8: the initial ratio is about1.257. Two fresh10-pair
repeats give0.98652 and0.96924; all44 requests pass. The same rendered logical-plan hash
appears in both binaries and all sessions. No displayed plan change explains
the variance; rendered plans do not expose all internal execution state. Preserve the original flag and both repeats. IPC Q16 is near the
threshold at1.09848 and is retained in the summaries. No >10% regression against638
is confirmed by the required repeats.

Every candidate Q7 sample across five modes has one implied predicate per nation
column instead of ten. IPC Q7 steady optimizer time falls14.768ms to0.743ms; total
median falls about15%. The older411.79ms optimizer measurement was a warmup.
Across638 steady suites, optimizer medians account for only1.59–2.94% of total
time. This fix cannot by itself explain or close the overall DuckDB gap. Context
phase timers omit other work; the timing residual must not be called pure teardown.

## GPU and resource gates

Same-binary GPU validation passes40 CPU-control and40 required-device requests
on the supported600k-row float fixture. Every device request records
`device_executed` and one successful device run, with a256MiB cache target.
This is not canonical SF10 GPU acceleration or a hard VRAM admission guarantee.
The16GiB host scope uses CPUs0–3 and the preserved repository NVRTC library.

Both250-million-row aggregate cap tests pass:1,000,003 groups, exact counts and
actual spilling of3,855,541,894 accounted bytes. Peak RSS is409MiB under the1GiB
cgroup and411MiB under2GiB RLIMIT_DATA with8GiB outer containment. These do not
certify all query-wide ownership boundaries.

## Provenance and remaining work

Source archive:640 files, SHA
`6c0f0416fbbed071514d0c5b03e2391b07c408de3f02641d6f83a2bd583420ea`.
Release binary, Lance+GPU:
`6465131895813cbb8798ed6fe2db9876a7a173ffac178a74fc3311d04c440792`.
Cap binary:
`6ea147ab10c9da8308f4f48f7c38c28217498b1d3916ff2e8f31d70759f50404`.
Evidence and reproducible drivers are under
`.scratch/optimizer-convergence-repair/`; final evidence is archived with every protected and isolating repeat.

Raw Parquet/native/Lance still exceed DuckDB suite time. Previous valid/nullable
TRY_CAST regressions, query-wide resource ownership and larger/cold/concurrent
workloads remain open. The next shared CPU work needs measured execution-boundary
attribution; a broad rewrite is not justified by the optimizer result.

Separate source risks remain: OR derivation keys distinct columns by display
strings, and diagnostic optimization bypasses normal statistics-aware rule
reconstruction. A concrete column-identity counterexample and independent test
design are in `next-identity-audit.md` within the scratch evidence; they have not
yet been executed or integrated. Keep their semantic investigation separate from
this frozen performance comparison.

### Older-control Q16 triage while comparison is running

IPC Q16 crosses10% against612 in the initial screen. In the separate638 and612
comparisons, control execute medians are263.689/260.783ms and candidate medians
289.473/292.500ms. Candidate optimizer medians are0.200/0.193ms, lower than both
controls. Rendered logical and physical plan hashes match across all samples.
These observations place the observed excess in the execution phase, but do
not establish an operator cause or exclude hidden plan/state differences.
Protected repeats are pending. Raw results and hashes are in ipc-q16-triage.json.

## Direct canonical SF10 comparison with612

Session82801 completed880 typed/time-gated requests. Settings and pair counts
match the638 development screen. Each comparison has its own fresh reference
calibration; do not multiply ratios from separate sessions to infer this result.

| Mode | Suite/612 | Geomean/612 | Suite/DuckDB | Flags >10% |
|---|---:|---:|---:|---|
| decoded_ipc | 0.9094 | 0.9235 | 0.4492 | q16 |
| raw_parquet | 0.9851 | 0.9833 | 2.3845 | none |
| native | 0.9843 | 0.9760 | 3.6193 | none |
| iceberg | 0.9776 | 0.9729 | 0.3168 | none |
| lance | 0.9343 | 0.9509 | 1.2372 | q08, q20 |

Protected session94513 now repeats IPC Q16 and Lance Q8/Q20 in two fresh10-pair
sessions per mode. No acceptance decision is made before those results.

## Protected outcomes and acceptance limit

Protected94513 completed132 typed/time-gated requests. Q16 repeats below the
threshold (about0.95670/0.97002); Lance Q8 also clears (about0.97719/1.04647).
Lance Q20 confirms a regression against612 (about1.13905/1.11504). Overall
performance acceptance is therefore rejected despite lower suite totals.

Two additional direct638→640 Q20 sessions66650 complete44 requests correctly,
with ratios1.05084/1.00352. The incremental optimizer patch does not reproduce a
>10% regression in those sessions, but the full candidate still fails the older
control gate. Do not remove the confirmed612 failures from the acceptance record.

Q20 phase evidence differs from Q16: protected640 parse medians are18.18/18.35ms
versus about0.11ms for612, while execute medians remain about66ms on both sides.
The source parse timer directly encloses unchanged sqlparser parsing. Per-sample
spikes vary, and638→640 repeats also show variable parser delays. Allocation
maintenance or scheduling is a hypothesis, not a finding; use memory-syscall and
request-boundary diagnostics on the frozen binaries before selecting a fix.
Do not hide this overhead by moving the benchmark timing boundary or disabling
resource protection.

## Verified archive

[Complete evidence](benchmarks/2026-09-07-optimizer-convergence/README.md) contains
7,010 hash-verified files. Archive SHA
`767e7f60e095a41c668e8846c7761f6149609086d77998520c9adb6308832b59`.
Archive86770 completed successfully. The record includes all1,760 full-screen
requests,220 protected/isolating requests,80 GPU/control requests, both cap
scenarios, frozen source/binaries, red/green tests and independent semantic oracles.
The candidate remains performance-rejected for the confirmed Q20 regression
against612. Future diagnostic evidence will be kept separately; this archive is
immutable. No engine or dependency changes followed the source freeze.
