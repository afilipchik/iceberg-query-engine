# Exact decimal aggregate output and ownership experiment

This is an implementation candidate, not an accepted performance improvement.
The baseline is frozen649, source archive SHA256
`d8957b73c8d52603004388744a58f3ec595a16b716c7b28e4f33054c7e371d07`,
binary SHA256
`1f189fc6ac191c6b390e1c966ba46be2320dd4800bfed8d32f8faf3937cc387f`.

The [phase profile](shared-aggregate-finalization-profile-2026-09-07.md) measured
roughly202–210ms worker maxima for Q18 decimal output construction and111–113ms
for state destruction in native/IPC. Construction formerly finalized each raw
aggregate into a Vec<ScalarValue>, built a second vector of scalar references,
then allocated Arrow output. This experiment removes those two vectors for
Decimal128 aggregate columns in raw and general state output. It leaves update
and merge order unchanged. It does not assume keys are unique or alter SQL plans.

`build_decimal_output` finalizes directly into an admitted i128 buffer and validity
bitmap. It reuses DecimalValue::rescale and validate_precision, preserves signed
scales and exact coefficients, rejects nondecimal values, and propagates sticky
aggregate errors before HAVING. ReservedBufferBuilder admits capacity before
allocation and iteration; Arrow buffers own the leases after return, including
through clones and slices. An error releases both partially constructed buffers.

The shared merge API now accepts an explicit MemoryPool reference. MorselAggregateExec
and SpillableHashAggregate pass their existing query pool through sequential,
disjoint and parallel shard finalizers. This is independent of Rayon thread-local
state. Legacy standalone AggregationState::build_output and materialized helper
calls use the bounded process pool. They are not evidence of full query ownership.
The scalar-array compatibility entrypoint shares the checked decimal conversion.

This contract covers the new decimal coefficient and validity buffers only.
Existing group-state allocations, group-key output, entry-reference vectors,
filter/concat kernels, provider collections and other aggregate output types
retain their existing accounting limitations. A successful cap test would not
close those gaps. Output admission may cleanly refuse a query that previously
allocated without reservations; refusal remains distinct from successful spill.

## Validation and retained evidence

`.scratch/decimal-output-repair/` contains frozen649 before copies, an exact
candidate patch, a650-file source manifest, the paused benchmark PID/start-tick
record, guarded idle verification and validation logs. The first six focused
tests passed with no ignores. Expanded full-library and selected integration
validation completed in session81904 (48GiB, one build job):845passed, zero
failures and10existing library ignores. All eight new tests passed. No textual
SKIP/unavailable markers occurred; ignored hardware tests are not certified by
this run. Formatting and diff whitespace checks passed. Optimized validation and
performance measurement remain pending.

Eight new tests cover exact rescaling above f64 precision, signed scales, NULLs,
zero, invalid metadata/type/precision, arithmetic and iterator errors, refusal
before iteration, partial-admission cleanup, retained clone/slice leases, shared
parallel-finalizer budgets, overflow despite false HAVING, actual raw/general
and small-state ingest with independent integer SUM/MIN expectations, empty
SQL cardinality, and overlapping-key raw shard merge above65,536thread groups.
The broader integration selection includes numeric/decimal reuse, aggregate
encodings/order/names, dictionary keys, DISTINCT/empty semantics, reservations
and bound-column identities. Missing hardware/fixtures must remain explicit skips.

## Performance decision, predeclared before candidate timings

First build and freeze the optimized Lance+GPU benchmark binary only after the
semantic tests pass. Before performance acceptance, run optimized exact-value
probes and the existing real-spill caps/device controls. Keep all failures.

The bounded screen compares candidate against uninstrumented649 using the
versioned paired harness: Q18, Q13 and Q1 across native, raw Parquet and decoded
IPC; all four startup-order/execution-offset combinations; ten steady pairs plus
warmups; sixteen threads/affinity0–15;40GiB query budget,48GiB process cap and96GiB
outer scope. Every cell uses fresh typed DuckDB references and the10× ceiling.
Preserve all raw samples and evaluate both means and medians, without dropping
outliers. A reduction in one query does not establish the required operator or
full-engine win. Confirm component attribution separately before accepting the
shared CPU milestone. Any protected>10%flag requires investigation and the
pre-existing full regression gates remain required.

The original649-vs647 full-provider run remains separately paused between cells;
its completed eighth native cell must be reaped by the original coordinator.
Do not mix candidate timings into that run or rebuild its retained binaries.
Resume that coordinator after serial candidate work with PID/start-tick and
binary hash checks. The remaining full workloads, resource/concurrency matrix,
Iceberg/Lance/GPU coverage and DuckDB leadership targets remain open.


### Optimized build and baseline checkpoint

Source651 is frozen and member-verified: archive SHA256
`5dfa82bdb7ffe2a0e5d37cea7d00cf73cdb3e27908b09e40aeb5745d5f25a18e`.
Release99346 is building benchmark_embedded and oom_cap_harness with Lance+GPU,
64GiB containment and one build job. The prepared optimized decimal oracle adds
18typed comparisons on Parquet/IPC and two overflow-refusal checks, with exact
Python integer/Decimal fixture construction; it has not run yet. Prepared drivers
parse successfully. The frozen production/example/Cargo hashes remain unchanged.

The independent baseline audit now validates3388requests in seven recorded cells.
The seventh raw-Parquet cell has649/647 suite ratio1.003858 and no>10%flags.
The completed eighth native cell summary is correct/time-gated, suite ratio
.990020 and no>10%flags; coordinator reap and the eighth independent prefix audit
remain pending. The first Lance cell's Q11 mean flag remains unresolved.


### Optimized semantic validation completed

Release99346 completed normally in10m46s. Frozen benchmark binary SHA256:
`9472e7d0cc7db5e52de429e2299508d98a798da28e859baf5eed43c237a5ac76`;
cap binary SHA256:
`875307d3ae495ea5db197d79a8d269fccd51be3374e12e84dca040ba5743d96b`.
Both use source651 and Lance+GPU features; no dependencies changed.

Session82235 completed the optimized oracle suite:89float/date,10dense-float,
56coercion,43timestamp and9bound-identity queries match their independent typed
references. All58primitive cases match the historical control;50match DuckDB and
eight pre-existing integer-division differences remain explicit. Literal probes
validate19canonical cases and one bare-NULL value contract. Both malformed decimal
metadata cases refuse; timestamp metadata matches. Borrowed-input expansion fits
the small pool; literal and both coercion expansions refuse by memory name.
See `.scratch/decimal-output-repair/optimized-validation-complete.json`.

The additional optimized decimal oracle validates18typed comparisons across
Parquet/IPC and two explicit overflow refusals. Its first run passed all value
comparisons and both engines refused overflow, but the script asserted the wrong
status spelling (`error` instead of the workers' `query_error`). The original
script/log/results are preserved; the corrected driver reran successfully in a
fresh decimal-oracle-02 directory with the same binary. This was a verifier bug,
not an engine correction or a relaxed expected result.

Real-spill caps are running in session44464 under8GiB outer containment. GPU
control/device validation and the predeclared balanced screen remain pending.
The original baseline coordinator remains paused between cells during this work.


### Resource/device validation and paired measurement launch

Cap44464 completed normally:250million rows,1,000,003exact groups and
3,855,541,894accounted spill bytes under both1GiB cgroup and2GiB RLIMIT_DATA
scenarios. Peak RSS was412/414MiB. Outer containment was8GiB. These are the
selected aggregate cap scenarios, not a complete memory matrix.

GPU24698 completed80typed/time-gated requests (40CPU control and40required device)
on the supported600,000-row float fixture. All40device requests report actual
device execution and one successful device run. The256MiB cache target is not
a hard VRAM-admission proof, and this is not canonical SF10 GPU certification.
Raw evidence is `.scratch/public-bench/gpu-resident-decimal-output-01/`.

Balanced screen60892 is now running alone in96GiB containment on affinity0–15.
The checked versioned harness uses16threads,40GiB query memory and48GiB process
cap. It preserves the predeclared12cells and792engine requests. No candidate
performance result is available at this checkpoint. The original full baseline
coordinator remains safely paused; it must resume after this serial screen.


### First two screen cells independently audited — preliminary

The native and raw-Parquet before-first/offset0cells completed132requests; all
typed comparisons and time gates passed. The independent prefix audit recomputed
exact sample membership, means, medians, suite totals and geometric means from
raw attempts and verified frozen binary identities. It reuses typed comparison
records rather than rerunning their Arrow oracles.

| Track (three-query subset) | Candidate/control suite | Geometric mean | >10%median/mean flags |
|---|---:|---:|---|
| Native | .959055 | .964262 | None |
| Raw Parquet | .950292 | .960752 | None |

Native Q18 median is1062.629→957.401ms (ratio.900974); Q13 is959.850→931.904ms
(ratio.970885). These are preliminary single-cell measurements, not a complete
operator attribution, full workload result, DuckDB win or performance acceptance.
Ten predeclared screen cells remain. Evidence: screen-audit-02.json and immutable
per-cell attempts under `.scratch/public-bench/decimal-output-screen-*`.


### Four-cell prefix audit

The next immutable audit validates264requests across four cells with no correctness,
timeout or>10%median/mean flags. IPC before-first/offset0has suite ratio.951102
and geometric mean.956836; native after-first/offset1has suite ratio.958382 and
geometric mean.959808. Eight cells remain. This preserves preliminary positive
evidence under a reversed native startup/execution order without promoting the
subset to a full performance conclusion. Task007 now links this current checkpoint
and labels the obsolete635construction checkpoint as historical; only the focused
regression-test checkbox is closed.


### Eight-cell prefix and next attribution boundary

The immutable screen-audit-08 validates528requests with no correctness/time or
>10%median/mean flags. Three native cells have subset suite ratios.959055, .958382
and.961948; three raw cells have.950292, .957186 and.961906; the first two IPC
cells have.951102 and.978095. Four cells remain; the observed startup variation
is retained rather than selecting the fastest cell.

A separate profile-components.py is prepared, not executed. It will compare the
same frozen649/651 binaries using existing diagnostic counters and per-request
log byte ranges across native/raw/Iceberg/Lance plus separately reported IPC.
It covers Q18/Q13/Q1, one warmup and three steady executions per side,120planned
requests, fresh typed DuckDB calibration and the10× ceiling. Startup order
alternates by case and execution order alternates by iteration. This diagnostic
will report overlapping stage wall times and process-counter limits explicitly;
it is not an additional latency certification or proof of full-query coverage.

After the current screen, resume the original full baseline coordinator first.
The component diagnostic must wait for an idle heavy-job boundary; never overlap
it with either latency run or repeatedly pause the baseline for speculative work.


## Complete bounded screen and resumed baseline

Session60892 completed normally. All12predeclared cells and792engine requests
passed exact sample-membership, typed comparison and fresh10×time gates. No
query/cell exceeds the protected1.10median or mean ratio. The independent audit
recomputes arithmetic from every attempt and preserves each prefix unchanged.
The following ratios are geometric means across the four startup/execution-order
cells, with no cell or sample removed.

| Track | Q18 candidate/control median ratio | Three-query subset suite ratio |
|---|---:|---:|
| Native | .920042 | .965091 |
| Raw Parquet | .905373 | .956368 |
| Decoded IPC | .924235 | .964569 |

Thus Q18 latency is7.6–9.5%lower across the balanced cells. The subset total
improves3.5–4.4%; Q1 is approximately unchanged, and Q13's small differences are
not attributed to the decimal algorithm. Individual Q18 median-ratio ranges are
.897722–.951902native,.886216–.924021raw,.908520–.935588IPC. Preserve that
variation rather than quoting only the strongest cell. This proves a bounded
end-to-end improvement for the targeted workload shape, not complete component
attribution, full22-query/provider acceptance, three-independent-session
confidence, or DuckDB leadership.

[The completed evidence archive](benchmarks/2026-09-07-decimal-output-ownership/README.md)
contains2396member-verified files, including frozen source, binary identities,
validation logs, the preserved verifier-status mistake, cap/GPU records, all
paired samples and the exact audit/summarization drivers. SHA256 manifest:
`22b1593fd41497f4b05dbd4fe5ef2354462f839cccd8012ebfc16dd2ea5ab55c`.

After archival, the guarded resume script verified frozen651 production source
and both original baseline binaries, then resumed coordinator2907628 with SIGCONT.
The original full-provider run now independently audits8cells/3872typed/time-gated
requests; twelve original cells remain. The first Lance Q11 mean flag remains
unresolved. No cell or retained binary was restarted or replaced.

Next complete that original baseline, run the prepared paired component diagnostic
at an idle boundary, and evaluate651across the full required provider/workload and
resource gates. Do not promote this screen to parent-task completion or let
continued profiling starve the already-running baseline.
