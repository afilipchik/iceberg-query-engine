# Aggregate fallback query-pool boundary — source audit

Status: source-level ownership gap identified on frozen651; runtime reproducer
queued but not yet executed. A source correction and regressions are prepared,
with compilation/runtime validation pending. The running full-provider baseline must finish before
any new engine/test/profile invocation. This report does not turn a source
hypothesis into a reproduced failure or invalidate completed typed comparisons.

## Concrete call chain

The new decimal output buffers use the correct query pool in shared morsel and
fused streaming finalization. On frozen651, two disk partition paths omit it:

1. `SpillableHashAggregateExec::execute` finalizes an ordinary resident/spilled
   partition by calling `hash_agg::aggregate_batches_external`.
2. `SpillableHashAggregateExec::aggregate_partition_chunked` calls the same helper
   after reading each repartitioned spill file.
3. That public helper has no MemoryPool argument and invokes `aggregate_batches`.
4. For supported decimal aggregates, `aggregate_exact_decimals` builds the shared
   AggregationState and calls its compatibility `build_output` method.
5. On651, that method delegates to `build_output_with_pool` with the **process**
   pool, not the spill operator's query pool. The returned decimal arrays retain
   real leases, but in the wrong accounting domain for the query limit.
6. The spill operator collects every partition's output in `all_results`, so
   individually small output partitions do not establish a bounded total result.

The subsequent call-site audit also found the no-spill fallback delegation:
when buffered input stays below the spill threshold, the spill operator creates
`HashAggregateExec`, which previously had no query pool field. Its sequential
and parallel exact decimal finalizers therefore also selected the process pool.
Direct planner constructions, including the delim-state aggregate branch, shared
that omission. Input fitting a threshold does not prove output ownership.

Starting points: [spill finalization](../src/physical/operators/spillable.rs:4136),
[repartitioned finalization](../src/physical/operators/spillable.rs:4429),
[materialized helper](../src/physical/operators/hash_agg.rs:1642),
[exact aggregate output](../src/physical/operators/hash_agg.rs:496), and
[compatibility pool](../src/physical/morsel_agg.rs:2937).

A separate materialized morsel helper passes the process pool and concatenates
its shard batches using Arrow. That concat is a general ownership boundary, but
it must **not** be presented as the demonstrated cause of this decimal gap:
`aggregate_batches_parallel` attempts `aggregate_exact_decimals` first, and the
supported decimal case returns before reaching that morsel/concat branch.
Similarly, `if let Ok` fallback selection can swallow errors on other paths;
reachability and an actual failure must be established before changing them.

## Prepared discriminating probe

`.scratch/decimal-output-repair/probe-fallback-budget.py` targets the unchanged
optimized651binary under4GiB containment, a2GiB process cap, one thread and a1MiB
query budget. It writes small Parquet row groups and checks all returned types and
values against an independent integer/Decimal oracle. No floating conversion is
used. Planned cases are a512-group control and100,000-group ordinary/requested-
fallback variants; each group has two rows and a non-group-key MIN input.

`MIN(DISTINCT u)` requests the ordinary fallback without changing MIN's value.
The probe records the optimized plan's retained DISTINCT marker rather than
assuming the optimizer preserves this route. Source `fused_streaming_eligible`
rejects distinct aggregates, while the exact materialized helper supports
DISTINCT MIN/MAX. Actual plans and logs remain necessary route evidence.

A completed exact100,000-group SUM needs at least1,600,000new coefficient bytes,
which alone exceed the1MiB query budget. Two-row sums differ from the source
values, so borrowing input buffers cannot explain those coefficients. Such an
output would reproduce the boundary escape. A named memory refusal is a different
outcome; a timeout, crash, unrelated error or failed probe is not proof of escape.
The small control distinguishes an unsupported setup from the targeted condition.

## Correction and validation boundary

Pass the owning query pool explicitly into both spill-fallback calls and through
the exact materialized aggregate helper. Keep any public compatibility API clearly
bounded by the process pool, while ensuring production query paths do not silently
select it. Preserve leases on every retained output partition; an output total
that cannot fit must refuse by memory name or use an actually bounded result
contract. Do not merely check each partition against the limit independently.

Add a regression that retains several partition outputs together, verifies the
shared query limit, checks release through clones/slices and checks cleanup on
error/cancellation. Include ordinary and forced-fallback modes and real spill;
retain the exact decimal/NULL/overflow tests. Do not swallow admission errors and
retry an unreserved algorithm. This correction does not by itself account for
all hash state, group keys, filter/concat kernels or collected provider inputs.

Keep this a separately frozen change from651, preserve the completed651screen,
and revalidate the affected semantic/resource routes before new timing claims.

The source correction now introduces a crate-private
`aggregate_batches_external_with_pool` entry point used by both spill callers.
The exact decimal helper passes this pool to `build_output_with_pool`; the
existing public compatibility entry point retains process-pool behavior.
`HashAggregateExec` now also accepts an owning pool through `with_memory_pool`;
all four direct planner construction sites and the spill operator's no-spill
delegate pass their pool. Its sequential and parallel exact-decimal branches
retain that pool. Compatibility constructors still default to the process pool.
Admission errors propagate directly from exact finalization. No fallback retry
or memory limit adjustment was added. This deterministic call-chain correction
was prepared before the runtime probe finishes; the unchanged frozen651 binary
still supplies the before-fix probe, so no before evidence is overwritten.

The new focused regression retains two independently finalized partition outputs
in one child pool, refuses a third despite ample parent capacity, checks exact
decimal values beyond f64 precision and NULL groups, and verifies slice lifetime
and error cleanup with ordinary and DISTINCT MIN inputs. The existing physical
decimal integration test now also asserts query ownership in fused, in-memory
fallback, and actual disk-spill modes. Its ordinary operator oracle runs both
sequential and parallel dispatch by coalescing the same input into fewer batches.
These tests have not yet run; formatting and diff whitespace
checks pass.

Session93502 queues validation in a48GiB scope after session98089 records the
baseline audit and before-fix probe as terminal. It pins hashes of the four
original source/test files plus the physical planner and fails if they change
before testing. Session72500 was deliberately terminated while still waiting
to expand coverage; its original driver/status/log are preserved. No test or
baseline was interrupted. Status and
logs live in `.scratch/aggregate-fallback-pool-repair/`. It owns the heavy slot
after the probe; do not overlap another build or profile. A completed before-fix
probe with nonzero exit still needs inspection to distinguish a reproduced
escape from a probe failure; neither is silently labeled successful coverage.


## Probe setup review

The embedded runner parses the configured memory string directly; the SQL entry
point creates a fresh child pool with that exact byte limit. Query results are
eagerly collected before metrics are captured and before untimed serialization.
Thus output ownership cannot be dismissed as a future external serialization
allocation. The probe now requires4GiB containment and affinityCPU0, sets one
Rayon/runtime thread, and disables core dumps. It requires the small control to
complete; unrelated refusal or setup failure is inconclusive, not a passing
resource-contract result. No engine invocation has occurred for this probe.

At the next idle boundary, run:

```bash
TMPDIR="$PWD/.scratch" PYTHONPATH="$PWD/scripts" SAFE_BUILD_MEM=4G SAFE_BUILD_JOBS=1 scripts/claude-safe-build.sh taskset -c 0 .scratch/venv-lance/bin/python .scratch/decimal-output-repair/probe-fallback-budget.py
```

If named refusal occurs before reaching fallback, retain it and use the existing
physical decimal fixture to isolate the boundary. Do not change thresholds or
remove guards merely to force the desired conclusion.


## Guarded successor queued

Session98089/supervisor3267903 is waiting under4GiB containment onCPU0. Its
budget-after-baseline.py guard requires the original baseline PID/start identity,
all20recorded successful cells, a terminal baseline process and an empty baseline
cgroup. It then reruns the complete independent9680-request audit before invoking
the budget probe. No probe engine is running at this checkpoint.

The successor owns the next heavy-job slot; inspect its status and terminal
result before starting component profiling. A nonzero probe result may be the
intended reproduction of a budget escape, an unrelated execution failure or a
probe error. Keep those outcomes separate. Guard failure must not trigger an
automatic baseline restart or a bypass of containment.

## Executed frozen651 SQL probe: no escape reproduced

The baseline terminated and its full9680-request audit passed. Successor98089
then ran the three prepared SQL cases and exited0 under4GiB containment.
The512-group control returns exact typed values with9280reserved peak bytes.
Both100,000-group cases refuse explicitly in `query 1`: coefficient admission
requests1,600,512bytes against the1,048,576-byte budget. Thus this SQL probe
**does not reproduce** the hypothesized fallback escape. Preserve its successful
refusals; do not weaken admission thresholds to manufacture a failure.

The successful small control's physical plan is MorselAggregate. Error responses
for large cases omit optimized/physical plans, so their false
`distinct_retained_in_optimized_plan` boolean is missing-evidence behavior in the
probe, not proof that the optimizer removed DISTINCT. The source call-chain gap
and pending physical regressions remain separate evidence. The prepared physical
fixtures bypass optimizer routing and explicitly exercise ordinary, no-spill
fallback and real disk-spill finalization. Session93502 has started compilation;
its results are not yet available. Full raw probe records, setups, hashes, logs,
Arrow control output and events live in
`.scratch/decimal-output-repair/fallback-budget-probe-01/`.

## First compiled regression results and corrected fixture expectations

Session93502 compiled the correction and passed the retained-partition ownership
unit test. The ordinary sequential/parallel integration oracle and shared-state
oracle pass. The real-spill fixture initially failed its old success expectation:
query usage130,774bytes plus a513-byte validity allocation exceeds its131,072-byte
limit. This is a named refusal, not a crash or wrong result. Its prior success
expectation did not include the newly charged retained decimal partitions.
The initial source, driver, status and failure log are preserved.

The physical test now retains that exact128KiB query configuration as an explicit
refusal case, asserting actual disk spill, zero leaked query reservations, and
spill-directory cleanup. A separate exact-value case keeps the same128KiB
operator spill configuration but gives its owning query pool1MiB for retained
results. It must still assert actual spill and all exact values/NULL groups.
This separates operator working/spill threshold from query output admission in
the fixture; no production budget or admission threshold changed. Session57498
is running this corrected integration regression. The remaining decimal output
unit suite was not reached by the first stop-on-failure driver and is still due.

## Default-feature validation passes; optimized build in progress

Session57498 passes all3physical integration tests, including sequential/parallel
ordinary execution, fused/no-spill fallback, actual-spill exact values, retained
slice ownership and the original128KiB refusal with reservation/file cleanup.
Session2869 then passes714library tests and64selected integrations. Together
with the3physical tests, this is781unique passes, not counting the ownership
unit test twice. Two existing library ignores remain: flatten_exists and the
IPC test requiring a dedicated QE_IPC_CACHE=auto process. No new ignores or
textual skips were introduced; Lance/GPU feature-specific tests are not included
in this default-feature total. Formatting and diff whitespace checks pass.

Frozen653 contains653member-verified files. Source archive SHA256:
`376718b9a604d89501274190ac85a2c59e0ccca9b328b62e2e826d176de4f56e`;
manifest SHA256:
`882dd9dec0202a79c59ce6fbae5ea93105869e13b0872834c4e1af1d6186a92b`.
Session90056 is building benchmark_embedded and oom_cap_harness with Lance/GPU,
release optimization, one build job and64GiB containment. It owns the heavy-job
slot. Do not run protected follow-up timing or other builds until it terminates.
Optimized semantics, caps, GPU controls and current-source timing remain due.

## Optimized653 validation passes; cap scenarios running

Release90056 completed0 in10m42s. The member-verified frozen653 benchmark binary
SHA256 is `211239eb294025e0651ad3f1c9a3ca699dddda791a59ec465da96f25c0ad567c`;
cap binary is `8d63d73c23ef15f7db41e229d10f4b90aa9e3a8803567ada5ff2c3ccd3e2e656`.
Session36510 exits0:89float/date,10dense-float,56coercion,43timestamp and9identity
queries match their typed references. All58primitive probes match the historical
control;50match DuckDB and8pre-existing integer-division differences remain.
The19canonical literal comparisons and bare-NULL value contract pass; invalid
decimal metadata refuses, and3timestamp metadata cases match. Borrowed input
fits the small pool; literal/two coercion expansions refuse by memory name.
A separate optimized decimal run passes18typed comparisons and2overflow
refusals across raw and IPC paths. Logs and individual results remain in the
candidate scratch directory, including all known mismatches.

Session15749 now runs the250million-row real-spill aggregate cap scenarios in
an8GiB outer scope. No cap result, GPU result, or performance acceptance is
claimed yet. Do not overlap another heavy job while15749 is live.

## Optimized cap and GPU controls pass

Session15749 completes0. Both aggregate cap scenarios return exact counts for
1,000,003groups from250million input rows and record3,855,541,894spill bytes.
The1GiB cgroup case peaks at413MiB RSS; the2GiB RLIMIT case peaks at408MiB.
The outer scope is8GiB. These scenarios validate the existing integer/count
spill workload; they do not replace the separate decimal output budget tests.

Session52893 completes0. The unchanged supported GPU float fixture produces
40correct CPU-control and40correct required-device samples, with no time-gate
failures. All40device records confirm device_executed and one successful device
run; the cache target remains256MiB. This is not canonicalSF10GPU coverage or
hardVRAM admission certification. Evidence is in
`.scratch/public-bench/gpu-resident-aggregate-fallback-pool-01/`.

Session44789 is now running the Lance/GPU-feature library followed by the
isolated ignored IPC test with QE_IPC_CACHE=auto. It owns the heavy slot until
terminal. The protected647/649 timing follow-up and current653 provider
performance screen still remain; no leadership or full query-memory claim is
made by the passing controls.

## Feature and isolated hardware checks pass

Session44789 completes0:776Lance/GPU-feature library tests pass; the dedicated
QE_IPC_CACHE=auto process explicitly runs and passes its previously ignored IPC
regression. Session38304 completes0: all8CUDA regressions run individually with
--exact --ignored in separate capped processes, each reporting1pass/0ignore.
The selected unique union is852passes:776feature-library +64selected integrations
+3physical aggregate tests +1isolated IPC +8CUDA tests. The714default library
passes overlap the feature library and are not added again. Only the pre-existing
flatten_exists library ignore remains unexecuted in this selected coverage.

All live validation scopes are now terminal. Session37654 has started the
predeclared647/649 protected follow-up: four flagged track/query combinations,
50steady pairs in each of four startup/execution orders, matched providers,
typed correctness and fresh time gates. This remains an earlier-candidate
regression investigation; do not attribute its timings to653. It owns the96GiB
heavy-job slot. Current653 timing and full acceptance remain due.

The completed653evidence archive contains588member-verified files at
`docs/benchmarks/2026-09-07-aggregate-fallback-query-pool/`; its SHA256manifest
hash is `9edc1134a6756e3146e9c9cb8cfc0bd0bb29362504eb970dbfad62edf8784bc3`.
The full external GPU sample directory remains separately preserved in scratch.
Protected647/649 follow-up is now terminal; see the separate follow-up report
for the repeated Lance Q17 regression and bounded maintenance diagnostics.
Session56436 now owns the96GiB heavy slot for the previously prepared649/651
aggregate-component diagnostic across all four required CPU tracks plus IPC.
Those measurements isolate the earlier output optimization and are not653
performance acceptance.
