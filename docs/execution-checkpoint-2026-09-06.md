# Historical execution checkpoint — 2026-09-06 UTC

This is historical measurement evidence. The current status is maintained in
[the existing epic](../.claude/epics/realistic-benchmarks-duckdb-leadership/execution-status.md)
and [AGENTS.md](../AGENTS.md); the scores below do not certify newer binaries.

The systemic IPC repair passes its focused failure reproduction, complete
provider measurement, raw regression, public-development and selected cap checks.
The engine still does not satisfy the overall DuckDB leadership or query-wide
memory contract. Current source is preserved separately from historical scores.

## What changed and why

A full-scale IPC query previously joined customers and suppliers on nation before
selective relationships, then attempted to collect roughly a billion candidate
pairs. Missing MemoryTable costing statistics and unbounded join output were
separate defects. Cached statistics now improve the plan; pull-driven inner joins
bound output chunks and yield during unmatched work. Dropping a stream stops
upstream probing. Exact key equality, NULL semantics and filters are retained.

Eager aggregation no longer uses estimated null counts or name-matched ranges as
semantic proofs, and floating multiplication stays before SUM. The cap harness
now rejects wrong results and unrelated errors instead of classifying every
QueryError as a successful memory refusal.

## Current measurements

Each SF10 row below covers 22 queries × 10 pairs × 3 independent sessions against
pinned DuckDB 1.4.4, with 16 threads, 40 GiB query and 48 GiB process limits.

| Mode | Engine / DuckDB suite | Outcome |
|---|---:|---|
| Raw Parquet CPU control | 2.547248× | 660 pairs pass; slower overall |
| Decoded IPC | 0.677528× | 660 pairs pass; 12/22 query wins |
| Iceberg | 0.348763× | 660 pairs pass; 21/22 query wins |
| Lance | 1.453202× | 660 pairs pass; slower overall |
| Native | — | Q1/Q6/Q15 time-gate failures; 622 completed answers correct |
| GPU routing | 2.556894× | 660 pairs pass, **zero device-executed samples** |

GPU CPU control is the identical ordinary raw execution path. Iceberg's warm
persistent-provider boundary is qualified in its separate audit. Lance uses the
recorded reference extension optimizer workaround for a decimal AVG defect.
These distinctions prevent declaring a cross-provider win from selected modes.

A same-time alternating screen improves Q9's median 12.9% with identical optimized
and physical plans; eight of ten pairs favor the candidate. Execution time
accounts for most of the difference. Four controls stay within 1.6%. The complete
raw gate has no query median over 10% slower than the immediately prior candidate.
An older Q9 regression against an earlier decimal-only snapshot remains open.

Public development gates pass all 43 ClickBench queries (129 pairs) and 113 JOB queries
(339 pairs), at 2.205× and 1.560× suite time. Full public workloads remain due.
The selected Rust gate passes 765 tests with one pre-existing ignored test, followed
by seven focused join-stream tests for the final cancellation change and two
cap-classifier tests. The harness passes 83 tests, zero skips. Four release cap
scenarios complete with actual spill and independent expected counts.

## Remaining fundamental problems

1. **Ordinary decimal expressions miss optimized execution paths.** Native and IPC
   Q6 warmups have the same physical plan and similar execution time near 300 ms,
   while native DuckDB calibrates near 23 ms. Decimal predicates decline the compiled
   path, scalar literals expand to full-length arrays, filtering gathers columns,
   and exact global aggregation collects input then processes batches serially.
   The completed CPU diagnostic shows about fifteen cores active and only about
   11 ms cumulative aggregate processing, making predicate/filter work the next
   target. Individual allocation and kernel costs remain unmeasured.
2. **Optimizer rules are not idempotent together.** OR derivation adds predicates
   that pushdown moves into scans; the next round adds them again. Q7 contains ten
   copies per alias. Per-rule change flags also prevent detecting a stable complete
   round. Ordinary binding time does not explain the native decimal execution gap.
3. **Memory safety remains incomplete.** Bounded join output is progress, but
   build/hash metadata, incoming batch bytes, wide gathers, queues, cached input
   and retained results do not yet all own query-wide reservations. Passing a
   process cap does not prove adherence to the smaller query budget.
4. **Format and capability differences matter.** Native and raw CPU remain slow;
   Lance remains slower under its qualified reference configuration. Canonical
   decimals do not use the GPU. Warm provider wins do not certify cold/open latency,
   object storage, delete/evolution semantics, concurrency or larger scale.

## Next bounded implementation sequence

1. Completed original-SQL decimal CPU/thread diagnostics under unchanged caps.
   Sixteen-thread IPC completes at 311.7–314.3 ms with 4.7 CPU seconds per request;
   three other cases time out. No major faults observed. Preserve these limits: no
   complete thread speedup or accepted suite score follows. Fix the independently
   identified Boolean NULL truth-table contract before performance changes.
2. If confirmed material, remove full-column scalar broadcasts using a general
   typed scalar/array comparison contract. Preserve casts, NULLs, dictionaries,
   decimal scale/precision, empty batches and selected CASE evaluation. Compare
   against independent typed expectations; do not specialize on query names.
3. Implement bounded, parallel exact decimal filter/reduction over provider-neutral
   batch streams, with owned partial state and checked merge/overflow. Remove
   collect-before-aggregate behavior without replacing it with unbounded queues.
4. Fix predicate idempotence with stable structural deduplication and whole-round
   convergence. Preserve residual OR conditions and outer-join barriers; verify
   whole-pipeline idempotence and typed multi-batch/NULL/duplicate results.
5. Continue owned memory through source/cache, joins, queues and final results,
   including cancellation and failure cleanup. Adopt each shared change only after
   focused semantic/resource checks, an alternating screen, protected regressions
   and affected-provider gates. Keep full public/scale/cold/layout/concurrency
   work in the existing epic; do not redefine success around faster modes.

## Evidence and implementation entry points

- [Complete candidate evidence](benchmarks/2026-09-06-ipc-systemic-repair/README.md)
- [Native decimal investigation](native-decimal-pipeline-investigation-2026-09-06.md)
- [Optimizer rule interaction](optimizer-overhead-investigation-2026-09-06.md)
- [Memory ownership design](next-memory-ownership-2026-09-06.md)
- [Iceberg timing boundary](iceberg-benchmark-boundary-investigation-2026-09-06.md)
- [Existing execution epic](../.claude/epics/realistic-benchmarks-duckdb-leadership/epic.md)


## Subsequent Boolean/scalar candidate

SQL Boolean NULL logic is corrected, and comparison constants retain scalar
storage through casts/coercion. Final tests: 557 library passed (one pre-existing
ignored), 13 Boolean/scalar regressions passed; 23 existing cast/membership/numeric
regressions passed before the final dictionary-scalar guard. Full native now
passes all 660 pairs, including previous Q1/Q6/Q15 timing failures, but remains
3.358422× DuckDB by suite time. Both alternating raw/IPC screens pass all 220
executions: Q6 improves 52.6%/75.3%, Q14 improves 26.6%/64.0%, with identical plans.
IPC Q18 is 2.6% slower. Other full provider gates are still running; the main table
above remains the previous candidate's results.

See [new contracts](boolean-and-scalar-contracts-2026-09-06.md) and
[preserved candidate evidence](benchmarks/2026-09-06-scalar-comparison/README.md).

## Scalar candidate completed validation checkpoint

Frozen binary `0d5cf9ec2f387a004bc88736a357ad214cb579b873bf8011a156a7e73aff720c`
passes all six canonical modes (3,960 pairs): native 3.358422×, decoded IPC
0.524594×, Iceberg 0.315968×, Lance 1.259869×, raw CPU 2.257953× and GPU routing
2.254746× DuckDB. Canonical GPU routing uses CPU for every measured sample; the
separate supported-float fixture passes 40/40 device samples and a fresh CPU
control. Public extracts pass all 129 ClickBench and 339 JOB pairs. The original
Q6 diagnostic passes all four configurations; IPC-16 median CPU work falls
4,730→1,020 ms and elapsed 313.683→76.473 ms.

The candidate's 88 matched IPC/Iceberg/Lance/raw query medians have no >10%
regression against the immediate frozen predecessor. Previous native failure
cases now complete, but native remains slower. No full query-memory, concurrency
or cross-workload leadership claim follows. See the scalar contract document
and evidence directory for exact provenance, qualifications and raw artifacts.
The next production change is the separately prepared queue-ownership patch;
its scores must remain separate from this frozen binary.
