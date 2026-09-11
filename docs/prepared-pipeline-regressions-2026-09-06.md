# Prepared pipeline: measured recovery and remaining regressions

The two integrated repairs restore preparation through Filter/column Project and
remove unused emitted roots from an already selected streaming scan. They pass
704 selected tests (one pre-existing ignored), release compilation and six
actual-spill cap cases. They are not sufficient for performance acceptance.
The frozen engine SHA256 is
`65becb409a176c3358bdf9fd0d15dd00817c6fedb5fc19b0ce73143820b9db66`.
All results below belong to that exact binary, not the unintegrated decimal draft.

## Paired screening evidence

Every query uses original canonical SF10 SQL, a complete typed oracle and three
fresh validated DuckDB calibrations. There is one gated warmup and three steady
requests per engine, against accepted scalar control `0d5cf9ec...`. Settings remain
16 threads, affinity 0–15, 40 GiB query / 48 GiB process / 96 GiB enclosing scope.
Every engine request must meet the exact ten-times-DuckDB ceiling. These are
single-session screens, not three-session certification or confidence intervals.

| Completed screen | Valid requests | Suite / scalar control | Suite / fresh DuckDB |
|---|---:|---:|---:|
| Raw Parquet | 176/176 | 1.269 | 2.912 |
| Decoded IPC | 176/176 | 1.141 | 0.575 |
| Native | 176/176 | 1.146 | 3.836 |
| Iceberg | 176/176 | 1.266 | 0.380 |
| Lance | 176/176 | 1.071 | 1.440 |

All five CPU screens completed (880/880 requests pass); performance acceptance
fails. GPU was not rerun on this candidate. Storage/reference
conditions are retained in each provider manifest; cross-mode ratios do not
isolate one operator or establish residency equivalence.

The initial separate Q19 screens pass all 16 requests. Median raw Q19 improves
444.7 → 324.6 ms (27%); IPC improves 549.6 → 446.9 ms (19%). All-query coverage
still finds protected regressions. Raw Q12 is 4.787×, Q21 3.153×, Q16 1.573×,
Q10 1.564× and Q18 1.121× scalar control. IPC and native also regress on
Q10, Q16 and Q21. Passing the ten-times ceiling does not clear these regressions.
The earlier full c4 matrix independently recorded many of the same regressions;
new measurements must not be merged with it as one candidate.

## Distinct remaining execution boundaries

These are source/plan hypotheses, not isolated causal measurements:

- **Nested joins:** Q10 and Q21 have nested join trees. Inner preparation currently
  requires a static probe gather bound; it does not recursively consume a
  prepared child. Preserve exact per-physical-variant gather metadata where it
  can be derived from actual build/probe data, then audit recursive preparation
  before changing admission. Initializing children twice is forbidden.
- **Uncorrelated subqueries in filters:** Q16 retains a `NOT IN (subquery)` Filter.
  Filter deliberately declines preparation for any subquery because future
  evaluation can initialize work using the query pool. Merely removing that
  guard is invalid. A separate design would need an immutable, fully initialized
  result and preserved NULL/error semantics, lifetime and budget ownership.
- **Variable-width output:** Q12 requires shipping mode and order priority
  strings. Projection cannot remove semantic outputs. Row counts and sampled
  lengths cannot prove repeated-gather bytes. Runtime byte-limited emission is
  a possible broader contract, but must price gathers before allocation and
  account for decoded tails; scanner-only postchecks do not solve this.
- **CPU arithmetic:** Q1's earlier diagnostic and source review establish repeated
  decimal aggregate roots. Batch-local exact reuse is a separate provider-neutral
  optimization. The scratch candidate is uncompiled and must not be presented
  as a measured improvement or as a fix for the join regressions.

## Next implementation order

1. The five-mode screen is complete; preserve and verify its full archive. Keep
   every protected regression; no new full acceptance matrix while known
   regressions remain unresolved.
2. Audit and implement recursive preparation only for already provably bounded
   Inner pipelines, with exact physical metadata, same-stream fallback and
   deterministic initialization/cancellation tests. Leave Semi/Anti finalization
   and spilled children declined unless their separate contracts are established.
3. Design initialization and ownership for immutable uncorrelated predicate
   results. Do not replace NOT IN with an ordinary anti join without full NULL
   semantics or infer non-NULL facts from samples.
4. Implement the narrowly scoped decimal reuse experiment after independent
   correctness and spill tests. Rebuild/freeze and run paired all-query screens;
   retain changes only with justified performance and unchanged semantics.
5. Treat variable-width producer admission as a resource-contract project:
   pre-gather byte planning, bounded cursors, admitted decoded data, cooperative
   cancellation and explicit oversized-value outcomes. Do not add arbitrary
   string length restrictions to make these benchmark queries parallel.
6. Finish GPU partition/hard-admission work and the complete provider, full public
   workload, cap, concurrency and leadership gates from the existing epic.

Evidence: `docs/benchmarks/2026-09-06-prepared-pipeline/`. The paired screening
runner is `.scratch/run_prepared_pipeline_screens.py`, terminal session 62780, exit 0 for correctness/time only.
Designs: `runtime-variable-output-quantum-design-2026-09-06.md` and
`decimal-aggregate-root-reuse-design-2026-09-06.md`. No source or dependency
changes were made while this frozen binary was being screened.

## Frozen585 source review while awaiting fresh measurements

The [next shared-boundary profile](next-shared-cpu-boundary-profile-2026-09-06.md) corrects one attribution risk: Q21 Semi/Anti operators declare one output partition, so their consumers bypass the multi-partition preparation queue; preparation decline alone cannot explain serialization there. Deeper build queues, probe collection, runtime-filter ordering and actual kernel CPU need measurement. Q12 still has a source-proven variable-width leaf/gather capability barrier. Historical timings belong only to the old65bec binary; neither finding attributes those timings to source585.
