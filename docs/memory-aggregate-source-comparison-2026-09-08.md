# Generic memory aggregation: measured next target

The frozen3ff CPU-control diagnostic rules out missing parallel input as the
explanation for this small floating-point Q1 failure. With `QE_AGG_PROF=1`, four
threads,4GiB query/8GiB process and a16GiB wrapper, the frontier reports four
partitions and four slots. Its copied-input bound is2,805,768bytes per slot and
reserved envelope11,223,072bytes. Four aggregate workers process74 evaluated
slices in parallel, with no serial slices or spill. This is a diagnostic custom
fixture, not canonical SF10 or a performance comparison with profiling disabled.

Q1 warmup completed in231.329321ms and exceeded the141.66072010993958ms gate.
All three dependent measured engine samples remain not_run. Q6's three measured
pairs passed. Profiling reports571,588 input rows in10 provider batches,
4.683ms expression evaluation,77.890ms routing and138.297ms worker processing
wall time. Ingestion is216.293ms; routing/processing are its components, not
additional independent totals. These spans are not exclusive CPU measurements.
No independent typed claim is made for the late Q1 warmup.

## Concrete source comparison

Our `physical/morsel_agg/row_router.rs::route` serially encodes every evaluated
group key, hashes it and appends its row index to a canonical owner's reserved
selection. `parallel_controllers.rs::ingest` then dispatches those selections.
`group_rows.rs` encodes each selected key again during group lookup/update.
This duplicated key preparation is a source fact. Its isolated cost and the
benefit of retaining prepared keys have not yet been measured. Current ownership
is valuable: equal keys belong to one worker, including NULL/NaN/zero and
dictionary-codebook equivalence, so final output needs no cross-worker state merge.

Local DuckDB snapshot `1c27c54f27bea397d18d86f7c0d6a9f1d4aa85f8`:

- `src/execution/physical_plan/plan_aggregate.cpp::ExtractAggregateExpressions`
  separates argument evaluation into a projection with bound references.
- `src/execution/operator/aggregate/physical_hash_aggregate.cpp::Sink` references
  the incoming vectors into aggregate payload and passes local/global sink states
  to its radix table. This illustrates a vector payload and explicit state
  ownership boundary; it does not prove DuckDB never rehashes or copies keys.

Local ClickHouse snapshot `a1b25f3f4beb3ba49aa3b73671cc244185331b86`:

- `src/Processors/Transforms/AggregatingTransform.cpp::consume` detaches the
  chunk's columns and passes them to `executeOnBlock` with persistent aggregation
  variants and column scratch. Its separate merge processors parallelize bucket
  or fixed-map merging. Such local-state/merge execution is an architectural
  alternative with its own memory and exact-state merge costs.

These are pinned local source observations, not current-upstream claims, and the
DuckDB checkout is not the benchmark wheel. No third-party implementation copied.

## Bounded next implementation

First measure a reusable prepared-key batch against the existing router/update
sequence, preserving canonical owners. Admit key bytes, offsets, hashes and
selection metadata before allocation; retain them through worker completion and
spill transitions. Reuse preparation for routing and exact equality. Never use
hash equality as key equality, dictionary IDs as logical keys, or re-evaluate
volatile expressions. Prefer a representation usable by ordinary and specialized
aggregates rather than a Q1-shaped path.

Before adopting it, test mixed-width keys, distinct dictionary codebooks, NULLs,
NaNs/signed zero, decimals, duplicate keys, empty batches and forced spill/refusal.
Compare selected-row ordering and exact partial states with independent oracles;
assert cleanup after failures. Benchmark cardinality and key-width sweeps and
protected canonical queries with balanced fresh processes. Extra retained keys
can worsen low-memory completion; reject a speedup that breaks those gates.
Only consider changing to worker-local states plus a final merge after measuring
the smaller change and validating exact SUM/AVG/distinct merge contracts.

Evidence: `docs/benchmarks/2026-09-08-memory-input-profile/`. Diagnostic process
25084 exited1 because the Q1 gate failed. No engine source changed, and no overall
acceptance or DuckDB leadership is claimed.

Follow-up source inspection: `state_rows.rs::prepare_arrays_indexed` validates
each argument's logical type and extent for every input row, then resolves each
fixed input into a scalar before updating transactional scratch. The enclosing
group ingestion already validates the arrays at batch entry. A future bound
batch representation could move stable type dispatch out of this row loop while
preserving per-cell NULL/dictionary checks and transactional rollback. This is a
source hypothesis, not a measured attribution or authorization to remove checks
without an equivalent lifetime-bound validation contract. Keep it separate from
the active retained-key component experiment so the first measurement remains
interpretable.
