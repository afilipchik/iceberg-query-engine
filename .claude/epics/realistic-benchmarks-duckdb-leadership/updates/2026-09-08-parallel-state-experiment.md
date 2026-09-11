# Next implementation: bounded parallel grouped state processing

Measured prerequisite: canonical SF10 Q13's inner ingestion is4.4 seconds at
1,4 and16 threads; total latency is10.693/7.897/7.340 seconds. All18 diagnostic
outputs pass the oracle. Hash-only substitution estimates roughly40ms saving
over its15.35 million rows and is not selected. No engine change is made by this
update. [Measurements](../../../../docs/aggregate-thread-scaling-2026-09-08.md).

Implement the following sequentially within the existing aggregate task. Do not
declare the parent complete from component tests or this one-query diagnostic.

1. Add a validated row-selection input to `GroupRows` and `IngestionController`.
   The cursor indexes positions in the retained selection, not physical row
   numbers. Every selected row commits once; admission denial retains the exact
   first unapplied selection position. Validate all indices before state changes.
   Retain existing contiguous input behavior through the same cursor contract.
   Tests must include sparse/reordered indices, duplicate indices, NULLs, empty
   selections, invalid bounds and mid-selection spill without input re-evaluation.
2. Add a query-owned router for evaluated batches in `live_spill.rs`. Compute
   worker ownership from complete canonical key bytes, never NDV/range estimates
   or provider dictionary codes. Begin with2/4 workers and a one-batch frontier;
   expose counts only for the experiment until selection is measured. Charge
   selection buffers, old-plus-new growth, task/channel metadata and shared batch
   ownership before allocation/publication. Release a batch only after every
   consumer finishes. Equal NULL/NaN/zero/dictionary logical keys must route alike.
3. Give each worker its own existing spillable controller under the same query
   parent pool. Divide advisory working group limits by active worker count;
   advisory limits never replace actual reservations. Pre-admit worker/writer
   startup before opening input. If the worker set cannot be created, choose the
   existing one-controller route before consuming input. Never replay input after
   a worker error or pressure failure. Join/drain all owned work on error and
   cancellation. Do not introduce detached Rayon jobs with unbounded lifetimes.
4. Finish each disjoint worker through its existing partial-state merge, then
   concatenate admitted complete-group outputs. Do not merge finalized AVG,
   variance or overflow states across workers. Exact key ownership is the proof
   that no group appears in two outputs. All-NULL and empty grouped outputs,
   dictionary codebook changes, repeated batches and all declared input
   partitions remain covered. Global/unsupported paths retain their existing
   correct pre-input routing.
5. Run semantic, actual-spill, cancellation and shared-pool refusal tests before
   release measurement. Include high-cardinality and low-cardinality inputs,
   skew, one hot key, variable/nested keys, encodings, decimal and floating states.
   The256KiB decimal spill regression and consuming-source tests must still pass.
   Instrument routing, worker ingestion, merge, wait and retained-memory peaks
   separately; never sum concurrent wall intervals as exclusive CPU.
6. Freeze current control and candidate. First compare canonical Q13 and generated
   grouped workloads with balanced orders and independent typed outputs. Reject
   the candidate if routing/queues/merge erase the gain or resource guarantees
   regress. If promising, run unchanged full canonical SF10 and protected cases
   across required providers, then the original broader workload/resource gates.
   Existing deadline failures remain failures; no query-name dispatch or budget
   relaxation. GPU and its valid same-binary CPU control remain separate work.

The router may itself become the bottleneck. If measured, investigate reuse of
canonical key work or batch typed hashing while preserving exact key identity;
do not pre-emptively add unsafe encoding or infer a proven physical partitioning
property from statistics. The original leadership contract is unchanged.
