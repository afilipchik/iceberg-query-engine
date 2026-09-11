# Encoded input and lifecycle source review — September 8, 2026

This follow-up uses the local source trees authorized by the user. DuckDB checkout
`1c27c54f27bea397d18d86f7c0d6a9f1d4aa85f8` and ClickHouse checkout
`a1b25f3f4beb3ba49aa3b73671cc244185331b86` are snapshots, not claims about latest
upstream. The DuckDB checkout is also distinct from the benchmark's 1.4.4 wheel.
No third-party code was copied into the engine.

## Findings

DuckDB's `extension/parquet/decoder/dictionary_decoder.cpp` initializes a reusable
dictionary, evaluates eligible filters over its values, then checks dictionary
IDs against the resulting lookup. Its ordinary read can return a dictionary
vector plus selection instead of copying strings per row. It validates dictionary
ID domains and explicitly limits filter eligibility according to NULL semantics.
See `InitializeDictionary`, `Read`, `DictionarySupportsFilter`, `CanFilter` and
`Filter` in the local checkout:
`/media/afilipchik/nvme6tb/src/duckdb/duckdb/`.

Our admitted `DictionaryUtf8Decoder::next` expands selected page prefixes into
new UTF8 values and offsets. `streaming_parquet_scan/admitted.rs::Reader::next`
then applies compiled static predicates and admitted gathers. This is bounded
and exact, but a low-cardinality string can be copied and compared once per row
before many rows are discarded. The legacy Arrow path already has some
dictionary-preserving filters; it is not evidence that the new admitted path has
the same capability. This is a source-level cost hypothesis, not a measured
fraction of query time or a demonstrated explanation for every slow query.

ClickHouse's `src/Processors/ResizeProcessor.cpp` schedules input according to
output demand. `src/Processors/Executors/PullingAsyncPipelineExecutor.cpp::cancel`
requests cancellation and joins its execution thread. These support two separate
contracts: bounded demand controls production; cancellation completion controls
when remaining work and memory ownership have actually ended. Our shared queue
previously conflated cancellation request with completion on error. The new
deterministic regression reproduces that race independently of any benchmark.
The current queue fix reaps producers before returning the first error. Async
stream drop continues to retain owners in the cancelled tasks until polls stop.

## Conditional next work

First measure the frozen admitted parallel-queue candidate. Confirm its actual
queue route and slot count in traces and retain the strict DuckDB ceiling.
If decode/filter remains a material bottleneck, profile an isolated scan/filter
component with exact output and admitted resource accounting before changing it.

A general encoded-filter implementation should:

1. Retain admitted dictionary values, ID buffers and NULL state with explicit
   page/row-group ownership; never compare raw IDs across different dictionaries.
2. Evaluate only supported deterministic single-column predicates over dictionary
   values, preserving SQL three-valued logic. Arbitrary expressions, multiple
   columns and predicates that keep NULL need explicit semantics or a correct
   fallback selected before consumption.
3. Produce an admitted selection and delay string expansion until survivors need
   flat output. Bound selection, lookup, scratch, dictionary and output storage
   together; a dictionary can be large even when an output batch is small.
4. Keep exact row alignment across columns, filtered-empty prefixes, repeated
   projections, dictionary/page transitions and V1/V2 definition levels. Errors
   cannot trigger decoder replay or loss of already-consumed rows.
5. Test duplicate dictionary values, NULL dictionary entries/row NULLs, empty and
   all-filtered output, invalid IDs, changed dictionaries, page boundaries, long
   UTF8, byte-budget denial, cancellation and retained output clones. Use an
   independent typed oracle for both admitted and legacy paths.
6. Measure dictionary cardinality and selectivity sweeps, long/short strings,
   plain/dictionary encoding, fragmented row groups and budget/concurrency cases.
   Then rerun protected SQL and provider gates. Do not tune to a Q12 literal or
   infer a workload win from a decoder microbenchmark.

This is a bounded follow-up option for the existing epic, not an instruction to
replace the engine with a copied architecture or to defer current validation.
