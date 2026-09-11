# Live aggregate attribution checkpoint

Added opt-in `QE_AGG_PROF` wall intervals to `morsel_agg/live_spill.rs` for
evaluated expressions, state ingestion, controller finish and output construction.
The diagnostic also reports input/output rows and batches and ingestion spill
bytes. Finish includes output; upstream work can overlap. These are not additive
exclusive CPU measurements. No output chunking or parallelism policy changed.

Formatting and diff whitespace checks pass. The initial release build failed on
the diagnostic's use of a nonexistent ReservedVec length accessor; corrected to
`as_slice().len()`. Preserve `release-build-final.log` for that compile failure.
The replacement build was confirmed running in tool session14035, logging to
`.scratch/live-schema-boundary/release-build-profile.log`. Its terminal result
must be checked before any dependent execution or another heavy build.

Prepared four Q13 diagnostic requests in
`.scratch/live-schema-boundary/q13-profile-requests.jsonl` using unchanged
canonical SF1 SQL. Once the release build succeeds, execute those requests with
the existing `.scratch/public-bench/live-spill-sf1-01/setup.json`,
`QE_AGG_PROF=1`, QE_MEM_CAP=12G and the memory-capped wrapper, preserving JSON
stdout and stderr separately. Compare outputs with the preserved typed oracle.
Use an explicit diagnostic watchdog, not the normal latency gate, to obtain
attribution for the slow query. Diagnostic timings cannot be used as benchmark
acceptance samples. Then run the uninstrumented canonical suite through the
versioned harness with its unchanged matched DuckDB time ceilings.

The previous frozen control remains
`.scratch/live-spill-integration/benchmark_embedded`. The six repaired dictionary
queries have debug typed validation, but current release correctness/performance,
Q13 attribution, fresh SF10 and provider/resource gates are still open.
