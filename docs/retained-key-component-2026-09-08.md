# Retained key preparation component — September 8, 2026

The optimized component test passed: one explicitly selected test, zero failures
or skips,932 other tests filtered,2.24s execution. Build/run54832 is terminal0.
All168 recorded source inputs match after completion. This adds test-only code
under `physical/morsel_agg/key_rows/preparation_benchmark.rs`; production routing
and aggregation remain unchanged.

The experiment compares two encoding/hash passes with one pass that appends
canonical bytes to admitted KeyRows and hashes to a ReservedVec, followed by
borrowed-key/hash reads. It includes fresh allocation each batch. Both sides
produce matching consumer checksums. This is a performance sanity check, not an
independent SQL or key-semantic oracle.

Each shape has eight alternating paired blocks,32 batches per side, after one
warmup per side, in one process. Inputs combine nullable UTF8 and Int64 columns.
The string width is a formatting minimum, so high-cardinality width1 includes
multi-digit strings. The query pool is16MiB; source Arrow payload is charged,
and all pool leases are released after each shape. No RSS or separate per-side
peak was recorded.

| Rows/batch | Cardinality parameter | Minimum string width | Retained / double encoding | Paired bootstrap95% |
|---:|---:|---:|---:|---:|
|128|6|1|0.5941|0.5511–0.6270|
|8192|6|1|0.5933|0.5923–0.5944|
|8192|8192|1|0.5964|0.5950–0.5978|
|8192|6|128|0.6010|0.5995–0.6026|
|8192|8192|128|0.6020|0.6001–0.6041|

Ratios are geometric means of block ratios. Bootstrap resamples eight paired
blocks10,000 times with a fixed seed; one-process intervals do not capture
between-session variability. The component excludes route-vector construction,
worker dispatch, hash-table equality/lookup, aggregate updates and spill. Its
roughly40% reduction cannot be applied to total query latency. The full eight-
block samples are preserved rather than selecting the best sample.

Command and source hashes are in the evidence archive. The command uses the
required48GiB safe-build wrapper, repository TMPDIR, four Rayon threads, one
build job, `--locked --release --lib` and the explicitly selected ignored test.
No optional GPU/Lance feature was requested for this library component.

Next implement a private prepared-key owner carrying exact layout identity,
canonical bytes, row extents and cached hashes. Let routing and state updates
borrow that owner through scoped worker completion and spill retries. Keep full
byte equality and exact first-unapplied-row semantics. Admission must occur before
allocation; preserve the ordinary route when preparation cannot fit before any
state mutation. Validate dictionary equivalence, NULL/float/decimal keys, duplicates,
empty input, forced spill and failure cleanup before measuring complete queries.

Immutable evidence: `docs/benchmarks/2026-09-08-retained-key-component/`.
No production speedup, provider completion or DuckDB leadership is claimed.
