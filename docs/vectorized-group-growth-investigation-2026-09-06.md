# Vectorized group-table growth: source finding

This is a source-level performance hypothesis, not a reproduced timing result.
`VectorizedGroupTable::maybe_resize` in `physical/operators/hash_agg.rs` walks all
existing groups when the bucket table exceeds 75% load. For each group's stored
(batch,row) reference, it calls `vectorized_hash::hash_arrays(key_arrays, row+1)`
and keeps just that last hash. Each resize therefore repeatedly hashes prefixes
of each batch rather than one key per group. Large batches and high group counts
can amplify work far beyond the number of groups; the initial bucket count is
fixed at 1,024. The vectorized path is also eligible for GROUP BY without any
aggregate expressions (its supported-aggregate check is vacuously true).

This is distinct from the repaired NULL/NaN equivalence bug and the float Boolean
builder regression. The latter benchmarks do not prove this growth path was hot.
Do not replace the current benchmark acceptance with a successful microbenchmark.

Next bounded experiment: independently verify `COUNT(*)` over `GROUP BY k` for
multiple cardinalities and Arrow batch sizes while keeping rows and key distribution
fixed. Preserve the full parse-to-consumed boundary, warmup and every sample, and
use fresh DuckDB references with the existing 10x ceiling. Capture resize count,
keys hashed and allocation bytes with opt-in generic tracing if needed. Compare
input rows hashed with group count before choosing a repair.

Potential repair choices are a shared single-row hash function (no per-group
retained allocation) or stored group hashes (8 bytes per group, requiring explicit
admission/accounting and resize scratch accounting). Both must keep equality and
hash contracts identical across insertion, resize, join/spill partitioning and
NULL/zero/NaN domains. Do not add an unaccounted cache solely to improve this path.
No source change is made for this finding yet.


## Current routing check

Read-only review confirms ordinary configured lowering selects morsel/spillable
aggregation; plain HashAggregate is the config-less fallback. Delimiter-state
aggregate lowering still constructs plain HashAggregate directly and merits a
separate resource-policy audit. VectorizedGroupTable is therefore not established
as a dominant path for the ordinary provider benchmark. Large plain grouped inputs
also prefer morsel-parallel aggregation above100000 rows before vector fallback.

A small frozen628 probe (4096 input rows,2048 keys, two outer thresholds) compared
ordinary GROUP BY/count and a correlated nested GROUP BY/count at64KiB and1MiB.
All four requests matched DuckDB. Ordinary physical plans use SpillableHashAggregate;
the tested correlated scalar remains inside Project and did not expose a DelimJoin
or vectorized-growth trace. This does not prove the delimiter-specific branch is
unreachable, nor certify its budget. Evidence:
`.scratch/try-cast-memory-repair/delim-aggregate-probe/`. It ran in a10GiB scope
with8GiB process protection during compilation, not as a latency measurement.
Do not optimize the resize loop as the supposed main bottleneck on this evidence.
