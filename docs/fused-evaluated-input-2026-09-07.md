# Fused aggregation: preserve evaluated input through routing

The disjoint fused path evaluated grouping expressions twice: once to route a
row to a worker, then again to update that worker's aggregate state. A volatile
expression could produce different keys, splitting one SQL group across workers.
Disjoint finalization then emitted duplicate groups without merging them.

`fused_aggregate_evaluated_keys` reproduces this with16 possible keys from
`CAST(FLOOR(RANDOM() * 16) AS BIGINT)` and16,384 input rows. Its oracle assumes
no random sequence or exact per-group counts: every output key must be unique,
within the expression's domain, and all counts must sum to16,384. On the preserved
pre-change source, the shared control passes and disjoint routing fails with
duplicate output groups. This is a reproduced semantic failure, not a timing
or statistical test. A first fixture compilation used an incorrect Rust helper
name; that log is retained separately from the red test result.

## Implementation

`physical/morsel_agg.rs::prepare_aggregate_batch` evaluates and normalizes the
group and aggregate inputs into a positional internal RecordBatch: group columns
followed by aggregate columns. The internal names do not participate in binding
the original SQL expressions; those evaluate against the original input schema.

`AggregationState::process_evaluated_batch` consumes those arrays without access
to the original expressions. Ordinary `process_batch` uses the same preparation
and update phases. The update method checks column arity before changing state.
RecordBatch slices retain the evaluated values and Arrow backing owners.

Disjoint fused producers prepare input before routing, use the normalized group
columns for aggregate-specific hash partitioning, and coalesce the resulting
internal batches. Workers apply them without evaluating expressions again.
Shared-channel workers still evaluate their own incoming batches. Existing
producer/worker failure supervision and cleanup remain in place.

This changes work placement: aggregate expression evaluation in the disjoint
path now runs in the producer before routing. It can reduce repeated evaluation
and routed payload, but also changes scheduling, metadata construction and the
size/lifetime of queued computed arrays. No optimized performance improvement is
claimed. Full ownership of expression scratch, copied routing/coalescing buffers,
queue headroom and metadata remains part of the open query-budget work.

This is the evaluated-input portion of the single-pass spill design. It is
**not** a partial-state codec, an admission cursor, or a repaired spill transition.
An update failure can still follow a committed row prefix; retrying that batch
would be incorrect. Group-budget fallback still re-executes the original input.

## Validation and evidence

All jobs are terminal. Tests used Lance/GPU features, Rayon4, one build job,
the48GiB memory wrapper and repository `.scratch` as TMPDIR. No CUDA execution
or optimized benchmark was performed in this step.

| Scope | Result |
|---|---|
| New routing fixtures, before |1 pass /1 fail, exit101 |
| Six selected integration targets, after |23 pass, exit0 |
| `physical::morsel_agg::` library tests |27 pass, exit0 |
| Real spill directory ownership tests |4 pass, exit0 |
| Existing consuming-source budget transitions |0 pass /2 fail, exit101 |

The54 selected passing tests have no ignores. They include input/stream/task
errors and cancellation, aggregate encoding and exact decimal merge, admitted
arena growth, NULL/dense domains and real spill cleanup. The new slice test
drops the original batch and prepared container, applies retained slices across
duplicate and NULL group boundaries, and checks exact decimal coefficients,
counts and the bitwise minimum of the originally evaluated random draws.

The unchanged budget-transition fixtures still report each source partition
executed twice. Their8KiB operator/32MiB query budgets were not increased. The
separate256KiB query-budget failure remains open; it was not rerun in this step.
These results do not replace full SQL/resource or parent benchmark gates.

Commands (each invocation uses the prefix
`TMPDIR="$PWD/.scratch" RAYON_NUM_THREADS=4 SAFE_BUILD_MEM=48G SAFE_BUILD_JOBS=1 scripts/claude-safe-build.sh`):

```text
cargo test --locked --features lance,gpu --test fused_aggregate_evaluated_keys
cargo test --locked --features lance,gpu --test fused_aggregate_evaluated_keys --test fused_aggregate_input_errors --test aggregate_encoding_contract --test aggregate_arena_admission --test decimal_root_reuse_aggregate_contract --test dense_aggregate_null_domain
cargo test --locked --features lance,gpu --lib physical::morsel_agg::
cargo test --locked --features lance,gpu --lib agg_sort_directory_ownership_tests
cargo test --locked --features lance,gpu --test fused_aggregate_budget_transition
```

`cargo fmt --all -- --check` passes. No commit or dependency change was made.
The production delta from the prior lifecycle source is confined to
`physical/morsel_agg.rs` and `physical/operators/spillable.rs`.
The [evidence archive](benchmarks/2026-09-07-fused-prepared-input/) preserves
before/after source, the incremental patch, fixtures, logs, status and636 input
hashes. Its16 members were verified against `SHA256.json`, whose hash is
`169d6be8e46649a604592e381d1dd146c1c64117c1e64868611ae260d36ece2c`.
Frozen662 performance results do not measure this change.

Next, use retained evaluated arrays with a pre-admitted applied-row cursor and
bounded partial-state flush/merge. Preserve internal AVG sum/count, decimal
coefficient/scale/overflow and NULL/seen semantics. Do not substitute replay,
increased fixture budgets or collection of all historical input. The existing
[implementation contract](../.claude/epics/realistic-benchmarks-duckdb-leadership/updates/2026-09-07-fused-spill-implementation.md)
and full parent acceptance scope remain active.
