# Aggregate ingestion: exact applied-row progress

The common morsel update path now reports the first unapplied row when admitted
state growth fails. It can resume the same evaluated batch without reapplying
its prefix. This supplies the ingestion contract needed by the pending bounded
state-spill controller; it does not yet make the fused operator spill without
re-executing input.

## Contract and implementation

`AggregationState::process_evaluated_from(batch, group_count, start_row)` returns
the batch end on success, or `IngestionFailure { next_row, error }`. The index is
relative to the supplied evaluated batch. The caller must retain that batch and
the aggregate state, and resume at `next_row` only for typed admission pressure.
Other errors remain terminal. Shape/range errors are checked before updates;
starting at the batch end is a no-op.

Raw arena insertion admits destination growth before updating the row's
accumulators. A denial therefore returns the current row, including the beginning
of a repeated-key run. A migration denial returns the entry row of the raw update
loop, because migration transfers already-applied state rather than applying new
input. Raw processing now reconciles retained generic entries even when
`raw_type` was assigned before a previous failed migration. Without that check,
resuming could bypass unfinished normalization.

Perfect-hash, combined-dictionary, raw, generic and global update loops respect
the supplied start offset. There is no per-row cursor allocation or expression
re-evaluation. Ordinary `process_evaluated_batch` delegates at offset0 and returns
the original typed error, preserving terminal behavior for existing callers.

The contract describes admitted failures in the current implementation. Generic
keys, perfect-hash metadata, NULL-state vectors, bare-float storage and nested
payloads still have unadmitted allocations. This change does not make those paths
resource-safe, nor does it certify recovery after arbitrary allocator failure,
panic or partially completed non-admission errors. The cursor also does not
choose when to flush against an operator working budget.

## Tests and exact outcomes

The pressure fixture contains1,024 rows with duplicate/NULL keys, exact large
decimal values, NULL decimal inputs, and nullable AVG inputs. An independent
ordered-map oracle computes every prefix's COUNT, decimal coefficient/seen state,
and AVG sum/count directly from the fixture formula. It checks:

- denial before the first row and inside a raw batch;
- denial while perfect-hash state migrates to raw storage;
- repeated denial without advancing the cursor or losing state;
- resumption after releasing a competing reservation, without duplicate updates;
- end-of-batch no-op and invalid-offset refusal without changing state.

The pool maximum remains4MiB throughout this test. Releasing a competing lease
tests the cursor contract, not successful spilling under the separate256KiB query
budget. No input rows or expressions are replayed to reconstruct state.

A separate test applies a37-row prefix and resumes the original512-row batch at
offset37 through perfect numeric, dictionary-plus-integer, generic string and
global paths. Every output group's count and exact floating sum matches an
independent oracle. The earlier retained-volatile-array and fused routing tests
remain in the broader gates.

| Final gate | Result |
|---|---|
| Full Lance/GPU-feature library |799 passed /0 failed /10 ignored, exit0 |
| Nine selected integration targets |39 passed /0 failed /0 ignored, exit0 |
| Unchanged consuming-source spill transitions |0 passed /2 failed, exit101 |

The838 passing tests are unique across these final library/integration targets;
the earlier two-test cursor run is included, not added again. The10 library
ignores are the existing flatten-EXISTS test, eight separately run actual-CUDA
tests, and the dedicated IPC-cache test. Hardware GPU, optimized performance and
dedicated cap harnesses were not run here. The two end-to-end budget fixtures
still observe two executions of each consuming input. The256KiB query-budget
fixture was not rerun. No spill-completion or performance acceptance is claimed.

All jobs are terminal. Commands used the prefix
`TMPDIR="$PWD/.scratch" RAYON_NUM_THREADS=4 SAFE_BUILD_MEM=48G SAFE_BUILD_JOBS=1 scripts/claude-safe-build.sh`:

```text
cargo test --locked --features lance,gpu --lib ingestion_cursor_tests
cargo test --locked --features lance,gpu --lib
cargo test --locked --features lance,gpu --test typed_memory_pressure --test memory_reservation_contract --test prepared_row_store_contract --test aggregate_arena_admission --test fused_aggregate_input_errors --test fused_aggregate_evaluated_keys --test aggregate_encoding_contract --test decimal_root_reuse_aggregate_contract --test dense_aggregate_null_domain
cargo test --locked --features lance,gpu --test fused_aggregate_budget_transition
```

`cargo fmt --all -- --check` passes. The production delta is confined to
`physical/morsel_agg.rs`, with one new private test module. No dependency change
or commit occurred. The [evidence archive](benchmarks/2026-09-07-aggregate-ingestion-cursor/)
preserves before/after source, patch, test fixture, logs, status and639 input
hashes. Its10 members were verified; `SHA256.json` hashes to
`1feea5b1833d45d713a882232f456fb2dfdc22fcb733ef9756d1e490e9f43f3e`.

Next, bind partial-state layouts and own their storage/codec scratch before
ingestion fills the budget. Flush and merge internal COUNT/SUM/AVG/extrema states
without finalizing prematurely, handle oversized partitions within the budget,
then wire the cursor into fused workers and remove the `Ok(None)` replay branch.
Retain the full [implementation and acceptance contract](../.claude/epics/realistic-benchmarks-duckdb-leadership/updates/2026-09-07-fused-spill-implementation.md).
