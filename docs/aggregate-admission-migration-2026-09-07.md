# Typed admission pressure and lossless aggregate migration

Two prerequisites for resumable aggregate spilling are now repaired. Memory-pool
denial has a typed identity, and aggregate representation migrations preserve
their source state when destination admission fails. The fused spill transition
itself remains incomplete: both consuming-source budget tests still fail by
executing each input partition twice.

## Typed pressure

`QueryError::MemoryLimit` records the limiting pool name, requested additional
bytes, used bytes and limit at the failed admission. `PoolState::grow` constructs
it after releasing the hierarchy lock. Failed admission still changes no pool
counters or peaks. A reservation whose growth fails retains its previous size.

`is_memory_limit()` follows shared deferred errors to the root. Recovery can
therefore distinguish query-pool denial from an execution error with identical
display text, allocator OOM, arithmetic failure or codec failure. The displayed
message and public query-log `kind()` remain `Execution` for compatibility; the
Rust enum gains a variant. Internal recovery must use the typed identity, not
the broad log category or text parsing. Wrappers that erase an error into text
do not acquire this classification.

The new tests check child and ancestor denial, exact snapshot fields, atomic
reservation/peak rollback, successful subsequent resize, retained error snapshots,
nested shared errors and lookalike execution/allocator errors. This classification
does not imply a failed operator has an applied-row cursor or is safe to retry.

## State migration

`AggregationState::normalize_raw` previously removed a generic group before
inserting it into the admitted raw arena. If admission failed, that group was
lost. `demote_raw_sums` drained its source map while inserting into the arena;
failure dropped the unprocessed drain suffix as well. Existing callers generally
returned an error, so this reproduction is internal state loss on an error path,
not a claim that those callers returned a successful partial result. It prevents
correct future recovery from the retained state.

Both migrations now remove each source entry only after its destination accepts
it. On the first failure, they retain the failed entry and all remaining entries.
Previously moved entries remain only in the destination. Generic normalization
also avoids cloning an entire key list. The traversal stays linear rather than
repeatedly restarting an iterator after each removal.

The regressions seed256 exact quarter-valued groups and an overlapping destination
group, then deny admission both with no extra headroom and after a real transferred
prefix. They compare every logical key/value before and after denial, release
the blocker, finish migration and verify that no prefix was applied twice. Both
tests failed before the repair and pass afterwards. The checks use exact binary
fractions and an independent ordered map; no random timing or row-count-only
comparison is involved.

This is admission-failure preservation during migration. It does not certify
transactional behavior for arbitrary panics, complete nested-value ownership,
integer overflow handling, all general-state allocations or bounded spill merge.
`process_evaluated_batch` still needs an applied-row cursor before it can resume
after a partially applied batch. Its raw-mode transition must reconcile retained
generic entries even when `raw_type` was assigned before a prior failed migration.

## Validation

All commands used the prefix
`TMPDIR="$PWD/.scratch" RAYON_NUM_THREADS=4 SAFE_BUILD_MEM=48G SAFE_BUILD_JOBS=1 scripts/claude-safe-build.sh`.
All jobs are terminal.

```text
cargo test --locked --features lance,gpu --lib migration_admission_tests
cargo test --locked --features lance,gpu --lib
cargo test --locked --features lance,gpu --test typed_memory_pressure --test memory_reservation_contract --test prepared_row_store_contract --test aggregate_arena_admission --test fused_aggregate_input_errors --test fused_aggregate_evaluated_keys --test aggregate_encoding_contract --test decimal_root_reuse_aggregate_contract --test dense_aggregate_null_domain
cargo test --locked --features lance,gpu --test fused_aggregate_budget_transition
```

| Gate | Result |
|---|---|
| Migration regressions before repair |0 passed /2 failed, exit101 |
| Initial full library |795 passed /1 failed /10 ignored, exit101 |
| Final full library |796 passed /0 failed /10 ignored, exit0 |
| Final nine integration targets |39 passed /0 failed /0 ignored, exit0 |
| Unchanged fused budget transitions |0 passed /2 failed /0 ignored, exit101 |

The initial library failure was an old test requiring the generic Execution
variant for pool denial. Its assertion now checks typed pressure; the exact
message and public category are separately verified. The final835 passing tests
do not include that earlier run or the preceding27-test typed-pressure run as
additional unique coverage.

The10 ignores are the existing flatten-EXISTS test, eight tests requiring
separate actual-CUDA execution, and one IPC-cache test requiring a dedicated
`QE_IPC_CACHE=auto` process. They were not run here. No optimized benchmark,
hardware GPU gate or dedicated low-memory cap harness ran in this step.
The separate256KiB query-budget failure remains open and was not rerun.

`cargo fmt --all -- --check` passes. No dependencies or commits changed.
Production changes are in `error.rs`, `execution/memory.rs` and
`physical/morsel_agg.rs`; the other changed existing files update test assertions
to the typed error. The [evidence archive](benchmarks/2026-09-07-aggregate-migration-admission/)
contains the before/after code, migration patch, fixtures, all outcomes and638
source-input hashes. Its21 members were verified against `SHA256.json`, hash
`adb4651dee4190b69ea2150d1c95894e5d528d9077c3cd7aaa5204463bd1fab6`.

Continue with the existing [single-pass spill contract](../.claude/epics/realistic-benchmarks-duckdb-leadership/updates/2026-09-07-fused-spill-implementation.md):
admit before logical row mutation, return the exact applied cursor, flush/merge
bounded partial states and resume retained evaluated arrays. Keep the original
input execution and volatile evaluation single-pass. Full provider/performance,
query-wide budgets and the parent acceptance gates remain open.
