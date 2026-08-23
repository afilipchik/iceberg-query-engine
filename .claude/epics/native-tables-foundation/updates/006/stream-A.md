---
issue: 006
stream: main
started: 2026-08-23T20:19:44Z
status: completed
---
## Scope
See .claude/epics/native-tables-foundation/006.md

## Progress

- Read task 003's Outcome (write path measured ~406MB peak RSS converting
  60M-row lineitem, bounded by the 1M-row/segment buffer) and task 004's
  Outcome (CTAS streams through `create_table_as_select`, never through
  `ExecutionContext::sql()`'s materializing path) first, per instructions.
- Independently re-derived both claims by reading `native_write.rs`
  (`write_batches_with_options`/`SegmentWriter`) and `context.rs`
  (`create_table_as_select`) directly, not just trusting the prior
  self-reports: confirmed the stream is driven batch-by-batch
  (`stream.try_next()`), buffering caps at `target_rows_per_segment`
  (default 1,000,000), and `sql()` explicitly refuses `CREATE TABLE`
  statements so CTAS can never reach its collecting path. Independently
  RE-RAN the write-side conversion myself (60M-row lineitem, 2.8GB
  parquet) under a 900MB hard cgroup cap (`claude-safe-build.sh`,
  `/usr/bin/time -v`): succeeded, 404,044 KB peak RSS, matching task 003's
  415,428 KB within run-to-run noise. Write path confirmed independently:
  no gap.
- Traced `ipc_cache::read_row_group` (the function `NativeTable::scan()`
  calls) directly: confirmed real `memmap2::Mmap` + `Buffer::
  from_custom_allocation` zero-copy reads, the exact same mechanism the
  existing parquet IPC sidecar cache already relies on. Read-side mmap
  mechanism confirmed sound.
- Went further than trusting "mmap is sufficient" as a general read-side
  conclusion: registered a real native table (60M-row lineitem) via the
  REAL `TableProvider` path (`serve --tables`, not a synthetic
  microbenchmark) and ran a full-table aggregate under real memory
  pressure. Found a REAL gap: `NativeTable::scan()` (the generic,
  non-parquet-fast-path `TableProvider` route every native-table query
  that isn't dense-direct-address-eligible takes) is NOT spill-aware — it
  materializes every active segment into one `Vec<RecordBatch>` before
  returning. Measured: needed ~1.6GB peak RSS (kernel `VmHWM`) and was
  OOM-killed (SIGKILL) under a bare 1GiB cgroup cap; the IDENTICAL query
  over the IDENTICAL data as plain Parquet (genuinely streaming via
  `ParallelParquetSource`) finished in 109ms under the same cap. Confirmed
  via a control test this is native-table-specific, not a general
  engine-wide characteristic every provider already shares.
- Implemented the sanctioned "hard, explicit cap" fix (spill integration
  doesn't fit a table-provider's own `scan()` method): `NativeTable`
  carries an optional `memory_budget_bytes`, set by `ExecutionContext::
  register_native_table` to `memory_limit * spill_threshold` — REUSING
  the exact formula `spillable.rs` already applies at 7 call sites, not a
  new concept. `scan()` refuses cleanly (named sizes, before touching any
  segment) if the table's declared on-disk size exceeds it.
  `shard_by_splits` propagates the budget to shards. A `NativeTable` built
  directly (bypassing `ExecutionContext`) is unaffected — `None` budget,
  today's behavior, preserving every existing test/call site.
- Verified the fix end-to-end against the exact adversarial scenario that
  failed: same 1GiB cgroup cap now gets a clean HTTP 400 in ~20ms with a
  named, actionable error (was: SIGKILL after several seconds); an
  adequately-sized `--memory-limit` (e.g. 8GB) still runs the same query
  successfully (588ms, correct results, matches every prior run's
  values).
- Messaged impl-nt-005-densepath (dense-direct-address task) with the
  exact formula/threshold, since their native-table fast path also calls
  `provider.scan_with_filter()` and therefore inherits this same gate —
  FYI only, no action needed on their end unless they hit a surprise
  refusal at large scale without configuring `--memory-limit`.
- New unit tests in `native_table.rs` (5): no-budget unaffected,
  comfortably-under-budget succeeds, over-budget refuses cleanly with the
  right error content, exactly-at-budget is not an off-by-one refusal,
  `shard_by_splits` propagates the budget and a shard that individually
  fits still succeeds.
- Full suite confirmed green: `cargo test --release`, private target dir —
  1060 passed, 0 failed, 1 ignored across every binary (lib 295,
  native_table_validation 12 incl. task 005's own new test,
  distributed_cluster 19 — M1/M2 gate unaffected — plus all others).
  `cargo fmt --all -- --check` clean for both changed files.
- Verified the fix end-to-end against the exact reproduction: same 1GiB
  cgroup cap that SIGKILL'd before the fix now returns a clean HTTP 400 in
  ~20ms with the exact byte counts named; an adequately-sized
  `--memory-limit` (8GB) still runs the same query successfully (588ms,
  correct results).
- Coordinated live with impl-nt-005-densepath over a shared, non-worktree
  checkout: they extracted their own `as_any()` hunk into its own commit
  (`baf9e15`) via `git apply --cached` so my uncommitted work stayed
  untouched on disk; verified afterward (`git diff` shows exactly my two
  files' hunks, `grep` for their symbols in the new HEAD confirms a clean
  split) before proceeding to my own commit.
- Status: **completed**. See `006.md`'s Outcome section for the full
  writeup (write path re-confirmed safe; read path had a real, measured
  gap — `NativeTable::scan()` not spill-aware, OOM-killed under a 1GiB cap
  where identical-data Parquet ran in 109ms — closed via a
  `memory_limit * spill_threshold` admission cap on `scan()`, the same
  formula `spillable.rs` already uses).
