# oom-safety-hardening task 005 — stream E progress

## Status: implementation landed, validation in progress

## What shipped (code)

- `src/execution/context.rs`:
  - `INSERT_ADMISSION_CHECK_NAME` ("insert/CTAS write-path admission
    check") + `INSERT_ADMISSION_DECODE_EXPANSION` (3x, calibrated against
    SF=10 lineitem: 59.1MB `total_byte_size` row group decodes to ~132MB
    Arrow = 2.24x, +slack).
  - `evaluate_insert_admission(...)` — pure decision core, unit-testable:
    `estimate = min(QE_INSERT_MERGE_CONCURRENCY, num_partitions) x
    max_source_row_group_bytes x 3`; refuses when `estimate >
    memory_limit * spill_threshold` (check_scan_budget's exact formula).
    `max_row_group_bytes == 0` (VALUES / memory tables / native-table
    sources, which check_scan_budget already guards) => always admit —
    never refuse on a guess.
  - `collect_scan_table_names` + `ExecutionContext::
    max_source_row_group_bytes` (footer-only parquet metadata read over
    the optimized plan's scanned tables) + `check_insert_write_admission`
    (env-traced via `QE_DEBUG_INSERT_ADMISSION=1`).
  - Wired into BOTH `create_table_as_select` and
    `insert_into_native_table`, immediately after
    `create_physical_plan` (partition count known) and before any
    `physical.execute(...)` call / target write-lock acquisition.
- `examples/oom_cap_harness.rs` (`scenario_insert` only — task 001's
  shared artifact, edit confined to this stream's scenario): engine
  memory_limit now `QE_HARNESS_INSERT_LIMIT` (default 512MB, matching
  the shell driver's 512M cap) instead of the pre-fix deliberate 8GB;
  rationale documented inline (pre-fix the limit provably had zero
  effect on this path; post-fix it is what drives the named refusal).
- Tests: 5 new unit tests in `context.rs`'s test module (SF=10
  calibration admit@2GiB / refuse@512MiB with exact byte counts + both
  knobs, partition-capped effective concurrency, concurrency-knob advice
  truthfulness, no-parquet-source admit) + new
  `tests/insert_admission_tests.rs` (4 integration tests over the real
  SQL surface + `data/tpch-1mb`: named refusal before any write, no
  false refusal at adequate budget, refused INSERT leaves target
  manifest byte-identical, VALUES/table-less statements admitted at a
  1MB limit).

## Calibration arithmetic (recorded)

- SF=10 lineitem: 58 row groups, max `total_byte_size` 59,120,848 B;
  scan partitions = min(32 threads, 58) = 32; effective streams =
  min(8, 32) = 8.
- estimate = 8 x 59,120,848 x 3 = 1,418,900,352 B (~1.42GB).
- 2GiB limit: budget = 1,717,986,918 B -> ADMIT (measured actual peak
  ~1.63GB completes under a 2GB cgroup cap per native-tables-mutation
  005).
- 512MiB limit: budget = 429,496,729 B -> REFUSE (pre-fix: journal-
  confirmed memcg SIGKILL, task 001 evidence table).

## Test results so far (all through claude-safe-build.sh, default features)

- `cargo test --lib execution::context`: 14/14 pass (5 new).
- `cargo test --test insert_admission_tests`: 4/4 pass (new file).
- Existing INSERT/CTAS-adjacent suites unchanged and green:
  native_insert_tests 9/9, native_delete_tests, native_update_tests
  12/12, native_table_validation 12/12, native_rollup_tests 11/11,
  native_rollup_refresh_tests 8/8, native_materialized_view_tests 7/7,
  native_rollup_qa_closeout_tests 16/16.
- `cargo fmt --all -- --check` clean.

## Pending validation

- [ ] harness insert scenario at 512M cap: SIGKILL -> clean named refusal
- [ ] SF=10 append (sql mode) under 2GB cgroup cap completes
- [ ] RSS spot re-measure (~1.6GB band, uncapped-ish leg)
