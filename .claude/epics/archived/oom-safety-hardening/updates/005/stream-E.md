# oom-safety-hardening task 005 — stream E progress

## Status: COMPLETE — all acceptance criteria met with real evidence (2026-08-29T22:23Z)

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

## Validation evidence (all runs contained: systemd scopes + QE_MEM_CAP; terminal never at risk)

1. **Harness insert @ 512M flips SIGKILL -> clean named refusal** (both
   levers; logs `.scratch/oom001/harness_task005_postfix/`):
   - pre-fix (task 001 final battery): cgroup 512M -> exit 137,
     journal-confirmed memcg SIGKILL.
   - post-fix: `RESULT scenario=insert lever=cgroup cap=512M exit=2
     peak_rss_mb=28 verdict=PASS reason=clean-refusal`, and rlimit lever
     `exit=2 peak_rss_mb=30 PASS`. Refusal text cites the check name,
     estimate 1,418,900,352 B (8 streams x 177,362,544 B/stream = 59,120,848 B
     max row group x 3), budget 429,496,729 B (memory_limit 536,870,912 x
     spill_threshold), and both knobs (--memory-limit,
     QE_INSERT_MERGE_CONCURRENCY). Matches the unit-test arithmetic
     digit-for-digit.
2. **SF=10 append under a 2GB cgroup cap COMPLETES (no false refusal)**:
   `QE_MEM_CHECK_MODE=sql QE_MEM_CHECK_LIMIT_GB=2` under
   `systemd-run -p MemoryMax=2G -p MemorySwapMax=0` + `/usr/bin/time -v`:
   exit 0, `inserted 60000000 rows (60 segments, version 2) in 8.98s`,
   peak RSS 1,682,372 KB (log `.scratch/oom005_sf10_2g.log`).
3. **RSS-reduction leg spot re-measured, unregressed**: same sql-mode run
   uncapped-ish (8G containment scope, default 6GB limit): exit 0, 60M
   rows in 9.96s, peak RSS 1,673,240 KB (~1.60GiB) — squarely in the
   documented 1.63-1.68GB bounded-merge band (pre-bounded-merge was
   5.38GB; 70% reduction intact). Note: sibling agents (tasks 004/007)
   were building/running concurrently on this box, and task 004's
   in-progress uncommitted edits to `native_table.rs`/`planner.rs` were
   present in the tree during these runs — numbers landed on the
   historical band anyway.
4. **Existing INSERT/CTAS tests unchanged and green** (default features,
   via claude-safe-build.sh): insert_admission_tests 4/4 (new),
   native_delete_tests 10/10, native_insert_tests 9/9,
   native_update_tests 12/12, native_table_validation 12/12,
   native_rollup_tests 11/11, native_rollup_refresh_tests 8/8,
   native_materialized_view_tests 7/7, native_rollup_qa_closeout_tests
   16/16, `--lib execution::context` 14/14 (5 new).
   `cargo fmt --all -- --check` clean.

## Commits

- d52ef20 "oom-safety-hardening 005: named pre-flight admission check for
  the INSERT/CTAS write path" (src/execution/context.rs,
  tests/insert_admission_tests.rs, examples/oom_cap_harness.rs
  scenario_insert only, this stream file).
