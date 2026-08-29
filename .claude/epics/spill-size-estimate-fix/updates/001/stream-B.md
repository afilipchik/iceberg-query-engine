---
issue: 001
stream: q12-validation-and-close
started: 2026-08-29T21:30:20Z
status: completed
completed: 2026-08-29T21:44:27Z
---
## Scope
Finish task 001's remaining non-handed-off criteria: Q12 native repro
validation (estimate sane, no spill, cell-exact, wall-time),
spill-test non-regression, default-suite green, task-file close-out.
## Progress
- Read task file / archived repro / fixed `estimate_batch_size` in full.
- Release binary at `target/release/query_engine` predated commit
  c983194 (in-binary mem cap) — rebuilding via
  `scripts/claude-safe-build.sh cargo build --release` before the Q12
  serve repro (background build in flight).
- Plan: (1) Q12 serve repro x3 with QE_SPILL_DEBUG=1 + QE_MEM_CAP=48G,
  (2) spill test suites, (3) default `cargo test`, (4) fmt check,
  (5) close-out.

## Q12 native-table validation (2026-08-29, all PASS)

Repro exactly per `.claude/epics/archived/spill-join-correctness/001.md`:
rebuilt `target/release/query_engine` at HEAD (8d519f5) via the safe-build
wrapper, then `systemd-run --user --scope -p MemoryMax=48G env
QE_MEM_CAP=48G QE_SPILL_DEBUG=1 ./target/release/query_engine serve
--bind 127.0.0.1:7791 --tables data/tpch-10gb-native --flight-bind none
--memory-limit 40G` ([mem-cap] banner printed as expected), then
`POST /sql?format=csv&distributed=0` with Q12's exact spec SQL from
`src/tpch/queries.rs`, 3 iterations.

- (a) Estimate reflects real content: re-ran the committed probe
  (`cargo run --release --example spill_size_estimate_check`), which
  computes the PRE-fix estimate (frozen copy) and the real content on the
  actual Q12 build side: pre-fix estimate 180,037,159,632 bytes
  (167.673 GiB, 5.240x the 32 GiB threshold — sole driver: the
  Dictionary(Int32,Utf8) `l_shipmode` column at 197,178,888 bytes per
  1,963-row batch); real content 42,408,624 bytes (~40.4 MiB, 0.001x
  threshold). The fixed production fallback
  (`ArrayData::get_slice_memory_size()`) computes keys*4 + dictionary
  values' content = the real-content number, i.e. ~tens of MB —
  behaviorally confirmed below.
- (b) No spill: with QE_SPILL_DEBUG=1, the serve log contains ZERO
  `compute_build_decision`/`build_with_partitioning`/`execute_spill_path`
  join-spill traces across all 3 runs (only the aggregate's
  `execute_fused_streaming` traces, ~80ms each). The join stayed on the
  in-memory path.
- (c) Cell-exact, all 3 runs: `MAIL,353822,529784` / `SHIP,352224,530051`
  — matches the DuckDB oracle exactly.
- (d) Wall-time: original documented "before" is ~150s (CLAUDE.md;
  archived repro measured 140-291s over 21 runs). Honest intermediate
  baseline: the spill-join-correctness O(n^2) writer fix had ALREADY
  brought this repro to 3-6s while still spilling (CLAUDE.md, epic
  close-out). After THIS fix (no spill at all): 0.19s / 0.17s / 0.17s
  (curl-measured end-to-end, 3 iterations) — ~17-35x vs the 3-6s
  still-spilling baseline, ~800x vs the original ~150s. Caveat: another
  agent (oom-safety-hardening stream) was active on this machine during
  measurement; given the result is sub-200ms and uniform, load skew can
  only have made these numbers WORSE, not better.

## Test suites (all via scripts/claude-safe-build.sh, all PASS)

- `cargo test --test spill_tests`: 7 passed, 0 failed.
- `cargo test --test spill_directory_collision_tests`: 1 passed, 0 failed.
- `cargo test spillable`: 15 lib-unit tests passed (incl.
  `dictionary_column_estimate_is_content_aware_not_mmap_capacity`) plus
  `partition_contract`'s spillable test, 0 failed.
- `cargo test` (default features, full suite): **1287 passed, 0 failed,
  1 ignored** — documented baseline 1285 + exactly 2 accounted-for new
  tests (the dictionary regression test from 2912456 and one
  `src/execution/memory.rs` mem-cap test from c983194). Zero
  regressions.
- `cargo fmt --all -- --check`: clean.

## Close-out

All remaining (non-handed-off) acceptance criteria PASS. Task 001 set to
`status: closed`; the oversized-build-side stress-case criterion is
formally handed off to `oom-safety-hardening` task 001 (in progress by
another agent, with the two committed adversarial repros as its starting
artifacts). Epic progress recalculated to 50% (1 of 2 tasks closed);
task 002 (broader sweep + 4-combo suite + docs + epic close) remains.
