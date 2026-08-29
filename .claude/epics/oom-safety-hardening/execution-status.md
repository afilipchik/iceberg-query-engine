# Execution Status — oom-safety-hardening (+ spill-size-estimate-fix dependency)

Started: 2026-08-29T21:30:20Z (branch: epic/spill-size-estimate-fix)
Updated: 2026-08-29T22:45:00Z

## Active Streams (wave 2)
- 007 / Stream C: join spill-path hash-table memory accounting (the
  confirmed root cause) — spillable.rs
- 004 / Stream D: streaming native-table scan into spilling consumers —
  native_table.rs + planner.rs
- 005 / Stream E: INSERT/CTAS formal admission check — context.rs

## Queued
- 002 SpillableHashAggregateExec streaming — after 007 (same file; reuse
  its accounting plumbing)
- 003 ExternalSortExec streaming — after 002
- 006 QA close-out — after all

## Completed
- 001 harness + root-cause (CLOSED 2026-08-29): accounting hole =
  unbudgeted execute_spill_path hash tables (~10-20x); 2026-08-28
  incident = bare uncapped repro run pre-hook (58.4G at kill);
  harness `examples/oom_cap_harness.rs` + `scripts/oom_cap_harness.sh`
  with pre-fix evidence for all 4 scenarios; 2 profiler deadlocks fixed
- spill-size-estimate-fix 001 (external, CLOSED): Q12 native 0.17-0.19s
  vs ~150s, no spill, cell-exact; suite 1287/0/1
