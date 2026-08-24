---
issue: 005
stream: main
started: 2026-08-24T08:00:00Z
completed: 2026-08-24T09:10:00Z
status: completed
---
## Scope
See .claude/epics/native-tables-mutation/005.md

## Progress

- Read phase 1 task 006 (memory safety methodology) and task 008 (gdb
  rigor precedent) Outcomes, plus this epic's tasks 001-004 Outcome
  sections in full, before starting.
- Built `examples/native_mutation_growth_check.rs`: real table grown to
  2573 segments / 4.67M rows via 3000 separate Append/Delete/Update
  calls (not synthetic). Findings: deletion-vector many-segments-shape
  ~13.1-13.7 bytes/entry (extrapolates to ~131MB at task 001's own
  literal "1000 segments x 1M rows x 1%" scale, larger than their "tens
  of MB" guess -- real but not urgent at current realistic scale, named
  as follow-up); manifest size grows perfectly linearly (bounded per
  op); scan()/statistics() stay roughly linear (no cliff); NEW finding —
  cumulative mutation-sequence cost is O(N^2) (every mutation re-reads +
  re-writes the whole manifest) — named, not fixed (fixing needs
  compaction, epic-out-of-scope); empty-segment-drop exception measured
  effective (~9.1KB/segment saved, real serde_json counterfactual).
- Built `examples/native_crash_kill_check.rs`: real cross-process `kill
  -9` against the REAL native_write building blocks (not a lock-only
  synthetic harness). 6/6 clean runs: lock auto-releases, manifest
  untouched, orphan segment harmless, table reads back exactly
  pre-crash, still writable after. Concurrent-writers mode: 3/3 clean
  runs, exactly one writer succeeds, the other gets a named
  QueryError::Storage.
- Re-measured the carried-forward 5.3GB SQL-path finding (task 002) at
  SF=100 (600M rows, 10x): 5.86GB, only +9% for 10x data -- CONFIRMED
  BOUNDED (partition count, capped by rayon thread count, drives it, not
  row count -- row-group size confirmed ~constant across scales via
  pyarrow metadata). Confirmed it does NOT fail safely before a fix:
  --memory-limit=1GB had zero effect on actual usage; a real
  SAFE_BUILD_MEM=2G cgroup cap got a KERNEL OOM-kill (journalctl -k
  confirmed directly). Fixed: `bounded_partition_merge` in
  src/execution/context.rs (flatten_unordered instead of select_all,
  QE_INSERT_MERGE_CONCURRENCY, default 8). Re-measured: SF=10 5.38GB ->
  1.63GB (-70%), SF=100 5.86GB -> 1.67GB (-71%), wall time neutral to
  22% FASTER. 2GB and 1GB caps now succeed (were OOM); 512MB still OOMs
  (narrower residual, named as follow-up -- no formal admission control
  on this path, unlike NativeTable::scan()'s check_scan_budget).
  Correctness reverified: all 21 pre-existing INSERT/CTAS tests
  unchanged + 3 new unit tests for the merge helper.
- Built `examples/native_delete_then_scan_budget_check.rs`: verified
  phase 1 task 006's memory-budget formula still holds post-DELETE
  against the SAME real 60M-row lineitem fixture task 006 itself used
  (hard-linked, not copied). On-disk bytes confirmed unchanged by a real
  10.7M-row DELETE; tight budget refuses cleanly citing the unchanged
  physical size; generous budget succeeds with the correct logical row
  count; both hold under a real 8G cgroup cap.
- Full suite green throughout (1185 passed, 1 pre-existing ignored, 0
  failed, run mid-task and as the final gate); `cargo build --release
  --examples` clean; `cargo fmt --all -- --check` clean.
- CLAUDE.md updated with a new "Mutation: memory safety +
  concurrency/crash-safety adversarial verification (task 005)" section.
- 005.md: status closed, checklist boxes checked, Outcome section
  appended with every finding's exact numbers and reproduction commands.
