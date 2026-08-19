---
name: ipc-default
status: completed
created: 2026-08-19T05:56:36Z
updated: 2026-08-19T05:56:36Z
progress: 100%
prd: .claude/prds/ipc-default.md
github: (will be set on sync)
---

# Epic: ipc-default

## Overview
Re-verdict the IPC sidecar at SF=100 post-join-wins (shares 001's
measurement with decode-path); if it pays, ship staleness detection +
auto-rebuild so it can default on, reporting both premises.

## Tasks Created
- [ ] 001.md - SF=100 re-verdict (shares decode-path 001 data) (parallel: false)
- [ ] 002.md - Staleness + rebuild lifecycle (correctness: stale never served) (parallel: false)
- [ ] 003.md - Default decision + QA both modes + docs + close (parallel: false)

## Epic close-out (2026-08-19)

v1 verdict overturned and root-caused: the SF=100 'no benefit' was
dual-format cache contention from the dict-coercion guard, not the
cache. v2 sidecars (dictionary-coerced storage, commit 2a9a6b4) close
the coverage gap; auto mode defaults reads on without surprise disk
cost; staleness = v2 stamp (version+size+mtime, stale never served,
rebuild automatic — exercised end-to-end by the v1->v2 migration of
three datasets). **SF=100 warm 48.3s = 0.72x DuckDB NATIVE, 1.21x
DuckDB on the same parquet; SF=10 5.1s. 22/22 cell-exact everywhere;
suites green in all three modes + lance.** Two arrow-rs sharp edges
documented in the commit (FileWriter dictionary replacement; dict_id 0
collisions).
