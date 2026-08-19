---
name: decode-path
status: completed
created: 2026-08-19T05:56:36Z
updated: 2026-08-19T05:56:36Z
progress: 100%
prd: .claude/prds/decode-path.md
github: (will be set on sync)
---

# Epic: decode-path

## Overview
Scan-side residue, attribution-first. The existing SF=100 IPC sidecars
give a free decode-cost oracle: warm IPC sweep vs warm parquet sweep,
per query — the delta IS arrow-rs decode (+RowFilter evaluation). Fix
what the biggest deltas name, on the same-parquet premise.

## Tasks Created
- [ ] 001.md - Decode-cost attribution via IPC A/B (GO/STOP) (parallel: false)
- [ ] 002.md - Implement the named fix(es) (parallel: false)
- [ ] 003.md - Gates + QA + docs + close (parallel: false)

## Epic close-out (2026-08-19)

Attribution GO; implementation merged into ipc-default (the sidecar IS
the decode fix). Outcome carried there: SF=100 48.3s warm, Q3 2.2s,
Q5 2.1s, Q21 3.3s. Same-parquet-premise number remains 65.1-65.8s and
stays reported; the decode gap on that premise is now a documented
storage-format cost, exactly like DuckDB native's.
