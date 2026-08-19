---
name: decode-path
description: Attack the scan/decode-side residue (Q1 2.8x, Q16 2.7x, Q14/Q6, Q18-subquery) on the same-parquet premise, attribution-first with a kill-switch
status: completed
created: 2026-08-19T05:56:36Z
---

# PRD: decode-path

## Executive Summary

At 65.1s SF=100 (1.62x like-for-like), the join side is at measured
floors; the residue concentrates in scans: Q1 3.4-3.7s vs duck-parquet
1.2s, Q16 1.9 vs 0.7, Q14 1.0 vs 0.85, plus Q18's subquery scan floor.
Attribute decode cost per query FIRST using the existing IPC sidecars
(decode-free reads isolate arrow-rs decode exactly), then implement the
highest-leverage bounded fix. Kill-switch: if attribution shows the
decode delta suite-wide < 2s, stop and record.

## Success Criteria

- G1: per-query decode-cost table (parquet vs IPC at SF=100, warm) with
  GO/STOP verdict.
- G2 (if GO): Q1 ≤ 2.7s, suite ≤ 63s, 22/22 cell-exact both scales, no
  regressions >5%, suites green.
- Constraints: same-parquet premise (no cache dependence in the
  reported number); memory-safe; commit-or-revert; oomsafe for heavy
  runs.

## Out of Scope

IPC lifecycle/default-on (that is the ipc-default epic); lance decode.
