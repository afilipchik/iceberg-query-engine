---
name: ipc-default
description: Re-measure the IPC sidecar cache at SF=100 post-join-wins; if it pays, build the lifecycle (staleness, rebuild) needed to default it on
status: completed
created: 2026-08-19T05:56:36Z
---

# PRD: ipc-default

## Executive Summary

The IPC sidecar cache was measured a wash at SF=100 (85.5 vs 85.6s) in
the close-parquet-gap epic — when the suite was join-bound. Joins are
now ~2x faster (65.1s), so decode is a much larger fraction; the
verdict deserves re-measurement. If warm-IPC wins at SF=100, ship the
lifecycle it lacks for default-on: staleness detection (parquet mtime/
size vs sidecar), auto-rebuild, and a clear premise note in the
benchmark reporting (native-storage comparison, like DuckDB-native).

## Success Criteria

- G1: SF=100 warm IPC vs parquet sweep table; verdict (default-on /
  keep opt-in / delete).
- G2 (if on): staleness+rebuild implemented and tested (stale sidecar
  NEVER served — correctness); both premises reported in benchmarks;
  22/22 cell-exact in IPC mode both scales; suites green in both modes.

## Out of Scope

Eviction policy beyond staleness (disk is cheap here); compression of
sidecars.
