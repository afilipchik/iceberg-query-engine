---
name: perf-marathon
description: Iterate performance ideas with measured verdicts until SF=100 warm-IPC ≤45s or ≥10 ideas tested; validate the week's changes work distributed (correctness + perf, sidecar interplay)
status: completed
created: 2026-08-20T03:38:14Z
---

# PRD: perf-marathon

## Executive Summary

State: SF=100 warm-IPC 48.3s (0.72x DuckDB native), like-for-like 65.1s
(at its measured floor per the decoder refutation). This epic runs an
idea-per-idea iteration loop — every idea gets a measured verdict
(WIN/NEUTRAL/REFUTED) on the scoreboard, commit-or-revert — until the
suite hits ≤45s warm-IPC or at least 10 ideas carry verdicts. In
parallel discipline: the distributed path (scatter/gather over 3 local
processes) must be re-validated against everything this week changed
(dict-flowing batches, join pruning, u32 emission, v2 sidecars), with a
measured distributed run and any sidecar/split interplay fixed.

## Success Criteria

- G1 (distributed): `cluster_local.sh` gate green; distributed TPC-H
  byte-compared vs single-process on the current HEAD in BOTH cache
  modes; sidecar behavior under row-range splits verified (partial-rg
  shards must not silently read whole row groups); verdicts recorded.
- G2 (iteration): ≥10 ideas on the scoreboard with measured verdicts,
  OR warm-IPC SF=100 ≤ 45s reached earlier. Idea sources: IDEAS.md
  backlog (seeded from evidence), plus whatever attribution surfaces.
- G3 (hygiene): every WIN lands cell-exact both scales + suites green
  both modes; every loss reverted; scoreboard + docs close the epic.

## Constraints

Measured on this box, serialized, via scripts/oomsafe.sh; noise ±3%;
run pairs for claims inside noise. Memory-safe always. Lance leg spot-
checked when an idea touches shared operators.
