---
name: perf-marathon
status: completed
created: 2026-08-20T03:38:14Z
updated: 2026-08-20T03:38:14Z
progress: 100%
prd: .claude/prds/perf-marathon.md
github: (will be set on sync)
---

# Epic: perf-marathon

## Overview

Scoreboard-driven iteration (IDEAS.md) + distributed re-validation.
Loop: pick highest-expected-value untested idea → cheapest honest
measurement → verdict on the scoreboard → commit-or-revert → repeat
until ≤45s warm-IPC or ≥10 verdicts.

## Tasks Created
- [ ] 001.md - Distributed re-validation + sidecar/split interplay (parallel: false)
- [ ] 002.md - Iteration loop over IDEAS.md (≥10 verdicts or ≤45s) (parallel: false)
- [ ] 003.md - QA close-out + scoreboard + docs (parallel: false)

## Epic close-out (2026-08-20)

11 ideas carry verdicts (scoreboard in IDEAS.md). Headline: idea #5
(survivor-size-gated 8k re-slice of IPC batches) — **SF=100 warm-IPC
48.3 → 47.1s (0.70x DuckDB native), Q9 to duck-parquet parity** — and
the distributed program re-validated end-to-end on current HEAD: M1 +
M2 gates PASS over 3 real processes at SF=10 in all three cache modes,
with the sidecar/split interplay proven safe by construction and a
cluster_local.sh --memory-limit passthrough fixing the harness gap that
masqueraded as 13 gather failures.

The residues now form ONE named class: big sparse aggregates (Q18
6.4s, Q20 4.0s, Q13 2.85s — 44M-150M sparse groups) → future program
item: dense group-id remapping. Everything else measures at parity or
architecture floors.
