---
name: perf-marathon
status: in-progress
created: 2026-08-20T03:38:14Z
updated: 2026-08-20T03:38:14Z
progress: 0%
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
