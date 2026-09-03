---
issue: 004
stream: sf100-baseline-oracle-harness
started: 2026-09-03T14:43:06Z
status: completed
---
## Scope
Q4 SF=100 DuckDB oracle, pinned HEAD baseline binary, Q4 refusing-budget
probe + physical plan capture, `semi-join` oom-cap harness scenario
(pre-fix verdict recorded).

## Progress
- Starting

## 2026-09-03T15:08:40Z — taken over by the coordinator session

- Stream B subagents (default model x3, Opus x1) all died on API 529
  before producing anything; the coordinator runs Stream B directly.
- Pinned binary: detached worktree at `.scratch/sjc3-004/wt` @
  570a8cb7d496b1a87c5c6e496a9226f2426b1d86 (pre-fix HEAD), built with its
  own `CARGO_TARGET_DIR=.scratch/sjc3-004/wt-target` under
  `claude-safe-build.sh` (SAFE_BUILD_MEM=48G, 3m38s, exit 0) →
  `.scratch/sjc3-004/bin/query_engine_head`. Stream A's edits to the
  main checkout cannot enter this binary.
- DuckDB Q4 SF=100 oracle over `data/tpch-100gb` parquet (32G cap,
  1.6s): 5 rows — 1-URGENT 999120, 2-HIGH 997632, 3-MEDIUM 998958,
  4-NOT SPECIFIED 999304, 5-LOW 998946 → `.scratch/sjc3-004/q4_oracle.json`.
- Q4 SF=100 probe runner: `.scratch/sjc3-004/run_q4.py <bin> <tag>
  <memory-limit>` (serve over `data/tpch-100gb-native`, POST /sql,
  cell-exact check vs oracle, sj-trace summary). 100G-limit leg launched
  on the pinned binary under MemoryMax=110G / QE_MEM_CAP=110G /
  QE_SPILL_DEBUG=1 (free showed 103G available).

## 2026-09-03T15:10:43Z — Q4 SF=100 budget ladder on the pinned PRE-FIX binary (570a8cb)

All legs: `serve --tables data/tpch-100gb-native --memory-limit <B>`,
`QE_SPILL_DEBUG=1`, cell-exact check vs the DuckDB oracle. 100G leg under
MemoryMax=110G/QE_MEM_CAP=110G; all others under MemoryMax=32G/QE_MEM_CAP=32G.
Logs: `.scratch/sjc3-004/probe_head100G.log`, `ladder_head.log`,
`ladder_head2.log`, per-leg `serve_head<B>.log`.

| budget | verdict | elapsed | join spill traces |
|---|---|---|---|
| 100G | HTTP 200, CELL-EXACT | 13.1s (cold) | none (fused agg only) |
| 16G | CELL-EXACT | 1.0s | none |
| 4G | CELL-EXACT | 0.9s | none |
| 1G | CELL-EXACT | 0.9s | none |
| 256M | CELL-EXACT | 0.9s | none |
| 128M | CELL-EXACT | ~1s | none |
| 96M | CELL-EXACT | ~1s | none |
| 80M | CELL-EXACT | ~1s | none |
| **64M** | **HTTP 400 REFUSED**: "SEMI join build side exceeds the memory budget, but the join spill path currently supports only INNER joins" | 0.2s | refusal raised inside the fused-streaming drain, swallowed once (re-execute from scratch), then surfaced to the client |
| 16M | REFUSED (same message) | 0.1s | same |

**The pruning epic's 100G premise no longer refuses** (the Dictionary
size-estimate fix moved the boundary, as task 003 found for Q13).
**ACCEPTANCE BUDGET = 64M** (largest tested budget that still refuses on
pre-fix code; 80M completes in memory). The fix must make Q4 complete
cell-exact at 64M (and 16M) with real join spill activity.

**Orientation (inferred from the measurements, since EXPLAIN is HTTP 501
"Statement type not supported" on this server):** the build side fits at
80M (threshold 64MB) and not at 64M (51MB), so it is on the order of
50-64MB — the date-filtered `orders` side (~5.7M rows). The filtered
`lineitem` side (~380M rows, >3GB of keys alone) could not fit at 256M
either, and 256M completed with zero spill traces. Hence build = LEFT
(orders) = the output side: `build_right_for_left = false`, i.e. the
**build-side-output (!swapped) orientation** — exactly the case
004-analysis.md fact 1 predicted for Q4, and the case that needs the
build-matched bitmap / per-chunk emission. No ON-clause filter can be
present on this join: the fixed binary's guard order (join-type first,
filter second) will confirm — a filter would produce the filter refusal
instead of completing.

## Stream B result (2026-09-03T18:54:09Z)

See `004.md` "Outcome" for the consolidated evidence table. Status:
completed; all deliverables of this stream landed in 67f7cea / d378744
and the close-out commit.
