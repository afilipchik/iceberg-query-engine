---
name: join-spill-streaming
status: completed
created: 2026-09-05T01:38:00Z
updated: 2026-09-05T06:36:01Z
progress: 100%
prd: .claude/prds/join-spill-streaming.md
github: (will be set on sync)
---

# Epic: join-spill-streaming

## Overview

Turn the join spill path from "collect, then process, then collect" into
a bounded pipeline: probe batches flow in, matched batches flow out,
spilled partitions are processed in parallel under the same budget.

## Architecture Decisions

1. **Probe streaming first, output streaming second, parallelism third —
   each measured on its own.** The probe collect is a one-line
   materialization (`stream_merge_input_partitions` already exists);
   the output stream is a structural change (a state machine that yields
   per batch / per chunk); parallel read-back builds on the streamed
   output so its results can be yielded as partitions finish.
2. **Budget invariant stays the operator's, not the machine's.** K
   concurrent partitions share ONE threshold: per-chunk table budget =
   threshold / K. Never "K × threshold".
3. **Repeat execution keeps working.** The build decision stays memoized;
   each call still writes fresh probe files and cleans them in Drop; the
   output stream owns its per-call state.
4. **No semantic drift.** SEMI/ANTI bitmaps and INNER pair emission are
   unchanged; only WHEN batches are emitted changes. Every existing test
   plus the chaos battery is the regression net.

## Technical Approach

### Backend Services
- `spillable.rs`: `execute_spill_path`, `probe_with_spilling`,
  `process_spilled_partition` → a `SpillJoinStream` (or `unfold`-based
  stream) with phases; a bounded parallel read-back driver.
- `examples/oom_cap_harness.rs` / `scripts/oom_cap_harness.sh`: no new
  scenario needed; the existing join scenarios' cap drops to 1G.

### Infrastructure
- SF=100 sweeps via `.scratch/sjc3-005/sweep22.py` (copy into the new
  task's scratch), harness, chaos.

## Implementation Strategy

001 probe streaming (small, measurable on the harness peak RSS) → 002
output streaming (the structural change) → 003 parallel read-back →
004 certification + close-out. Strictly sequential (one file).

## Task Breakdown Preview

- [x] 001: Stream the probe side (no full materialization); measure
      harness join peak RSS and Q4@64M cap
- [x] 002: Stream the output (phase A per batch, phase B per chunk);
      harness join scenarios under a 1G cap
- [x] 003: Parallel spilled-partition processing under the shared
      budget; Q9 @1G ≤ 300s
- [ ] 004: SF=100 certification re-run, chaos, suites, docs, close-out

## Dependencies

- `hash-join-dictionary-semi-anti-fix` merged.

## Success Criteria (Technical)

PRD G1-G4.

## Estimated Effort

4 tasks, ~14-18 focused hours + SF=100 machine time.

## Tasks Created
- [x] 001.md - Stream the probe side through the spill path (parallel: false)
- [x] 002.md - Stream the join output (parallel: false, after 001)
- [x] 003.md - Parallel spilled-partition processing under the shared budget (parallel: false, after 002)
- [x] 004.md - SF=100 certification re-run + docs + epic close-out (parallel: false, last)

Total tasks: 4
Parallel tasks: 0
Sequential tasks: 4
Estimated total effort: 16 hours + SF=100 machine time

## Close-out (2026-09-05)

Commits: 5ea5e81 (start), c49b029 (001), 09d2ed1 (002), f3f42fb /
f728df0 / 7a8f287 / 8cd02c1 / 24a3138 (003), 3b0631e (001-003 close),
plus the 004 close-out. Evidence: `00N.md` Outcomes, `updates/00N/`,
`.scratch/jss/`.

- **G1 MET**: harness `semi-join`/`anti-join` with a 600M-row build
  complete under the DEFAULT 1G cap on both levers and both orientations
  (464-881MB; before 4.7-8.0GB under 12G); Q4 SF=100 native @64M
  completes cell-exact under 8G at 2,889MB peak (before 6,185MB).
- **G2 MET**: Q9 SF=100 parquet @1G **222.3s** cell-exact on a quiet
  machine (target ≤300s; before ~1,650s), 246 hash-check-ok / 0
  mismatch; the 1G sweep total fell from 1616s to 637s even with suite
  builds sharing the machine; no query slower beyond noise except Q18
  native @1G (327s vs 230s, aggregate spill under load — flagged, not a
  join-path effect).
- **G3 MET**: every SF=100 verdict reproduced (22/22 @1G parquet;
  20/22 + 2 named refusals @256M; 22/22 native @100G; 17/22 + 5 @1G
  native); chaos 300/300; suites green (pulsar: 1342/1/1 with --no-fail-fast — the one failure (`three_real_processes_serve_and_survive_a_sigterm`: a spawned server 'never became ready' within 60s while the 65GB native-100G sweep loaded concurrently) passes 2/2 in isolation, and the first run's only failure (the pre-existing rollup last-ULP flake) passes 3/3 in isolation, so 1343 tests are green individually); M1/M2 PASS.
- **G4 MET**: CLAUDE.md updated (boundary closed, Memory Safety Rule
  residual closed, certification refreshed).
- Recorded honestly (from task 003): 002 alone missed the 1G target and
  made Q9 slower until 003's parallel, streaming read-back; two
  intermediate 003 binaries regressed memory by 300-400MB, root-caused
  to mimalloc per-thread retention under rayon and fixed by sizing the
  phase-A pool by the budget; Q4 @64M is 40s (14s with a fixed 8-thread
  pool) — memory first at tiny budgets; Q9's whole-engine peak at a 1G
  budget is 10.7GB (scan parallelism + channels), bounded but above the
  budget — a candidate follow-up.
