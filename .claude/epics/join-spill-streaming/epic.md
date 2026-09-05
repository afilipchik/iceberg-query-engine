---
name: join-spill-streaming
status: in-progress
created: 2026-09-05T01:38:00Z
updated: 2026-09-05T03:03:56Z
progress: 0%
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

- [ ] 001: Stream the probe side (no full materialization); measure
      harness join peak RSS and Q4@64M cap
- [ ] 002: Stream the output (phase A per batch, phase B per chunk);
      harness join scenarios under a 1G cap
- [ ] 003: Parallel spilled-partition processing under the shared
      budget; Q9 @1G ≤ 300s
- [ ] 004: SF=100 certification re-run, chaos, suites, docs, close-out

## Dependencies

- `hash-join-dictionary-semi-anti-fix` merged.

## Success Criteria (Technical)

PRD G1-G4.

## Estimated Effort

4 tasks, ~14-18 focused hours + SF=100 machine time.

## Tasks Created
- [ ] 001.md - Stream the probe side through the spill path (parallel: false)
- [ ] 002.md - Stream the join output (parallel: false, after 001)
- [ ] 003.md - Parallel spilled-partition processing under the shared budget (parallel: false, after 002)
- [ ] 004.md - SF=100 certification re-run + docs + epic close-out (parallel: false, last)

Total tasks: 4
Parallel tasks: 0
Sequential tasks: 4
Estimated total effort: 16 hours + SF=100 machine time
