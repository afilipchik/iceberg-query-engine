---
name: spill-join-correctness-2
status: in-progress
created: 2026-08-27T07:44:44Z
updated: 2026-08-27T09:30:00Z
progress: 20%
prd: .claude/prds/spill-join-correctness-2.md
github: (will be set on sync)
---

# Epic: spill-join-correctness-2

## Overview

Follow-on to the archived `spill-join-correctness` epic. That epic
reliably reproduced the wrong-answer bug (~0.34% pooled rate), fixed a
real, separate O(n²) slowness bug, disproved its leading root-cause
hypothesis (non-idempotent join-child re-execution), and closed with the
wrong-answer bug's root cause unconfirmed — correctly, per its own
"no guess-fixes" gate. This epic picks the investigation back up with a
genuinely new lead this session's research surfaced (a real Trino bug fix
with the identical symptom shape), alongside independently valuable
hardening: a real OOM hole, a fault-injection testing methodology, and
three already-found sibling bugs.

## Architecture Decisions

- **No guess-fixes, same gate as the prior epic.** Task 001 (the
  hash/derived-state-consistency investigation) does not get to skip
  straight to a fix — it must reach real, direct evidence (instrumentation
  catching the actual mismatch in the act) before task 001 or any later
  task attempts a change to the duplicate-counting mechanism itself. If
  it can't, that's reported as a complete, honest outcome, exactly like
  the prior epic's own task 001.
- **The other three workstreams (OOM fix, fault-injection harness,
  sibling bugs) do NOT depend on task 001's outcome.** Each has its own
  independent, already-understood root cause or design target. Sequenced
  after task 001 purely for wall-clock reasons (the harness built in task
  003 is useful FOR task 001's own investigation if task 001 needs more
  than instrumentation alone — check whether task 001 needs it before
  building it fresh) and to avoid concurrent edits to the same file
  (`spillable.rs`) across parallel agents.
- **Fault-injection over deterministic simulation.** The research
  synthesis's top-ranked correctness technique (FoundationDB-style
  simulation testing) is a large investment; this epic builds the
  cheaper, directly-actionable version instead — forced spill at chosen
  points, differential comparison against a non-spilling reference.

## Technical Approach

### Hash/derived-state-consistency investigation (task 001)
Read `.claude/epics/archived/spill-join-correctness/` in full first —
especially task 001's Outcome (the disproven re-execution hypothesis,
what instrumentation already exists behind `QE_SPILL_DEBUG`) and task
003's Outcome (the blast-radius characterization). Instrument (don't
just re-read) the build/probe/spill/unspill sequence to directly compare
any derived/cached value used before and after a spill round-trip for
the SAME logical data — the join-key hash, partition-routing hash,
dictionary-encoding state, or anything else computed once and trusted
again later. Direct evidence or an honest "not confirmed" — no guessing.

### OOM fix (task 002)
`SpillableHashJoinExec::execute`'s documented hole: the entire build side
is collected into memory before the spill decision is ever made. Replace
with streaming/incremental size tracking that can trigger spilling before
full materialization — Photon's two-phase reservation pattern (reserve/
possibly-spill, then a guaranteed-spill-free allocation phase) is the
reference design.

### Fault-injection harness (task 003)
A reusable mechanism to force a spill at chosen points during build/
probe and compare against a non-spilling reference execution of the
identical query (row-count/checksum invariants) — cheap enough to run
hundreds of trials, unlike the prior epic's manual repro loop.

### Sibling bugs (task 004)
Three already-characterized, unfixed bugs from the prior epic's own
findings: spill-directory collision (add per-process disambiguation,
e.g. PID, to the default spill path), `LIMIT` not enforced under spill
for `ORDER BY...LIMIT`, and a sort-spill run-file-not-found crash — each
with its own regression test.

### QA close-out (task 005)
Full suite, docs, honest G1-G5 verdicts, epic close.

## Task Breakdown Preview

- 001: Hash/derived-state-consistency investigation — the new hypothesis,
  gated, no-guess-fixes (parallel: false, entry point)
- 002: Fix the collect-fully-then-decide OOM hole (parallel: false,
  independent of 001's outcome)
- 003: Fault-injection/differential testing harness (parallel: false,
  independent of 001's outcome, but may reuse instrumentation task 001
  builds)
- 004: Fix the three known sibling bugs (parallel: false, independent of
  001's outcome)
- 005: QA close-out (parallel: false, depends on everything)

Total tasks: 5
Estimated total effort: genuinely uncertain for task 001 (same honest
framing the prior epic used for its own task 001) — the other four are
better-bounded.

## Dependencies

- `src/physical/operators/spillable.rs` — the sole file the core
  mechanism lives in.
- `.claude/epics/archived/spill-join-correctness/` — read in full before
  starting; don't re-derive what's already established.
- `.claude/plans/research/2026-08-27-modern-olap-research-synthesis.md`
  §2.6 — the Trino/Photon analogs and fault-injection methodology
  pointer.
- `scripts/claude-safe-build.sh` for every build.

## Success Criteria (Technical)

- G1: hash/derived-state hypothesis gets an explicit, evidenced verdict.
- G2: if confirmed, a minimal fix + failing-without-it regression test;
  if not, an honest, complete "not confirmed" outcome.
- G3: the OOM hole is fixed, verified with a real previously-OOMing query.
- G4: a reusable fault-injection harness exists, used for real.
- G5: all three sibling bugs fixed with regression tests; full suite
  green; docs updated.

## Estimated Effort

- 001: genuinely uncertain — L, possibly XL. This epic's real risk.
- 002: M.
- 003: M.
- 004: S-M.
- 005: S-M.

## Tasks Created
- [x] 001.md - Hash/derived-state-consistency investigation (parallel: false)
      — CLOSED. Confidence gate MET WITH DIRECT EVIDENCE: the new
      `KeyChecksum` write-vs-read instrumentation caught a real
      spill-directory collision (two concurrent `query_engine` processes
      sharing the same PID-less default `spill_path`) producing silent
      wrong answers, not just the "loud crash only" `spill-join-
      correctness` task 003 previously characterized. Fixed
      (PID-disambiguated default spill path, `src/execution/memory.rs`),
      validated via a regression test (fails-without/passes-with), a
      deliberate controlled 2-process collision reproduction (3/3
      collided pre-fix, 0/8 collided post-fix across native+parquet), and
      615 clean single-process trials post-instrumentation. Full suite
      green, 4/4 feature combinations. **NOTE for task 004**: this fix
      already resolves task 004's own "spill-directory collision" sibling
      bug target — task 004 should verify and cross-reference rather than
      re-fix the same field. See `001.md`'s Outcome for full detail.
- [ ] 002.md - Fix the collect-fully-then-decide OOM hole (parallel: false)
- [ ] 003.md - Fault-injection/differential testing harness (parallel: false)
- [ ] 004.md - Fix the three known sibling bugs (parallel: false) — NOTE:
      one of its three targets (spill-directory collision) is already
      fixed by task 001; task 004 should verify/cross-reference, not
      duplicate.
- [ ] 005.md - QA close-out (parallel: false)

Total tasks: 5
Parallel tasks: 0
Sequential tasks: 5
Estimated total effort: genuinely uncertain, dominated by task 001

## Task 001 close-out (2026-08-27)

Closed with a landed fix — NOT the "reliably reproduced, not yet
root-caused" outcome the epic's own Architecture Decisions anticipated as
equally valid. The hash/derived-state-consistency hypothesis, in its
literal Trino PR #25892 form (two different hash-VALUE-generator
implementations disagreeing on the same logical key), was not what was
found. What WAS found, via the exact "instrument, don't just re-read"
discipline the epic's Architecture Decisions require: a structurally
adjacent mismatch — the join-key data ITSELF, not just its hash, differed
between what a spilling execution wrote and what an unrelated, CONCURRENT
execution's own writes/deletes caused a later read to see, because both
processes shared one PID-less default spill directory. Caught directly
(a `HASH-MISMATCH` trace line showing ~10x row inflation, correlated with
a wrong final query answer in the same trial), root-caused, fixed with a
one-line change, and validated from three independent angles (a
deterministic unit test, a deliberate self-contained collision
reproduction, and a 615-trial clean sweep). Full detail, including the
mid-investigation worktree-isolation incident (an almost poetic parallel
to the bug itself — two unrelated concurrent agents colliding on one
shared resource), in `001.md`'s Outcome section.
