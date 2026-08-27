---
name: spill-join-correctness-2
description: Follow-on hardening of SpillableHashJoinExec's spill path -- a new, concrete root-cause hypothesis, a real OOM hole, a fault-injection test harness, and three known sibling bugs
status: completed
created: 2026-08-27T07:44:44Z
updated: 2026-08-27T14:30:00Z
---

> **Status note (2026-08-27).** `completed` reflects the epic running its
> full course and closing (all 5 tasks closed, each meeting its OWN
> acceptance criteria), NOT that this PRD's own headline goal — root-cause
> and fix the archived `spill-join-correctness` epic's main wrong-answer
> bug — was reached. **That bug remains OPEN and unfixed — root cause
> still unconfirmed**, exactly matching the precedent set by the prior
> `spill-join-correctness` PRD's own status note. What this epic actually
> delivered: task 001's hash/derived-state-consistency hypothesis, in its
> literal form, was NOT confirmed — but the same "instrument, don't just
> re-read" investigation caught something structurally adjacent, and
> arguably more serious, in the act: the spill-directory-collision bug the
> prior epic characterized as "fails loudly, never silently wrong" is
> actually capable of a SILENT WRONG ANSWER under concurrent-process
> collision, not just a crash — now fixed (PID-embedded default spill
> path). Task 002 closed a real, documented collect-fully-then-decide OOM
> hole (verified with a real kernel OOM-kill pre-fix under a real cgroup
> cap). Task 003 built a permanent fault-injection/differential testing
> harness and ran it for 2,330 trials (8x the prior epic's own 290-trial
> total), 0 mismatches — confidence-tightening, not a new discovery. Task
> 004 fixed the two remaining named sibling bugs (LIMIT not enforced under
> spill; a sort-spill run-file-not-found crash), with zero changes to
> `SpillableHashJoinExec`'s own build/probe/partition-routing code. See
> `.claude/epics/archived/spill-join-correctness-2/epic.md`'s close-out
> and `CLAUDE.md`'s "Mutation: QA close-out" section for the full picture
> before picking the main bug back up.

# PRD: spill-join-correctness-2

## Executive Summary

The `spill-join-correctness` epic (closed 2026-08-25) reliably reproduced
a real, low-rate (~0.34% pooled), silent wrong-answer bug in
`SpillableHashJoinExec`'s spill path, fixed a real, separate O(n²)
slowness bug, and closed with the wrong-answer bug's root cause
unconfirmed — per the epic's own explicit "no guess-fixes" gate, no fix
was attempted. This session's modern-OLAP research synthesis
(`.claude/plans/research/2026-08-27-modern-olap-research-synthesis.md`,
§2.6) surfaced a genuine production analog the prior epic did not have:
a real, merged Trino bug fix (PR #25892) caused by spilling using one
hash-generator implementation and unspilling using a *different* one,
silently grouping values incorrectly — the same operator class, the
same "spill-write vs. spill-read state mismatch" symptom shape, not a
hypothesis the prior epic tested or ruled out. This PRD picks the
investigation back up with that concrete lead, alongside independently
valuable hardening the research also surfaced: a real OOM hole in the
spill path's own admission control, a fault-injection testing
methodology to replace ad hoc trial repetition, and three already-found,
already-named sibling bugs the prior epic characterized but did not fix.

## Problem Statement

Four independent, real problems live in or around
`SpillableHashJoinExec`'s spill path (`src/physical/operators/
spillable.rs`), all documented with evidence in the archived
`spill-join-correctness` epic:

1. The headline wrong-answer bug itself — open, root cause unconfirmed.
2. `execute()` collects the ENTIRE build side into memory before ever
   checking whether it should spill — a documented hole where an
   oversized build side can OOM before the spill decision even runs.
3. No systematic testing methodology exists for the spill/unspill
   boundary specifically — the prior epic's own investigation was
   limited by an expensive (140-291s pre-fix, 3-6s post-fix) manual
   repro loop, and its own "best remaining hypotheses" list names a
   downsized synthetic repro as the highest-leverage unattempted next
   step.
4. Three sibling bugs found during the prior epic's characterization
   sweep, all real, none fixed: a spill-directory collision for
   co-located `serve` processes, `LIMIT` not enforced under spill for
   `ORDER BY...LIMIT` queries, and a sort-spill run-file-not-found
   crash.

## User Stories

**As the engine's maintainer**, I want the still-open wrong-answer bug
investigated with a concrete, well-motivated hypothesis rather than
further open-ended trial-and-error, and I want an honest outcome either
way.
- Acceptance: the hash/derived-state-consistency hypothesis (below) is
  checked with direct evidence (instrumentation, not inference). If
  confirmed, a minimal, targeted fix lands with a regression test that
  fails without it. If not confirmed, this is reported as a complete,
  valid outcome — per the prior epic's own "no guess-fixes" precedent,
  no fix is attempted against an unconfirmed mechanism.

**As someone running a query whose build side happens to be larger than
available memory**, I want the engine to spill before it OOMs, not
after collecting the whole build side into memory first.
- Acceptance: the admission-control/spill decision is made without
  requiring the full build side to be memory-resident first — a build
  side that would OOM under the old collect-first behavior now spills
  cleanly instead.

**As a future engineer investigating a spill-path bug**, I want a fast,
reusable way to hunt for spill/unspill correctness issues, not a
140-291-second manual repro loop.
- Acceptance: a fault-injection/differential testing harness exists that
  can force a spill at chosen points during build/probe and assert
  row-count/checksum invariants against a non-spilling reference
  execution of the same query, cheaply enough to run hundreds of trials.

**As an operator running multiple `serve` processes on one host, or a
user running `ORDER BY...LIMIT`/large sorts under memory pressure**, I
want the three already-identified sibling bugs fixed.
- Acceptance: each of the three bugs (spill-directory collision,
  LIMIT-under-spill, sort-spill crash) has a regression test that fails
  without the fix and passes with it.

## Functional Requirements

1. Investigate whether any derived/cached value used during build/probe
   — join-key hash, partition-routing hash, dictionary-encoding state,
   or any other value computed once and relied on again later — is
   guaranteed identical between the in-memory-path computation and the
   value recomputed after reading the same data back from a spill file.
   This is THE concrete, new, well-motivated hypothesis this PRD exists
   to test (see Trino PR #25892 in the research synthesis).
2. Fix the collect-fully-then-decide OOM hole with proper
   streaming/incremental size tracking that can trigger the spill
   decision before the whole build side is resident — a two-phase
   reservation discipline (reserve/possibly-spill, then a
   guaranteed-spill-free allocation phase, per Photon's documented
   pattern) is the reference design, not a mandate to copy it exactly.
3. Build a fault-injection/differential testing harness at the
   spill/unspill boundary: force a spill at chosen points during
   build/probe, compare against a non-spilling reference execution of
   the identical query (row-count and/or checksum invariants).
4. Fix the three sibling bugs, each with its own regression test.

## Non-Functional Requirements

- **No guess-fixes.** If the hash/derived-state hypothesis can't be
  confirmed with direct evidence, no fix is attempted against it — this
  mirrors the prior epic's own explicit, successful precedent.
- **Cell-exact validation always**, against an independent DuckDB
  oracle where applicable, never row-count-only.
- **Memory safety never regresses.** The OOM fix must not introduce a
  new unbounded-memory path of its own.
- Every build through `scripts/claude-safe-build.sh`.

## Success Criteria

- G1: the hash/derived-state-consistency hypothesis gets an explicit,
  evidenced confirmed/not-confirmed verdict — not a guess either way.
- G2: if confirmed, a minimal, targeted fix lands with a failing-without-
  it regression test; if not confirmed, this is reported as a complete,
  honest outcome per the "no guess-fixes" precedent.
- G3: the collect-fully-then-decide OOM hole is fixed, verified with a
  real oversized-build-side query that previously OOM'd and now spills
  cleanly.
- G4: a reusable fault-injection/differential testing harness exists and
  is used for at least this epic's own investigation, cheap enough to
  run at least an order of magnitude more trials than the prior epic's
  manual repro loop could afford.
- G5: all three sibling bugs fixed with regression tests; full suite
  green; docs updated (`CLAUDE.md`'s spill-related sections, the
  archived `spill-join-correctness` epic's own residue list).

## Constraints & Assumptions

- Builds directly on the archived `spill-join-correctness` epic's own
  findings (`.claude/epics/archived/spill-join-correctness/`) — read its
  Outcome sections before starting, don't re-derive what's already
  established (the ~0.34% rate, the disproven re-execution hypothesis,
  the O(n²) fix already shipped, the exact sibling-bug details).
- The wrong-answer bug is confirmed NOT native-table-specific and
  confirmed reachable via distributed (scatter) execution — this PRD's
  scope is the general spilling join mechanism, not a native-table-only
  fix.

## Out of Scope

- General spill-path architecture changes beyond what's needed for the
  OOM fix (e.g. a full streaming rewrite of the spill path) — named as
  future work by the prior epic, not attempted here unless the
  hash-consistency investigation's own findings require it.
- Deterministic simulation testing (FoundationDB-style) — the research
  synthesis's own top-ranked correctness technique, but a large
  investment; the fault-injection harness (functional requirement 3) is
  the cheaper, directly-actionable version this PRD builds instead.
- Any change to `SpillableHashAggregateExec`'s or `ExternalSortExec`'s
  own separate spill mechanisms, beyond the sort-spill crash fix
  (functional requirement 4) — the join spill path is this PRD's focus.

## Dependencies

- `src/physical/operators/spillable.rs` — the sole file the core
  mechanism lives in.
- `.claude/epics/archived/spill-join-correctness/` — the prior epic's
  full investigation, read before starting.
- `.claude/plans/research/2026-08-27-modern-olap-research-synthesis.md`
  §2.6 — the Trino/Photon analogs and the fault-injection methodology
  pointer.
- `scripts/claude-safe-build.sh` for every build.
