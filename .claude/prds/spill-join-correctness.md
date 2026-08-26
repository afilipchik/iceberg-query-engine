---
name: spill-join-correctness
description: Root-cause and fix a silent wrong-answer bug in SpillableHashJoinExec's spill path, discovered via native-tables-mutation but general-purpose and pre-existing
status: completed
created: 2026-08-24T14:19:55Z
updated: 2026-08-25T00:00:00Z
---

> **Status note (2026-08-25).** `completed` reflects the epic running its
> full course and closing (all 4 tasks closed, merged to `main`), NOT
> that this PRD's own headline goal was reached. **The wrong-answer bug
> itself remains OPEN and unfixed — root cause never confirmed.** What
> the epic actually delivered: a reliable repro (4.8%→0.34% rate
> estimate as trials accumulated), a disproven leading hypothesis, a
> real ~40-90x fix for a separate O(n²) slowness issue found along the
> way, and a blast-radius characterization (not native-table-specific,
> confirmed distributed-exposed, three new unrelated bugs found and
> reported unfixed). See `.claude/epics/archived/spill-join-correctness/
> epic.md`'s close-out and `CLAUDE.md`'s "Mutation: QA close-out"
> section for the full picture before picking this back up.

# PRD: spill-join-correctness

## Executive Summary

The `native-tables-mutation` epic's final QA task fixed a crash in
`ExternalSortExec`/`SpillableHashJoinExec`'s spill paths (a Dictionary-
vs-declared-schema mismatch, present since before that epic, simply
never triggered by an existing test). Fixing the crash unmasked a
second, deeper, genuinely distinct problem: TPC-H Q12's spilling INNER
join, once it stopped crashing, completed but returned an answer exactly
**2x too high** (`high_line_count` 707644 vs an independent DuckDB
oracle's 353822), and took ~320 seconds instead of the normal
150-350ms. That task correctly declined to guess-fix a silent-wrong-
answer bug in core join logic under time pressure, documenting it
instead. The orchestrating session independently re-verified before
merging: the extreme slowness reproduced identically twice (~150s,
fresh table and the same table after a real DELETE); the 2x wrong answer
did **not** reproduce in either attempt — narrowing, not clearing, the
risk (a clean 2.0000x ratio does not read as noise).

This PRD is that follow-up. `SpillableHashJoinExec` is one of the
"ALWAYS-used spillable operators" CLAUDE.md's memory-safety rule
mandates for every join in this engine — this is not a narrow, optional
code path.

## Problem Statement

Two entangled open questions, not yet resolved:

1. **Correctness**: does `SpillableHashJoinExec`'s spill path
   (`execute_spill_path`/`build_with_partitioning`/`probe_with_spilling`/
   `process_spilled_partition`) sometimes double-count matched rows? If
   so, under what precise condition — and does it affect plain parquet
   tables too, or only shapes reachable through native tables today?
2. **Performance**: is the ~150-320s runtime (vs. 150-350ms for every
   comparable query) a symptom of the SAME mechanism (e.g., a partition
   or run being processed twice would plausibly explain both the 2x
   count AND roughly-2x-or-more the work), or a second, independent
   pathology?

Both were found via native tables (whose lack of scan-level filter
pushdown pushes larger inputs into the join, and whose disk footprint
differs from parquet's, changing which memory-budget/spill decisions get
made) but neither mechanism inspected so far is native-table-specific —
`build_with_partitioning`/`probe_with_spilling` operate on `RecordBatch`
streams regardless of source. A plain-parquet spilling join at
sufficient scale is the real, general-purpose risk surface.

## User Stories

**As anyone running a sufficiently large join that spills**, I want a
correct answer or a clean, named refusal — never a silently wrong one.
- Acceptance: the exact Q12-at-SF=10-against-a-mutated-native-table
  reproduction (documented in CLAUDE.md's "Mutation: QA close-out"
  section) is used as the primary repro target; if it doesn't reproduce
  reliably on the first attempt, the investigation's own first job is
  finding a reliable trigger before attempting to root-cause anything
  from static reading alone (which already failed once — see Constraints).
- Acceptance: once root-caused, a broad adversarial sweep (varied join
  shapes, sizes, both native-table and plain-parquet sources) confirms
  no sibling instance of the same bug class survives.

**As the engine's maintainer**, I want the extreme slowness understood
and either fixed or clearly explained as a separate, bounded issue — not
left as an unexplained 500-1000x outlier.
- Acceptance: a verdict, with evidence, on whether the slowness and the
  duplication share a root cause.

## Functional Requirements

1. Establish a RELIABLE reproduction of the wrong-answer symptom before
   attempting to fix anything. The orchestrating session's own two
   attempts (fresh native table; the same table after one plain
   `DELETE`) did NOT reproduce it — the original trigger was a full
   `CREATE→INSERT→DELETE→UPDATE` sequence (see CLAUDE.md for the exact
   steps) and may depend on that specific segment/deletion-vector
   history, or on non-deterministic partition-count selection under
   spill pressure. Do not skip this step.
2. Root-cause via runtime instrumentation (counters, tracing, a
   debugger), not static re-reading alone — the prior investigation
   already read `execute_spill_path` end to end and concluded the
   partition/spill bookkeeping "reads as mutually-exclusive-by-
   construction on inspection," meaning the bug is not visible by eye
   and needs to be caught in the act.
3. A precise, minimal fix once (and only once) the mechanism is
   confidently understood — no guess-fixes.
4. Determine whether the ~150-320s runtime shares the duplication's root
   cause or is a separate issue; address or clearly document accordingly.
5. Broad adversarial validation: the bug's suspected generality (any
   sufficiently large spilling INNER join) must be checked against
   plain-parquet sources too, not just native tables, and against
   multiple join shapes/sizes, not just Q12.
6. Update `native-tables-mutation`'s own "live P0" documentation
   (CLAUDE.md, `.claude/epics/archived/native-tables-mutation/epic.md`)
   once this is resolved — remove or correct the open-risk framing.

## Non-Functional Requirements

- **No guess-fixes on a silent-wrong-answer bug.** If root-cause
  investigation does not reach real confidence within a genuinely
  serious, well-resourced attempt, the honest outcome is stopping and
  documenting — exactly the judgment call `native-tables-mutation`'s own
  task 006 already made once. Do not let time pressure produce a
  "probably fixed" that isn't verified.
- **Cell-exact validation, always** — this program's standing rule,
  with more force than usual given the stakes.
- **No bare `cargo build`/`test`/`bench`** — every build through
  `scripts/claude-safe-build.sh`.
- **No regression** to the crash fix or the k-way-merge fix
  `native-tables-mutation` task 006 already shipped — both must stay
  green throughout.

## Success Criteria

- G1: a reliable reproduction of the wrong-answer symptom exists (or an
  honest, evidenced conclusion that the original observation could not
  be reliably reproduced, with the investigation's own best explanation
  for why — e.g., a specific non-determinism identified and named).
- G2: if reproduced, the exact duplication mechanism is identified with
  runtime evidence (not inference), and a fix lands with a regression
  test that fails without it.
- G3: the extreme-slowness question gets an explicit verdict — same root
  cause, or separately investigated and addressed/documented.
- G4: a broad sweep (several spilling join shapes, both source types)
  finds no other live instance of the same bug class, or names what it
  found.
- G5: full suite green in all feature combinations; `native-tables-
  mutation`'s own documentation is updated to reflect the resolved (or
  more precisely understood) state.

## Constraints & Assumptions

- The prior investigation (native-tables-mutation task 006) already
  spent real effort reading the relevant functions end-to-end and did
  NOT find the mechanism by inspection — this PRD assumes static
  reading alone is insufficient and instrumentation/debugging is
  required from the start, not a fallback after inspection fails again.
- The orchestrating session's own reproduction attempts are real,
  negative data points, not just "untested" — factor them into scoping
  the first task rather than re-deriving from zero.
- This may be a genuinely hard, multi-session investigation. The PRD's
  own success criteria explicitly allow for "reliably reproduced but not
  yet fully root-caused, honestly reported" as a valid, if incomplete,
  outcome for a single epic — matching this program's standing culture
  around hard problems.

## Out of Scope

- Any broader rewrite of the spill/partition architecture (e.g., the
  "streaming rewrite of the join spill path" future-work item CLAUDE.md
  already names) — this PRD is a targeted correctness fix, not an
  architecture change, unless the root cause turns out to require one
  (in which case, stop and re-scope rather than silently expanding).
- Distributed-context spilling joins specifically (the M1/M2 gates'
  scope) — check they're unaffected, but this PRD's primary target is
  the single-process path the original finding used.

## Dependencies

- `src/physical/operators/spillable.rs` (`SpillableHashJoinExec`,
  `ExternalSortExec`) — the sole file in question.
- The exact reproduction steps and numbers already documented in
  `CLAUDE.md`'s "Mutation: QA close-out" section and
  `.claude/epics/archived/native-tables-mutation/006.md`'s Outcome
  section — read both in full before starting.
- `scripts/claude-safe-build.sh` for every build.
