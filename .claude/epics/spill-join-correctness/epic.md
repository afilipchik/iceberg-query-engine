---
name: spill-join-correctness
status: in-progress
created: 2026-08-24T14:19:55Z
updated: 2026-08-24T14:19:55Z
progress: 0%
prd: .claude/prds/spill-join-correctness.md
github: (will be set on sync)
---

# Epic: spill-join-correctness

## Overview

Root-cause and fix (or honestly bound) a silent wrong-answer bug in
`SpillableHashJoinExec`'s spill path — one of this engine's ALWAYS-used
join operators. Discovered via `native-tables-mutation`, general-purpose
in scope. Staged as: get a reliable repro → instrument and root-cause →
fix → adversarially validate → document. No stage after the first is
attempted until the one before it succeeds with real confidence.

## Architecture Decisions

- **Reliable repro before anything else, no exceptions.** The
  orchestrating session's own re-verification (documented in CLAUDE.md)
  did NOT reproduce the wrong answer with a simple post-CREATE `DELETE`
  — only the full `CREATE→INSERT→DELETE→UPDATE` sequence the original
  finding used did. Task 001 must nail down what specifically triggers
  it before task 002 can proceed; guessing at a fix against an
  unreliable repro risks shipping something that looks fixed but isn't.
- **Instrumentation over inspection.** `native-tables-mutation` task 006
  already read `execute_spill_path`/`build_with_partitioning`/
  `probe_with_spilling`/`process_spilled_partition` end to end and
  concluded the bookkeeping "reads as mutually-exclusive-by-construction
  on inspection" — i.e., reading the code again is unlikely to find what
  reading the code already failed to find once. Task 001 adds real
  runtime counters/tracing (per-partition row-in vs row-out counts, a
  per-batch processed-marker, whatever the actual investigation finds it
  needs) to CATCH the duplication in the act.
- **Slowness and duplication investigated together, verdict stated
  explicitly.** A partition or run processed twice would mechanically
  explain both symptoms at once — check this hypothesis directly (e.g.,
  does per-partition instrumentation show a partition counted or
  processed more than once?) before assuming they're separate.
- **No guess-fixes.** If task 001 can't reach real confidence in the
  mechanism, task 002 does not proceed this epic — the epic closes with
  an honest, evidenced "reliably reproduced, not yet root-caused" state,
  matching this program's own standing culture (e.g., `radix-execution`'s
  kill-switch, `native-tables-mutation` task 006's own original judgment
  call not to guess-fix this same bug).
- **Fix validated against BOTH the original repro AND a broad sweep.**
  The suspected generality (any sufficiently large spilling INNER join,
  not just native tables) must be checked against plain-parquet sources
  too — a fix that only helps native tables while the same mechanism
  remains reachable via parquet would be an incomplete fix.

## Technical Approach

### Investigation tooling
New, temporary (or permanent, if useful going forward — task 001's call)
instrumentation in `spillable.rs`: per-partition/per-run row-count
tracking through the build/spill/probe/merge sequence, gated behind an
env var following this file's own existing `HJ_TIMING`/`AGG_TIMING`-
style convention if one doesn't already cover this. A minimal, fast
reproduction (smaller than SF=10/60M-rows if a smaller shape still
triggers it — check this early, since a 150-320s reproduction loop makes
iteration expensive) is worth real effort to find, but do not substitute
a smaller repro for the real one without confirming they hit the same
mechanism.

### Fix
Scoped by whatever task 001 actually finds — likely a duplicate-
processing or duplicate-emission point in the partition/spill/probe
sequence. A minimal, targeted change, not a rewrite, unless the root
cause genuinely requires one (stop and re-scope if so, per the PRD's Out
of Scope).

### Validation
Cell-exact against DuckDB for the original repro; a broad sweep across
join shapes/sizes/source types (native table AND plain parquet) for
other live instances of the same class; full suite; M1/M2 distributed
gates (spilling joins may occur in distributed contexts too).

### Documentation
CLAUDE.md's own "Mutation: QA close-out" section and
`.claude/epics/archived/native-tables-mutation/epic.md`'s close-out both
currently frame this as a live P0 open risk — update both once resolved
(or more precisely bounded).

## Implementation Strategy

1. Reliable repro + root-cause via instrumentation — gates everything;
   may conclude the epic if confidence isn't reached.
2. Fix, validated immediately against the repro.
3. Broad adversarial sweep for sibling instances + the slowness verdict.
4. QA close-out: full suite, cell-exact, docs, epic close.

## Task Breakdown Preview

- 001: Reliable reproduction + instrumented root-cause investigation
  (parallel: false, gates everything; may end the epic if root cause
  isn't confidently found)
- 002: Fix + immediate validation against the repro (parallel: false,
  depends on 001 reaching a confident root cause)
- 003: Broad adversarial sweep (other shapes/sources) + slowness verdict
  (parallel: false, depends on 002)
- 004: QA close-out — full suite, cell-exact, docs, epic close
  (parallel: false, depends on everything)

Total tasks: 4
Estimated total effort: genuinely uncertain — task 001 alone could be a
full focused session on its own, given the prior investigation's own
"not found by inspection" result.

## Dependencies

- `src/physical/operators/spillable.rs` — the sole file this bug lives
  in, per the prior investigation.
- `CLAUDE.md`'s "Mutation: QA close-out" section and
  `.claude/epics/archived/native-tables-mutation/006.md`'s Outcome
  section — the exact reproduction steps and numbers already on record;
  read both before starting, don't re-derive.
- `scripts/claude-safe-build.sh` for every build.

## Success Criteria (Technical)

- G1: reliable repro established (or an honest, evidenced "could not
  reliably reproduce" conclusion with a real explanation).
- G2: if reproduced, root cause identified with runtime evidence and
  fixed, with a regression test that fails without the fix.
- G3: explicit verdict on whether the slowness shares the duplication's
  root cause.
- G4: broad sweep confirms no sibling instance survives, or names what
  it found.
- G5: full suite green; native-tables-mutation's documentation updated.

## Estimated Effort

- 001: genuinely uncertain — L, possibly XL if the repro itself is hard
  to pin down. This is the epic's real risk.
- 002: S-M once 001 succeeds (a targeted fix, not a rewrite, per scope).
- 003: M (breadth of the sweep matters more than depth per case).
- 004: S-M.
- Total: could be one focused session or could span several, depending
  entirely on task 001 — do not pad estimates here, this is real
  uncertainty, not unwillingness to estimate.

## Tasks Created
- [ ] 001.md - Reliable reproduction + instrumented root-cause investigation (parallel: false)
- [ ] 002.md - Fix + immediate validation against the repro (parallel: false)
- [ ] 003.md - Broad adversarial sweep + slowness verdict (parallel: false)
- [ ] 004.md - QA close-out — full suite, cell-exact, docs, epic close (parallel: false)

Total tasks: 4
Parallel tasks: 0
Sequential tasks: 4
Estimated total effort: genuinely uncertain, dominated by task 001
