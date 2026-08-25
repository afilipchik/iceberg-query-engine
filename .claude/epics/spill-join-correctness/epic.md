---
name: spill-join-correctness
status: in-progress
created: 2026-08-24T14:19:55Z
updated: 2026-08-25T02:00:00Z
progress: 75%
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

## Re-scope after task 001 (2026-08-24)

Task 001 closed with the wrong-answer root cause genuinely not found
(see its Outcome), so per this epic's own "no guess-fixes" gate, task
002 as originally scoped (fix the wrong-answer bug) correctly does not
proceed. Task 001 also, however, landed a SEPARATE finding with its own
confirmed root cause: `append_to_parquet` in `spillable.rs` does a full
read-entire-file + rewrite-to-temp + atomic-rename on every spill
append, an O(n²)-ish disk I/O pattern that plausibly explains most of
the 140-291s runtime even on the 20/21 CORRECT runs. That's a real,
independently fixable, low-risk problem discovered as a side effect of
hunting the correctness bug — sitting on it until the wrong-answer
mystery resolves (uncertain timeline, possibly never, given task 001's
own effort/result ratio) wastes a known win. Orchestrating-session
decision, with the user's explicit sign-off: split the remaining work
along this line rather than block everything on the harder problem.

- **Task 002 (re-scoped)**: fix the O(n²) `append_to_parquet` pattern.
  Confirmed root cause, does NOT require the wrong-answer mechanism to
  be understood. Validated by wall-clock time on task 001's repro
  (140-291s → target well under that) plus cell-exact correctness
  (unchanged) on both the repro and the full suite.
- **Task 003 (re-scoped)**: blast-radius CHARACTERIZATION of the
  wrong-answer bug specifically — not a fix attempt (none is possible
  without a confirmed mechanism; the gate still applies to this half).
  Determine whether it's native-table-specific or reachable via plain
  Parquet sources too, and which other spilling TPC-H queries are
  exposed. Sequenced after 002 (not logically dependent on it, but
  002's fix should make the many large spilling-join trials this sweep
  needs to run far cheaper in wall-clock time).
- **Wrong-answer root cause**: remains open, undiscovered, and
  explicitly NOT re-attempted by either 002 or 003. It stays documented
  as a live, tracked, low-frequency (~4.8% observed) correctness risk
  in `CLAUDE.md` until someone picks the investigation back up — most
  likely via task 001's own named next step (a downsized synthetic
  repro to make iteration cheap enough for hundreds of trials).

## Task Breakdown Preview

- 001: Reliable reproduction + instrumented root-cause investigation
  (parallel: false, gates everything; may end the epic if root cause
  isn't confidently found) — CLOSED, root cause not found, see re-scope
  note above.
- 002 (re-scoped): Fix the confirmed O(n²) `append_to_parquet` spill
  slowness (parallel: false, depends on 001; does NOT depend on the
  wrong-answer root cause)
- 003 (re-scoped): Blast-radius characterization of the (still
  unfixed) wrong-answer bug — parquet vs. native, which queries;
  explicitly not a fix attempt (parallel: false, depends on 002) —
  CLOSED. Headline: parquet, forced into the identical spill code path,
  is statistically indistinguishable from native (0/80 wrong each, this
  task); only Q12 spills at any realistic SF=10/SF=100 memory-limit;
  spilling joins DO occur in distributed (scatter) execution, 0/40 wrong,
  but a distinct spill-directory-collision bug was found there (reported,
  not fixed); epic-wide cumulative 1/290 wrong (0.34%, 95% CI
  [0.01%,1.91%]) — tighter and lower than task 001's own standalone
  4.76% point estimate, NOT evidence of a fix. See 003.md's Outcome for
  full detail, including two more distinct bugs found (LIMIT not
  enforced under spill; a sort-spill run-file crash), both unrelated to
  this bug and both reported, not fixed.
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
  reliably reproduce" conclusion with a real explanation). MET — 21
  runs, 4.8% wrong, see 001's Outcome.
- G2: if reproduced, root cause identified with runtime evidence and
  fixed, with a regression test that fails without the fix. NOT MET for
  the wrong-answer mechanism specifically (root cause not found; see
  re-scope note) — this is an accepted, documented partial outcome per
  the epic's own gate, not a failure to try.
- G3: explicit verdict on whether the slowness shares the duplication's
  root cause. MET (partial, evidenced verdict) — slowness fully
  explained by "computation ran ~2x" via the chaos test; wrongness is
  NOT explained by the same mechanism in its simplest form. See 001's
  Outcome.
- G4: broad sweep confirms no sibling instance survives, or names what
  it found. RE-SCOPED and MET — task 003 characterized the wrong-answer
  bug's blast radius instead of sweeping for survivors of a fix (none
  exists yet). Result: only Q12 spills at any realistic SF=10/SF=100
  memory-limit (both sources); parquet, forced into the identical spill
  code path, is statistically indistinguishable from native (0/80 wrong
  each) — no evidence the bug is native-specific; spilling joins DO occur
  in distributed (scatter) execution, 0/40 wrong there too; epic-wide
  cumulative 1/290 (0.34%). Three NEW, unrelated bugs found and reported
  (not fixed, per the gate): a spill-directory collision across
  concurrent same-host `serve` processes, a LIMIT-not-enforced-under-spill
  bug, and a sort-spill run-file crash. See 003.md's Outcome for full
  detail.
- G5: full suite green; native-tables-mutation's documentation updated.
- G6 (added at re-scope): the confirmed O(n²) `append_to_parquet`
  slowness is fixed, with before/after wall-clock evidence on task
  001's repro and no correctness regression (cell-exact, full suite).
  MET — task 002: 140-291s (task 001's 21-run baseline) -> 3-5s per
  run (this task's own 40-trial validation, 30 fresh + 10 warm), a
  ~40-90x speedup; 40/40 cell-exact vs the DuckDB oracle; full suite
  green in all four feature combinations, each exactly +2 over the
  native-tables-mutation task 006 baseline (the two new tests this
  task added, zero regressions): default 1190/0/1, lance 1255/0/2,
  gpu 1190/0/1, pulsar 1193/0/1. See 002's Outcome for full detail.

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
- [x] 001.md - Reliable reproduction + instrumented root-cause investigation (parallel: false)
- [x] 002.md - (re-scoped) Fix confirmed O(n²) `append_to_parquet` spill slowness (parallel: false)
- [x] 003.md - (re-scoped) Blast-radius characterization of the wrong-answer bug (parallel: false)
- [ ] 004.md - QA close-out — full suite, cell-exact, docs, epic close (parallel: false)

Total tasks: 4
Parallel tasks: 0
Sequential tasks: 4
Estimated total effort: genuinely uncertain, dominated by task 001
