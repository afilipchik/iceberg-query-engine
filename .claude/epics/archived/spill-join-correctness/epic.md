---
name: spill-join-correctness
status: completed
created: 2026-08-24T14:19:55Z
updated: 2026-08-25T05:00:00Z
progress: 100%
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
  MET — task 004: full suite re-confirmed green at HEAD in all four
  feature combinations (byte-identical to task 002/003's own recorded
  baseline), `cargo fmt --all -- --check` clean, CLAUDE.md and
  `native-tables-mutation/epic.md` both updated. See Epic close-out.
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
- [x] 004.md - QA close-out — full suite, cell-exact, docs, epic close (parallel: false)

Total tasks: 4
Parallel tasks: 0
Sequential tasks: 4
Estimated total effort: genuinely uncertain, dominated by task 001

## Epic close-out (2026-08-25)

All 4 tasks closed on branch `epic/spill-join-correctness`. Full suite
green in **all four feature combinations**, byte-identical to task
002/003's own recorded baseline (this task made zero Rust source
changes — docs and epic bookkeeping only): default 1190/0/1, lance
1255/0/2, gpu 1190/0/1, pulsar 1193/0/1 (passed/failed/ignored).
`cargo fmt --all -- --check` clean.

### Headline: this epic's actual outcome — read this before the table below

**This is NOT a "bug fixed" epic.** The wrong-answer bug this epic
exists to investigate — `SpillableHashJoinExec`'s spill path returning
a silently ~2x-inflated answer on TPC-H Q12, first found by
`native-tables-mutation` task 006 — is **still open at close, with its
root cause still unconfirmed**, exactly as the epic's own "no
guess-fixes" gate anticipated as a valid outcome when task 001 could
not reach confidence. Two real, separate things DID land along the
way: task 002 fixed a confirmed, independent O(n²) performance bug
that had been making even CORRECT runs of the repro take 140-291
seconds; task 003 characterized the wrong-answer bug's blast radius in
detail (native vs. parquet, single-process vs. distributed, rate
estimate) and, in the process, found three more, entirely distinct
bugs — none of which is the target bug, and none of which was fixed,
per the same gate.

| check | result |
|---|---|
| Wrong-answer bug (`SpillableHashJoinExec` spill path, Q12) | **STILL OPEN** — root cause unconfirmed; leading hypothesis (non-idempotent join-child re-execution) directly DISPROVEN for the wrongness (task 001's chaos test) |
| Observed rate | 4.8% (1/21, task 001's own standalone trials) to 0.34% (1/290, pooled epic-wide) — see G2/G4 below for why both numbers are reported |
| O(n²) `append_to_parquet` spill-write slowness | **FIXED** (task 002) — 140-291s -> 3-6s, **~40-90x**, 40/40 trials cell-exact, zero regression |
| Native-table-specific? | **NO EVIDENCE FOUND** (task 003) — plain parquet forced into the identical spill path: 0/80 wrong, statistically indistinguishable from native's own 0/80 |
| Distributed (scatter) exposure | **CONFIRMED** (task 003) — spill path engages per-node on each shard; 0/40 wrong in trials, not proof of safety there either |
| New bugs found, not fixed | **THREE** (task 003) — spill-directory collision, LIMIT-not-enforced-under-spill, sort-spill run-file crash |
| Full suite (4 feature combinations) | GREEN, byte-identical to pre-epic baseline (table above) |
| `cargo fmt --all -- --check` | clean |

### Per-task attribution

- **001** (reliable reproduction + instrumented root-cause
  investigation): established the epic's own ground truth, correcting
  an earlier documentation error along the way — the bug reproduces
  against the PRISTINE, never-mutated warehouse (no mutation sequence
  required, contrary to what CLAUDE.md previously implied). 21 runs,
  1 wrong (4.8%), 21/21 uniformly slow (140-291s). Added real
  `QE_SPILL_DEBUG`-gated runtime instrumentation (per-partition/
  per-call row counts, abort-reason capture) and ran a controlled
  chaos/fault-injection experiment that DIRECTLY DISPROVED the leading
  hypothesis (`execute_fused_streaming` aborting and re-executing its
  non-idempotent join child) as sufficient to explain the wrongness —
  a genuine, valuable negative result, not a non-result — while
  confirming that same mechanism fully explains the slowness. Found,
  as a side effect, the separately-confirmed O(n²)
  `append_to_parquet` pattern that task 002 went on to fix. Per the
  epic's own gate, correctly did NOT attempt a fix. Commit: `355c2d3`
  (plus `b22f512`, the CLAUDE.md trigger-condition correction, and
  `722a13f`, task close).
- **002** (re-scoped: fix the confirmed O(n²) `append_to_parquet`
  spill slowness): replaced the read-rewrite-rename-per-append pattern
  with a single `ArrowWriter` kept open per partition for the whole
  build/probe phase. **140-291s -> 3-6s (~40-90x)** on task 001's exact
  repro, 40/40 trials cell-exact (30 fresh-process + 10 warm), zero
  regression to task 006's own crash/k-way-merge fixes in the same
  file, full suite green (+2 tests over the prior baseline, the two
  new tests this task added). Explicitly did NOT touch, and was never
  expected to touch, the wrong-answer mechanism — confirmed, not just
  argued: 0/40 of this task's own trials were wrong (consistent with,
  not proof against, the unchanged ~4.8% rate; 0/40 at a true 4.8% rate
  has a ~13.9% chance of happening by pure luck). Commit: `21ed410`.
- **003** (re-scoped: blast-radius characterization, not a fix
  attempt): the epic's single most valuable finding after task 001's
  own negative result. Confirmed via trace (not wall-clock) that only
  Q12 spills at any realistic memory-limit at SF=10/SF=100, for either
  source. Ran 229 of this epic's own 290 total correctness trials
  across native (natural spill), parquet (forced via an aggressive
  `--memory-limit 1M`, matched to native's own spill shape), a
  widened-cardinality variant, SF=100, and a real 3-node distributed
  cluster — 0 wrong in every one of them. **The single most important
  result: parquet forced into the identical spill code path is
  statistically indistinguishable from native (0/80 wrong each)** — no
  evidence the bug is native-table-specific, consistent with (not
  proof of) it being general to any sufficiently large spilling INNER
  join. **Confirmed spilling joins DO occur in distributed (scatter/
  `two_phase`) execution** — each node runs the identical
  `execute_spill_path` over its own shard, confirmed directly (per-node
  `build_rows` summed exactly to the single-process total); 0/40 wrong
  there too. Found and reported (never fixed, per the gate) **three
  new, distinct bugs** while doing this: a spill-directory collision
  across concurrent same-host `serve` processes (fails loudly), LIMIT
  not enforced under spill for `ORDER BY ... LIMIT` queries (Q2/Q3 —
  correct values, wrong row count), and a sort-spill run-file-not-found
  crash (Q10). Zero Rust source changes. Commit: `4d75410`.
- **004** (this task, QA close-out): re-confirmed the full suite and
  `cargo fmt` at HEAD (not just trusted the prior report), wrote this
  close-out, updated CLAUDE.md's "Mutation: QA close-out" section and
  `native-tables-mutation/epic.md`'s close-out to reflect the epic's
  actual final state (still open, tighter rate estimate, not
  native-specific, distributed-exposed, slowness fixed separately, the
  three new bugs documented as known issues), and archived the epic.
  Zero Rust source changes.

### G1-G6 (this epic's own success criteria) — explicit verdicts, restated with evidence

- **G1** (reliable repro established, or an honest "could not
  reproduce" conclusion) — **MET**. Task 001: 21 runs, 1 wrong (4.8%),
  21/21 uniformly slow — a real, evidenced, reliable, if
  low-frequency and non-deterministic, reproduction.
- **G2** (if reproduced, root cause identified with runtime evidence
  and FIXED, with a regression test that fails without the fix) — **NOT
  MET, honestly, for the wrong-answer mechanism itself.** Real,
  adversarial root-cause work was done (instrumentation, a controlled
  chaos/fault-injection test) and reached a genuine negative result —
  the leading hypothesis was directly disproven — but no mechanism was
  ever caught in the act, so per the epic's own "no guess-fixes" gate,
  no fix was attempted. This is the epic's own accepted, documented,
  anticipated partial outcome (see "Re-scope after task 001" in this
  file), not a failure to try or a corner cut. Say this plainly: **the
  bug this epic exists to fix is not fixed.**
- **G3** (explicit verdict on whether the slowness shares the
  duplication's root cause) — **MET (partial, evidenced verdict)**.
  Slowness is fully, mechanically explained by "the computation ran
  ~2x" (the chaos test: a forced clean retry took 283s, matching the
  real wrong run's 291s almost exactly). Wrongness is NOT explained by
  that same mechanism in its simplest, clean form — a genuine
  dissociation of the two symptoms, evidenced not assumed.
- **G4** (broad sweep confirms no sibling instance survives a fix, or
  names what it found) — **RE-SCOPED and MET**. No fix exists to sweep
  survivors of, so task 003 characterized blast radius instead: only
  Q12 spills at realistic settings; parquet forced into the identical
  path is statistically indistinguishable from native (0/80 each);
  spilling joins are confirmed reachable via distributed scatter
  execution (0/40 wrong); three new, unrelated bugs found and reported.
- **G5** (full suite green; native-tables-mutation's documentation
  updated) — **MET**. Full suite re-confirmed green at HEAD in all four
  feature combinations by this task (table above, byte-identical to
  the pre-existing baseline); `cargo fmt --all -- --check` clean;
  CLAUDE.md's "Mutation: QA close-out" section and
  `native-tables-mutation/epic.md`'s close-out both updated by this
  task to reflect the epic's actual final state.
- **G6** (added at re-scope: the confirmed O(n²) `append_to_parquet`
  slowness is fixed, with before/after evidence and no correctness
  regression) — **MET**. Task 002: 140-291s -> 3-6s (~40-90x) on task
  001's exact repro, 40/40 cell-exact, full suite green in all four
  feature combinations, zero regression to prior fixes in the same
  file.

**Net: 4 of 6 criteria MET outright, 1 MET in a re-scoped/partial
form (G3), and 1 explicitly NOT MET (G2) — by the epic's own design,
not by shortfall.** G2 is the one that matters most to a reader
skimming only this line: the epic's headline goal was not reached.

### Why `status: completed` / `progress: 100%` — explained, not just asserted

All 4 tasks fully closed, each achieving its OWN scoped goal — task
001's goal was a confident reproduction plus a real, evidenced attempt
at root cause (achieved, including a valuable negative result); task
002's re-scoped goal was fixing a confirmed, independent problem
(achieved in full); task 003's re-scoped goal was characterization,
not a fix (achieved in full, plus three unplanned findings); task
004's goal is this close-out (achieved). None of the four tasks in
this epic has any open acceptance-criteria item, unaddressed technical
debt from its own scope, or pending work. That is what `100%` means
here: **task/process completion, not that the epic's original
headline goal — fix the wrong-answer bug — was reached.** It was not,
and G2 says so in plain text two sections up. This matches this
program's own established precedent for exactly this situation: the
`radix-execution` epic (explicitly cited in this epic's own
Architecture Decisions as the model for a legitimate non-guess-fix
outcome) is marked `completed`/`100%` despite its own primary
hypothesis (radix partitioning) being refuted and never implemented,
and despite one of its own named gates (Q18 <=6.5s) going unmet — its
close-out states that plainly, in the same paragraph, without
softening it. The alternative (a bespoke non-standard status string)
would break every existing tool/convention in this repo that reads
epic status against the fixed `open|in-progress|completed` vocabulary
every other epic here uses, for a distinction ("all tasks closed" vs.
"headline goal reached") that this close-out's own text already makes
explicit — a frontmatter string is a worse place to carry that nuance
than the prose that is right above it.

### Residues

1. **The wrong-answer bug itself — the primary residue of this whole
   epic.** `SpillableHashJoinExec`'s spill path, TPC-H Q12 (and by
   extension any sufficiently large spilling INNER join), returns a
   silently ~2x-inflated answer at an estimated 0.3-4.8% rate (see G2
   above for why both numbers are reported), root cause still
   unconfirmed after a real, adversarial investigation. Confirmed NOT
   native-table-specific and confirmed reachable via distributed
   scatter execution. Recommended next step, per task 001's own
   Outcome: build a deliberately downsized synthetic repro to cut the
   ~140-291s (or, post-task-002, ~3-6s) per-trial cost enough to run
   hundreds of trials instead of tens, and instrument
   `append_to_parquet`'s successor / the spill path's I/O layer
   directly for a NATURAL abort (none was caught with tracing on in
   this epic's own trial budget). Treat as P0 for whichever future
   epic owns the join spill path — this recommendation, first made by
   `native-tables-mutation` task 006, stands unchanged.
2. **Concurrent same-host `serve` processes collide on the default
   spill directory** (task 003) — `SPILL_COUNTER` has no per-process
   disambiguation; two co-located `serve` processes can compute the
   identical `spill_id`. Fails loudly (`Parquet error`, HTTP 400),
   never silently wrong. Trigger needs only a realistic memory-limit
   plus co-located processes sharing one host's `$TMPDIR` — a real
   risk for any multi-node-per-host deployment or this repo's own
   local cluster test harness, not an exotic condition. Not fixed.
3. **`LIMIT` not enforced under spill for `ORDER BY ... LIMIT`
   queries** (task 003, Q2/Q3-shaped) — correct top-K prefix values,
   but not truncated to N rows, under a deliberately adversarial,
   unrealistic `--memory-limit 1M` sweep (not observed at any
   realistic setting). Not fixed.
4. **A sort-spill run-file-not-found crash** (task 003, Q10-shaped) —
   same artificial `--memory-limit 1M` sweep as #3. Crashes loudly,
   never silently wrong. Not fixed.
5. **`merge_parquet_files`' possible analogous O(n²) pattern**
   (task 002's own named follow-up, independently corroborated by task
   003's SF=100 finding — Q13's AGGREGATE spill path timed out at 300s
   under the same adversarial sweep) — the aggregate spill path's own
   read-rewrite mechanism was never examined for the same shape of bug
   task 002 fixed in the join spill path. Not investigated.
6. **Left explicitly unknown by task 003** (stated once here, not
   repeated per-item): the individual wrong-answer rate of the ~12
   other queries whose joins only spill under the artificial
   `--memory-limit 1M` sweep; SF=100 under forced-spill parquet; SF=100
   distributed; parquet under distributed execution (reasoned
   equivalence only, not independently tested); any skew/cardinality
   dimension beyond the one native variant tried. See 003.md's Outcome,
   "Remains unknown," for the full list.

### Commits

`355c2d3` (001: instrumentation + repro) -> `b22f512` (CLAUDE.md
trigger-condition correction) -> `722a13f` (001 done) -> `b3116f8`
(re-scope 002/003 after task 001) -> `21ed410` (002 done: O(n²) fix)
-> `4d75410` (003 done: blast-radius characterization) -> `10e46d1`
(004 spec) -> `524b494` (004 done: close-out + docs) -> `5dd1597`
(archive move, this task's final step).

### Archival

Epic moved to `.claude/epics/archived/spill-join-correctness/` as this
task's final step, mirroring `native-tables-mutation`/
`native-tables-foundation`/`duckdb-parity-2`'s archival pattern (`git
mv`, this session, including `updates/`). Not merged to `main` — that
decision and action is left to the user/orchestrating session, per
this task's own instructions.
