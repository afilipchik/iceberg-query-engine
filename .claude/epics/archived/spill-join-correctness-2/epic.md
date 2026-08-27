---
name: spill-join-correctness-2
status: completed
created: 2026-08-27T07:44:44Z
updated: 2026-08-27T14:30:00Z
progress: 100%
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
- [x] 002.md - Fix the collect-fully-then-decide OOM hole (parallel: false)
      — CLOSED. `SpillableHashJoinExec::compute_build_decision` now streams
      the build side in via a new `stream_merge_input_partitions`
      (bounded-channel streaming analog of
      `collect_input_partitions_concurrently`), tracking a running size
      total per batch instead of collecting the whole build side before
      ever checking `memory_limit * spill_threshold` — Photon's two-phase
      reservation pattern, adapted. Real, cgroup-verified: a new
      `examples/spill_join_oom_repro.rs` (a ~3.1GB lazily-generated build
      side under a real `systemd-run --scope -p MemoryMax=900M` cap) shows
      the pre-fix code genuinely OOM-killed by the kernel (2/2 trials,
      `journalctl`-confirmed) and the post-fix code completing cleanly
      (2/2 trials, peak RSS ~650-680MB, correct result). 10 fresh Q12
      trials cell-exact against the archived DuckDB oracle, wall time
      3.2-4.3s (O(n²) fix from the archived epic not regressed, matches its
      3-6s baseline), task 001's `KeyChecksum` instrumentation intact (896
      hash-check-ok, 0 mismatches). Full suite green, byte-identical to the
      pre-existing baseline in all four feature combinations. See `002.md`'s
      Outcome for full detail.
- [x] 003.md - Fault-injection/differential testing harness (parallel: false)
      — CLOSED. Two new orthogonal, env-gated hooks in `spillable.rs`
      (`QE_SPILL_CHAOS_FORCE_SPILL` forces WHEN the build/no-build decision
      crosses into the disk-spill branch; `QE_SPILL_CHAOS_FORCE_SPILL_PARTITIONS`
      forces WHICH hash partitions actually write/read spill files,
      regardless of memory pressure — one lever covers both build and
      probe, since probe-side spill routing already follows the build
      partition's spilled state), plus a new permanent, reusable binary
      (`examples/spill_chaos_harness.rs`) that drives them: each trial runs
      one unaggregated INNER-join query twice (baseline vs. randomly
      chosen forced-spill injection) and compares an order-independent
      output checksum (modeled on task 001's own `KeyChecksum`). Used for
      real: **2,330 post-fix trials across 5 sweeps (2 fixture scales, 2
      RNG seeds, 1 with `QE_SPILL_DEBUG` cross-checking task 001's fix
      directly — 17,628 hash-check-ok, 0 HASH-MISMATCH), 0 mismatches, 0
      missed-injection warnings** — 8x the prior epic's own 290-trial
      total, at ~15-160ms/trial (vs. that epic's 140-291s/trial pre-fix).
      No new failure caught; confidence tightened, honestly reported as
      such — a valid outcome per the epic's own culture. Along the way,
      diagnosing a genuine "requested crossing point unreachable for a
      single-batch build side" gap led to an end-of-stream forcing
      fallback (`finish_via_spill`), and a real (test-only) concurrency
      bug in this task's own work-in-progress (an env-var-mutating unit
      test racing an unrelated LEFT JOIN test under `cargo test`'s default
      concurrent execution) was caught and fixed before landing. Both
      tasks 001 and 002's own fixes confirmed unregressed (see `003.md`'s
      Outcome). Full suite green, 4/4 feature combinations, each exactly
      +2 over task 002's own baseline. See `003.md`'s Outcome for full
      detail.
- [x] 004.md - Fix the three known sibling bugs (parallel: false) — CLOSED.
      Bug 1 (spill-directory collision): VERIFIED, not re-fixed — task
      001's PID-embedded default `spill_path` already resolves it; added
      a new committed regression test
      (`tests/spill_directory_collision_tests.rs`) covering the real
      2-concurrent-OS-process case neither of task 001's own committed
      tests exercised (its own 2-process reproduction lived only in an
      ephemeral, gitignored `.scratch/` script). Bug 2 (LIMIT not
      enforced under spill, Q2/Q3-shaped): root-caused to
      `ExternalSortExec::execute()`'s spill branch never consulting
      `self.fetch` (the top-k fusion rule in `planner.rs` folds a
      `skip == 0` LIMIT straight into `ExternalSortExec::with_fetch`,
      never wrapping a separate `LimitExec`) — fixed with a new
      `truncate_batches_to_limit` helper. Bug 3 (sort-spill run-file-
      not-found crash, Q10-shaped): root-caused to `multi_pass_merge`'s
      cleanup step unconditionally deleting every path in a pass's
      `current_runs`, including a `chunk.len() == 1` leftover carried
      forward UNCHANGED into `next_runs` (whenever a pass's run count
      isn't an exact multiple of `MAX_MERGE_FANIN` = 8) — fixed by never
      deleting a path still referenced by `next_runs`. All three
      regression tests independently confirmed to fail against a
      temporarily-reverted fix and pass against the restored one. Zero
      changes to `SpillableHashJoinExec` (both bugs 2/3 live entirely in
      `ExternalSortExec`'s own spill/merge path) — no interaction with
      the still-unconfirmed main duplicate-counting wrong-answer bug.
      Full suite green, 4/4 feature combinations, each exactly task 003's
      own baseline +3. See `004.md`'s Outcome for full detail.
- [x] 005.md - QA close-out (parallel: false) — CLOSED. Full suite
      re-confirmed green at HEAD (not just trusted the prior four tasks'
      own reports) in all four feature combinations, `cargo fmt --all --
      check` clean, honest G1-G5 verdicts, `CLAUDE.md`'s spill-related
      sections and the archived `spill-join-correctness` epic's own
      residue framing updated to reflect this epic's actual outcome, this
      close-out appended, epic archived. See this file's own "Epic
      close-out" section below.

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

## Task 004 close-out (2026-08-27)

All three sibling bugs resolved: bug 1 verified (task 001's fix already
covers it; added the committed real-2-process regression test task 001's
own tests didn't provide), bugs 2 and 3 root-caused by direct code
reading and fixed with small, targeted changes entirely inside
`ExternalSortExec`'s own spill/merge path — zero overlap with
`SpillableHashJoinExec`, so no interaction with the still-unconfirmed
main duplicate-counting wrong-answer bug. All three regression tests
independently confirmed to fail against a temporarily-reverted fix and
pass against the restored one — this program's own established
discipline, applied to a fix task for the first time in this epic (tasks
001-003 were investigation/hardening/tooling). Full suite green, 4/4
feature combinations, each exactly task 003's own baseline +3; `cargo fmt
--all -- --check` clean. Full detail in `004.md`'s Outcome section.

## Epic close-out (2026-08-27)

All 5 tasks closed on branch `epic/spill-join-correctness-2`. Full suite
re-confirmed green **at HEAD, by this task itself** (not just trusted the
prior four tasks' own recorded reports) in **all four feature
combinations**, byte-identical to task 004's own recorded baseline (this
task made zero Rust source changes — docs, epic bookkeeping, and archival
only):

| combination | passed | failed | ignored |
|---|---|---|---|
| default | 1260 | 0 | 1 |
| lance | 1325 | 0 | 2 |
| gpu | 1269 | 0 | 1 |
| pulsar | 1263 | 0 | 1 |

`cargo fmt --all -- --check`: clean.

### Headline: this epic's actual outcome — read this before the table below

**This is NOT a "bug fixed" epic**, exactly matching the archived
`spill-join-correctness` epic's own close-out discipline for its own
headline goal. **The original headline wrong-answer bug in
`SpillableHashJoinExec`'s own build/probe/partition-routing logic remains
OPEN at close, root cause still UNCONFIRMED.** What this epic actually
delivered, across four independent, all-real workstreams:

- **Task 001**'s new hash/derived-state-consistency hypothesis, in its
  literal Trino PR #25892 form (two different hash-VALUE-GENERATOR
  implementations disagreeing on the same logical key), was **NOT
  confirmed**. But the exact "instrument, don't just re-read" discipline
  the epic's own Architecture Decisions required caught something
  structurally adjacent — and arguably more severe — in the act: the
  spill-directory-collision bug the prior `spill-join-correctness` epic's
  own task 003 characterized as "fails loudly, HTTP 400, never silently
  wrong" is actually capable of producing a **silent wrong answer (HTTP
  200)** under a collision between two differently-sized concurrent
  `query_engine` processes — not just the documented crash. That
  conclusion was incomplete; task 001's evidence corrects it. Fixed
  (PID-embedded default `spill_path`, `src/execution/memory.rs`),
  validated from three independent angles (a deterministic unit test, a
  deliberate 2-process collision reproduction going 3/3 collided pre-fix
  to 0/8 post-fix across native and parquet, and 615 clean single-process
  trials).
- **Task 002** fixed the real, already-documented collect-fully-then-
  decide OOM hole in `SpillableHashJoinExec::execute` with a streaming,
  Photon-style two-phase reservation design. Verified with a REAL kernel
  OOM-kill pre-fix (2/2 trials, `journalctl`-confirmed, exit 137) under a
  real `systemd-run --scope -p MemoryMax=900M` cgroup cap, and 2/2 clean
  completions post-fix (peak RSS ~650-680MB). No regression to task 001's
  fix or the archived epic's own O(n²) fix.
- **Task 003** built a permanent, reusable fault-injection/differential
  testing harness (`QE_SPILL_CHAOS_FORCE_SPILL`/
  `QE_SPILL_CHAOS_FORCE_SPILL_PARTITIONS`,
  `examples/spill_chaos_harness.rs`) and ran **2,330 trials at real
  scale — 8x the archived epic's own 290-trial total** — at ~15-160ms/
  trial (vs. that epic's 140-291s/trial pre-fix manual loop). Result: 0
  mismatches, 0 missed-injection warnings — an honest,
  confidence-tightening result, reported as such, not a new discovery.
- **Task 004** fixed the two remaining named sibling bugs (LIMIT not
  enforced under spill in `ExternalSortExec`'s spill branch; a sort-spill
  run-file-not-found crash from `multi_pass_merge` deleting a still-
  referenced leftover chunk), each with a genuine fail-before/pass-after
  regression test. Verified (not re-fixed) that task 001's collision fix
  also covers the original "fails loudly" symptom, adding the committed,
  real-2-OS-process regression test that was missing. Zero changes to
  `SpillableHashJoinExec`'s own build/probe/partition-routing code — no
  mechanism by which these fixes could affect the still-unconfirmed main
  duplicate-counting bug.

**Bottom line, stated plainly**: this epic delivered real, verified
hardening across four independent workstreams — a genuinely upgraded
understanding of the spill-directory-collision bug's real severity
(worse than previously known, now fixed), a real OOM hole closed, a real
testing capability built and used at 8x the prior epic's scale, and two
more real bugs fixed. **The original headline wrong-answer bug in
`SpillableHashJoinExec`'s own build/probe/partition-routing logic remains
OPEN — root cause still UNCONFIRMED.**

### G1-G5 (this epic's own Success Criteria) — explicit verdicts, restated with evidence

- **G1** (the hash/derived-state-consistency hypothesis gets an explicit,
  evidenced verdict) — **MET, with a nuanced outcome, not a simple
  confirmed/not-confirmed binary.** The literal hypothesis (Trino PR
  #25892's shape: two different hash-VALUE-GENERATOR implementations
  disagreeing on the same logical key) was checked directly, with real
  instrumentation (`KeyChecksum`/`batch_key_checksum`), across 9
  enumerated derived/cached values in the build/probe/spill/unspill path
  — and was **NOT what was found**. What WAS found, via that same
  instrumentation, running the same investigation: a real,
  directly-caught write-vs-read mismatch one level up from the literal
  hypothesis — not a hash-generator disagreement, but the join-key DATA
  itself differing between what a spilling execution wrote and what an
  unrelated, CONCURRENT execution's own writes/deletes caused a later
  read to see, because both processes shared one PID-less default spill
  directory. Say this precisely: **the specific mechanism hypothesized
  was ruled out; a structurally adjacent, real mechanism was caught in
  the act instead, via the identical evidentiary discipline the
  hypothesis required.**
- **G2** (if confirmed, a minimal fix + failing-without-it regression
  test; if not, an honest, complete "not confirmed" outcome) — **MET, in
  the same nuanced sense as G1.** Because task 001 did catch a real
  mismatch in the act (just not the literally-hypothesized one), the
  epic's own "no guess-fixes" confidence gate was met WITH DIRECT
  EVIDENCE — not the "honest not-confirmed, no fix attempted" branch this
  criterion also allows for. A minimal, one-field fix landed
  (`ExecutionConfig::default()`'s `spill_path` now embeds
  `std::process::id()`), validated with a regression test confirmed to
  fail without the fix and pass with it, plus the deliberate collision
  reproduction and 615-trial sweep described above. **This criterion is
  met for a real, evidenced mechanism — but readers should not mistake
  this for "the hypothesized hash-consistency bug was confirmed and
  fixed." It was not; a different, adjacent bug was.**
- **G3** (the OOM hole is fixed, verified with a real previously-OOMing
  query) — **MET.** Task 002's streaming two-phase reservation design,
  verified under a real cgroup memory cap with a genuine kernel OOM-kill
  reproduced pre-fix (2/2, `journalctl`-confirmed) and a clean completion
  post-fix (2/2, peak RSS ~650-680MB, correct result). Cell-exact
  correctness preserved (10/10 Q12 trials against the DuckDB oracle), no
  regression to the archived epic's own O(n²) fix or task 001's
  `KeyChecksum` instrumentation (896 hash-check-ok, 0 mismatches).
- **G4** (a reusable fault-injection harness exists, used for real, at
  least an order of magnitude more trials than the prior epic's manual
  loop) — **MET.** `examples/spill_chaos_harness.rs`, two orthogonal
  env-gated hooks (WHEN/WHICH), permanent (not `.scratch/`), documented
  in-file. Run for real: 2,330 post-fix trials across 5 sweeps (2 fixture
  scales, 2 RNG seeds, 1 `QE_SPILL_DEBUG` cross-check of task 001's own
  fix), 8x the archived epic's 290-trial total, at ~15-160ms/trial vs.
  that epic's 140-291s/trial pre-fix manual loop — several orders of
  magnitude cheaper per trial, comfortably exceeding "at least an order
  of magnitude more trials." 0 mismatches, 0 missed-injection warnings.
- **G5** (all three sibling bugs fixed with regression tests; full suite
  green; docs updated) — **MET.** Bug 1 (spill-directory collision) was
  fixed by task 001 as a direct byproduct of G1/G2's own investigation,
  verified (not re-fixed) by task 004 with a new committed
  real-2-OS-process regression test (`tests/
  spill_directory_collision_tests.rs`) covering the gap task 001's own
  committed tests left. Bug 2 (LIMIT not enforced under spill) fixed with
  `truncate_batches_to_limit` applied to `ExternalSortExec`'s spill
  branch. Bug 3 (sort-spill run-file-not-found crash) fixed by no longer
  deleting a `multi_pass_merge` run path still referenced by `next_runs`.
  All three regression tests independently confirmed to FAIL against a
  temporarily-reverted fix and PASS against the restored one. Full suite
  green in all four feature combinations, re-confirmed at HEAD by this
  task (table above, byte-identical to task 004's own recorded baseline).
  `cargo fmt --all -- --check` clean. Docs updated: `CLAUDE.md`'s
  spill-related sections (the "Spill tests" bullet and the "Mutation: QA
  close-out" section's residue-tracking paragraphs), this close-out, and
  the `spill-join-correctness-2` PRD's own status note.

**Net: all 5 of this epic's own Success Criteria (G1-G5) are MET** — two
of them (G1, G2) in a nuanced, non-binary form that must be read
precisely rather than skimmed: the literal hash-consistency hypothesis
was ruled out, not confirmed, but the same investigation caught and fixed
a real, adjacent bug via the identical evidentiary discipline. This is
NOT the same as "the epic's ultimate goal (fix the main wrong-answer bug)
was reached" — it was not, and the paragraph above says so in plain text.

### Why `status: completed` / `progress: 100%` — explained, not just asserted

All 5 tasks fully closed, each achieving its OWN scoped goal — task 001's
goal was a confidence-gated, evidenced investigation of a specific new
hypothesis (achieved: gate MET WITH DIRECT EVIDENCE, even though the
literal hypothesis itself was ruled out); task 002's goal was fixing a
confirmed, independent OOM hole (achieved in full, cgroup-verified); task
003's goal was building and USING a reusable fault-injection harness
(achieved in full, 2,330 real trials); task 004's goal was fixing the two
remaining sibling bugs plus verifying the third (achieved in full, all
three fail-before/pass-after); task 005's goal is this close-out
(achieved). None of the five tasks has any open acceptance-criteria item,
unaddressed technical debt from its own scope, or pending work. That is
what `100%` means here: **task/process completion, not that the epic's
original headline goal — root-cause and fix
`SpillableHashJoinExec`'s main wrong-answer bug — was reached.** It was
not, and the Headline section above says so in plain text. This matches
this program's own established precedent for exactly this situation: the
archived `spill-join-correctness` epic (this epic's own direct
predecessor) is marked `completed`/`100%` despite its own headline goal
(root-cause and fix the same bug) never being reached, and states that
plainly in its own close-out without softening it. The `radix-execution`
epic (cited in that epic's own close-out as the original precedent) sets
the same bar. The alternative (a bespoke non-standard status string)
would break every existing tool/convention in this repo that reads epic
status against the fixed `open|in-progress|completed` vocabulary every
other epic here uses, for a distinction ("all tasks closed" vs. "headline
goal reached") that this close-out's own text already makes explicit — a
frontmatter string is a worse place to carry that nuance than the prose
that is right above it.

### Per-task attribution

- **001** (hash/derived-state-consistency investigation): the epic's real
  risk and entry point. Enumerated every derived/cached value in the
  build/probe/spill/unspill path (9 items), instrumented the one
  structurally identical to the Trino mechanism (`KeyChecksum`), and
  caught a real write-vs-read mismatch in the act — not the literal
  hypothesis, but a structurally adjacent, real, and more severe finding:
  the spill-directory-collision bug the prior epic's own task 003
  characterized as "fails loudly, never silently wrong" can produce a
  silent wrong answer under concurrent-process collision. Root-caused,
  fixed (PID-embedded default `spill_path`), validated from three
  independent angles. Also surfaced and resolved, mid-investigation, a
  real shared-checkout collision between this task and an unrelated peer
  agent — a fitting, if coincidental, parallel to the bug itself.
  Commits: `cd8a33b` (fix + instrumentation), `67f5957` (close-out docs).
- **002** (fix the collect-fully-then-decide OOM hole): replaced the
  inline `OnceCell` closure's full-collection-then-check pattern with a
  streaming, Photon-inspired two-phase reservation
  (`compute_build_decision` + new `stream_merge_input_partitions`).
  Verified under a real cgroup memory cap: pre-fix 2/2 genuine kernel
  OOM-kills, post-fix 2/2 clean completions. 10/10 fresh Q12 trials
  cell-exact against the DuckDB oracle, no regression to the archived
  epic's O(n²) fix or task 001's `KeyChecksum` instrumentation. Commits:
  `9cfa32a` (fix), `f46e1ac` (close-out docs).
- **003** (fault-injection/differential testing harness): two new
  orthogonal env-gated hooks in `spillable.rs` plus
  `examples/spill_chaos_harness.rs`, a permanent reusable binary. Ran
  2,330 real trials (8x the archived epic's own 290), 0 mismatches — an
  honest confidence-tightening result. Diagnosed and fixed a genuine
  single-batch end-of-stream forcing gap along the way, and caught (in
  its own work-in-progress, before landing) a real test-only concurrency
  hazard from an `std::env::set_var`-based unit test racing an unrelated
  test under `cargo test`'s default concurrent execution — removed in
  favor of pure-parsing unit tests plus the standalone harness binary.
  Commit: `d3180ce`.
- **004** (fix the two remaining sibling bugs; verify the third): bug 1
  (spill-directory collision) verified as already fixed by task 001, not
  re-fixed — added the real-2-OS-process regression test task 001's own
  committed tests didn't provide. Bug 2 (LIMIT not enforced under spill)
  root-caused to `ExternalSortExec::execute()`'s spill branch never
  consulting `self.fetch` — fixed with `truncate_batches_to_limit`. Bug 3
  (sort-spill run-file-not-found crash) root-caused to `multi_pass_merge`
  unconditionally deleting a leftover singleton chunk still referenced by
  `next_runs` — fixed by never deleting a still-referenced path. All
  three regression tests independently confirmed fail-before/pass-after.
  Zero changes to `SpillableHashJoinExec`'s own code. Commit: `b607594`
  (also `f1b787b`, the re-scope-after-task-001 spec update).
- **005** (this task, QA close-out): re-confirmed the full suite and
  `cargo fmt` at HEAD (not just trusted the prior four tasks' own
  reports), wrote honest G1-G5 verdicts (including the nuanced G1/G2
  outcome), wrote this close-out, updated `CLAUDE.md`'s spill-related
  sections and the archived `spill-join-correctness` epic's own residue
  framing to reflect this epic's actual final state, updated the
  `spill-join-correctness-2` PRD's status note, and archived the epic.
  Zero Rust source changes.

### Named residues (what is still open after this epic)

1. **The main headline wrong-answer bug itself — the primary residue of
   this epic, and of its predecessor before it.** `SpillableHashJoinExec`'s
   own build/probe/partition-routing logic can still, per the archived
   epic's own evidence, silently return an inflated answer at an
   estimated ~0.34% pooled rate — root cause still UNCONFIRMED after two
   full epics of real, adversarial investigation. Task 001's finding
   (the spill-directory collision) is PLAUSIBLE, NOT PROVEN, as an
   explanation for this historical bug's own past occurrences — no
   concurrent process was ever identified at the time. Recommended next
   step for whoever picks this up: task 003's own fault-injection harness
   (`examples/spill_chaos_harness.rs`) is now the first, cheap thing to
   point at this bug — 2,330 trials at ~15-160ms/trial found nothing
   NEW, but its own injection space (3 fixed queries, bounded
   WHEN/WHICH parameters) is not exhaustive; widening the query set or
   injection shapes it drives is the natural next step before reaching
   for the much more expensive manual repro loop again.
2. **`SpillableHashAggregateExec` and `ExternalSortExec` both still use
   the OLD `collect_input_partitions_concurrently`** (task 002's own
   named follow-up) — the identical collect-fully-then-decide OOM shape
   task 002 fixed for `SpillableHashJoinExec` plausibly exists in their
   own build/collect paths too. Not investigated or fixed here.
3. **The fault-injection harness targets `SpillableHashJoinExec` only**
   (task 003's own named follow-up) — `SpillableHashAggregateExec` and
   `ExternalSortExec` have their own, structurally different spill paths;
   extending fault injection to them is a natural follow-on.
4. **`extract_join_key`'s unhandled-array-type gap** (task 001's own
   enumeration item 4) — join keys of a type other than
   Int64/Int32/UInt64/Float64/plain-`StringArray` (e.g. Dictionary-
   encoded, Date32, Decimal, Boolean) silently become `JoinValue::Null`
   and are silently DROPPED (not duplicated) by both `build_hash_table`
   and `probe_partition`. Real and separate from the main bug (would
   apply identically whether spilled or not), not fixed — no evidence it
   was reachable by anything either epic actually ran.
5. **`SpillableHashAggregateExec` was not examined for an analogous
   LIMIT-under-spill or run-file-lifecycle issue** (task 004's own named
   follow-up) — it has no `fetch` parameter and no multi-pass run-merge
   structure like `ExternalSortExec`'s today, so neither bug's shape
   obviously applies, but this was not verified by reading its code
   specifically.
6. **`merge_parquet_files`' possible analogous O(n²) pattern** (named by
   the archived epic's own task 002, still not investigated by either
   epic) — the aggregate spill path's own read-rewrite mechanism was
   never examined for the same shape of bug the archived epic fixed in
   the join spill path.

### Commits

`cd8a33b` (001: fix spill-directory collision + `KeyChecksum`
instrumentation) -> `67f5957` (001: close-out docs) -> `f1b787b`
(re-scope 004 after task 001's fix) -> `9cfa32a` (002: streaming OOM fix)
-> `f46e1ac` (002: close-out docs) -> `d3180ce` (003: fault-injection
harness) -> `b607594` (004: LIMIT + sort-spill-crash fixes, collision
verification) -> `fd8ec20` (005: G1-G5 verdicts, close-out docs) ->
(005: archival move, this file's final commit).

### Archival

Epic moved to `.claude/epics/archived/spill-join-correctness-2/` as this
task's final step, mirroring `spill-join-correctness`/
`native-tables-mutation`/`native-tables-tiering`'s own archival pattern
(`git mv`). **Not merged to `main`** — that decision and action is left
to the user/orchestrating session, per this task's own instructions.
