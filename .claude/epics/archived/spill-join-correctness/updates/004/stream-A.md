# Stream A: QA close-out — full suite, cell-exact, docs, epic close

Task: `.claude/epics/spill-join-correctness/004.md`
Branch: `epic/spill-join-correctness` (pre-existing, not created by this
task). Depends on 001/002/003, all closed.

## Reading, in the order the task spec required

1. `.claude/epics/spill-join-correctness/004.md` — this task's own spec,
   full acceptance criteria (the "three new residue bugs" item, added
   after the original spec by commit `10e46d1`).
2. `.claude/epics/spill-join-correctness/epic.md` — full epic, "Re-scope
   after task 001" section, Success Criteria G1-G6 (five original + G6
   added at re-scope — the epic has SIX criteria, not five, confirming
   the task spec's own "G1-G5" phrasing in a couple of places is stale
   relative to the epic's own later edit).
3. `001.md`, `002.md`, `003.md` — full Outcome sections, in full, per
   the task's own instruction that these are "the load-bearing source of
   truth for everything you write in this close-out." Key numbers
   extracted: task 001 = 21 trials/1 wrong (4.8%), leading hypothesis
   disproven for wrongness via a chaos test (283s forced-retry -> correct
   answer), fully explains slowness; task 002 = O(n²)
   `append_to_parquet` fix, 140-291s -> 3-6s (~40-90x), 40/40 trials
   correct, +2 tests over baseline; task 003 = 229 trials/0 wrong this
   task (290 epic-wide/1 wrong = 0.34%), parquet-forced-spill 0/80 vs
   native 0/80, distributed scatter 0/40 wrong (confirmed spill path
   DOES engage per-node), three new bugs found.
4. `updates/001/stream-A.md`, `updates/002/stream-A.md`,
   `updates/003/stream-A.md` — skimmed for corroborating detail beyond
   what the Outcome sections already summarized. The Outcome sections
   turned out to be sufficiently thorough (including exact evidence,
   numbers, and reasoning) that no claim in this close-out required
   digging further into the raw logs beyond one specific check (see
   "Precision correction" below, which DID require re-reading task
   003's Outcome closely).
5. `CLAUDE.md` in full — specifically the "Mutation: QA close-out"
   section (search "2x-inflated") and the immediately-following "Current
   limitations" section under "Native Tables," both of which this task
   edits.
6. `.claude/epics/archived/native-tables-mutation/epic.md`'s close-out
   section in full — both as the thing to edit (residue #1) and as the
   house-style reference for this epic's own close-out (per this task's
   own "Technical Details" pointer).

## House-style survey before writing

Before drafting the close-out, surveyed every other epic's frontmatter
in this repo to calibrate the `status`/`progress` decision:

```
grep -H "^status:\|^progress:" .claude/epics/*/epic.md .claude/epics/archived/*/epic.md
```

Every single epic in this repo (14 checked) is `status: completed` /
`progress: 100%`, INCLUDING `radix-execution` — which this epic's own
Architecture Decisions section explicitly cites as its own precedent for
a legitimate non-guess-fix outcome. Read `radix-execution/epic.md`'s own
close-out directly: it refutes its own primary hypothesis (radix
partitioning), never implements it, and states outright that one of its
own named gates (Q18 <=6.5s) was NOT met — yet is marked
`completed`/`100%`, with that fact stated plainly in the same close-out
paragraph, not hidden. This is the closest on-point precedent available
in this repo for the exact question this task's own spec poses
("decide honestly between `status: completed`... vs. something more
explicit"), and it directly supports `completed`/`100%` here too,
provided the close-out text itself states plainly (not just implies)
that the epic's headline goal (fix the bug) was not reached. Adopted
that reading, and said so explicitly in the close-out's own dedicated
subsection ("Why `status: completed` / `progress: 100%` — explained, not
just asserted") rather than leaving the frontmatter number
unexplained, per the task spec's own explicit instruction.

## Full suite + fmt, re-confirmed at HEAD

First attempt (`Bash run_in_background`) mistakenly redirected output to
`/tmp/qe_test_default.log` — a direct violation of CLAUDE.md's own "DO
NOT use /tmp" rule. Caught immediately once the background result
arrived (the harness's own separate output capture briefly showed the
tail of the run, confirming exit 0, before the stray `/tmp` file was
deleted). Corrected for every subsequent run: created
`.scratch/spill_join_004_qa/` (in-repo, matching this program's own
`.scratch/` convention already used by tasks 001-003) and redirected all
four feature-combination logs there instead.

Commands run, exactly matching task 003's own recorded invocations
(`updates/003/stream-A.md`, "Full suite + fmt" section) so this run is a
true apples-to-apples re-confirmation, not a differently-configured one:

```
scripts/claude-safe-build.sh cargo test --release \
  > .scratch/spill_join_004_qa/test_default.log 2>&1

PROTOC=.scratch/tools/protoc/bin/protoc \
  scripts/claude-safe-build.sh cargo test --release --features lance \
  > .scratch/spill_join_004_qa/test_lance.log 2>&1

LD_LIBRARY_PATH=$PWD/.venv/lib/python3.12/site-packages/nvidia/cuda_nvrtc/lib \
  scripts/claude-safe-build.sh cargo test --release --features gpu \
  > .scratch/spill_join_004_qa/test_gpu.log 2>&1

scripts/claude-safe-build.sh cargo test --release --features pulsar \
  > .scratch/spill_join_004_qa/test_pulsar.log 2>&1

cargo fmt --all -- --check
```

`target/release` was still warm from task 003's own build (~2 hours
earlier per `ls -la`), and this task made zero Rust source changes, so
each combination ran in well under a minute (no recompilation needed
beyond incremental test-harness relinking) rather than the multi-minute
cost a cold build would carry — consistent with task 003's own note that
its re-run was "much faster than task 002's own report" for the same
reason.

Results, summed via `grep -oE '[0-9]+ passed; [0-9]+ failed; [0-9]+
ignored' <log> | awk '{p+=$1; f+=$3; i+=$5} END{...}'` (the first attempt
used a broken field-split that miscounted "ok." as a numeric field;
caught and fixed before trusting any number):

| combo | passed | failed | ignored | matches baseline? |
|---|---|---|---|---|
| default | 1190 | 0 | 1 | yes, exact |
| lance | 1255 | 0 | 2 | yes, exact |
| gpu | 1190 | 0 | 1 | yes, exact |
| pulsar | 1193 | 0 | 1 | yes, exact |

All four: `EXITCODE=0`. `cargo fmt --all -- --check`: `FMT_EXIT=0`.

`git status --short` checked after every edit in this task: only
documentation/epic files ever appeared (CLAUDE.md,
`native-tables-mutation/epic.md`, and this epic's own directory) — zero
Rust source files touched, as expected for a QA/docs close-out task.

## Precision correction: the three residue bugs' actual trigger conditions

The task spec's own acceptance-criteria bullet (and the human-written
task description above it) frames all three of task 003's newly-found
bugs as occurring "under an artificial/extreme `--memory-limit` forcing
condition, not normal operation." Re-reading task 003's Outcome section
closely (specifically its "5. Distributed (M1/M2)" subsection) before
copying that framing verbatim showed it is only accurate for TWO of the
three:

- Bugs (2) LIMIT-not-enforced and (3) sort-spill crash: BOTH found under
  "6. Bonus finding: broader 'under extreme memory pressure' sweep" at
  `--memory-limit 1M` — genuinely the artificial/extreme condition.
- Bug (1) spill-directory collision: found under "5. Distributed (M1/M2)
  — confirmed exposed, plus a real surprise," which uses `native @ 40G`
  throughout (the SAME realistic memory-limit task 001 itself used, not
  the adversarial 1M setting) — the actual adversarial ingredient is
  THREE CONCURRENT `serve` processes sharing one host's `$TMPDIR`, a
  distributed-testing-specific condition, not a memory-limit one.

Chose to document each bug's real trigger condition precisely rather
than force-fit all three into the "extreme memory-limit" framing,
because: (a) the task's own acceptance criterion explicitly requires
"trigger condition ... stated honestly," which is a stronger instruction
than matching the human-written summary verbatim; (b) this repo's own
CLAUDE.md has an established, repeated pattern of correcting an earlier
imprecise claim with evidence rather than silently carrying it forward
(the "CORRECTION" callouts already present in the exact sections this
task edits are themselves examples of this pattern); (c) getting bug
(1)'s trigger condition right materially changes its practical severity
read — "only reachable under an artificial extreme setting" understates
a real risk for ordinary multi-node-per-host deployments, which is a
worse error than a small deviation from the handed-down summary.
Recorded this correction explicitly in three places so it can't be
missed or silently reverted: 004.md's own acceptance-criteria checklist
(inline), 004.md's own Outcome section, and the actual CLAUDE.md/epic.md
prose itself (which states bug (1)'s trigger correctly, not "extreme
memory-limit").

## Documentation edits made, in order

1. `CLAUDE.md`, "Mutation: QA close-out" section: inserted a new
   dated paragraph ("Epic close-out (`spill-join-correctness` epic,
   tasks 002-004, closed 2026-08-25)") immediately after the existing
   task-001 "CORRECTION" paragraph and before the pre-existing "Full
   suite, all four feature combinations" table — continuing that
   section's own established pattern of appending chronological,
   dated corrections/updates adjacent to the specific claim they
   update, rather than only at the very end of the section. Covers:
   O(n²) fix confirmed (cite ~40-90x); wrong-answer bug still open,
   both rate estimates with reconciliation; native/parquet parity
   confirmed empirically; distributed exposure confirmed; all three
   new bugs with individually-correct trigger conditions and honest
   severity; a closing "bottom line" paragraph stating plainly this is
   not a "bug fixed" epic.
2. `CLAUDE.md`, "Current limitations" section (Native Tables):
   appended a shorter "Further update" paragraph directly after the
   existing native-tables-mutation "CORRECTION" paragraph, updating its
   own "three real levers" framing (scan-level pruning would NOT close
   this specific bug, now that it's confirmed not native-specific) and
   cross-referencing the fuller paragraph above rather than repeating
   it.
3. `.claude/epics/archived/native-tables-mutation/epic.md`: appended
   an "Update (`spill-join-correctness` epic, closed 2026-08-25)"
   paragraph to the end of residue #1 (the join-spill duplicate-
   counting bug), preserving the original text unchanged above it —
   matching this program's own established "append, don't silently
   rewrite" convention for correcting an archived document.
4. `.claude/epics/spill-join-correctness/epic.md`:
   - Frontmatter: `status: in-progress` -> `completed`, `progress: 75%`
     -> `100%`, `updated` bumped.
   - Success Criteria section: added an explicit "MET" verdict + one
     sentence of evidence to G5 (previously stated with no verdict —
     the other five criteria already had one, recorded at re-scope
     time and by tasks 001-003 themselves).
   - Tasks Created checklist: `004.md` checked off.
   - Appended the full "## Epic close-out (2026-08-25)" section:
     suite/fmt status, headline ("NOT a bug fixed epic"), summary
     table, per-task attribution (all four tasks, every commit hash),
     G1-G6 verdicts restated with evidence, an explicit
     status/progress justification subsection, a residues list (wrong-
     answer bug as primary residue + 3 new bugs + 2 smaller
     leftovers), the commit chain, and an archival note.
5. `.claude/epics/spill-join-correctness/004.md` (this task's own
   spec file): checked off every acceptance-criteria and
   Definition-of-Done box, added the precision-correction note inline
   in the first checklist item, appended this task's own Outcome
   section.
6. `.claude/epics/spill-join-correctness/updates/004/stream-A.md`
   (this file): created.

## Archival

Final step: `git mv .claude/epics/spill-join-correctness
.claude/epics/archived/spill-join-correctness` (whole directory,
including this `updates/` tree), matching `native-tables-mutation`'s own
precedent (git-mv the whole directory, same session as the close-out
commit).

## Commit

This task's own changes committed on `epic/spill-join-correctness`
(not merged to `main` — left for the orchestrating session per this
task's own process notes). See the epic close-out's own "Commits"
section for the full chain including this task's hash(es).
