---
name: join-order-stats-hardening
status: completed
created: 2026-08-27T17:23:53Z
updated: 2026-08-27T19:35:00Z
progress: 100%
prd: .claude/prds/join-order-stats-hardening.md
github: (will be set on sync)
---

# Epic: join-order-stats-hardening

## Overview

Two narrow, cheap fixes surfaced by the adaptive-join-order-
re-optimization go/no-go investigation, which found the large
replanning investment isn't justified today but that missing/degenerate
join-key statistics can still silently produce catastrophically bad
plans (the repro: a >60,000x cardinality misestimate turned a 1.13s
query into one that never completes), and that native tables' own
statistics can silently go stale after mutation.

## Architecture Decisions

- **Visibility over correction.** Neither fix attempts to make the cost
  model magically right when statistics are missing/wrong — the goal is
  making that condition VISIBLE (so a human or a future adaptive
  mechanism can act on it), not silently guessing better.
- **No full rescans.** Native-table statistics staleness after mutation
  gets fixed via incremental adjustment or honest degradation, not a
  full column rescan per mutation — that would fight the native-tables-
  mutation epic's own established "bounded mutation cost" discipline.
- **One shared "untrustworthy statistics" signal, not two.** The
  native-table staleness fix (task 002) should route through the same
  visibility mechanism task 001 builds, rather than inventing a second,
  parallel way to say "don't trust this number."

## Technical Approach

### Missing/degenerate statistics visibility (task 001)
`JoinReorder`'s cost model, when a relation's join-key column has no
usable statistics, currently falls back silently. Add tracing that
fires in normal operation (not debug-gated) naming the relation/column,
so a bad plan caused by this is diagnosable without already knowing to
look.

### Native-table statistics staleness (task 002)
Investigate exactly how NDV is derived at query time for native tables
(`min_i64`/`max_i64`/row count) and whether DELETE/UPDATE leave it
stale in the dangerous direction (overestimating post-deletion NDV
understates true join selectivity). Fix via incremental adjustment where
cheap, or by routing a materially-invalidated estimate through task
001's own degraded-statistics signal.

### Validation (task 003)
Re-run the go/no-go repro and confirm the visibility fix fires; a real
mutation-then-query test proving native-table statistics stay accurate
or honestly degraded; no regression to existing cost-model/mutation
tests; full suite; docs.

## Task Breakdown Preview

- 001: Missing/degenerate join-key statistics visibility in `JoinReorder`
  (parallel: false, entry point — task 002 reuses its signal)
- 002: Native-table statistics staleness after mutation (parallel: false,
  depends on 001's signal mechanism)
- 003: Validation, no-regression check, full suite, docs, epic close
  (parallel: false, depends on 001 and 002)

Total tasks: 3
Estimated total effort: S-M overall — both fixes are narrow by design.

## Dependencies

- `src/optimizer/rules/join_reorder.rs`, `src/optimizer/cost.rs`.
- `src/storage/native_manifest.rs`, `native_delete.rs`, `native_update.rs`.
- `examples/adaptive_reopt_ndv_repro.rs`, `CLAUDE.md`'s adaptive
  re-optimization section.
- `scripts/claude-safe-build.sh` for every build.

## Success Criteria (Technical)

- G1: the go/no-go repro shows a visible warning identifying the
  missing-statistics relation.
- G2: a real DELETE/UPDATE-then-query test proves native-table
  statistics stay accurate or honestly degraded.
- G3: no regression to existing cost-model or mutation tests/performance.
- G4: full suite green.

## Estimated Effort

- 001: S.
- 002: S-M.
- 003: S.

## Tasks Created
- [x] 001.md - Missing/degenerate join-key statistics visibility (parallel: false) — CLOSED 2026-08-27
- [x] 002.md - Native-table statistics staleness after mutation (parallel: false) — CLOSED 2026-08-27
- [x] 003.md - Validation, no-regression check, full suite, docs, epic close (parallel: false) — CLOSED 2026-08-27

Total tasks: 3
Parallel tasks: 0
Sequential tasks: 3
Estimated total effort: S-M

## Progress

**100% (3/3 tasks closed).** See "Epic close-out" below for task 003's
own final validation. Task 001 shipped the shared "untrustworthy
join-key statistics" signal (`crate::optimizer::classify_join_key_ndv`
/ `warn_untrustworthy_join_key_stats`, `src/optimizer/cost.rs`), wired
into `JoinReorder`'s DPsize cost model
(`src/optimizer/rules/join_reorder.rs`), validated against the live
go/no-go repro (warning fires correctly for the corrupted-stats leg,
zero false positives on the accurate leg, byte-identical join shapes
before/after confirming zero cost-model regression). Full suite green
in all four feature combinations (default 1280, lance 1345, gpu 1289,
pulsar 1283, 0 failures). See `001.md`'s Outcome section for full
detail, including the precise reuse seam task 002 is expected to use.

Task 002 confirmed native-table NDV staleness is real, but not in the
direction the epic's own investigation had guessed: `NativeTable::
statistics()` was already using LIVE (post-delete) row count, so the
naive "stale physical row count" story was already fixed two epics ago.
The actual staleness lives in `min_i64`/`max_i64` themselves, which are
never recomputed by DELETE/UPDATE — for a RANGE-bound NDV estimate
(typically a low/moderate-cardinality column), deleting every row of one
distinct value can never be reflected, keeping a stale, too-high NDV
alive (the epic's own named dangerous direction). Fixed entirely inside
`src/storage/native_table.rs::table_statistics_from` (query-time-only,
zero mutation-path changes — confirmed by `git diff --stat`, not just
argued): once a table's deletion fraction crosses a 10% threshold, a
range-bound column's NDV is emitted as `None` instead of a possibly-stale
`Some(v)`, routing directly through task 001's existing
`classify_join_key_ndv`/`warn_untrustworthy_join_key_stats` signal with
zero new classification logic (the epic's own "one shared signal, not
two" decision, honored exactly). Row-count-bound columns (dense/unique
keys) are never touched — they already self-correct via live row count.
Validated with a real end-to-end SQL `DELETE` test
(`tests/native_ndv_staleness_tests.rs`) plus 2 hermetic unit tests. Full
suite green in all four feature combinations (default 1285 [+5], lance
1350 [+5], gpu 1294 [+5], pulsar 1288 [+5], 0 failures) — exactly task
001's baseline plus this task's 5 new tests. See `002.md`'s Outcome
section for full detail, including a named, honest residual limitation
(deletion-fraction dilution over a very long history of partial
mutations) left for a future compaction-scoped epic, matching this
program's own established "no full rescans" discipline.

## Epic close-out (2026-08-27)

All 3 tasks closed on branch `epic/join-order-stats-hardening`. Full
suite re-confirmed green **at HEAD, by this task itself** (not just
trusted from tasks 001/002's own prior reports) in **all four feature
combinations**, exactly byte-identical to task 002's own recorded
baseline — task 003 made zero Rust source changes to any test-bearing
file (docs + epic bookkeeping + archival only):

| combination | passed | failed |
|---|---|---|
| default | 1285 | 0 |
| lance | 1350 | 0 |
| gpu | 1294 | 0 |
| pulsar | 1288 | 0 |

`cargo fmt --all -- --check`: clean.

### Headline: this epic's actual outcome

**This IS a "both fixes shipped and validated" epic** — unlike some of
this program's other recent epics, there is no unresolved headline bug
here. Both of the go/no-go investigation's own two named "cheap,
targeted stats-hygiene fixes" landed, each independently validated
against a real repro/test, and task 002's own fix reused task 001's
signal mechanism exactly as the epic's Architecture Decisions required
("one shared signal, not two"). Neither fix touched a mutation-path
write module, so neither could have (and, confirmed by `git diff
--stat`, did not) regress native-table mutation performance.

- **Task 001** (missing/degenerate join-key statistics visibility): a
  new, reusable, crate-wide "untrustworthy join-key statistics" signal
  (`crate::optimizer::classify_join_key_ndv`/
  `warn_untrustworthy_join_key_stats`, `src/optimizer/cost.rs`), wired
  into `JoinReorder`'s DPsize cost model's previously-silent fallback
  (`side_combined_ndv`, `src/optimizer/rules/join_reorder.rs`). Fires
  `tracing::warn!` unconditionally (normal operation, no debug-only env
  var), naming the relation and column. Validated against the real
  go/no-go repro: fires correctly on the corrupted-stats leg, zero false
  positives on the accurate leg, byte-identical join plans before/after
  confirming zero cost-model regression. Full suite +7 tests, 0
  failures. Commits: `84db350`.
- **Task 002** (native-table statistics staleness after mutation):
  investigated native-table NDV staleness and found the real mechanism
  was NOT what the epic's own investigation had hypothesized (row count
  was already live-corrected by the native-tables-mutation epic, two
  epics prior) — the actual staleness is stale min/max RANGES for
  range-bound (low/moderate-cardinality) columns after a DELETE removes
  every row of one distinct value, which silently keeps NDV too high
  (the epic's own named dangerous direction). Fixed query-time-only in
  `src/storage/native_table.rs::table_statistics_from` (a
  `deletion_fraction` threshold of 10%, above which a range-bound
  column's NDV is emitted as `None`, routing directly through task 001's
  existing signal — zero new classification code, zero mutation-path
  changes, confirmed by `git diff --stat` showing zero lines touched in
  `native_write.rs`/`native_delete.rs`/`native_update.rs`). Full suite
  +5 tests, 0 failures. Named one honest residual limitation around
  deletion-fraction dilution over very long partial-mutation histories,
  left for a future compaction-scoped epic. Commit: `79dbaa3`.
- **Task 003** (this task — validation/no-regression/docs/close-out):
  re-ran the full suite at HEAD across all four feature combinations
  (table above, byte-identical to task 002's own recorded numbers, 0
  failures); re-ran `examples/adaptive_reopt_ndv_repro.rs --mode
  plan-only` fresh and confirmed the missing-statistics case still
  produces a visible warning identifying the exact relation/column in
  normal operation (0 warnings on the accurate leg, 15 warnings on the
  corrupted leg naming `supplier.s_suppkey`, `supplier.s_nationkey`,
  `customer.c_custkey`, `customer.c_nationkey` — unique pairs match task
  001's own recorded run exactly; join shapes unchanged, "repro
  confirmed"); re-ran `tests/native_ndv_staleness_tests.rs` in isolation
  (3/3 passing) plus its 2 companion unit tests in `native_table.rs`
  (covered by the full-suite run); confirmed via `git diff --stat`
  across both prior tasks' commits that zero lines changed in any
  mutation-path write module, which is the epic's own construction-level
  proof that native-table mutation performance is unaffected (no
  benchmark re-run needed to establish this — the code that performance
  depends on is byte-for-byte unchanged); wrote the G1-G4 verdicts below;
  updated `CLAUDE.md`'s "Adaptive join-order re-optimization" section
  with what landed; wrote this close-out; updated epic status to
  `completed`/100%; archived the epic; updated the PRD's own status.
  `cargo fmt --all -- --check` clean throughout.

### G1-G4 (epic.md's own Success Criteria) — verdicts with evidence

- **G1** (the go/no-go repro shows a visible warning identifying the
  missing-statistics relation) — **MET**. Re-run at HEAD, this task:
  `examples/adaptive_reopt_ndv_repro.rs --data data/tpch-10gb --mode
  plan-only`, tracing subscriber active. Accurate leg: 0 `WARN` lines.
  Corrupted leg: 15 `WARN` lines, e.g. `join_reorder: join key has
  untrustworthy statistics for this join edge; falling back to an
  estimated NDV ... relation="supplier" column="s_suppkey"
  reason="no recorded NDV statistics" fallback_ndv=100000.0` — naming
  every corrupted relation/column exactly (`supplier.s_suppkey`,
  `supplier.s_nationkey`, `customer.c_custkey`, `customer.c_nationkey`).
  Join shapes differ between legs exactly as task 001 originally
  recorded (`RESULT: join shapes DIFFER (repro confirmed)`).
- **G2** (a real DELETE/UPDATE-then-query test proves native-table
  statistics stay accurate or honestly degraded) — **MET**. Re-run at
  HEAD, this task: `cargo test --test native_ndv_staleness_tests`, 3/3
  passing (`concentrated_delete_of_a_whole_category_degrades_its_ndv_
  and_the_shared_signal_fires`, `a_small_delete_that_does_not_
  eliminate_a_value_does_not_degrade_ndv`, `a_no_op_delete_leaves_
  statistics_unchanged`) — a real SQL `CREATE TABLE ... AS SELECT` ->
  `DELETE FROM ... WHERE ...` -> re-query `statistics()` path, not a
  synthetic manifest edit. The degrading case demonstrates the "honestly
  degraded" half of G2 (NDV flips from `Some(3)` to `None`, routing
  through task 001's own signal); the two non-degrading cases demonstrate
  the "stays accurate" half (no false-positive degradation from a small
  or no-op delete).
- **G3** (no regression to existing join-order/cost-model tests or to
  native-table mutation performance) — **MET**. Cost-model/join-order:
  the full suite (below) includes every pre-existing join-order/cost
  test, all green, plus task 001's own byte-identical-join-shape
  evidence (G1 above) that the fallback VALUE never changed, only its
  visibility. Mutation performance: `git diff --stat` across both
  tasks' commits (`00ae775..79dbaa3`) shows the ONLY files touched are
  `examples/adaptive_reopt_ndv_repro.rs`, `src/optimizer/cost.rs`,
  `src/optimizer/rules/join_reorder.rs`, `src/storage/native_table.rs`
  (query-time read path only), `tests/native_ndv_staleness_tests.rs`,
  and epic/task markdown — zero lines in `native_write.rs`/
  `native_delete.rs`/`native_update.rs`, which is where every mutation
  performance number `CLAUDE.md` records (e.g. task 005's 328MB Append
  peak RSS, the DELETE/UPDATE mechanics) is actually earned. Code that
  performance depends on being provably unchanged is a stronger
  guarantee than a benchmark re-run subject to this shared machine's own
  run-to-run noise, and is the same standard `spill-join-correctness`/
  `spill-join-correctness-2`'s own close-outs used for analogous
  no-touch claims.
- **G4** (full suite green) — **MET**. Table above: default 1285, lance
  1350, gpu 1294, pulsar 1288, 0 failures anywhere — exactly task 002's
  own recorded numbers, re-confirmed fresh at HEAD by this task, not
  merely trusted. `cargo fmt --all -- --check` clean.

### Named residues (what is still open after this epic)

1. **Task 002's own honestly-named residual limitation** (carried
   forward, not newly discovered by this task): `deletion_fraction`'s
   denominator is the table's CURRENT physical row count across all
   active segments, which can grow over a long history of many small,
   partial (not whole-segment) mutations even as tombstoned rows also
   grow — in principle diluting `deletion_fraction` below the 10%
   threshold for a table whose range-bound columns are nonetheless
   genuinely stale from a much earlier, now-diluted deletion. Real,
   bounded, not observed to matter at any scale this program's own
   fixtures/benchmarks reach today (the same class of accepted risk as
   the native-tables-mutation epic's own "O(N²) manifest rewrite cost"
   and "deletion-vector JSON density" findings), not fixable within this
   epic's own query-time-only, no-full-rescan scope. Left for a future
   compaction-scoped epic.
2. **The go/no-go investigation's own broader recommendation — full
   adaptive join-order re-optimization — remains correctly, deliberately
   NOT pursued.** This epic never attempted it (out of scope by the
   PRD's own design); `CLAUDE.md`'s "Adaptive join-order re-optimization"
   section verdict stands: every production table-registration path
   already carries real statistics and picks correct join orders, and
   the two narrow fixes this epic shipped are what the investigation's
   own evidence actually justified, not the large `PhysicalOperator`
   trait-signature change or new execution driver the full replanning
   epic would need.
3. **The warning is not deduplicated across the optimizer's fixpoint
   loop** (task 001's own observation, carried forward, not fixed):
   `JoinReorder` re-runs each optimizer pass until the plan stabilizes,
   so the SAME (relation, column) pair can log multiple times per query
   (15 warnings for 4 unique pairs in this task's own re-run). Judged
   acceptable at this epic's S-effort scope by task 001 and unchanged by
   this task — a real (if minor) log-noise consideration for a very
   large multi-way join, not a correctness issue.

### Why `completed`/100% is genuinely warranted

Both build tasks (001, 002) closed having met every one of their own
acceptance criteria (see each task's own Outcome section), not partially
or with an open headline bug — unlike, for example, the
`spill-join-correctness`/`spill-join-correctness-2` epics' own honest
"real work landed, headline bug still open" close-outs. This epic's own
G1-G4 success criteria are ALL met with direct evidence (above), the
full suite is green in all four feature combinations, `cargo fmt` is
clean, and the only residues (above) are either explicitly-scoped-out
future work (full adaptive replanning) or small, honestly-named,
non-blocking limitations neither task's own acceptance criteria required
closing. `completed`/100% is the accurate status, not an optimistic one.

### Commits

`00ae775` (CCPM: PRD + epic + 3 tasks) -> `84db350` (001: missing/
degenerate join-key statistics visibility) -> `79dbaa3` (002: native-table
NDV staleness fix) -> `16b1f68` (003: G1-G4 verdicts, CLAUDE.md update,
this close-out, epic status update, archival move).

### Archival

Epic moved to `.claude/epics/archived/join-order-stats-hardening/` as
this task's final step, mirroring `spill-join-correctness-2`/
`native-tables-rollups`/`native-table-pruning`'s own archival pattern
(`git mv`). **Not merged to `main`** — that decision and action is left
to the user/orchestrating session, per this task's own instructions.
