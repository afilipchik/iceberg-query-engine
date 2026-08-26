---
name: native-tables-rollups
status: completed
created: 2026-08-26T06:43:51Z
updated: 2026-08-26T18:00:00Z
progress: 100%
prd: .claude/prds/native-tables.md
github: (will be set on sync)
---

# Epic: native-tables-rollups

## Overview

Phase 4 of the `native-tables` PRD: materialized rollups. Foundation
(CREATE/read) and mutation (INSERT/DELETE/UPDATE) are both shipped and
archived. Per an explicit decision point the PRD itself named ("revisit
[tiering vs. rollups priority] with the user once the foundation phase
ships"), the user chose rollups next over GPU/RAM/disk tiering — the
real target use case is many concurrent dashboard viewers hitting a
known query set, not single ad-hoc analysis.

A dedicated research pass (read-only, this epic's own task 000
equivalent, folded into this file rather than a separate task since it
produced no code) confirmed the PRD's own framing: this is "the largest
net-new infrastructure of the whole program." Concretely, NONE of the
following exist anywhere in this codebase today: a field to record
"this table is a rollup of X" (checked `NativeManifest`, the `Catalog`
trait, the `TableProvider` trait — no home for this fact anywhere), a
mechanism to match one query's plan against another registered plan for
semantic equivalence/subsumption (checked every `OptimizerRule` — each
operates on one plan tree in isolation with no catalog access; the
closest precedent, `VectorSearchPushdown`, matches a hard-coded shape
within the SAME plan, not against an externally stored definition), or
any background/scheduled-refresh infrastructure (the only recurring
background loop in the codebase, `discovery_loop` in
`distributed/server.rs`, is cluster peer-membership probing — unrelated
to data).

One real gift from the research: `CREATE MATERIALIZED VIEW <name> AS
SELECT ...` parses natively at the pinned sqlparser 0.62
(`Statement::CreateView { materialized: true, query, .. }`, zero grammar
work) and is currently completely unhandled in `binder.rs` (falls to the
generic `NotImplemented` catch-all) — this is the DDL surface to use,
not a bespoke `CREATE ROLLUP` keyword that would need real grammar work.

## Architecture Decisions

- **Narrow slice first, not the general vision in one epic.** Full
  subsumption reasoning (can a query whose GROUP BY is coarser than a
  rollup's, or whose aggregate needs re-derivation from stored
  components, be answered from it) is, per the research, "a genuinely
  new algorithm class for this codebase" — nothing here reasons about
  equivalence between two independently-planned queries. This epic ships
  EXACT-match rollups only (single base table, GROUP BY aggregate,
  structurally-normalized-identical query shape). Coarser matching is
  explicitly future work, named not attempted, matching this PRD's own
  "each phase independently useful, no phase blocks on a later phase's
  completion" (G4) discipline.
- **Risk-first sequencing.** The core, novel, unprecedented piece — can
  a query be matched against a registered rollup definition and
  transparently answered from it, correctly, with the result provably
  taking that path — is task 001, built against a programmatic
  registration API (no SQL DDL yet), before any time is spent on DDL
  parsing polish. If task 001 can't reach a real, evidenced, working
  substitution mechanism, task 002 (the SQL surface built on top of it)
  does not proceed — matching `spill-join-correctness`'s own "no
  guess-fixes"/gate precedent, adapted to "no building a user-facing
  surface on an unproven mechanism."
- **Substitution does NOT live inside `OptimizerRule`.** That trait's
  `optimize(&self, plan: &LogicalPlan) -> Result<LogicalPlan>` has
  no catalog/registry access by design — every existing rule is
  deliberately plan-local. Rollup matching needs a rollup registry,
  so task 001 must decide where this actually plugs into the pipeline
  (most likely a pass with real catalog access, positioned before or
  around `Optimizer::optimize()` in `ExecutionContext`'s execution
  path — mirroring how DDL statements are already special-cased before
  `Binder` runs) — a genuine design decision, not assumed here.
- **Refresh model: refresh-on-write, not a new scheduler.** The research
  found zero existing scheduled/background-refresh infrastructure and
  named refresh-on-write (piggyback the mutation epic's proven
  INSERT/DELETE/UPDATE entrypoints) as the lowest-risk option — no new
  process/thread lifecycle management. A genuine background scheduler
  is explicitly out of scope for this epic.
- **Always correct, even if that means not fast.** If a rollup is stale
  or a query doesn't cleanly exact-match, fall back to the normal
  base-table plan — never serve a stale or approximated answer labeled
  as current. Matches this program's memory-safety-is-never-negotiable/
  correctness-first culture applied to freshness instead of memory.
- **Provenance always visible (PRD G5).** Whenever a rollup answers a
  query, that fact must be visible in output/logging — never silently
  indistinguishable from "the engine got faster at the real query,"
  named explicitly by the PRD as a real risk given how materialized-view
  stories usually get marketed.
- **Explicitly NOT this epic**: subsumption/coarser-grouping matching,
  Gravitino metastore discoverability (native tables aren't reachable
  through Gravitino's `format` dispatch at all today — a real,
  pre-existing gap, but not this epic's to fix), distributed rollups,
  any new background scheduler.

## Technical Approach

### Data model
Extend `NativeManifest` (`src/storage/native_manifest.rs`) with an
additive, `#[serde(default)]` field recording rollup identity: base
table name, the defining query (both raw SQL and enough of its bound
`LogicalPlan` shape to match against), and staleness bookkeeping tied to
the base table's own snapshot/version. A rollup's actual row data is
stored using the existing native-table segment format unchanged — a
rollup IS a native table, just one with this extra manifest fact
recorded, matching how the mutation epic treated deletion vectors as an
additive manifest extension.

### Matching mechanism
Task 001's core deliverable. Structural comparison after normalizing
both the incoming query's plan and the rollup's stored defining plan the
same way (exact base table, exact GROUP BY expression set, exact
requested aggregate set) — order-independence and column-set comparison
decided and documented explicitly by task 001, not left implicit (the
research flagged `GROUP BY a, b` vs `GROUP BY b, a` as a real, easy-to-
get-wrong gotcha). On match, substitute a scan against the rollup's own
native-table storage (plus whatever reshaping — column order, aliases —
the original query needs) in place of the base-table plan.

### DDL surface
`Statement::CreateView` (`materialized: true`) arm in `binder.rs`,
mirroring `Statement::CreateTable`'s existing pattern: refuse unsupported
`CreateView` fields by name, require `query`, redirect away from the
generic `sql()` entrypoint to a dedicated `&mut self` one
(`create_materialized_view` or similar) matching `create_table_as_select`/
`insert_into_native_table`'s existing precedent exactly.

### Refresh
On successful INSERT/DELETE/UPDATE against a base table (existing
`native_write`/`native_delete`/`native_update` entrypoints), either
eagerly recompute dependent rollups inline or mark them stale for
lazy recompute on next match attempt — task 003's call, documented
explicitly either way.

### Validation
Cell-exact: rollup-answered results vs. the same query computed directly
against the base table vs. an independent DuckDB oracle, for every
shape this epic ships. Explicit tests proving the fallback path (stale
rollup, non-matching query shape) stays correct. Full suite; docs.

## Task Breakdown Preview

- 001: Rollup data model + the matching/substitution mechanism itself,
  proven via a programmatic registration API (no SQL yet) — the epic's
  real risk, gates 002 (parallel: false)
- 002: SQL DDL surface (`CREATE MATERIALIZED VIEW ... AS SELECT ...`)
  wired to task 001's registration API (parallel: false, depends on 001
  reaching a working mechanism)
- 003: Staleness/refresh-on-write model against existing
  INSERT/DELETE/UPDATE entrypoints (parallel: false, depends on 002)
- 004: QA close-out — cell-exact validation across every shape shipped,
  fallback-path tests, full suite, docs, epic close (parallel: false,
  depends on everything)

Total tasks: 4
Estimated total effort: genuinely uncertain, dominated by task 001 —
this is new-algorithm-class work with zero precedent in this codebase,
matching the same honest-uncertainty framing `spill-join-correctness`
task 001 used.

## Dependencies

- `src/storage/native_manifest.rs`, `native_write.rs`, `native_table.rs`
  — the storage layer a rollup's row data reuses unchanged.
- `src/optimizer/mod.rs` and `src/optimizer/rules/vector_search.rs` (the
  closest existing shape-match-and-substitute precedent, though it does
  not solve the external-registry problem this epic needs).
- `src/planner/binder.rs`'s `Statement::CreateTable`/`Insert` arms — the
  proven "constrain a standard AST node, refuse the rest by name,
  redirect off the generic `sql()` path" pattern to replicate for
  `CreateView`.
- `src/execution/context.rs`'s `create_table_as_select`/
  `insert_into_native_table`/`delete_from_native_table`/
  `update_native_table` — the dedicated-entrypoint pattern to extend.
- `scripts/claude-safe-build.sh` for every build.

## Success Criteria (Technical)

- G1: a rollup can be defined (programmatically in task 001, via SQL in
  task 002), populated, and an exact-matching query is transparently
  answered from it — cell-exact vs. both direct base-table computation
  and an independent DuckDB oracle.
- G2: a non-matching query (different GROUP BY, different aggregate set,
  a filter, etc.) or a stale rollup correctly falls back to the normal
  base-table plan — never silently wrong, never silently stale.
- G3: provenance is always visible when a rollup answers a query (PRD
  G5) — never indistinguishable in output/logging from the base-table
  path.
- G4: refresh model is explicit and documented (task 003), validated
  against the mutation epic's existing INSERT/DELETE/UPDATE paths with
  no regression to them.
- G5: full suite green; native-tables PRD's status note updated to
  reflect this phase's actual outcome, whatever it is.

## Estimated Effort

- 001: genuinely uncertain — L, possibly XL. New algorithm class, zero
  precedent, real epic risk.
- 002: S-M once 001 has a working mechanism (mostly proven-pattern DDL
  plumbing).
- 003: S-M.
- 004: S-M.

## Tasks Created
- [x] 001.md - Rollup data model + matching/substitution mechanism (parallel: false)
      — CLOSED 2026-08-26. Confidence gate MET: a real, evidenced,
      working substitution mechanism, cell-exact validated (direct
      computation + independent DuckDB oracle at real SF=1 scale),
      order-independence/aliasing decided and tested explicitly, three
      non-matching-shape fallback cases plus staleness-after-mutation
      all verified cell-exact (not just "doesn't crash"), provenance via
      `QueryMetrics::rollup_answered`, full suite green in all four
      feature combinations (+34 tests, zero regression). Recommendation:
      task 002 should proceed. Full detail: `001.md`'s own Outcome
      section.
- [x] 002.md - SQL DDL surface — CREATE MATERIALIZED VIEW (parallel: false)
      — CLOSED 2026-08-26. `CREATE MATERIALIZED VIEW <name> AS SELECT
      ...` wired to task 001's `register_rollup`, zero sqlparser grammar
      work, zero changes to task 001's own matching/substitution
      mechanism (confirmed by empty diff on `native_rollup.rs`). All 17
      `CreateView` struct fields refused by name where unsupported;
      `IF NOT EXISTS` decided explicitly (error, mirrors `CREATE TABLE`'s
      own precedent); `sql()`'s DDL/DML redirect pattern extended to a
      fifth case; REPL wired; checked and documented that registration is
      NOT reachable via HTTP `/sql`/Flight (mirrors CTAS's own
      pre-existing boundary) while subsequent MATCHING is, since it
      shares the real `sql()` path. 11 new end-to-end tests, full suite
      green in all four feature combinations (+11 each, 0 regressions).
      Full detail: `002.md`'s own Outcome section.
- [x] 003.md - Staleness/refresh-on-write model (parallel: false)
      — CLOSED 2026-08-26. EAGER refresh chosen and documented explicitly
      (LAZY has no viable call site: the only place a rollup is matched,
      `sql()`/`optimized_plan()`, is `&self`, and the only refresh
      mechanism, `register_rollup`, is `&mut self` — making LAZY work
      would need either an invasive `sql()` signature change or new
      interior-mutability infrastructure, both outside this task's
      scope). Wired into all three of `ExecutionContext::
      insert_into_native_table`/`delete_from_native_table`/
      `update_native_table` (a deliberate layering choice explained in
      full in the task's own Outcome — NOT literally inside
      `native_write.rs`/`native_delete.rs`/`native_update.rs`, which have
      no SQL/registry awareness by design, mirroring task 001's own
      "why not an OptimizerRule" reasoning). Multi-rollup case verified
      (one mutation refreshes ALL dependents). A failed refresh never
      fails the base table's own mutation and leaves the rollup
      correctly stale, verified with a REAL induced I/O failure (not
      simulated) via a permission-denial test, with the fallback answer
      confirmed cell-exact. Performance measured, not assumed: 3-5.6x
      mutation latency at SF=1 with 1-3 rollups registered, root-caused
      to a full base-table rescan per rollup (the honest cost of EAGER
      full-recompute refresh vs. a much harder, out-of-scope incremental
      merge). Full suite green in all four feature combinations (+8 tests
      each, 0 regressions); zero changes to `native_write.rs`/
      `native_delete.rs`/`native_update.rs`/`native_rollup.rs` (confirmed
      by `git diff`). Full detail: `003.md`'s own Outcome section.
- [x] 004.md - QA close-out — cell-exact validation, full suite, docs, epic close (parallel: false)
      — CLOSED 2026-08-26. Broader validation sweep (3 distinctly-shaped
      rollups simultaneously live via real DDL, cell-exact vs. both
      direct base-table computation and an independent DuckDB oracle,
      real SF=1 scale) and fallback-correctness sweep (8 new tests: 4
      DDL-registered non-matching shapes + 3 mutation-triggered-refresh
      cases + 1 multi-rollup case, all through the real DDL + ordinary
      `sql()`/mutation SQL surface) both PASS. Found and fixed one real,
      pre-existing, general SQL binder bug (`extend_projection_for_sort`'s
      Aggregate bailout) while running the sweep — confirmed general, not
      rollup-specific, regression-tested, zero suite regressions. Full
      suite green in all four feature combinations (+9 tests each, 0
      regressions); M1/M2 distributed gates re-run (not merely skipped,
      given this task's own binder.rs change touches shared code) and
      PASS. G1-G5 all MET. Full detail: `004.md`'s own Outcome section and
      the Epic close-out section below.

Total tasks: 4
Parallel tasks: 0
Sequential tasks: 4
Estimated total effort: genuinely uncertain, dominated by task 001 (as
predicted going in — new-algorithm-class work with zero precedent).
Tasks 002-004 were each S-M as estimated, no surprises versus the plan.
Epic complete, all 4 tasks closed — see Epic close-out below.

## Epic close-out (2026-08-26)

All 4 tasks shipped and validated on branch `epic/native-tables-rollups`
(commits `760595b`..`d209403` for tasks 001-003, `8904592` and this
task's own remaining doc/archive commits for task 004 — see Commits
below). Full suite green in **all four feature combinations** (default
1252/0/-, lance 1317/0/-, gpu 1252/0/-, pulsar 1255/0/- — passed/failed,
zero failures anywhere), `cargo fmt --all -- --check` clean, M1 and M2
distributed gates re-confirmed PASS via a real 3-process cluster
(`scripts/cluster_local.sh verify` / `verify-m2`) — re-run deliberately,
not skipped, because this task's own `binder.rs` fix (below) touches
shared planning code every query path uses, unlike tasks 001-003 (which
had zero `src/distributed/` changes and correctly did not need to
re-run these gates).

### Headline: what this epic actually delivered

`CREATE MATERIALIZED VIEW <name> AS SELECT ...` — real, working
materialized rollups. A query gets transparently, correctly answered
from a registered rollup when it matches exactly (same base table, same
GROUP BY set, same aggregate set — order/alias-independent, no
subsumption). A mutated base table's dependent rollup(s) are
automatically, eagerly refreshed as part of the mutation itself, with
provenance always visible on both the read side
(`QueryMetrics::rollup_answered`) and the write side
(`RollupRefreshOutcome`). A non-matching query, a rollup whose refresh
failed, or a rollup that predates a base-table mutation always falls
back correctly to the base table — never silently wrong, never silently
stale. This is a real, working, cell-exact-validated feature at real
scale (SF=1, 6,000,000-row `lineitem`) — **not** a partial or
honest-negative-result epic like `spill-join-correctness`. All three
build tasks (001-003) met their own gates and shipped real capability;
this task's own broader validation (below) independently re-confirms
that verdict rather than merely repeating it, and closes two real
coverage gaps the per-task tests left open.

### Per-task attribution

- **001** (matching/substitution mechanism): built the core
  matching/substitution mechanism from scratch — genuinely new-algorithm-
  class work with zero precedent anywhere in this codebase, confirmed by
  a dedicated research pass (no `OptimizerRule` has catalog access;
  `VectorSearchPushdown` matches only within one plan, never against an
  external registry). Confidence gate MET. Cell-exact validated at real
  scale (SF=1, 6,000,000-row `lineitem`) against both direct base-table
  computation and an independent DuckDB oracle. Explicit fallback
  correctness for 3 non-matching shapes plus staleness after a real
  base-table mutation. Provenance via `QueryMetrics::rollup_answered`.
- **002** (SQL DDL surface): `CREATE MATERIALIZED VIEW <name> AS SELECT
  ...` wired onto task 001's mechanism with ZERO sqlparser grammar work
  (the pinned sqlparser 0.62 already parses this shape natively) and
  ZERO changes to task 001's own matching/substitution code
  (`native_rollup.rs`, confirmed by an empty diff). Working end to end
  through the ordinary `sql()` path (the SAME path HTTP `/sql`/Flight
  use) for MATCHING; registration itself is REPL/direct-API only, the
  same boundary CTAS already has. `IF NOT EXISTS` decided as an explicit
  error, matching `CREATE TABLE`'s own precedent.
- **003** (refresh-on-write): made staleness real via EAGER
  refresh-on-write, wired into all three of INSERT/DELETE/UPDATE at the
  `ExecutionContext` layer — a deliberate, reasoned choice (NOT literally
  inside `native_write.rs`/`native_delete.rs`/`native_update.rs`, which
  have zero SQL/registry awareness by design) explained in full in the
  task's own Outcome. Multi-rollup case verified (one mutation refreshes
  ALL dependents, sequentially, to bound peak memory). Performance
  measured honestly: 3-5.6x relative slowdown per mutation per attached
  rollup (tens of ms absolute, SF=1) from full-recompute-per-mutation — a
  real, named cost, not hidden. A genuine induced-failure test (chmod
  read-only mid-mutation) proves the fallback path stays correct even
  when refresh itself fails.
- **004** (this task, QA close-out): independently re-confirmed all of
  the above rather than trusting the prior reports — re-ran the full
  4-combination suite at HEAD (below), and built two genuinely new
  validation artifacts closing real gaps: a broader multi-shape sweep
  (3 rollups simultaneously live, DuckDB oracle, real scale) and a
  fallback sweep that, for the first time in the epic, combines
  DDL-registered rollups with real mutation-triggered refresh through
  the ordinary SQL surface. Found and fixed a real, general, pre-existing
  SQL binder bug surfaced by the sweep (see below) — a genuine, if
  rollups-unrelated, contribution beyond the task's own narrow charter.
  Updated the PRD status note, wrote this close-out, and archived the
  epic.

### Broader validation sweep and fallback sweep (task 004, new artifacts)

Two real gaps in the epic's own per-task coverage, closed:

1. **No prior test combined "several distinctly-shaped rollups
   simultaneously live against one base table" with "an independent
   DuckDB oracle."** Task 001's own DuckDB-oracle check validated exactly
   ONE rollup shape; task 003's own multi-rollup test validated TWO
   rollups but only against a direct-computation reference, never
   DuckDB. `examples/native_rollup_multi_shape_check.rs` + `scripts/
   native_rollup_multi_shape_check.py` register THREE distinctly-shaped
   rollups (2/1/3 GROUP BY columns respectively, in varied order;
   aggregate sets from 2 to 4 functions, including MIN/MAX on a DATE
   column — new coverage) simultaneously against one `lineitem_native`
   table (real SF=1 scale, `data/tpch-1gb`, 6,000,000 rows), all
   registered via the real `CREATE MATERIALIZED VIEW` DDL text. Each is
   queried with a differently-phrased query through ordinary `sql()`,
   provenance-confirmed, and compared three ways (rollup vs. DuckDB,
   direct vs. DuckDB, rollup vs. direct) with this repo's own
   established float tolerance. **PASS, all 3 shapes, all 3 comparisons.**
2. **No prior test combined "rollup registered via real DDL text" with
   "base table mutated via real DML text, refresh fires, a subsequent
   ordinary query is still correctly answered."** Task 002's own tests
   never touch mutation; task 003's own refresh tests register every
   rollup via `register_rollup`, never via the DDL.
   `tests/native_rollup_qa_closeout_tests.rs` (8 new tests) closes this:
   a `CREATE MATERIALIZED VIEW`-registered rollup correctly falls back
   for 4 distinct non-matching shapes, and correctly survives an
   INSERT-, DELETE-, and UPDATE-triggered refresh — each verified through
   the real mutation SQL entrypoints and the ordinary `sql()` path
   afterward, cell-exact against an independently-mutated reference
   context. A final test registers TWO DDL-created rollups on one table
   and confirms one mutation refreshes both. **PASS, 8/8.**

Per task 002's own established precedent (no real HTTP-server round-trip
test was added there either — validated at the `ExecutionContext::sql()`
level, the exact function the HTTP handler calls), these new tests follow
the identical, already-established test depth rather than standing up a
real `serve` process — registration/mutation are not reachable over
HTTP/Flight regardless (confirmed unchanged, matching CTAS's own
boundary), so a real server would not exercise anything these direct
`ExecutionContext` calls don't already exercise.

### A real, pre-existing, general SQL binder bug found and fixed (not rollup-specific)

While running the broader sweep, query shape B (`ORDER BY l_shipmode`
where the SELECT list only exposes that same GROUP BY column under a
different alias, `l_shipmode AS mode`, placed LAST after several
aggregates) crashed at EXECUTION time with `Column not found: l_shipmode`
— not a graceful fallback, a hard error. Root-caused precisely, not
guessed: `extend_projection_for_sort` (`src/planner/binder.rs`) had a
blanket bailout for any `Project` whose child is an `Aggregate`, added
2026-08-09 alongside the function itself (the vector-search feature) as
a conservative, never-revisited scope limit (confirmed via `git blame`
— the original author simply never exercised the Aggregate case, not a
deliberate fix for some other bug). `bind_order_by` never validates a
bare identifier against the schema at bind time (it just emits
`Expr::Column` unconditionally), so with no rescue mechanism the Sort
node's own input silently lacked its sort key and physical execution
failed downstream with a confusing runtime error instead of a clean
bind-time one.

**Confirmed general and pre-existing, not rollup-specific** — isolated
before attempting any fix, per this program's own discipline: the
identical query shape crashes identically against plain in-memory TPC-H
data with zero rollup ever registered
(`query_engine sql "SELECT COUNT(*) AS cnt, MAX(l_shipdate) AS latest,
MIN(l_shipdate) AS earliest, SUM(l_quantity) AS total_qty, l_shipmode AS
mode FROM lineitem GROUP BY l_shipmode ORDER BY l_shipmode" --sf 0.01`).

**Fixed with a small, high-confidence, scoped change**: removed the
blanket Aggregate bailout. Safety argument, not just a hope: the
function's own resolve-based check (`input_schema.resolve_column(col)`,
already unconditionally applied a few lines below the removed bailout)
already gates every widened column — for an Aggregate's `input_schema`
(`AggregateNode::schema()`), that check can only ever succeed for a
column that IS already one of the Aggregate's own output fields (a GROUP
BY key or an aggregate expression's own result), never a raw
pre-aggregation row column, so GROUP BY semantics cannot be violated by
removing the bailout. Verified, not just argued: the fixed query now
returns 7 correctly-ordered rows (AIR, FOB, MAIL, RAIL, REG AIR, SHIP,
TRUCK — alphabetical ASC, matching `ORDER BY l_shipmode`'s own
semantics), not merely "doesn't crash." Regression test:
`test_order_by_group_key_under_its_original_name_when_aliased_in_select`
in `tests/sql_comprehensive.rs`. Full suite (all four feature
combinations, below) confirms zero regressions from this change — and
because it touches shared planning code, the M1/M2 distributed gates
were re-run specifically because of it (both PASS), not skipped the way
tasks 001-003 correctly could.

### Full suite, all four feature combinations, through `scripts/claude-safe-build.sh`, re-confirmed at HEAD

| combo | task 003 baseline | this task | delta | failed |
|---|---|---|---|---|
| default | 1243 | **1252** | +9 | 0 |
| lance | 1308 | **1317** | +9 | 0 |
| gpu | 1243 | **1252** | +9 | 0 |
| pulsar | 1246 | **1255** | +9 | 0 |

Every combination is exactly task 003's own baseline plus this task's 9
new tests (8 in `native_rollup_qa_closeout_tests.rs` + 1 regression test
in `sql_comprehensive.rs` for the binder fix) — zero regression anywhere,
confirmed by exact arithmetic, not merely "still green." `cargo fmt --all
-- --check` clean. M1 GATE: PASS (5/5 checks — cluster view agreement,
5 TPC-H queries byte-identical across all 3 nodes and the single-process
binary, health/ready endpoints, Flight==HTTP parity on 4 shapes across
all 3 nodes, SIGTERM survival). M2 GATE: PASS (4/4 checks — work-division
imbalance ≤1.1 at 3 and 8 nodes, 13 cell-exact scatter-path aggregate
queries vs. DuckDB, 13 cell-exact gather-path queries incl. joins/
subqueries/DISTINCT/ORDER BY+LIMIT/STDDEV/CTE vs. DuckDB, refusals
correctly named).

### G1-G5 (this epic's own Success Criteria) — verdicts with evidence

- **G1** (a rollup can be defined — programmatically in task 001, via SQL
  in task 002 — populated, and an exact-matching query is transparently
  answered from it, cell-exact vs. both direct base-table computation and
  an independent DuckDB oracle) — **MET**. Task 001's own DuckDB-oracle
  check (SF=1, one shape, programmatic registration) plus this task's own
  broader 3-shape sweep (also SF=1, via the real DDL surface, three
  simultaneous rollups) both independently confirm this at real scale.
- **G2** (a non-matching query — different GROUP BY, different aggregate
  set, a filter, etc. — or a stale rollup correctly falls back to the
  normal base-table plan — never silently wrong, never silently stale)
  — **MET**. Task 001's 3 non-matching-shape tests + 1 staleness test
  (programmatic API); this task's own 4 additional DDL-registered
  non-matching-shape tests, all cell-exact, none merely "doesn't crash."
- **G3** (provenance is always visible when a rollup answers a query —
  never indistinguishable in output/logging from the base-table path)
  — **MET**. `QueryMetrics::rollup_answered` (read side, task 001) and
  `RollupRefreshOutcome`/`{Insert,Delete,Update}Result::rollups_refreshed`
  (write side, task 003) are structured, directly-checkable fields —
  every test in the epic (34 + 11 + 8 + 9 = 62 rollup-specific tests
  total) asserts on them directly, never infers provenance from timing or
  side effects. `QE_DEBUG_ROLLUP=1` additionally traces every
  match/no-match/staleness decision to stderr.
- **G4** (refresh model is explicit and documented — task 003 —
  validated against the mutation epic's existing INSERT/DELETE/UPDATE
  paths with no regression to them) — **MET**. EAGER chosen and justified
  in `refresh_dependent_rollups`'s own doc comment (task 003, every
  alternative considered and rejected explicitly); this task's own
  full-suite run re-confirms the mutation epic's own regression tests
  (`spill_tests.rs`, `native_delete_tests.rs`, `native_update_tests.rs`)
  are still 100% green, and this task's new multi-rollup DDL test
  (`two_ddl_registered_rollups_on_one_table_both_survive_one_mutations_
  refresh`) adds one more independent confirmation of the multi-rollup
  case beyond task 003's own.
- **G5** (full suite green; native-tables PRD's status note updated to
  reflect this phase's actual outcome) — **MET**. Full-suite table above,
  all four combinations, zero failures. `.claude/prds/native-tables.md`'s
  status note updated by this task to reflect 3-of-4 phases shipped.

**All 4 tasks fully closed, each having met its own acceptance criteria
— `status: completed`, `progress: 100%` genuinely warranted**, not
asserted by convention: task 001's confidence gate was explicitly MET
(not partial); task 002 shipped zero-regression DDL on top of an
unmodified mechanism; task 003 shipped a working, measured refresh model
with zero regressions; this task independently re-verified all of the
above at HEAD (not merely trusted the prior reports) and found the
result to actually hold, plus closed two real coverage gaps and one
real, general bug the epic's own work surfaced.

### Residues / explicitly out of scope (named as one class, matching this program's convention — unchanged by this QA task, which validated the epic's existing scope more broadly, not expanded or narrowed it)

1. **No subsumption/coarser-grouping matching.** Only exact-shape rollups
   are ever matched — a query whose GROUP BY is coarser than a rollup's,
   or whose aggregate needs re-derivation from stored components, always
   falls back to the base table. Named by the epic itself as "a
   genuinely new algorithm class," explicitly out of scope for a future
   epic to pick up.
2. **Native tables (rollups included) are not reachable through
   Gravitino** — a pre-existing gap from the foundation epic, not this
   epic's to fix.
3. **No distributed rollups.** Confirmed, not just asserted: zero
   `src/distributed/` changes across tasks 001-003; this task's own
   `binder.rs` fix DOES touch shared planning code, which is exactly why
   the M1/M2 gates were re-run rather than skipped — both PASS, so this
   task's own change introduced no distributed regression either.
4. **No new background scheduler.** Refresh is entirely refresh-on-write
   (EAGER, task 003) — there is no periodic/scheduled recompute of any
   kind.
5. **Rollups only over already-registered NATIVE base tables** — a
   rollup over plain parquet/Iceberg/Lance is refused; staleness
   bookkeeping needs a real `(table_id, version)` pair only a native
   table's manifest provides.
6. **Refresh cost is a full base-table rescan per dependent rollup, not
   an incremental/delta merge** — measured 3-5.6x mutation latency at
   SF=1 with 1-3 rollups (task 003); a materially larger, genuinely new
   algorithm-class effort, named as future work, not attempted.
7. **No `ALTER`/`DROP`/`REFRESH MATERIALIZED VIEW`** — re-running `CREATE
   MATERIALIZED VIEW` under the same name, or a mutation's own automatic
   refresh, are the only "refresh" available.
8. **Rollup-of-a-rollup chains are not automatically cascaded** beyond
   one hop — never built or tested by any task.
9. **The join-spill duplicate-counting bug** (`spill-join-correctness`
   epic, still OPEN, root cause unconfirmed) is unrelated to this epic
   entirely (a pre-existing `SpillableHashJoinExec` issue found by the
   mutation epic's own QA close-out) and was not touched, exercised, or
   affected by anything in this epic — named here only because the PRD's
   own status note mentions it as a standing, tracked, non-blocking item.

### Commits

`760595b` (001 core mechanism) → `8a04a80` (001 integration tests) →
`a545df3` (001 DuckDB oracle) → `ee6d115` (001 CLAUDE.md) → `5d9f9e5`
(001 closed, 25%) → `b0eb81e` (002 DDL surface) → `dba130e` (002 closed,
50%) → `2ecae93` (003 refresh-on-write) → `d209403` (003 closed, 75%) →
`8904592` (004 broader validation sweep + fallback sweep + binder fix,
full suite +9 x4 combos, M1/M2 re-confirmed) → this task's own remaining
docs/archive commit(s).

### Archival

Epic moved to `.claude/epics/archived/native-tables-rollups/` as this
task's final step, mirroring `native-tables-foundation`/
`native-tables-mutation`'s archival pattern (`git mv`, this session). Not
merged to `main` — that decision and action is left to the user/
orchestrating session per this task's own instructions.
