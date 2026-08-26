---
name: native-tables-rollups
status: in-progress
created: 2026-08-26T06:43:51Z
updated: 2026-08-26T12:00:00Z
progress: 25%
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
- [ ] 002.md - SQL DDL surface — CREATE MATERIALIZED VIEW (parallel: false)
- [ ] 003.md - Staleness/refresh-on-write model (parallel: false)
- [ ] 004.md - QA close-out — cell-exact validation, full suite, docs, epic close (parallel: false)

Total tasks: 4
Parallel tasks: 0
Sequential tasks: 4
Estimated total effort: genuinely uncertain, dominated by task 001 (now
closed — see its own Outcome for what remains: tasks 002-004)
