# Optimizer overhead investigation — 2026-09-06

Read-only attribution proposal; no production changes or engine runs were made
for this investigation. Subsequent recorded plans confirm predicate duplication,
and source inspection identifies its rule interaction below. Its contribution
to runtime, and repeated expensive join enumeration, remain unmeasured.

## Recorded evidence

The 630-row native adapter fixture passes all 66 typed comparisons but fails
eight time ceilings: Q7 three times, Q10 twice and Q19 three times. Evidence:
`.scratch/public-bench/native-repeat-01/{manifest.json,report.json,samples.jsonl}`.
The comparison below uses the existing IPC run at
`.scratch/public-bench/adapters-public-contracts-02/decoded_ipc/`; this is
cross-run attribution evidence, not a controlled performance A/B.

| Query | Native optimization | IPC optimization | Native DuckDB calibration median | IPC calibration median |
|---|---:|---:|---:|---:|
| Q7 | 16.95–17.10 ms | 15.20 ms | 1.241 ms | 3.542 ms |
| Q10 | 7.86–9.48 ms | 2.79 ms | 0.867 ms | 2.538 ms |
| Q19 | 6.43–6.47 ms | 6.46 ms | 0.658 ms | 1.749 ms |

Native Q7 execution itself takes 0.198–0.223 ms; Q19 takes 0.088–0.102 ms.
Their engine costs are dominated by optimization. Q19 has effectively identical
optimization cost and physical operator shape in IPC. Its native failure reflects
the faster matched native DuckDB reference and consequently lower 10× ceiling.
Q7 has only a modest optimizer difference. Q10 has a substantial optimizer
difference and a different join shape. These facts do not establish native scan
I/O or metadata retrieval as the dominant cause. Preserve the matched reference
and unchanged ceiling rather than treating the IPC pass as native certification.

## Source inspection

- [`Optimizer::optimize` and `optimize_with_rules`](../src/optimizer/mod.rs)
  rebuild statistics-aware rules per optimization, cloning the complete statistics
  map into five rules. Several structural-proof rules no longer consume those
  statistics. The loop permits ten full passes and formats both full plans using
  `Debug` after every rule to detect changes. Formatting cost and pass counts
  are currently not attributed separately.
- [`JoinReorder::needs_reordering`](../src/optimizer/rules/join_reorder.rs)
  accepts even a single inner join because enumeration also chooses build-side
  orientation. Consequently an unchanged join component can be reconsidered in
  later passes. Whether this actually causes repeated expensive enumeration or
  oscillation in these runs has **not been measured**.
- [`ExecutionContext::collect_table_statistics`](../src/execution/context.rs)
  visits every registered provider per SQL call. The SQL optimization timer
  includes this collection and rule construction, not just rewrite execution.
- [`NativeTable::statistics` and `table_statistics_from`](../src/storage/native_table.rs)
  reconstruct active segment lists, row/byte sums and column statistics from
  in-memory manifest state. This inspection found no per-call disk read in that
  method. Segment count and schema width can affect cost; the recorded timings
  do not isolate it.

**Diagnostic trap:** `Optimizer::optimize_with_diag()` currently calls the default
rule path directly, bypassing the statistics-aware reconstruction in `optimize()`.
Using it as the diagnostic control changes optimizer behavior and cannot faithfully
attribute the measured native path.

## Confirmed predicate growth in the subsequent SF10 provider gate

The existing Q7 records in
`.scratch/public-bench/canonical-sf10-provider-gates-02/decoded_ipc/samples.jsonl`
contain ten identical `n2.n_name IN ('GERMANY', 'FRANCE')` conjuncts nested in
the nation scan filter, and ten copies of the corresponding n1 predicate. The
cross-table OR remains above the joins. This is observed plan duplication in
the frozen candidate gate, not a synthetic SQL substitution or a new run made
for this review. The evidence file was still growing when inspected; the final
gate's completion and timing conclusions belong to its separately retained report.

The source mechanism is concrete:

1. [`DeriveOrPredicates::augment_predicate`](../src/optimizer/rules/derive_or_predicates.rs)
   (lines 57–85 at inspection) flattens only the current Filter's conjunctions.
   It derives implied per-column IN lists from an OR and checks for duplicates
   only against that local list, using expression display strings.
2. [`PredicatePushdown::pushdown`](../src/optimizer/rules/predicate_pushdown.rs)
   (lines 138–162) moves the implied single-side predicates through supported
   joins and aliases. At the destination scan it combines an existing predicate
   with the incoming predicate using `existing AND combined`, without deduplication.
3. The original cross-side OR remains above the join. On the next optimizer
   pass, its Filter no longer contains the previously derived IN predicates:
   they now live in descendant scans. Derivation therefore adds them again,
   and scan pushdown appends another copy. Ten recorded copies are consistent
   with the configured ten-pass limit.

The rule's documented local idempotence does not imply pipeline idempotence.
This confirms a plan-growth defect; it does **not** quantify time spent deriving,
formatting, evaluating repeated predicates, or re-enumerating joins. Do not yet
attribute the native timing failures or any expected speedup to this mechanism.

There is a second convergence issue to address when designing the fix.
`Optimizer::optimize_with_rules` (lines 168–195) records whether **any individual
rule** changed the plan during a round. Deduplicating at the scan alone can leave
the loop running ten times: derivation adds a temporary conjunct, pushdown removes
the redundant copy, and the final plan equals the round's starting plan while
`changed` remains true. Diagnose and fix the actual statistics-aware path reached
by `Optimizer::optimize`; `optimize_with_diag` is not an equivalent substitute.

## General fix alternatives and proof boundaries

A bounded first candidate is stable conjunction deduplication at destination
filters/scans, paired with equality-based convergence across a **complete rule
round**. Flatten AND nodes, preserve first-occurrence order, and remove only
structurally equal predicates whose deterministic semantics justify idempotence.
For such predicates SQL three-valued logic preserves `p AND p = p`, including
NULL. Avoid a blanket rewrite of volatile expressions or evaluation-sensitive
functions without a stated contract. Retain the original OR: its per-column IN
lists are necessary conditions, not an equivalent replacement for correlated
disjuncts.

An alternative is to make derivation consult proven predicates already enforced
by its input subtree. This needs scoped column identity and lineage through
aliases/projections, plus explicit join-type and NULL-extension rules. It must
not infer that a predicate on one side of an outer join holds above the join or
move it across an existing pushdown barrier. This approach is more invasive than
local destination normalization and a whole-round fixed-point check.

Use structural expression/plan equality, not display text or hash equality alone
as semantic proof. Preserve qualified column identity, ordinal/schema scope,
literal type and decimal scale, cast failure mode, collation where applicable,
and NULL behavior. Do not conflate differently ordered or typed IN lists merely
because they look similar. If structural equality is unavailable for a domain,
decline that deduplication rather than guess. Stable derivation order is also
required: the current derivation iterates a HashMap of columns, which is not a
canonical ordering. A hash may accelerate candidate lookup only if exact equality
checks resolve collisions.

Full-round equality proves that the composed rule sequence reached a fixed point
only for deterministic rules with the same inputs/configuration. Preserve the
iteration cap as a backstop and keep the final PackedJoinKeys phase in its
existing position. Detecting an arbitrary cycle or lowering the cap must not be
treated as proof that all rewrite opportunities have converged.

## Focused implementation acceptance tests

- Compose derivation, both pushdown placements and the full statistics-aware
  optimizer over a small two-table disjunctive predicate. Assert exactly one
  implied IN predicate per destination and structural equality after optimizing
  the result again. A rule-only local-idempotence test would miss this defect.
- Cover preexisting scan filters, multiple independent OR predicates, aliases
  with repeated column names, reordered conjuncts and distinct literal/cast
  domains. Preserve original OR residuals and predicates that are not identical.
- Compare typed results with an independent small oracle using duplicates,
  NULLs, empty inputs and multiple batches/partitions. Include LEFT/RIGHT/FULL
  joins to verify existing pushdown and NULL-extension barriers remain intact.
- Exercise full-round termination with deterministic rules that make harmless
  intermediate changes but return the same final plan; assert convergence
  without exhausting ten rounds. Also retain a case requiring more than one
  round, ensuring useful later rewrites are not skipped.
- Run complete adapter and canonical workloads after focused tests, preserving
  all cases and the matched 10× ceiling. Measure rule counts, predicate counts
  and wall time separately before claiming runtime improvement. Implement only
  after the current frozen candidate gates finish.

## Next bounded experiment

1. Add opt-in instrumentation to the existing statistics-aware optimization path.
   Separate per-provider statistics time, rule construction, each rule's execution
   and comparison-formatting time. Record iteration counts, changed-rule sequences
   and join-enumerator invocation counts. Keep diagnostic emission outside primary
   performance claims; do not switch to the behaviorally different diagnostic API.
2. Run all 22 existing adapter fixture queries for native and IPC with identical
   threads, resource limits and timing boundaries. Preserve exact input/SQL hashes,
   plans, every failure and independent typed DuckDB validation. This establishes
   attribution rather than latency acceptance.
3. Select one generic candidate from the evidence: structural equality instead of
   repeated full formatting; avoiding enumeration of proven-unchanged join
   components; or sharing immutable statistics and avoiding redundant copies.
   Preserve plan convergence and semantic-proof rules. Do not lower the iteration
   cap or select behavior using query IDs.
4. Validate the candidate on the complete fixture workload and canonical SF10 with
   matched before/after provenance. Check join orientation, duplicates, NULLs,
   aliases and multi-pass rewrite interactions if convergence logic changes.
   Accept only measured improvement without correctness or protected regressions.

No source edit, test execution, engine invocation or benchmark was performed for
this audit. The next experiment requires its own coordinated contained run through
`scripts/claude-safe-build.sh`, using repository `.scratch` for TMPDIR.
