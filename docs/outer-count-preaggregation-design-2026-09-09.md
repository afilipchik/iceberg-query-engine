# Outer-count preaggregation: source-backed next candidate — 2026-09-09

## Evidence and scope

Canonical SF10 Q13 currently sends14,845,369 probe rows through the outer join
and emits15,345,388 joined rows into the first aggregate. The bounded pipeline
has reduced wall time, but that work remains. The frozen acdb8c51 diagnostics
retain16 admitted producer slots; they do not demonstrate optimal logical work.
See [contiguous-copy evidence](contiguous-resident-copy-2026-09-09.md).

The existing `EagerAggregation::try_rewrite_left_count` recognizes one left group
key, one equality and one right-column COUNT. It removes the final aggregate only
when `properties::proves_unique_column` proves the left key structurally unique.
That requirement is correct. Base-table NDV estimates must never replace it.
The rule currently has no path that preaggregates while retaining the final
aggregate when left uniqueness is unknown. This is a source finding, not a
measured gain or an implemented rewrite.

## General algebra and required semantics

For a left row l, define c(l) as the number of matching right rows whose counted
column is not NULL. The original grouped COUNT is the sum of c(l) over all left
rows in a group. Preaggregate the right relation by its equijoin key with COUNT
of that column. Left join the counts to the original left rows, then retain the
final grouping and sum COALESCE(partial_count,0). Duplicate left rows must remain:
they contribute repeatedly, exactly as in the original join. NULL join keys do
not match; their contribution is zero. A group with only unmatched left rows
still emits zero. Empty scalar aggregation needs an explicit final COALESCE to
restore COUNT's zero instead of SUM's NULL. The engine promotes SUM(Int64) to
Int64; preserve that declared output and checked numeric behavior.

Start from the original operator semantics: pure equijoin, direct right-column
non-DISTINCT counts and left-only grouping expressions whose evaluation order is
safe. Right-side predicates already applied within the right subtree stay there.
Residual ON predicates involving both sides, COUNT(DISTINCT), preserved-right
joins, and volatile/fallible expressions require their own algebra; leave the
existing correct plan when no proof exists. Do not treat estimated ranges,
uniqueness or foreign keys as semantic facts.

Internal partial-count/key names must be fresh across both child schemas and
original outputs. Resolve columns by qualified identity/ordinal; preserve output
names, types, nullability and schema order. Do not repeatedly preaggregate an
already reduced subtree on later optimizer iterations. The existing unique-left
shortcut remains valid only with its structural proof.

## Independent validation before performance claims

Use a hand-computed multiset oracle covering duplicate left keys, duplicate right
keys, nullable counted values, NULL join keys, unmatched groups, empty left and
right inputs, multiple batches/partitions, reordered projection/qualified aliases,
internal-looking user column names, and ordinary/admitted/spill execution.
Exercise optimizer convergence. Verify both the retained-final-aggregate route
and the structurally unique route. Misleading NDV statistics must not change
answers or erase duplicate left multiplicity.

Use statistics only to choose a cost-effective rewrite: preaggregation adds work
when the right input is already nearly unique. Preserve source/filter estimates
as estimates, and use a bounded admitted aggregate that can spill. A plan-shape
assertion alone does not establish resource safety or faster execution.

First reproduce the expected generic plan limitation; then implement and run
semantic gates. Measure frozen Q13 and other applicable workloads, followed by
protected queries and provider/resource/concurrency gates. Canonical Q13 is the
motivating observation, never a routing condition. The later fde271b1 full screen is complete: Q13 and Q17 account for31.65%of
raw median-suite excess, with Q13 alone17.10%. Those query-level differences
do not identify operator CPU shares. The current retained-input progress-credit
release must finish its frozen resource/regression validation before this separate
optimizer change. See the [current provider screen](borrowed-fixed-provider-screen-2026-09-10.md).


## Current implementation details to preserve or repair

A fresh source read during the progress-credit release build confirms that the
existing unique-left shortcut still hardcodes `__ea_cnt` and creates unqualified
preaggregate fields while retaining the original join key expressions. This is a
source-level alias/qualification risk, not a reproduced wrong-result claim.
Before broadening the rewrite, include a structurally unique left subtree whose
user columns contain `__ea_cnt`, qualified right keys and alias projections in the
regression matrix. Require exact output schema and typed values, not just rewrite
activation. Fresh internal identities and correctly rebound join references belong
to the same semantic implementation contract as duplicate preservation.

## First executable semantic probe after the current provider run

Source review during a4103dfa provider25249 confirms the existing
`left_count_requires_proof_on_exact_left_subtree` regression checks rewrite
activation, not execution of the rewritten plan. Add an executed typed case with
left input `__ea_cnt=[1,1,2,NULL]`, right input `fk=[1,1,2,NULL]` and
`v=[10,NULL,NULL,99]`, using a structurally unique left derived table:

```sql
SELECT l.__ea_cnt, COUNT(r.v) AS n
FROM (SELECT DISTINCT __ea_cnt FROM left_source) AS l
LEFT JOIN right_source AS r ON l.__ea_cnt = r.fk
GROUP BY l.__ea_cnt
```

Independent expected multiset: `(1,1),(2,0),(NULL,0)`. Preserve output names/types,
qualified identity and NULL behavior; assert activation separately so a silently
declined rule cannot satisfy the optimized-path coverage. Keep the corresponding
non-DISTINCT left case: `(1,2),(2,0),(NULL,0)`, proving duplicate multiplicity remains.
After25249 completed and its1,420-file archive verified, the executed test
reproduced two binding errors: missing `r.fk`, then ambiguous `__ea_cnt` after
repairing the key identity. Both are repaired in current source. The12-case
matrix checks both planner modes, unique/nonunique left inputs and either empty
side;23 related integrations pass. See
[binding contract](left-count-binding-contract-2026-09-11.md). No performance
measurement belongs to this newer optimizer source yet.

Only after that contract is repaired should the retained-final-SUM generalization
be implemented and measured. Canonical Q13 is motivation for avoiding unnecessary
joined rows, never an activation condition. Preserve numeric overflow/error behavior,
optimizer convergence, ordinary/specialized execution and the independent oracle.
