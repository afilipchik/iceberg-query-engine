# Predicate Reordering Optimizer Rule - Design Document

## Origin

This rule was developed on the upstream `main` branch (commit `edbea72`) and is being
integrated into the `ea/ui_and_fixups` branch. The upstream's companion `join_reordering.rs`
is NOT being integrated because the user's branch already has a superior `join_reorder.rs`
(1140 lines, graph-based approach with cross-join elimination).

## Problem Statement

When a SQL query has multiple AND conditions in a WHERE clause, the evaluation order
affects performance. Consider:

```sql
SELECT * FROM lineitem
WHERE l_shipdate >= DATE '1994-01-01'      -- range: ~30% selectivity
  AND l_discount BETWEEN 0.05 AND 0.07     -- range: ~2% selectivity
  AND l_quantity < 24                       -- range: ~46% selectivity
```

If we evaluate conditions left-to-right, we process all rows through the first filter.
If we reorder to evaluate the most selective predicate first (BETWEEN at ~2%), we can
skip evaluating later predicates for 98% of rows.

## How Other Systems Do It

### DuckDB
- Uses **heuristic selectivity** based on operator type (equality ~10%, range ~33%)
- Reorders during the optimizer phase as a simple rule
- Enhances with actual column statistics when available (NDV, min/max)
- Source: `src/optimizer/filter_pushdown.cpp`

### Velox (Meta)
- Uses **statistics-based selectivity** when available from Hive metastore
- Falls back to heuristic estimates similar to our approach
- Also considers expression evaluation cost (simple column compare vs. function call)

### DataFusion (Apache Arrow)
- `PredicateReorder` rule sorts predicates by estimated selectivity
- Uses `0.1` for equality, `0.3` for range, `0.05` for IS NULL, etc.
- Very similar to our implementation approach

## Design Decisions

### 1. Selectivity Heuristics

We use conservative heuristics matching DataFusion's approach:

| Predicate Type | Estimated Selectivity | Rationale |
|---|---|---|
| `col = literal` | 0.1 | Assumes ~10 distinct values |
| `col < literal` (range) | 0.3 | Assumes uniform distribution, ~30% pass |
| `LIKE 'prefix%'` | 0.2 | Prefix match is fairly selective |
| `LIKE '%suffix'` | 0.5 | Leading wildcard, not very selective |
| `IS NULL` | 0.05 | Most columns are non-null |
| `IS NOT NULL` | 0.95 | Most columns are non-null |
| `IN (list)` | min(len/100, 0.5) | Proportional to list size |
| `BETWEEN` | 0.1 | Range predicate, usually selective |
| `col != literal` | 0.9 | Most rows pass inequality |
| Subqueries/others | 0.5 | Default, no strong heuristic |

### 2. Integration Point in Optimizer Pipeline

The rule runs AFTER all other optimization passes in the current pipeline:

```
ConstantFolding → PredicatePushdown → FlattenDependentJoin → SubqueryDecorrelation
  → JoinReorder → PredicatePushdown → ProjectionPushdown → **PredicateReordering**
```

**Rationale**: By running last, it operates on predicates that have already been
pushed down to the optimal position. It doesn't change WHAT gets filtered, only
the ORDER in which conjuncts are evaluated.

### 3. Cost Model Integration

Uses the existing `CostEstimator` from `src/optimizer/cost.rs` for row count
estimates. This is a lightweight integration: the selectivity heuristics are
self-contained, and `CostEstimator` is only used for the `is_unique_column_comparison`
path (currently a placeholder).

### 4. What This Rule Does NOT Do

- Does NOT split conjunctions across multiple Filter nodes (that's PredicatePushdown's job)
- Does NOT consider expression evaluation cost (future enhancement)
- Does NOT use actual column statistics from Parquet row groups (future enhancement)
- Does NOT reorder OR disjunctions (much more complex, diminishing returns)

## Impact Estimate

On TPC-H at SF=0.1:
- **Q6** (3 range predicates): Potential 10-20% improvement from better predicate order
- **Q12** (date range + enum equality): Moderate improvement
- **Q19** (multiple OR + AND): Limited improvement (OR disjunctions not reordered)
- Overall: Low-risk, modest-reward optimization that establishes infrastructure
  for future statistics-based improvements

## Files Modified

- `src/optimizer/rules/predicate_reordering.rs` — NEW: the rule implementation
- `src/optimizer/rules/mod.rs` — add module and re-export
- `src/optimizer/mod.rs` — register in optimizer pipeline (after ProjectionPushdown)
