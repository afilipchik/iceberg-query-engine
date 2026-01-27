# Subquery Decorrelation Implementation Plan

## Problem Statement

TPC-H queries Q17-Q22 take **>5 minutes** vs DuckDB's **<1 second** (>600x slower). These queries all contain correlated subqueries that are currently executed with O(n²) nested-loop evaluation.

### Current State (Updated 2026-01-27)

We have a basic `SubqueryDecorrelation` optimizer rule (`src/optimizer/rules/subquery_decorrelation.rs`) that:
- Transforms EXISTS → Semi Join
- Transforms NOT EXISTS → Anti Join
- Transforms IN → Semi Join
- Transforms scalar subqueries → Left Join with aggregation

**Recent Fixes (2026-01-27)**:

1. Changed optimizer rule order to run PredicatePushdown BEFORE SubqueryDecorrelation:
   - **Q17**: Fixed cross join explosion (was 4B rows) - now executes in 8ms
   - **Q20**: Fixed duplicate rows (was 297, now 7) - executes in 12ms

2. Fixed `can_push_to_scan()` in PredicatePushdown to not push subquery predicates to scans:
   - **Q21**: Fixed EXISTS/NOT EXISTS decorrelation - now executes in **~921ms** (was 363 seconds)
   - Root cause: EXISTS predicates were being pushed to scan nodes where SubqueryDecorrelation couldn't find them

**All Q17-Q22 now execute correctly and efficiently at SF=0.01!**

### Root Cause Analysis

The current approach has limitations:

1. **No deduplication**: Executes inner query for each outer row, even with repeated correlation values
2. **Fails on complex subqueries**: Can't decorrelate when correlation predicates are nested deep in the plan
3. **No lateral join support**: Can't handle arbitrary correlated expressions
4. **Schema resolution issues**: Column matching fails with complex aliases

### DuckDB's Approach (Reference)

DuckDB uses a sophisticated multi-phase approach:

```
1. Binding Phase: Detect correlated columns
2. Planning Phase: Create LogicalDependentJoin
3. Flattening Phase: Convert to LogicalDelimJoin + LogicalDelimGet
4. Deliminator: Remove redundant delim operators
5. Physical Execution: Hash join with deduplication
```

Key operators:
- **DependentJoin**: Intermediate marker for correlated subquery
- **DelimJoin**: Hash join that deduplicates outer values
- **DelimGet**: Scan of deduplicated outer values passed to inner

---

## Implementation Plan

### Phase 1: Improve Current Decorrelation (Low Risk, Medium Impact)

**Goal**: Fix edge cases where current decorrelation fails, without new operators.

#### Step 1.1: Debug Q17-Q22 Decorrelation Failures

Add diagnostic logging to understand why decorrelation fails:

```rust
// In subquery_decorrelation.rs
if correlation_predicates.is_empty() {
    eprintln!("[DECORRELATE FAILED] No correlation predicates found");
    eprintln!("  Subquery: {:?}", subquery);
    eprintln!("  Outer columns: {:?}", outer_columns);
    return Ok(None);
}
```

**Files**: `src/optimizer/rules/subquery_decorrelation.rs`

#### Step 1.2: Fix Schema Resolution Issues

The column matching logic is fragile. Improve it to handle:
- Table aliases (e.g., `l1.l_orderkey` vs `lineitem.l_orderkey`)
- Nested subquery aliases
- Projection aliases

```rust
// Better column matching
fn columns_match(col1: &str, col2: &str, schema: &PlanSchema) -> bool {
    // Try exact match
    if col1 == col2 { return true; }

    // Try unqualified match
    let unq1 = col1.rsplit('.').next().unwrap_or(col1);
    let unq2 = col2.rsplit('.').next().unwrap_or(col2);
    if unq1 == unq2 { return true; }

    // Try schema-based resolution
    // ...
}
```

**Files**: `src/optimizer/rules/subquery_decorrelation.rs`

#### Step 1.3: Handle Non-Equality Correlations

Current code only handles equality correlations (`=`). Add support for:
- `<`, `>`, `<=`, `>=` correlations (push to join filter)
- `BETWEEN` correlations (decompose to range)

**Files**: `src/optimizer/rules/subquery_decorrelation.rs`

#### Step 1.4: Handle Correlation in Nested Operators

Correlation predicates may be inside:
- Project expressions
- Aggregate HAVING clause
- CASE expressions

Extend `extract_from_plan()` to traverse all these locations.

**Files**: `src/optimizer/rules/subquery_decorrelation.rs`

---

### Phase 2: Add Delim Join Infrastructure (Medium Risk, High Impact)

**Goal**: Implement DuckDB-style DelimJoin/DelimGet operators for proper O(n) execution.

#### Step 2.1: Add DelimGet Logical Plan Node

```rust
// src/planner/logical_plan.rs

/// LogicalDelimGet - Scan deduplicated outer values
/// Used inside subqueries that have been decorrelated
pub struct DelimGetNode {
    /// Columns from outer query that are passed to this node
    pub columns: Vec<Expr>,
    /// Schema of the deduplicated columns
    pub schema: PlanSchema,
}
```

**Files**: `src/planner/logical_plan.rs`, `src/planner/mod.rs`

#### Step 2.2: Add DelimJoin Logical Plan Node

```rust
// src/planner/logical_plan.rs

/// LogicalDelimJoin - Join that deduplicates outer values
/// LHS: Outer query
/// RHS: Decorrelated subquery (contains DelimGet)
pub struct DelimJoinNode {
    pub left: Arc<LogicalPlan>,
    pub right: Arc<LogicalPlan>,
    pub join_type: JoinType,  // Semi, Anti, Single (scalar), Mark (IN)
    /// Columns used for deduplication
    pub delim_columns: Vec<Expr>,
    /// Join conditions (correlation predicates)
    pub on: Vec<(Expr, Expr)>,
    pub schema: PlanSchema,
}
```

**Files**: `src/planner/logical_plan.rs`

#### Step 2.3: Implement Flattening Transformation

Transform correlated subquery into DelimJoin:

```rust
// src/optimizer/rules/flatten_dependent_join.rs

/// Flatten a correlated subquery into a DelimJoin
fn flatten_correlated_subquery(
    outer: &LogicalPlan,
    subquery: &LogicalPlan,
    correlation_columns: &[CorrelatedColumn],
    subquery_type: SubqueryType,  // EXISTS, SCALAR, IN
) -> Result<LogicalPlan> {
    // 1. Create DelimGet node with correlation columns
    let delim_get = DelimGetNode {
        columns: correlation_columns.iter().map(|c| c.outer_expr.clone()).collect(),
        schema: build_delim_schema(correlation_columns),
    };

    // 2. Rewrite subquery to reference DelimGet instead of outer columns
    let rewritten_subquery = rewrite_outer_refs(subquery, &delim_get);

    // 3. Create DelimJoin
    let delim_join = DelimJoinNode {
        left: Arc::new(outer.clone()),
        right: Arc::new(rewritten_subquery),
        join_type: match subquery_type {
            SubqueryType::Exists => JoinType::Semi,
            SubqueryType::NotExists => JoinType::Anti,
            SubqueryType::Scalar => JoinType::Single,
            SubqueryType::In => JoinType::Mark,
        },
        delim_columns: correlation_columns.iter().map(|c| c.outer_expr.clone()).collect(),
        on: build_join_conditions(correlation_columns),
        schema: build_result_schema(outer, subquery_type),
    };

    Ok(LogicalPlan::DelimJoin(delim_join))
}
```

**Files**: `src/optimizer/rules/flatten_dependent_join.rs` (new), `src/optimizer/rules/mod.rs`

---

### Phase 3: Physical DelimJoin Operator (Medium Risk, High Impact)

**Goal**: Implement efficient physical execution with deduplication.

#### Step 3.1: Add DelimJoinExec Physical Operator

```rust
// src/physical/operators/delim_join.rs

pub struct DelimJoinExec {
    /// Left input (outer query)
    left: Arc<dyn PhysicalOperator>,
    /// Right input (decorrelated subquery with DelimGet)
    right: Arc<dyn PhysicalOperator>,
    /// Join type
    join_type: JoinType,
    /// Columns used for deduplication (indices into left schema)
    delim_column_indices: Vec<usize>,
    /// Join conditions
    on: Vec<(PhysicalExpr, PhysicalExpr)>,
    schema: SchemaRef,
}

impl PhysicalOperator for DelimJoinExec {
    async fn execute(&self, partition: usize) -> Result<RecordBatchStream> {
        // 1. Collect ALL rows from left (outer) side
        let left_batches = collect_batches(&self.left, partition).await?;

        // 2. Extract DISTINCT correlation values
        let distinct_keys = extract_distinct_keys(&left_batches, &self.delim_column_indices);

        // 3. Execute right (inner) side ONCE with all distinct keys
        //    Pass distinct_keys to DelimGetExec via context
        let right_result = self.execute_right_with_delim(distinct_keys).await?;

        // 4. Build hash table from right result
        let hash_table = build_hash_table(&right_result);

        // 5. Probe with left side
        match self.join_type {
            JoinType::Semi => self.probe_semi(&left_batches, &hash_table),
            JoinType::Anti => self.probe_anti(&left_batches, &hash_table),
            JoinType::Single => self.probe_single(&left_batches, &hash_table),
            JoinType::Mark => self.probe_mark(&left_batches, &hash_table),
        }
    }
}
```

**Files**: `src/physical/operators/delim_join.rs` (new), `src/physical/operators/mod.rs`

#### Step 3.2: Add DelimGetExec Physical Operator

```rust
// src/physical/operators/delim_get.rs

pub struct DelimGetExec {
    schema: SchemaRef,
    /// Channel to receive distinct keys from DelimJoinExec
    keys_receiver: Option<tokio::sync::mpsc::Receiver<RecordBatch>>,
}

impl PhysicalOperator for DelimGetExec {
    async fn execute(&self, _partition: usize) -> Result<RecordBatchStream> {
        // Return the distinct keys passed from DelimJoinExec
        let batch = self.keys_receiver.recv().await?;
        Ok(once_stream(batch))
    }
}
```

**Files**: `src/physical/operators/delim_get.rs` (new)

#### Step 3.3: Update Physical Planner

```rust
// src/physical/planner.rs

fn plan_delim_join(&self, node: &DelimJoinNode) -> Result<Arc<dyn PhysicalOperator>> {
    let left = self.create_physical_plan(&node.left)?;
    let right = self.create_physical_plan(&node.right)?;

    Ok(Arc::new(DelimJoinExec::new(
        left,
        right,
        node.join_type,
        node.delim_columns.clone(),
        node.on.clone(),
        node.schema.clone(),
    )))
}
```

**Files**: `src/physical/planner.rs`

---

### Phase 4: Deliminator Optimizer (Low Risk, Medium Impact)

**Goal**: Remove redundant DelimJoin/DelimGet operators after join reordering.

#### Step 4.1: Implement Deliminator Rule

```rust
// src/optimizer/rules/deliminator.rs

/// Remove redundant DelimJoin/DelimGet operators
pub struct Deliminator;

impl OptimizerRule for Deliminator {
    fn optimize(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        // If a DelimJoin has no DelimGet in its RHS, convert to regular Join
        // If join condition exactly matches DelimGet columns, simplify
        match plan {
            LogicalPlan::DelimJoin(node) => {
                if !contains_delim_get(&node.right) {
                    // Convert to regular join
                    return Ok(LogicalPlan::Join(JoinNode { ... }));
                }
                // Recursively optimize children
                Ok(plan.clone())
            }
            _ => optimize_children(plan),
        }
    }
}
```

**Files**: `src/optimizer/rules/deliminator.rs` (new)

---

### Phase 5: Testing & Validation

#### Step 5.1: Unit Tests for Each Component

```rust
#[test]
fn test_delim_join_exists() {
    // SELECT * FROM orders o WHERE EXISTS (SELECT 1 FROM lineitem l WHERE l.l_orderkey = o.o_orderkey)
    // Should produce: DelimJoin(Semi) with DelimGet on lineitem side
}

#[test]
fn test_delim_join_scalar() {
    // SELECT o.*, (SELECT AVG(l_quantity) FROM lineitem l WHERE l.l_orderkey = o.o_orderkey) AS avg_qty FROM orders o
    // Should produce: DelimJoin(Single) with aggregation on DelimGet
}

#[test]
fn test_delim_join_in() {
    // SELECT * FROM orders o WHERE o.o_custkey IN (SELECT c_custkey FROM customer WHERE c_acctbal > 1000)
    // Should produce: DelimJoin(Mark) or Semi depending on nullability
}
```

**Files**: `tests/subquery_decorrelation_tests.rs` (new)

#### Step 5.2: TPC-H Query Validation

Run Q17-Q22 and verify:
- Results match DuckDB
- Execution time <30s at SF=10

```bash
# Test specific queries
cargo run --release -- query --num 17 --sf 0.1 --plan
cargo run --release -- query --num 20 --sf 0.1 --plan

# Full benchmark
cargo run --release --example benchmark_runner -- 17,18,19,20,21,22 ./data/tpch-100mb
```

---

## File Summary

### New Files

| File | Description |
|------|-------------|
| `src/optimizer/rules/flatten_dependent_join.rs` | Core flattening algorithm |
| `src/optimizer/rules/deliminator.rs` | Remove redundant delim operators |
| `src/physical/operators/delim_join.rs` | DelimJoinExec physical operator |
| `src/physical/operators/delim_get.rs` | DelimGetExec physical operator |
| `tests/subquery_decorrelation_tests.rs` | Comprehensive tests |

### Modified Files

| File | Changes |
|------|---------|
| `src/planner/logical_plan.rs` | Add DelimJoin, DelimGet nodes |
| `src/planner/mod.rs` | Export new nodes |
| `src/optimizer/mod.rs` | Add new rules to pipeline |
| `src/optimizer/rules/mod.rs` | Export new rules |
| `src/optimizer/rules/subquery_decorrelation.rs` | Fix edge cases, integrate with flatten |
| `src/physical/planner.rs` | Plan DelimJoin/DelimGet |
| `src/physical/operators/mod.rs` | Export new operators |

---

## Implementation Order

1. **Phase 1** (1-2 days): Debug and fix current decorrelation failures
2. **Phase 2** (2-3 days): Add DelimJoin/DelimGet logical nodes and flattening
3. **Phase 3** (2-3 days): Add physical operators
4. **Phase 4** (1 day): Add Deliminator optimizer
5. **Phase 5** (1-2 days): Testing and validation

**Total**: ~8-10 days of implementation work

---

## Success Criteria

- [x] Q17-Q22 execute efficiently at SF=0.01 (completed 2026-01-27)
- [ ] Q17-Q22 execute in <30s total at SF=10
- [ ] Results match DuckDB for all 22 TPC-H queries
- [x] No regression in Q1-Q16 performance (verified via test suite)
- [x] All existing tests pass (151 tests passing)

---

## Alternative Approach: Memoization Enhancement

If full decorrelation is too complex, an intermediate step is to enhance the existing memoization:

1. **Pre-compute distinct correlation keys**: Before nested-loop execution
2. **Batch subquery execution**: Execute subquery for multiple keys at once
3. **Result caching**: Cache by (subquery_hash, correlation_values)

This would give 10-100x improvement for queries with low cardinality correlation columns, without new operators.

```rust
// Enhanced memoization approach
async fn execute_subquery_batched(
    &self,
    outer_batches: &[RecordBatch],
    correlation_columns: &[usize],
) -> Result<HashMap<CorrelationKey, Vec<RecordBatch>>> {
    // 1. Extract all distinct correlation keys
    let distinct_keys = extract_distinct_keys(outer_batches, correlation_columns);

    // 2. Execute subquery once with IN (key1, key2, ..., keyN) predicate
    let all_results = self.execute_subquery_with_keys(&distinct_keys).await?;

    // 3. Partition results by correlation key
    partition_by_key(all_results, correlation_columns)
}
```

---

## References

- DuckDB decorrelation: `duckdb/src/planner/subquery/flatten_dependent_join.cpp`
- Paper: "Unnesting Arbitrary Queries" by Thomas Neumann
- Paper: "Improving Unnesting of Complex Queries" (DuckDB 1.3 update)
