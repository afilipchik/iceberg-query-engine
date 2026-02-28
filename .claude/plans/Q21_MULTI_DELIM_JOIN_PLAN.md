# Q21 Multi-DelimJoin Implementation Plan

## Goal
Complete the DelimJoin optimization for multi-EXISTS patterns (Q21: `EXISTS(...) AND NOT EXISTS(...)`) to achieve 2790x improvement on Q21, 12x overall.

## Background

### Problem
- Q21 has `WHERE EXISTS(l2) AND NOT EXISTS(l3)` - two correlated subqueries on the same column
- Current `FlattenDependentJoin` only handles single EXISTS/IN (see `flatten_dependent_join.rs:165`)
- Multi-EXISTS falls back to O(n²) row-by-row execution via `SubqueryExecutor`
- Result: Q21 takes 61 seconds instead of 22ms (2790x slower than DuckDB)

### Root Cause
The `try_flatten_filter` function in `flatten_dependent_join.rs:147-211` returns `None` when there are multiple subqueries:
```rust
if subquery_exprs.len() > 1 {
    return Ok(None);  // Falls through to SubqueryDecorrelation which does row-by-row
}
```

### Solution Architecture
Create a `MultiDelimJoinNode` that:
1. Collects distinct correlation values from the outer side ONCE
2. Passes them to ALL inner subqueries via a shared `DelimGet`
3. Evaluates each EXISTS/NOT EXISTS independently
4. Combines results using Semi/Anti join semantics

## Constraints

1. **Correctness First**: Never sacrifice correctness for performance
2. **Simple Code**: Prefer readable, maintainable code over clever optimizations
3. **Use Benchmark Harness**: Validate with TPC-H benchmark after each change
4. **Incremental Changes**: One small change at a time, test after each

## Files to Modify

| File | Purpose |
|------|---------|
| `src/planner/logical_plan.rs` | Add `MultiDelimJoinNode` definition |
| `src/optimizer/rules/flatten_dependent_join.rs` | Implement `try_flatten_multi_exists` |
| `src/physical/planner.rs` | Handle `MultiDelimJoin` in physical planning |
| `src/physical/operators/delim_join.rs` | Add `MultiDelimJoinExec` physical operator |
| `src/physical/operators/mod.rs` | Export new operator |

## Step-by-Step Implementation

### Step 1: Add MultiDelimJoinNode to Logical Plan (30 min)

**File**: `src/planner/logical_plan.rs`

**What**: Add a new node type for multiple delim joins

```rust
/// Multi-DelimJoin for multiple EXISTS/NOT EXISTS with same correlation
///
/// Example: WHERE EXISTS(SELECT * FROM l2 WHERE l2.key = l1.key AND l2.x = y)
///               AND NOT EXISTS(SELECT * FROM l3 WHERE l3.key = l1.key)
///
/// This creates a single distinct set of l1.key values that all inner
/// subqueries can probe against.
#[derive(Debug, Clone)]
pub struct MultiDelimJoinNode {
    /// The outer (left) side of the join
    pub left: Arc<LogicalPlan>,
    /// Inner sides - one per EXISTS/NOT EXISTS
    /// Each is a subquery rewritten to probe DelimGet
    pub inner_sides: Vec<Arc<LogicalPlan>>,
    /// Join type for each inner side (Semi for EXISTS, Anti for NOT EXISTS)
    pub join_types: Vec<JoinType>,
    /// Columns to deduplicate from outer side
    pub delim_columns: Vec<Expr>,
    /// Join conditions for each inner side
    pub on: Vec<(Expr, Expr)>,
    /// Output schema (same as left schema for Semi/Anti joins)
    pub schema: PlanSchema,
}
```

**Add to LogicalPlan enum**:
```rust
pub enum LogicalPlan {
    // ... existing variants ...
    MultiDelimJoin(MultiDelimJoinNode),
}
```

**Verification**: `cargo check` should pass

### Step 2: Implement try_flatten_multi_exists (1 hour)

**File**: `src/optimizer/rules/flatten_dependent_join.rs`

**What**: Detect multi-EXISTS patterns and create MultiDelimJoin

**Key Logic**:

```rust
/// Try to flatten multiple EXISTS/NOT EXISTS with same correlation into MultiDelimJoin
fn try_flatten_multi_exists(
    outer: &LogicalPlan,
    subquery_exprs: &[Expr],
    other_predicates: Vec<Expr>,
) -> Result<Option<LogicalPlan>> {
    // 1. Check all are EXISTS/NOT EXISTS (not IN or scalar)
    let mut all_exists = Vec::new();
    for expr in subquery_exprs {
        if let Expr::Exists { subquery, negated } = expr {
            all_exists.push((*negated, subquery.as_ref()));
        } else {
            return Ok(None); // Can't handle mixed types
        }
    }

    // 2. Extract correlation from each subquery
    let mut correlations: Vec<(bool, Vec<CorrelationColumn>, LogicalPlan)> = Vec::new();
    for (negated, subquery) in &all_exists {
        let outer_columns = collect_plan_column_names(outer);
        let (corr_columns, decorrelated) = extract_correlation_info(subquery, &outer_columns)?;

        if corr_columns.is_empty() {
            return Ok(None); // Not correlated, can't use DelimJoin
        }

        correlations.push((*negated, corr_columns, decorrelated));
    }

    // 3. Check all share the SAME correlation columns
    let first_corr = &correlations[0].1;
    for (_, corr_cols, _) in &correlations[1..] {
        if corr_cols.len() != first_corr.len() {
            return Ok(None);
        }
        // Check each correlation column matches
        for (a, b) in first_corr.iter().zip(corr_cols.iter()) {
            if a.outer_expr != b.outer_expr {
                return Ok(None);
            }
        }
    }

    // 4. Create DelimGet node
    let delim_id = next_delim_id();
    let delim_schema = build_delim_schema(first_corr);
    let delim_get = LogicalPlan::DelimGet(DelimGetNode {
        columns: first_corr.iter().map(|c| c.outer_expr.clone()).collect(),
        schema: delim_schema,
        delim_id,
    });

    // 5. Rewrite each inner subquery to probe DelimGet
    let mut inner_sides = Vec::new();
    let mut join_types = Vec::new();

    for (negated, corr_cols, decorrelated) in correlations {
        let rewritten = rewrite_with_delim_get(&decorrelated, &corr_cols, &delim_get, false)?;
        inner_sides.push(Arc::new(rewritten));
        join_types.push(if negated { JoinType::Anti } else { JoinType::Semi });
    }

    // 6. Create MultiDelimJoin
    let delim_columns: Vec<Expr> = first_corr.iter().map(|c| c.outer_expr.clone()).collect();
    let join_on: Vec<(Expr, Expr)> = first_corr.iter()
        .map(|c| (c.outer_expr.clone(), Expr::column(&c.inner_col)))
        .collect();

    let multi_delim = LogicalPlan::MultiDelimJoin(MultiDelimJoinNode {
        left: Arc::new(outer.clone()),
        inner_sides,
        join_types,
        delim_columns,
        on: join_on,
        schema: outer.schema(),
    }));

    // 7. Apply remaining predicates if any
    if other_predicates.is_empty() {
        Ok(Some(multi_delim))
    } else {
        let combined = combine_predicates(other_predicates);
        Ok(Some(LogicalPlan::Filter(FilterNode {
            input: Arc::new(multi_delim),
            predicate: combined,
        })))
    }
}
```

**Update try_flatten_filter**:

```rust
fn try_flatten_filter(node: &FilterNode) -> Result<Option<LogicalPlan>> {
    let (subquery_exprs, other_predicates) = extract_subquery_predicates(&node.predicate);

    if subquery_exprs.is_empty() {
        return Ok(None);
    }

    // NEW: Try multi-EXISTS first
    if subquery_exprs.len() > 1 {
        if let Some(multi) = try_flatten_multi_exists(&(*node.input), &subquery_exprs, other_predicates)? {
            return Ok(Some(multi));
        }
        // Fall through to single subquery handling
    }

    // ... existing single subquery handling ...
}
```

**Verification**: `cargo check` should pass

### Step 3: Add MultiDelimJoinExec Physical Operator (1 hour)

**File**: `src/physical/operators/delim_join.rs`

**What**: Execute MultiDelimJoin by evaluating each inner side

**Key Logic**:

```rust
/// Multi-DelimJoin execution operator
///
/// Executes multiple EXISTS/NOT EXISTS checks using shared distinct values
pub struct MultiDelimJoinExec {
    left: Arc<dyn PhysicalOperator>,
    inner_sides: Vec<Arc<dyn PhysicalOperator>>,
    join_types: Vec<JoinType>,
    delim_columns: Vec<Expr>,
    on: Vec<(Expr, Expr)>,
    schema: SchemaRef,
    state: Arc<DelimState>,
}

#[async_trait]
impl PhysicalOperator for MultiDelimJoinExec {
    async fn execute(&self, partition: usize) -> Result<RecordBatchStream> {
        // Phase 1: Collect outer side and build distinct values
        let outer_stream = self.left.execute(partition).await?;
        let outer_batches: Vec<RecordBatch> = outer_stream.try_collect().await?;

        // Build distinct values for correlation columns
        let distinct_batch = build_distinct_values(&outer_batches, &self.delim_columns)?;
        self.state.set_distinct_values(distinct_batch, self.build_delim_schema()?);

        // Phase 2: For each inner side, evaluate EXISTS/NOT EXISTS
        let mut match_sets: Vec<HashSet<u64>> = Vec::new();

        for (i, inner_op) in self.inner_sides.iter().enumerate() {
            let inner_stream = inner_op.execute(partition).await?;
            let inner_batches: Vec<RecordBatch> = inner_stream.try_collect().await?;

            // Build hash set of matching correlation keys
            let matches = build_match_set(&inner_batches, &self.on)?;
            match_sets.push(matches);
        }

        // Phase 3: Filter outer rows based on all conditions
        let filtered = filter_outer_rows(
            &outer_batches,
            &self.delim_columns,
            &match_sets,
            &self.join_types,
        )?;

        Ok(Box::pin(futures::stream::iter(filtered.into_iter().map(Ok))))
    }
}
```

**Verification**: `cargo test --lib` should pass

### Step 4: Wire into Physical Planner (30 min)

**File**: `src/physical/planner.rs`

**What**: Handle `LogicalPlan::MultiDelimJoin` in `create_physical_plan`

```rust
LogicalPlan::MultiDelimJoin(node) => {
    let left = self.create_physical_plan(&node.left)?;

    // Create shared state for DelimGet
    let state = Arc::new(DelimState::new());

    // Create physical operators for each inner side
    // Note: They need access to the same DelimState
    let mut inner_sides = Vec::new();
    // ... create inner operators with state injection ...

    let exec = MultiDelimJoinExec::new(
        left,
        inner_sides,
        node.join_types.clone(),
        node.delim_columns.clone(),
        node.on.clone(),
        Arc::new(plan_schema_to_arrow(&node.schema)),
        state,
    );

    Ok(Arc::new(exec))
}
```

**Verification**: `cargo test --lib` should pass

### Step 5: Test with Q21 (30 min)

**Command**:
```bash
# First verify Q21 still returns correct results
cargo run --release -- query --num 21 --sf 0.01

# Then benchmark
cargo run --release -- benchmark --sf 0.01
```

**Expected Results**:
- Q21 should return correct row count (matching DuckDB)
- Q21 time should drop from 61s to ~1-2s at SF=0.1

## Testing Strategy

### Unit Tests
1. Add test for `try_flatten_multi_exists` with two EXISTS
2. Add test for EXISTS + NOT EXISTS combination
3. Add test for fallback when correlations don't match

### Integration Tests
1. Run all 22 TPC-H queries at SF=0.01
2. Verify Q21 row count matches expected
3. Run full benchmark at SF=0.1

### Regression Tests
```bash
# Run after each step
cargo test --lib
cargo test --test sql_comprehensive
```

## Rollback Plan

If issues arise:
1. `MultiDelimJoin` creation returns `None` -> falls back to existing path
2. Physical planner missing case -> returns `NotImplemented` error
3. No crashes - just slower execution

## Success Criteria

1. All existing tests pass
2. Q21 returns correct results at all scale factors
3. Q21 time at SF=0.1 drops from 61s to <5s (10x improvement)
4. Other queries unaffected

## Notes for Implementation

### Key Insight
The critical insight is that Q21's two EXISTS subqueries BOTH correlate on `l1.l_orderkey`. This means:
- We extract distinct `l1.l_orderkey` values ONCE
- We probe BOTH inner subqueries with the SAME values
- We combine results: row matches if EXISTS matches AND NOT EXISTS doesn't match

### Common Pitfalls
1. **Schema mismatch**: Ensure DelimGet schema matches what inner joins expect
2. **Column qualification**: l1.l_orderkey vs l_orderkey - be consistent
3. **Hash function**: Use same hash for outer distinct and inner probing
4. **NULL handling**: EXISTS with NULL correlation should be handled correctly

### Debugging Tips
1. Add debug logging to `try_flatten_multi_exists` to see when it's triggered
2. Print the correlation columns detected for each subquery
3. Verify DelimGet returns expected distinct values
4. Check match sets contain expected keys

## Estimated Time

| Step | Time | Notes |
|------|------|-------|
| Step 1 | 30 min | Add node type |
| Step 2 | 1 hour | Implement flatten logic |
| Step 3 | 1 hour | Add physical operator |
| Step 4 | 30 min | Wire planner |
| Step 5 | 30 min | Test and debug |
| **Total** | **3.5 hours** | |

## Progress Tracking

- [x] Step 1: Add MultiDelimJoinNode
- [x] Step 2: Implement try_flatten_multi_exists
- [x] Step 3: Add MultiDelimJoinExec
- [x] Step 4: Wire into Physical Planner
- [x] Step 5: Test with Q21
- [x] Run full benchmark
- [ ] Update CLAUDE.md with results

## Results (2026-02-13)

**Q21 Performance:**
- Before: 61 seconds (O(n²) row-by-row execution)
- After: 41ms (**1500x improvement!**)

**Full Benchmark (SF=0.01):**
- Total time: 991ms (was ~67 seconds)
- 20/22 queries successful
- **67x overall improvement**
