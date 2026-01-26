# Join Reordering Bug Fix

## Date
2025-01-23

## Bug Description

The `JoinReordering` optimizer rule was losing joins when processing left-deep join trees. For a query like:

```sql
SELECT u.name FROM users u JOIN orders o ON u.id = o.user_id JOIN users u2 ON u.id = u2.id
```

The optimizer would remove the second join, resulting in:
```sql
SELECT u.name FROM users u JOIN orders o ON u.id = o.user_id
```

## Root Cause

In `greedy_join_reorder()`, the algorithm accumulates join results by wrapping them in `LogicalPlan::Join` after each iteration. At the end, it incorrectly unwrapped the accumulated result to extract only the innermost join's children, losing all outer joins.

**Incorrect logic** (original code):
```rust
// After processing all joins
let (left, right) = if let LogicalPlan::Join(join_node) = current_left.as_ref() {
    (join_node.left.clone(), join_node.right.clone())
} else { ... };

Ok(JoinNode {
    left,
    right,
    ...
})
```

For a left-deep tree `LogicalPlan::Join(Join(A, B))`:
- `join_node.left` = A (loses Join(A, B)!)
- `join_node.right` = B
- Returns: `Join(A, B)` instead of the accumulated `Join(Join(A, B), C)`

## Fix

The fix is to NOT unwrap the accumulated join tree. Simply return the final `result` which already contains the complete left-deep join tree:

```rust
// Build a left-deep join tree by processing joins sequentially
let mut result = JoinNode { /* first join */ };

for join in joins.iter().skip(1) {
    result = JoinNode {
        left: Arc::new(LogicalPlan::Join(result.clone())),
        right: join.right.clone(),
        ...
    };
}

Ok(result)  // result already contains the full tree!
```

## Key Learnings

### DO:
- **DO** return the accumulated join result directly - it already contains the full tree structure
- **DO** understand that in left-deep join trees, the left side IS often another join
- **DO** use `Arc::new(LogicalPlan::Join(...))` to wrap joins when they become inputs to other joins

### DON'T:
- **DON'T** unwrap nested join nodes to get their "children" - this loses the join structure
- **DON'T** assume that extracting left/right children from a Join gives you the complete result
- **DON'T** unwrap `LogicalPlan::Join` unless you specifically want to examine its internal structure

## Pattern: Left-Deep Join Trees

A left-deep join tree like `((A JOIN B) JOIN C)` is structured as:
```
Join(
    left: Join(
        left: A,
        right: B
    ),
    right: C
)
```

When building such trees incrementally:
- After first iteration: `result = Join(A, B)`
- After second iteration: `result = Join(left: Join(result), right: C)`
  - Which becomes: `Join(left: Join(Join(A, B)), right: C)`

The key insight: each iteration wraps the PREVIOUS result in a new Join, creating the nesting. The final result IS the complete tree.

## Self-Joins

When the same table is joined multiple times with different aliases (e.g., `users u` and `users u2`):
- Each reference is a SEPARATE scan node with its own schema
- The schemas differ in qualified column names (`u.id` vs `u2.id`)
- The `same_table()` function must consider aliases, not just table names

**Bug in original code**: `same_table()` compared only `table_name`, ignoring aliases. This caused `users u` and `users u2` to be treated as identical.

## References

- **DataFusion**: Does NOT have a join reordering optimizer. Uses simpler join elimination rules instead (`eliminate_join.rs`, `eliminate_cross_join.rs`)
- **Pattern**: DataFusion's optimizer rules use a `rewrite()` method that returns `Transformed<T>` to indicate whether changes were made
