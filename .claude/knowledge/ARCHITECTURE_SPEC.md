# Query Engine Architecture Specification

> **Purpose**: A living document capturing architectural decisions, patterns, and learnings. This serves as a "knowledge skyscraper" for future development sessions.

---

## Core Architecture Principles

### 1. Query Processing Pipeline

```
SQL → Parser → Binder → Optimizer → Physical Planner → Execution
       (AST)   (LogicalPlan) (OptimizedPlan) (PhysicalOperator) (RecordBatch stream)
```

### 2. Key Design Decisions

| Decision | Rationale | Trade-offs |
|----------|-----------|------------|
| Streaming execution | Memory-efficient, handles large datasets | Slightly more complex operator interface |
| Hash-based joins/aggregations | O(n) vs O(n²) for nested loops | Memory overhead for hash tables |
| Custom Result<T> type alias | Simpler error handling | Only one error type (QueryError) |
| Arc<LogicalPlan> for children | Efficient sharing/cloning | Reference counting overhead |

---

## Logical Plan Architecture

### Node Types and Their Patterns

```rust
// Each node follows this pattern:
pub struct SomeNode {
    pub input: Arc<LogicalPlan>,  // Children are always Arc-wrapped
    // ... node-specific fields
    pub schema: PlanSchema,       // Output schema
}
```

### Adding a New Logical Plan Node

**Steps** (must do ALL of these):

1. **Add struct** in `src/planner/logical_plan.rs`:
   ```rust
   #[derive(Debug, Clone, PartialEq)]
   pub struct NewNode {
       pub input: Arc<LogicalPlan>,
       pub schema: PlanSchema,
       // ... other fields
   }
   ```

2. **Add to enum** `LogicalPlan`:
   ```rust
   pub enum LogicalPlan {
       // ... existing variants
       NewVariant(NewNode),
   }
   ```

3. **Update `schema()` method**:
   ```rust
   LogicalPlan::NewVariant(node) => node.schema.clone(),
   ```

4. **Update `children()` method**:
   ```rust
   LogicalPlan::NewVariant(node) => vec![&node.input],
   ```

5. **Update `with_new_children()` method**:
   ```rust
   LogicalPlan::NewVariant(node) => LogicalPlan::NewVariant(NewNode {
       input: children.into_iter().next().unwrap(),
       // ... copy other fields
   }),
   ```

6. **Update `fmt_indent()` for display**

7. **Add to ALL optimizer rules**:
   - `cost.rs` - cost estimation
   - `join_reorder.rs` - join optimization
   - `predicate_pushdown.rs` - predicate pushdown
   - `projection_pushdown.rs` - column pruning
   - `subquery.rs` - column substitution (if has subqueries)

8. **Add to physical planner** in `src/physical/planner.rs`

---

## Physical Operator Architecture

### Operator Interface

```rust
#[async_trait]
pub trait PhysicalOperator: Send + Sync {
    fn schema(&self) -> SchemaRef;
    fn children(&self) -> Vec<Arc<dyn PhysicalOperator>>;
    fn name(&self) -> &str;
    fn output_partitions(&self) -> usize { 1 }
    async fn execute(&self, partition: usize) -> Result<RecordBatchStream>;
}
```

### Adding a New Physical Operator

**Steps**:

1. **Create operator** in `src/physical/operators/new_op.rs`:
   ```rust
   #[derive(Debug)]
   pub struct NewOpExec {
       input: Arc<dyn PhysicalOperator>,
       schema: SchemaRef,
       // ... other fields
   }

   #[async_trait]
   impl PhysicalOperator for NewOpExec {
       fn name(&self) -> &str { "NewOp" }
       fn schema(&self) -> SchemaRef { self.schema.clone() }
       fn children(&self) -> Vec<Arc<dyn PhysicalOperator>> {
           vec![self.input.clone()]
       }
       async fn execute(&self, partition: usize) -> Result<RecordBatchStream> {
           // Implementation
       }
   }
   ```

2. **Export from `mod.rs`**:
   ```rust
   pub use new_op::NewOpExec;
   ```

3. **Add to physical planner**:
   ```rust
   LogicalPlan::NewVariant(node) => {
       let input = self.create_physical_plan(&node.input)?;
       Ok(Arc::new(NewOpExec::new(input, ...)))
   }
   ```

---

## DelimJoin Architecture (Subquery Optimization)

### The Problem
Correlated subqueries like `EXISTS (SELECT * FROM t2 WHERE t2.key = t1.key)` execute O(n×m) times - once per outer row.

### The Solution: DelimJoin/DelimGet

```
Before (O(n×m)):
  Filter(Scan(t1), EXISTS(Scan(t2) WHERE t2.key = t1.key))
  → For each t1 row, execute the subquery

After (O(n+m)):
  DelimJoin(
    left: Scan(t1),
    right: Join(DelimGet, Scan(t2)),
    on: (t1.key, t2.key)
  )
  → Extract DISTINCT t1.key values
  → Execute inner ONCE with all values
  → Hash join
```

### Key Components

| Component | Purpose |
|-----------|---------|
| `DelimJoinNode` | Logical plan for decorrelated subquery |
| `DelimGetNode` | Receives distinct correlation values |
| `DelimJoinExec` | Physical execution of the join |
| `DelimGetExec` | Physical scan of distinct values |
| `DelimState` | Shared state between DelimJoin and DelimGet |

### Multi-DelimJoin Pattern (Q21 Optimization)

For `EXISTS(...) AND NOT EXISTS(...)` with same correlation:

```rust
MultiDelimJoinNode {
    left: Arc<LogicalPlan>,           // Outer query
    inner_sides: Vec<Arc<LogicalPlan>>, // One per EXISTS/NOT EXISTS
    join_types: Vec<JoinType>,          // Semi/Anti for each
    delim_columns: Vec<Expr>,           // Correlation columns
    on: Vec<(Expr, Expr)>,             // Join conditions
}
```

**Execution**:
1. Collect outer rows, extract distinct correlation values
2. Store in shared `DelimState`
3. Execute each inner side (they read from `DelimState`)
4. Build hash sets of matching keys
5. Filter outer rows: keep if all EXISTS match AND all NOT EXISTS don't match

---

## Optimizer Rule Patterns

### Rule Interface

```rust
pub trait OptimizerRule: Send + Sync {
    fn name(&self) -> &str;
    fn optimize(&self, plan: &LogicalPlan) -> Result<LogicalPlan>;
}
```

### Pattern: Recursive Transformation

```rust
fn optimize(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
    // 1. First transform children recursively
    let new_children: Vec<Arc<LogicalPlan>> = plan.children()
        .iter()
        .map(|c| self.optimize(c).map(Arc::new))
        .collect::<Result<Vec<_>>>()?;

    // 2. Apply transformation to current node
    let transformed = plan.with_new_children(new_children);

    // 3. Node-specific logic
    match &transformed {
        LogicalPlan::Filter(node) => { /* ... */ }
        _ => Ok(transformed),
    }
}
```

### Rule Execution Order (IMPORTANT!)

```rust
// In src/optimizer/mod.rs
impl Optimizer {
    pub fn new() -> Self {
        Self {
            rules: vec![
                Box::new(ConstantFolding),        // 1. Simplify constants
                Box::new(PredicatePushdown),       // 2. Push predicates down
                Box::new(SubqueryDecorrelation),   // 3. Decorrelate subqueries
                Box::new(FlattenDependentJoin),    // 4. Create DelimJoins
                Box::new(JoinReorder),             // 5. Optimize join order
                Box::new(PredicatePushdown),       // 6. Push new predicates
                Box::new(ProjectionPushdown),      // 7. Prune columns
            ],
        }
    }
}
```

**Why this order**:
- Predicate pushdown before decorrelation: prevents pushing EXISTS predicates to scan
- Join reorder after decorrelation: needs proper join structure
- Second predicate pushdown: new predicates from joins

---

## Common Patterns and Idioms

### Error Handling

```rust
// Custom Result type - only takes 1 generic (success type)
pub type Result<T> = std::result::Result<T, QueryError>;

// Usage in closures with collect
let results: Result<Vec<_>> = items
    .iter()
    .map(|item| some_fallible_operation(item))
    .collect();  // Collects Results, short-circuits on error
```

### Arc Patterns

```rust
// Cloning Arc is cheap (just increments ref count)
let cloned = Arc::clone(&some_arc);

// For trait objects
let operator: Arc<dyn PhysicalOperator> = Arc::new(MyOperator::new(...));
```

### Lock Handling

```rust
// Always use expect() with meaningful messages
let mut guard = self.some_lock.lock().expect("lock poisoned");
let guard = self.some_lock.read().expect("read lock poisoned");
let mut guard = self.some_lock.write().expect("write lock poisoned");
```

---

## Performance Insights

### Benchmark Commands

```bash
# Quick test
cargo test --lib

# Full SQL tests
cargo test --test sql_comprehensive

# TPC-H at scale
cargo run --release -- benchmark --sf 0.01
cargo run --release -- benchmark --sf 0.1

# Single query
cargo run --release -- query --num 21 --sf 0.01
```

### Performance Patterns

| Pattern | Cost | Optimization |
|---------|------|--------------|
| Correlated subquery | O(n×m) | DelimJoin → O(n+m) |
| Hash join | O(n+m) | Already optimal |
| Nested loop join | O(n×m) | Reorder to hash |
| Column projection after scan | Extra I/O | Push projection to scan |

---

## Testing Strategy

### Unit Tests
```bash
cargo test --lib
```
- Test individual components in isolation
- Located in `#[cfg(test)]` modules

### Integration Tests
```bash
cargo test --test sql_comprehensive
```
- End-to-end SQL execution tests
- Located in `tests/` directory

### TPC-H Queries
```bash
cargo test tpch
```
- 22 queries from TPC-H benchmark
- Located in `src/tpch/queries.rs`

---

## Session Checklist

When starting a new session, check:
- [ ] Read `CLAUDE.md` for project overview
- [ ] Read this document for architecture details
- [ ] Run `cargo test --lib` to verify codebase state
- [ ] Check `.claude/plans/` for any in-progress work

When ending a session:
- [ ] Update relevant documentation
- [ ] Run `cargo test --lib` to ensure nothing is broken
- [ ] Commit changes locally
- [ ] Update this knowledge document with new learnings

---

## Change Log

| Date | Change | Impact |
|------|--------|--------|
| 2026-02-13 | Added MultiDelimJoin for Q21 | 1500x improvement on Q21 |
| 2026-02-13 | Fixed lock panics with expect() | Safer parallel execution |
| 2026-02-13 | Implemented ProjectionMask | 1.5-2x on scan-heavy queries |
| 2026-02-13 | Wired morsel parallelism | 4-8x on scans when enabled |
