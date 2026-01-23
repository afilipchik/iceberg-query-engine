# Query Engine - AI Agent Guide

This document is designed to help AI agents (Claude, Copilot, etc.) understand and work with this codebase effectively.

---

## MANDATORY RULES FOR AI AGENTS

### Documentation Update Rule

**ON EVERY CONTEXT COMPACTION, THIS DOCUMENTATION MUST BE UPDATED.**

When your context is compacted/summarized, you MUST:
1. Review any code changes made during the session
2. Update this `CLAUDE.md` file to reflect:
   - New files or modules added
   - Changes to architecture or design decisions
   - New types, functions, or patterns introduced
   - Updated file structure
   - New dependencies added
   - Any other significant changes

This ensures the documentation remains accurate and useful for future AI agent sessions.

---

## Project Overview

**What**: A high-performance SQL query engine built from scratch in Rust, targeting Apache Iceberg tables.

**Purpose**: Execute analytical SQL queries with top-tier TPC-H benchmark performance.

**Technology Stack**:
- Language: Rust 2021 edition
- Data Format: Apache Arrow 53 (columnar)
- Async Runtime: Tokio
- SQL Parser: sqlparser-rs 0.52

## Architecture

### Query Processing Pipeline

```
SQL String
    │
    ▼
┌─────────────────┐
│     Parser      │  src/parser/
│  (sqlparser-rs) │
└────────┬────────┘
         │ SQL AST
         ▼
┌─────────────────┐
│     Binder      │  src/planner/binder.rs
│    (Planner)    │
└────────┬────────┘
         │ LogicalPlan
         ▼
┌─────────────────┐
│    Optimizer    │  src/optimizer/
│  (Rule-based)   │
└────────┬────────┘
         │ Optimized LogicalPlan
         ▼
┌─────────────────┐
│Physical Planner │  src/physical/planner.rs
└────────┬────────┘
         │ PhysicalOperator tree
         ▼
┌─────────────────┐
│   Execution     │  src/physical/operators/
│  (Streaming)    │
└────────┬────────┘
         │ Stream<RecordBatch>
         ▼
    Query Results
```

### Key Design Decisions

1. **Streaming Execution**: All operators implement async streaming via `BoxStream<RecordBatch>`
2. **Hash-based Operations**: Joins and aggregations use hash-based algorithms
3. **Rule-based Optimization**: Three main rules: predicate pushdown, projection pushdown, constant folding
4. **In-memory Storage**: Tables stored as Arrow RecordBatches (Iceberg integration planned)

## File Structure

```
src/
├── lib.rs                    # Public API exports
├── main.rs                   # CLI entry point
├── error.rs                  # QueryError enum and Result type
│
├── parser/
│   └── mod.rs                # SQL parsing wrapper (uses sqlparser-rs)
│
├── planner/
│   ├── mod.rs                # Module exports
│   ├── binder.rs             # SQL AST → LogicalPlan conversion
│   ├── logical_plan.rs       # LogicalPlan enum and node types
│   ├── logical_expr.rs       # Expression types (Expr, ScalarValue, etc.)
│   └── schema.rs             # PlanSchema, SchemaField, Column
│
├── optimizer/
│   ├── mod.rs                # Optimizer struct and OptimizerRule trait
│   ├── rules/
│   │   ├── mod.rs            # Rule exports
│   │   ├── predicate_pushdown.rs
│   │   ├── projection_pushdown.rs
│   │   └── constant_folding.rs
│   └── cost.rs               # Cost model (placeholder)
│
├── physical/
│   ├── mod.rs                # Module exports
│   ├── plan.rs               # PhysicalOperator trait
│   ├── planner.rs            # LogicalPlan → PhysicalOperator conversion
│   └── operators/
│       ├── mod.rs            # Operator exports
│       ├── scan.rs           # MemoryTableExec, TableProvider trait
│       ├── filter.rs         # FilterExec, expression evaluation
│       ├── project.rs        # ProjectExec
│       ├── hash_join.rs      # HashJoinExec
│       ├── hash_agg.rs       # HashAggregateExec
│       ├── sort.rs           # SortExec
│       └── limit.rs          # LimitExec
│
├── storage/                  # External storage providers
│   ├── mod.rs                # Module exports (ParquetTable)
│   └── parquet.rs            # ParquetTable - reads Parquet files/directories
│
├── execution/
│   ├── mod.rs                # Module exports
│   ├── context.rs            # ExecutionContext (main entry point)
│   └── memory.rs             # Memory tracking
│
└── tpch/
    ├── mod.rs                # TPC-H module exports
    ├── generator.rs          # TpchGenerator for test data + Parquet export
    ├── schema.rs             # TPC-H table schemas
    └── queries.rs            # All 22 TPC-H queries

data/                         # Generated test data (gitignored)
├── tpch-1mb/                 # SF=0.001 - 8 Parquet files
├── tpch-10mb/                # SF=0.01 - 8 Parquet files
└── tpch-100mb/               # SF=0.1 - 8 Parquet files
```

## Core Types Reference

### Entry Point

```rust
// Main execution entry point
use query_engine::{ExecutionContext, ParquetTable};

let mut ctx = ExecutionContext::new();

// Register in-memory table
ctx.register_table("users", schema, batches);

// Register Parquet file or directory
ctx.register_parquet("orders", "/path/to/orders.parquet")?;

// Register custom table provider
let table = Arc::new(ParquetTable::try_new("/path/to/data")?);
ctx.register_table_provider("lineitem", table);

let result = ctx.sql("SELECT * FROM users").await?;
// result.batches: Vec<RecordBatch>
// result.row_count: usize
// result.metrics: QueryMetrics
```

### Logical Plan Nodes

| Node Type | Purpose | Key Fields |
|-----------|---------|------------|
| `Scan` | Table read | `table_name`, `schema`, `projection`, `filter` |
| `Filter` | WHERE clause | `input`, `predicate` |
| `Project` | SELECT columns | `input`, `exprs`, `schema` |
| `Join` | JOIN operations | `left`, `right`, `join_type`, `on`, `filter` |
| `Aggregate` | GROUP BY | `input`, `group_by`, `aggregates` |
| `Sort` | ORDER BY | `input`, `order_by` |
| `Limit` | LIMIT/OFFSET | `input`, `skip`, `fetch` |
| `Distinct` | DISTINCT | `input` |
| `Union` | UNION | `inputs`, `schema` |
| `SubqueryAlias` | AS alias | `input`, `alias` |

### Expression Types (`Expr`)

| Variant | Purpose | Example |
|---------|---------|---------|
| `Column(Column)` | Column reference | `col("id")` |
| `Literal(ScalarValue)` | Constant value | `42`, `'hello'` |
| `BinaryExpr` | Binary ops | `a + b`, `x = y` |
| `UnaryExpr` | Unary ops | `NOT x`, `-y` |
| `Aggregate` | Agg functions | `SUM(x)`, `COUNT(*)` |
| `ScalarFunc` | Scalar functions | `UPPER(name)` |
| `Cast` | Type cast | `CAST(x AS INT)` |
| `Case` | CASE expression | `CASE WHEN...` |
| `Alias` | Column alias | `expr AS name` |

### Physical Operators

| Operator | Purpose | Algorithm |
|----------|---------|-----------|
| `MemoryTableExec` | Read table data | Sequential scan |
| `FilterExec` | Apply predicates | Row-by-row evaluation |
| `ProjectExec` | Column projection | Expression evaluation |
| `HashJoinExec` | Join tables | Build-probe hash join |
| `HashAggregateExec` | Aggregation | Hash-based grouping |
| `SortExec` | Sort rows | Arrow's sort kernels |
| `LimitExec` | Limit rows | Stream truncation |

### Join Types

```rust
pub enum JoinType {
    Inner,   // Only matching rows
    Left,    // All left + matching right
    Right,   // Matching left + all right
    Full,    // All rows from both
    Semi,    // Left rows with match (no right columns)
    Anti,    // Left rows without match
    Cross,   // Cartesian product
}
```

### Aggregate Functions

```rust
pub enum AggregateFunction {
    Count,         // COUNT(*)
    CountDistinct, // COUNT(DISTINCT x)
    Sum,           // SUM(x)
    Avg,           // AVG(x)
    Min,           // MIN(x)
    Max,           // MAX(x)
}
```

### Error Types

```rust
pub enum QueryError {
    Parse(String),       // SQL parsing failed
    Plan(String),        // Planning error
    Bind(String),        // Binding error
    Type(String),        // Type mismatch
    Execution(String),   // Runtime error
    TableNotFound(String),
    ColumnNotFound(String),
    NotImplemented(String),
    // ... and more
}
```

## Common Tasks

### Adding a New Scalar Function

1. Add variant to `ScalarFunction` enum in `src/planner/logical_expr.rs`
2. Update `fmt::Display` impl for the new function
3. Add type inference in `Expr::data_type()` match arm
4. Implement evaluation in `src/physical/operators/filter.rs` `evaluate_expr()`
5. Add parsing support in `src/planner/binder.rs` `bind_function()`

### Adding a New Aggregate Function

1. Add variant to `AggregateFunction` enum in `src/planner/logical_expr.rs`
2. Update parsing in `src/planner/binder.rs`
3. Implement in `src/physical/operators/hash_agg.rs`:
   - Add to `Accumulator` trait implementation
   - Handle in accumulator update and finalize

### Adding a New Optimizer Rule

1. Create new file in `src/optimizer/rules/`
2. Implement `OptimizerRule` trait:
   ```rust
   pub trait OptimizerRule: Send + Sync {
       fn name(&self) -> &str;
       fn optimize(&self, plan: &LogicalPlan) -> Result<LogicalPlan>;
   }
   ```
3. Register in `Optimizer::new()` in `src/optimizer/mod.rs`

### Adding a New Physical Operator

1. Create file in `src/physical/operators/`
2. Implement `PhysicalOperator` trait:
   ```rust
   #[async_trait]
   pub trait PhysicalOperator: Send + Sync {
       fn schema(&self) -> SchemaRef;
       fn children(&self) -> Vec<Arc<dyn PhysicalOperator>>;
       async fn execute(&self, partition: usize) -> Result<RecordBatchStream>;
   }
   ```
3. Add conversion in `src/physical/planner.rs`
4. Export in `src/physical/operators/mod.rs`

### Running Tests

```bash
# All tests
cargo test

# Specific module
cargo test parser
cargo test planner
cargo test optimizer
cargo test physical

# TPC-H specific
cargo test tpch
```

### Running Benchmarks

```bash
# TPC-H benchmark
cargo bench --bench tpch

# With specific scale factor
cargo run --release -- benchmark --sf 0.1

# Single query
cargo run --release -- query --num 1 --sf 0.01 --plan
```

## Code Patterns

### Builder Pattern for Plans

```rust
let plan = LogicalPlanBuilder::scan("orders", schema)
    .filter(Expr::column("amount").gt(Expr::literal(ScalarValue::Int64(100))))
    .project(vec![Expr::column("id"), Expr::column("amount")])?
    .sort(vec![SortExpr::new(Expr::column("amount")).desc()])
    .limit(0, Some(10))
    .build();
```

### Expression Building

```rust
// Column reference
let col = Expr::column("amount");
let qualified = Expr::qualified_column("orders", "id");

// Literals
let lit = Expr::literal(ScalarValue::Int64(100));

// Binary operations (chainable)
let expr = Expr::column("a")
    .add(Expr::column("b"))
    .multiply(Expr::literal(ScalarValue::Float64(1.5.into())));

// Comparisons
let predicate = Expr::column("price").gt(Expr::literal(ScalarValue::Float64(10.0.into())))
    .and(Expr::column("status").eq(Expr::literal(ScalarValue::Utf8("active".into()))));

// Aggregates
let agg = Expr::Aggregate {
    func: AggregateFunction::Sum,
    args: vec![Expr::column("amount")],
    distinct: false,
};
```

### Async Streaming Pattern

```rust
// Physical operators return streams
let stream: RecordBatchStream = operator.execute(partition).await?;

// Consume with TryStreamExt
use futures::TryStreamExt;
let batches: Vec<RecordBatch> = stream.try_collect().await?;

// Or process incrementally
while let Some(batch) = stream.try_next().await? {
    process_batch(batch);
}
```

### Plan Traversal Pattern

```rust
// Recursive plan transformation
fn transform_plan(plan: &LogicalPlan) -> Result<LogicalPlan> {
    // First transform children
    let new_children: Vec<Arc<LogicalPlan>> = plan.children()
        .iter()
        .map(|c| transform_plan(c).map(Arc::new))
        .collect::<Result<Vec<_>>>()?;

    // Then transform current node
    let transformed = plan.with_new_children(new_children);

    // Apply node-specific logic
    match &transformed {
        LogicalPlan::Filter(node) => { /* ... */ }
        _ => Ok(transformed),
    }
}
```

## Testing Approach

### Unit Tests
Each module has inline `#[cfg(test)]` modules testing individual components.

### Integration Tests
Located in `tests/` directory, test full query execution paths.

### TPC-H Tests
`src/tpch/` contains all 22 TPC-H queries for regression testing.

### Test Helpers

```rust
// Create test context with sample data
fn create_test_context() -> ExecutionContext {
    let mut ctx = ExecutionContext::new();
    // ... register tables
    ctx
}

// Common assertions
assert_eq!(result.row_count, expected);
assert_eq!(result.schema.fields().len(), expected_cols);
```

## Performance Considerations

1. **LTO enabled**: Link-time optimization for release builds
2. **Single codegen unit**: Better optimization at cost of compile time
3. **Arrow kernels**: Use Arrow's optimized compute kernels where possible
4. **Hash-based algorithms**: Preferred for joins and aggregations
5. **Streaming**: Avoid materializing full results when possible

## Dependencies (Key Crates)

| Crate | Purpose |
|-------|---------|
| `arrow` | Columnar data format |
| `sqlparser` | SQL parsing |
| `tokio` | Async runtime |
| `futures` | Stream utilities |
| `async-trait` | Async trait methods |
| `hashbrown` | Fast hash maps |
| `thiserror` | Error derive macros |

## CLI Commands

```bash
# Generate TPC-H data
query_engine generate --sf 0.01

# Run specific TPC-H query
query_engine query --num 1 --sf 0.01 --plan

# Run all TPC-H queries
query_engine benchmark --sf 0.01 --iterations 3

# Execute custom SQL
query_engine sql "SELECT * FROM lineitem LIMIT 10" --sf 0.01
```

## Future/Planned Features

Based on the codebase structure, these appear to be planned but not fully implemented:
- Full Apache Iceberg integration (currently in-memory only)
- Cost-based optimization (cost.rs exists but is minimal)
- Parallel execution (partition parameter exists but single-threaded)
- More scalar functions
- Window functions

## Quick Reference: Where to Find Things

| Looking for... | Location |
|----------------|----------|
| SQL parsing | `src/parser/mod.rs` |
| Query planning | `src/planner/binder.rs` |
| Logical plan types | `src/planner/logical_plan.rs` |
| Expression types | `src/planner/logical_expr.rs` |
| Optimizer rules | `src/optimizer/rules/*.rs` |
| Physical execution | `src/physical/operators/*.rs` |
| Main entry point | `src/execution/context.rs` |
| TPC-H queries | `src/tpch/queries.rs` |
| TPC-H schemas | `src/tpch/schema.rs` |
| Error types | `src/error.rs` |
