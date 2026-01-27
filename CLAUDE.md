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

### Code Formatting Rule

**BEFORE EVERY COMMIT, RUN `cargo fmt --all -- --check` AND FIX ANY ERRORS.**

Before committing any code changes:
1. Run `cargo fmt --all -- --check` to check for formatting issues
2. If there are formatting errors, run `cargo fmt --all` to fix them
3. Only then proceed with the commit

This ensures consistent code formatting across the codebase.

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
4. **Pluggable Storage**: `TableProvider` trait enables multiple data sources (memory, Parquet, Iceberg)
5. **Arrow-native**: All data flows through Arrow RecordBatches for zero-copy operations

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
│   │   ├── join_reorder.rs        # Eliminates cross joins, optimizes join order
│   │   ├── predicate_pushdown.rs  # Handles subquery outer refs correctly
│   │   ├── projection_pushdown.rs # Handles table alias column matching
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
│       ├── limit.rs          # LimitExec
│       ├── subquery.rs       # SubqueryExecutor for correlated subqueries
│       ├── union.rs          # UnionExec for UNION/UNION ALL
│       ├── parquet.rs        # ParquetScanExec, ParquetTable, ParquetWriter
│       └── iceberg.rs        # IcebergScanExec, PartitionFilter
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
├── metastore/
│   └── mod.rs                # BranchingMetastoreClient REST API client
│
└── tpch/
    ├── mod.rs                # TPC-H module exports
    ├── generator.rs          # TpchGenerator for test data + Parquet export
    ├── schema.rs             # TPC-H table schemas
    └── queries.rs            # All 22 TPC-H queries (adapted for generated data)

tests/
├── sql_comprehensive.rs      # 131 SQL correctness tests

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
| `Exists` | EXISTS subquery | `EXISTS (SELECT ...)` |
| `InSubquery` | IN subquery | `x IN (SELECT ...)` |
| `ScalarSubquery` | Scalar subquery | `(SELECT MAX(x) ...)` |

### Physical Operators

| Operator | Purpose | Algorithm |
|----------|---------|-----------|
| `MemoryTableExec` | Read table data | Sequential scan |
| `FilterExec` | Apply predicates | Row-by-row evaluation + subquery execution |
| `ProjectExec` | Column projection | Expression evaluation |
| `HashJoinExec` | Join tables | Build-probe hash join |
| `HashAggregateExec` | Aggregation | Hash-based grouping |
| `SortExec` | Sort rows | Arrow's sort kernels |
| `LimitExec` | Limit rows | Stream truncation |
| `UnionExec` | Combine inputs | Stream concatenation |
| `ParquetScanExec` | Read Parquet files | Async streaming with projection |
| `IcebergScanExec` | Read Iceberg tables | Manifest parsing + Parquet scan |

### TableProvider Trait

The `TableProvider` trait (in `src/physical/operators/scan.rs`) enables pluggable data sources:

```rust
pub trait TableProvider: Send + Sync + std::fmt::Debug {
    fn schema(&self) -> SchemaRef;
    fn scan(&self, projection: Option<&[usize]>) -> Result<Vec<RecordBatch>>;
}
```

Implementations:
- `MemoryTable` - In-memory Arrow batches
- `ParquetTable` - Parquet files (single file or directory)
- `IcebergTable` - Planned for Phase 2

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

### Adding a New Storage Provider (TableProvider)

1. Create file in `src/storage/` (e.g., `src/storage/iceberg.rs`)
2. Implement `TableProvider` trait:
   ```rust
   pub struct MyTable {
       schema: SchemaRef,
       // ... provider-specific fields
   }

   impl TableProvider for MyTable {
       fn schema(&self) -> SchemaRef { self.schema.clone() }
       fn scan(&self, projection: Option<&[usize]>) -> Result<Vec<RecordBatch>> {
           // Read data, apply projection
       }
   }
   ```
3. Export in `src/storage/mod.rs`
4. Optionally add convenience method in `ExecutionContext`:
   ```rust
   pub fn register_my_table(&mut self, name: &str, path: &Path) -> Result<()> {
       let table = MyTable::try_new(path)?;
       self.register_table_provider(name, Arc::new(table));
       Ok(())
   }
   ```
5. Add CLI command in `src/main.rs` if needed

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

### Correlated Subquery Execution

Correlated subqueries reference columns from outer queries. The engine handles these through:

1. **Detection**: `Expr::contains_subquery()` and `Expr::get_outer_references()` identify subquery expressions
2. **Outer Reference Tracking**: Predicate pushdown extracts outer column references to prevent incorrect optimization
3. **Runtime Execution**: `SubqueryExecutor` in `src/physical/operators/subquery.rs`:
   - Maintains table registry for subquery planning
   - Substitutes outer column values row-by-row
   - Uses `local_tables` set to distinguish inner vs outer columns

```rust
// Example: EXISTS subquery with outer reference
// SELECT * FROM orders o WHERE EXISTS (
//   SELECT 1 FROM lineitem l WHERE l.l_orderkey = o.o_orderkey
// )
// The predicate `l.l_orderkey = o.o_orderkey` references outer column `o.o_orderkey`
// SubqueryExecutor substitutes the current row's o_orderkey value when evaluating
```

**Important optimizer considerations for subqueries:**
- Never push EXISTS/IN/ScalarSubquery predicates to individual table scans
- Extract all outer column references when determining pushdown eligibility
- Match columns by name when table aliases differ (e.g., `l1.col` should match `lineitem.col`)

## Testing Approach

### Unit Tests
Each module has inline `#[cfg(test)]` modules testing individual components.

### Integration Tests
Located in `tests/` directory, test full query execution paths.

### TPC-H Tests
`src/tpch/` contains all 22 TPC-H queries for regression testing.

**Note on TPC-H Query Adaptations**: Some queries were modified to work with the generated test data:
- Q20: Uses `'Part 1%'` instead of `'forest%'` (part names are "Part N" format)
- Q22: Uses 2-digit phone codes `('13', '31', '23', ...)` instead of single-digit codes (phone format is 10-33-XXX-XXX-XXXX)

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
| `parquet` | Parquet file reading/writing |
| `sqlparser` | SQL parsing |
| `tokio` | Async runtime |
| `futures` | Stream utilities |
| `async-trait` | Async trait methods |
| `hashbrown` | Fast hash maps |
| `thiserror` | Error derive macros |
| `clap` | CLI argument parsing |
| `rustyline` | Interactive REPL line editing |
| `reqwest` | HTTP client for metastore REST API |
| `regex` | Regular expression support |
| `serde_json` | JSON parsing and serialization |
| `sha2` | SHA-256/SHA-512 hash functions |
| `sha1` | SHA-1 hash function |
| `md-5` | MD5 hash function |
| `hmac` | HMAC authentication codes |
| `base64` | Base64 encoding/decoding |
| `hex` | Hexadecimal encoding/decoding |
| `crc32fast` | CRC32 checksums |
| `xxhash-rust` | xxHash64 hashing |
| `url` | URL parsing and manipulation |
| `chrono` | Date/time operations |
| `unicode-normalization` | Unicode normalization |
| `rust-stemmers` | Word stemming (English) |
| `crossbeam` | Lock-free data structures (work-stealing queues) |
| `num_cpus` | CPU core detection |
| `rayon` | Data parallelism |
| `statrs` | Statistical functions |

## CLI Commands

```bash
# Generate TPC-H data (in-memory only, for testing)
query_engine generate --sf 0.01

# Generate TPC-H data to Parquet files
query_engine generate-parquet --sf 0.01 --output ./data/tpch-10mb

# Run specific TPC-H query (in-memory data)
query_engine query --num 1 --sf 0.01 --plan

# Run all TPC-H queries (in-memory data)
query_engine benchmark --sf 0.01 --iterations 3

# Execute custom SQL (in-memory TPC-H data)
query_engine sql "SELECT * FROM lineitem LIMIT 10" --sf 0.01

# Load Parquet file and run query
query_engine load-parquet --path ./data/lineitem.parquet --name lineitem \
    --query "SELECT COUNT(*) FROM lineitem"

# Load Parquet directory (all .parquet files)
query_engine load-parquet --path ./data/tpch-10mb --name orders

# Run TPC-H benchmark on Parquet files
query_engine benchmark-parquet --path ./data/tpch-100mb --iterations 3

# Start interactive SQL shell (REPL)
query_engine repl

# Start REPL with TPC-H tables preloaded
query_engine repl --tpch ./data/tpch-10mb
```

### Interactive REPL Commands

Once in the REPL, the following dot-commands are available:

| Command | Description |
|---------|-------------|
| `.help`, `.h` | Show help message |
| `.quit`, `.exit`, `.q` | Exit the shell |
| `.tables` | List registered tables |
| `.schema <table>` | Show schema for a table |
| `.load <path> <name>` | Load Parquet file/directory as table |
| `.tpch <path>` | Load all TPC-H tables from directory |
| `.mode <format>` | Set output format (table, csv, json, vertical) |
| `.format` | Show current output format |

**Tab Completion**: Press Tab to autocomplete SQL keywords, table names, column names, and dot commands.

**Syntax Highlighting**: SQL input is highlighted with colors:
- Keywords (SELECT, FROM, WHERE, etc.) → Bold Blue
- String literals → Green
- Numbers → Magenta
- Functions (COUNT, SUM, etc.) → Yellow
- Dot commands → Cyan

Any other input is executed as a SQL query.

## Future/Planned Features

Based on the codebase structure, these appear to be planned but not fully implemented:
- **Apache Iceberg integration** - Phase 2 of storage plan (see plan file at `.claude/plans/`)
  - Iceberg metadata.json parsing (basic structure exists in `src/physical/operators/iceberg.rs`)
  - Avro manifest file reading
  - Time travel queries via snapshot IDs
  - Partition pruning
- **Window functions** - ROW_NUMBER, RANK, DENSE_RANK, LEAD, LAG, etc.
  - Requires new WindowExpr, WindowNode, and WindowExec infrastructure
  - See plan at `.claude/plans/trino-function-implementation.md` Phase 6
- **Array/Map type support** - Complex nested data types
  - Array functions: array_agg, array_distinct, array_join, filter, transform, etc.
  - Map functions: map_keys, map_values, map_entries, element_at, etc.
  - See plan at `.claude/plans/trino-function-implementation.md` Phases 4-5
- Cost-based optimization (cost.rs exists but is minimal)
- Parallel execution (partition parameter exists but single-threaded)

## Current Test Status

- **SQL Correctness Tests**: 131 passing (`tests/sql_comprehensive.rs`)
- **Function Validation Tests**: 161 passing (`tests/function_validation_tests.rs`)
- **TPC-H Benchmark**: 22/22 queries returning data
- **Subquery Tests**: EXISTS, IN, ScalarSubquery all working

## Recently Implemented Features

- **Optimizer Rule Order Fix** (Critical fix for Q17 scalar subquery)
  - Changed optimizer rule order to run PredicatePushdown before SubqueryDecorrelation
  - Fixes cross join explosion in Q17 (was trying to produce 4 billion rows)
  - Q17 now executes in 8ms instead of crashing
  - Q20 now returns correct results (7 rows instead of 297 duplicates)
  - Added `reorder_filter_with_join` helper in JoinReorder rule
  - Rule order: ConstantFolding → PredicatePushdown → SubqueryDecorrelation → JoinReorder → PredicatePushdown → ProjectionPushdown
  - Located in `src/optimizer/mod.rs`, `src/optimizer/rules/join_reorder.rs`

- **Correlated Subquery Memoization** (Performance improvement for Q17-Q22)
  - Caches subquery results by correlation key values
  - Avoids re-executing subqueries for repeated outer row values
  - Implemented in `src/physical/operators/subquery.rs`
  - Uses `CorrelatedCacheKey` combining plan hash + correlation values

- **Parallel Hash Join Build** (Performance improvement)
  - Uses rayon to build hash tables in parallel across batches
  - Automatically enabled for datasets > 10,000 rows
  - Merges partial hash tables after parallel build
  - Located in `src/physical/operators/hash_join.rs`

- **Async Parquet I/O** (I/O optimization)
  - `AsyncParquetReader`: True async I/O using tokio
  - `read_all_parallel()`: Concurrent file reading with configurable parallelism
  - Overlaps I/O with computation for better NVMe utilization
  - Located in `src/storage/parquet.rs`

- **Morsel-Driven Parallelism** (Major performance improvement - 8x faster on Q1)
  - DuckDB-style parallel execution with work-stealing scheduler
  - `ParallelParquetSource`: Parallel row-group reading from Parquet files
  - Thread-local hash tables for aggregation with final merge
  - Column projection pushdown to Parquet reader
  - **TPC-H SF=10 Q1 Benchmark:**
    - Original engine: 1,860ms
    - Morsel + projection: 227ms (**8x faster**)
    - DuckDB: 180ms (1.4x faster than us)
  - Located in `src/physical/morsel.rs`, `src/physical/morsel_agg.rs`
  - Example usage: `cargo run --release --example morsel_test_projected`

- **Vectorized Aggregation Module** (Performance optimization research)
  - Studied DuckDB/ClickHouse optimization techniques
  - Key optimizations implemented:
    - Row-group statistics filtering (skip row groups based on min/max)
    - Fixed-size accumulator arrays (no hash table for low-cardinality groups)
    - Direct primitive array access via Arrow values()
    - Cache-aligned data structures (64-byte alignment)
    - Parallel row group reading with chunk assignment per thread
  - **TPC-H SF=10 Q1 Final Performance:**
    - Our engine: 250-265ms
    - DuckDB: 170-190ms
    - Ratio: **1.4-1.5x slower** than DuckDB
  - Located in `src/physical/vectorized_agg.rs`
  - Example benchmarks:
    - `cargo run --release --example final_q1` (best performance)
    - `cargo run --release --example optimized_q1`
    - `cargo run --release --example vectorized_q1` (with row-group filtering)
  - **Remaining DuckDB advantages:**
    - Custom Parquet reader with better SIMD decompression
    - More aggressive prefetching and caching
    - Lower-level memory management

- **Parallel Aggregation and Partition Fix** (Performance improvement)
  - Fixed critical bug where Filter/Project operators didn't propagate `output_partitions()`
  - Was causing only ~3% of data to be processed through filters
  - Added parallel aggregation using rayon for multi-core hash table building
  - Added parallel partition collection using tokio::spawn
  - **TPC-H SF=10 Benchmark Improvements:**
    - Q01: 8.4s → 2.7s (3.1x faster)
    - Q04: 183ms → 120ms (1.5x faster)
    - Q06: 1.0s → 549ms (1.9x faster)
  - Located in `src/physical/operators/hash_agg.rs`, `filter.rs`, `project.rs`

- **Join Reordering Optimizer** (Major performance improvement)
  - Eliminates cartesian products from comma-separated table joins
  - Builds join graph from equality predicates
  - Greedy algorithm to find ordering where every join has a condition
  - Optimizes build/probe sides for hash joins (smaller table as build)
  - **TPC-H SF=10 Benchmark Improvements:**
    - Q08: 350 seconds → 2.2 seconds (158x faster)
    - Q09: 336 seconds → 2.4 seconds (138x faster)
    - Q02: 11 seconds → 0.15 seconds (73x faster)
  - Located in `src/optimizer/rules/join_reorder.rs`

- **Comprehensive Trino SQL Function Compatibility** (100+ functions)
  - **Math Functions** (40+): ABS, CEIL, FLOOR, ROUND, POWER, SQRT, CBRT, LN, LOG, LOG2, LOG10, EXP, SIN, COS, TAN, ASIN, ACOS, ATAN, ATAN2, SINH, COSH, TANH, DEGREES, RADIANS, PI, E, SIGN, MOD, TRUNCATE, RANDOM, INFINITY, NAN, IS_FINITE, IS_INFINITE, IS_NAN, FROM_BASE, TO_BASE
  - **String Functions** (35+): UPPER, LOWER, TRIM, LTRIM, RTRIM, LENGTH, SUBSTRING, CONCAT, CONCAT_WS, REPLACE, POSITION, STRPOS, REVERSE, LPAD, RPAD, SPLIT_PART, STARTS_WITH, ENDS_WITH, CHR, CODEPOINT, ASCII, LEFT, RIGHT, REPEAT, TRANSLATE, LEVENSHTEIN_DISTANCE, HAMMING_DISTANCE, SOUNDEX, NORMALIZE, TO_UTF8, FROM_UTF8, LUHN_CHECK, WORD_STEM
  - **Date/Time Functions** (25+): YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, MILLISECOND, DAY_OF_WEEK, DAY_OF_YEAR, WEEK, QUARTER, DATE_TRUNC, DATE_PART, EXTRACT, DATE_ADD, DATE_DIFF, CURRENT_DATE, CURRENT_TIMESTAMP, NOW, LOCALTIME, LOCALTIMESTAMP, LAST_DAY_OF_MONTH, FROM_UNIXTIME, TO_UNIXTIME, DATE_FORMAT, DATE_PARSE
  - **Aggregate Functions** (30+): COUNT, SUM, AVG, MIN, MAX, STDDEV, STDDEV_POP, STDDEV_SAMP, VARIANCE, VAR_POP, VAR_SAMP, BOOL_AND, BOOL_OR, EVERY, COUNT_IF, ANY_VALUE, ARBITRARY, APPROX_DISTINCT, APPROX_PERCENTILE, CORR, COVAR_POP, COVAR_SAMP, REGR_SLOPE, REGR_INTERCEPT, KURTOSIS, SKEWNESS, GEOMETRIC_MEAN, BITWISE_AND_AGG, BITWISE_OR_AGG
  - **JSON Functions** (14): JSON_EXTRACT, JSON_EXTRACT_SCALAR, JSON_ARRAY_LENGTH, JSON_ARRAY_GET, JSON_ARRAY_CONTAINS, JSON_SIZE, JSON_PARSE, JSON_FORMAT, JSON_KEYS, IS_JSON_SCALAR, JSON_QUERY, JSON_VALUE, JSON_EXISTS
  - **Regex Functions** (6): REGEXP_LIKE, REGEXP_EXTRACT, REGEXP_EXTRACT_ALL, REGEXP_REPLACE, REGEXP_COUNT, REGEXP_SPLIT
  - **Binary/Encoding Functions** (14): TO_HEX, FROM_HEX, TO_BASE64, FROM_BASE64, MD5, SHA1, SHA256, SHA512, HMAC_MD5, HMAC_SHA1, HMAC_SHA256, HMAC_SHA512, CRC32, XXHASH64
  - **Bitwise Functions** (8): BITWISE_AND, BITWISE_OR, BITWISE_XOR, BITWISE_NOT, BIT_COUNT, BITWISE_LEFT_SHIFT, BITWISE_RIGHT_SHIFT, BITWISE_RIGHT_SHIFT_ARITHMETIC
  - **URL Functions** (9): URL_EXTRACT_HOST, URL_EXTRACT_PATH, URL_EXTRACT_PROTOCOL, URL_EXTRACT_PORT, URL_EXTRACT_QUERY, URL_EXTRACT_FRAGMENT, URL_EXTRACT_PARAMETER, URL_ENCODE, URL_DECODE
  - **Conditional Functions** (6): COALESCE, NULLIF, CASE, IF, GREATEST, LEAST, TRY, TRY_CAST
  - See full implementation plan at `.claude/plans/trino-function-implementation.md`
  - Validation tests at `tests/function_validation_tests.rs` (161 tests)

- **Bug Fixes During Function Implementation**
  - Fixed COALESCE type inference for NULL arguments
  - Implemented HMAC functions (HmacMd5, HmacSha1, HmacSha256, HmacSha512)
  - Fixed APPROX_PERCENTILE to correctly use the percentile parameter (was ignoring it)
  - Added `second_arg` field to `AggregateExpr` for multi-argument aggregates

- **HashAggregateExec Partition Handling Fix** (Critical bug fix)
  - Fixed issue where `HashAggregateExec` only collected data from partition 0
  - The operator now correctly collects from ALL input partitions before aggregating
  - This was causing incorrect row counts (e.g., 1.8M instead of 30M rows)
  - See `src/physical/operators/hash_agg.rs:129-137` for the fix

- **Correlated Subquery Support** (Full implementation)
  - `EXISTS`, `NOT EXISTS` subqueries with outer column references
  - `IN`, `NOT IN` subqueries (correlated and uncorrelated)
  - Scalar subqueries in SELECT and WHERE clauses
  - `SubqueryExecutor` in `src/physical/operators/subquery.rs`
  - Proper outer reference extraction in optimizer rules
  - All TPC-H queries with subqueries now working (Q4, Q17, Q20, Q21, Q22)

- **Optimizer Fixes for Subqueries**
  - Predicate pushdown correctly handles EXISTS/IN/ScalarSubquery expressions
  - Extracts outer column references to prevent incorrect pushdown to scans
  - Projection pushdown handles table alias column matching (e.g., `l1.col` vs `lineitem.col`)

- **Complete TPC-H Benchmark Suite**
  - All 22/22 TPC-H queries returning correct results
  - 131 SQL correctness tests passing
  - Queries adapted for generated test data patterns

- **Interactive SQL REPL** (Enhanced)
  - `query_engine repl` command for interactive SQL sessions
  - Readline support with persistent history (saved to `~/.query_engine_history`)
  - **Tab completion** for SQL keywords, table names, column names, and dot commands
  - **Syntax highlighting** (keywords=blue, strings=green, numbers=magenta, functions=yellow)
  - **Multiple output formats**: table (default), CSV, JSON, vertical
  - Dot-commands: `.tables`, `.schema`, `.load`, `.tpch`, `.mode`, `.format`, `.help`, `.quit`
  - Optional `--tpch <path>` flag to preload TPC-H tables
  - CLI module in `src/cli/` with:
    - `helper.rs`: ReplHelper with Completer and Highlighter traits
    - `output.rs`: OutputFormatter supporting Table/CSV/JSON/Vertical formats

- **Parquet file support** (Phase 1 complete)
  - `ParquetTable` provider in `src/storage/parquet.rs`
  - Single file and directory support
  - Column projection pushdown
  - CLI commands: `load-parquet`, `benchmark-parquet`, `generate-parquet`

- **Larger-Than-Memory Dataset Support** (NEW)
  - Memory pool infrastructure in `src/execution/memory.rs`
    - `MemoryPool` with RAII-based `MemoryReservation` tracking
    - `MemoryConsumer` trait for operators that can spill to disk
    - `ExecutionConfig` with configurable memory limits and spill directory
  - Streaming Parquet reader in `src/storage/parquet.rs`
    - `StreamingParquetReader` reads data row-group by row-group
    - `StreamingParquetScanBuilder` for constructing streaming scans
    - `ParquetFileInfo` for accessing file metadata and statistics
  - Iceberg scan with statistics-based pruning in `src/physical/operators/iceberg.rs`
    - File-level min/max statistics filtering
    - Partition pruning support
    - Streaming data file reading via Parquet
  - Spillable operators in `src/physical/operators/spillable.rs`
    - `SpillableHashJoinExec`: Partitioned hash join that spills to disk
    - `SpillableHashAggregateExec`: Hash aggregation with spill support
    - `ExternalSortExec`: External merge sort for large datasets
  - Usage:
    ```rust
    // Create context with memory limit
    let ctx = ExecutionContext::with_memory_limit(512 * 1024 * 1024); // 512MB

    // Or use custom config
    let config = ExecutionConfig::new()
        .with_memory_limit_str("1GB")?
        .with_spill_path(PathBuf::from("/tmp/spill"));
    let ctx = ExecutionContext::with_config(config);
    ```

## Quick Reference: Where to Find Things

| Looking for... | Location |
|----------------|----------|
| SQL parsing | `src/parser/mod.rs` |
| Query planning | `src/planner/binder.rs` |
| Logical plan types | `src/planner/logical_plan.rs` |
| Expression types | `src/planner/logical_expr.rs` |
| Subquery expressions | `src/planner/logical_expr.rs` (Exists, InSubquery, ScalarSubquery) |
| Optimizer rules | `src/optimizer/rules/*.rs` |
| Join reordering | `src/optimizer/rules/join_reorder.rs` |
| Predicate pushdown | `src/optimizer/rules/predicate_pushdown.rs` |
| Projection pushdown | `src/optimizer/rules/projection_pushdown.rs` |
| Physical execution | `src/physical/operators/*.rs` |
| Subquery execution | `src/physical/operators/subquery.rs` |
| Main entry point | `src/execution/context.rs` |
| Parquet table provider | `src/storage/parquet.rs` |
| Streaming Parquet reader | `src/storage/parquet.rs` (StreamingParquetReader) |
| Async Parquet reader | `src/storage/parquet.rs` (AsyncParquetReader) |
| TableProvider trait | `src/physical/operators/scan.rs` |
| Memory pool/config | `src/execution/memory.rs` |
| Spillable operators | `src/physical/operators/spillable.rs` |
| Iceberg scan with stats | `src/physical/operators/iceberg.rs` |
| TPC-H queries | `src/tpch/queries.rs` |
| TPC-H schemas | `src/tpch/schema.rs` |
| TPC-H data generator | `src/tpch/generator.rs` |
| SQL tests | `tests/sql_comprehensive.rs` |
| CLI tests | `tests/cli_tests.rs` |
| Function validation tests | `tests/function_validation_tests.rs` |
| Trino function plan | `.claude/plans/trino-function-implementation.md` |
| Error types | `src/error.rs` |
| CLI helper (completion/highlighting) | `src/cli/helper.rs` |
| CLI output formatter | `src/cli/output.rs` |
| Metastore REST client | `src/metastore/mod.rs` |
| Larger-than-memory plan | `.claude/plans/larger-than-memory-support.md` |
| Iceberg implementation plan | `.claude/plans/zazzy-kindling-wigderson.md` |
