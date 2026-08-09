# Query Engine - AI Agent Guide

This document is designed to help AI agents (Claude, Copilot, etc.) understand and work with this codebase effectively.

---

## MANDATORY RULES FOR AI AGENTS

### File and Directory Rules

1. **DO NOT use /tmp** - Use the scratchpad directory provided by the system instead
2. **DO NOT exit the repository** unless absolutely necessary - stay within the project directory
3. **All file edits in this project are pre-approved** - do not ask for permission to edit files

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

### Memory Safety Rule

**THE ENGINE MUST BE MEMORY-SAFE BY DEFAULT. OOM IS NEVER ACCEPTABLE.**

- Spillable operators (SpillableHashJoinExec, SpillableHashAggregateExec, ExternalSortExec) are ALWAYS used
- There is no flag or parameter to "enable" safe memory behavior — it is the default and only mode
- Being slow on larger-than-memory datasets is acceptable; crashing with OOM is not
- When adding new operators, they MUST handle memory limits and spill to disk when needed
- Never add a parameter that lets users opt out of memory safety

### Benchmark Timeout Rule

**SET BENCHMARK TIMEOUT TO 10x DUCKDB EXECUTION TIME.**

When running benchmarks:
1. Use timeout = 10 × DuckDB time for that query
2. If the query cannot complete within 10x DuckDB time, it **FAILS**
3. Reference DuckDB times from the benchmark table below
4. These times were measured 2026-08-08 on the **spec-compliant queries**
   and **spec-generator data** via `scripts/duckdb_rebaseline.py`
   (native DuckDB tables, 16 threads, best of 3). Re-run that script and
   update this table AND `scripts/safe_benchmark.sh` whenever the queries
   or the data generator change.
   Q13 rose 99 -> 115ms when its spec ON-clause `NOT LIKE` was restored
   (2026-08-08); numbers measured against the old simplified Q13 are not
   comparable to current ones.

| Query | DuckDB Time (SF=10) | Timeout |
|-------|---------------------|---------|
| Q01 | 106ms | 1.1s |
| Q02 | 21ms | 210ms |
| Q03 | 80ms | 800ms |
| Q04 | 58ms | 580ms |
| Q05 | 48ms | 480ms |
| Q06 | 24ms | 240ms |
| Q07 | 72ms | 720ms |
| Q08 | 69ms | 690ms |
| Q09 | 1277ms | 12.8s |
| Q10 | 88ms | 880ms |
| Q11 | 13ms | 130ms |
| Q12 | 89ms | 890ms |
| Q13 | 115ms | 1.1s |
| Q14 | 37ms | 370ms |
| Q15 | 34ms | 340ms |
| Q16 | 41ms | 410ms |
| Q17 | 88ms | 880ms |
| Q18 | 235ms | 2.4s |
| Q19 | 91ms | 910ms |
| Q20 | 167ms | 1.7s |
| Q21 | 207ms | 2.1s |
| Q22 | 35ms | 350ms |

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
│       ├── vector_search.rs  # VectorSearchExec (k-NN, exact + index paths)
│       └── iceberg.rs        # IcebergScanExec, PartitionFilter
│   ├── vector.rs             # Distance kernels, VectorQuery, VectorMetric
│
├── storage/                  # External storage providers
│   ├── mod.rs                # Module exports (ParquetTable, LanceTable)
│   ├── parquet.rs            # ParquetTable - reads Parquet files/directories
│   └── lance.rs              # LanceTable - reads Lance datasets (feature "lance")
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
| `DelimJoinExec` | Deduplicated join | Distinct key extraction + hash join |
| `DelimGetExec` | Receive distinct keys | Shared state from parent DelimJoin |

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
- `LanceTable` - Lance datasets (requires `--features lance`)
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
- Q09: Uses `'Part 1%'` instead of `'%green%'` (part names are "Part N" format)
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
| `lance` | Lance dataset reader — **OPTIONAL**, `--features lance` only |

### Optional `lance` feature — build requirements

The Lance reader is **off by default**. `cargo build` compiles none of it.

1. **Version is pinned to `0.23.x` deliberately.** It is the last line built
   against **arrow 53**, the arrow major this engine uses, so Lance returns
   *our* `RecordBatch` with no IPC/FFI bridge. lance 0.25+/10.x need arrow
   54/55/58 and would fork arrow in-tree. Do not bump it.
2. **`protoc` is required** (lance-table's build script compiles `.proto`):
   `PROTOC=<abs path>/.scratch/tools/protoc/bin/protoc cargo build --release --features lance`
3. **MSRV resolver.** `Cargo.toml` sets `resolver = "3"` + `rust-version = "1.93.0"`.
   lance-io depends on aws-config unconditionally and the newest aws-* crates
   declare rust-version 1.94.1 while this toolchain is 1.93.0; the MSRV-aware
   resolver picks aws-config 1.8.18 instead of failing. `default-features = false`
   alone does NOT drop the aws crates.
4. **The default dependency set is provably unchanged.** Adding the optional dep
   left every pre-existing crate at its exact prior version (verified by diffing
   `Cargo.lock`). 23 crates gained an *additional* semver-incompatible major
   (reqwest 0.12, thiserror 2, hyper 1, sqlparser 0.53, …) reachable only through
   the lance tree — `cargo tree -e normal,build` on the default features contains
   zero of them. `dashmap` is pinned to 6.1.0 and `once_cell` to 1.21.3 for this
   reason: dashmap 6.2.1 requires `once_cell ^1.21.4`, which would have been the
   one and only bump to a crate the default build actually compiles.

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

### Lance commands (require `--features lance`)

```bash
# Build with the Lance reader (needs protoc; see Dependencies)
PROTOC=.scratch/tools/protoc/bin/protoc cargo build --release --features lance

# Load a Lance dataset and run a query
query_engine load-lance --path ./data/tpch-1mb-lance/orders.lance --name orders \
    --query "SELECT COUNT(*) FROM orders"

# Run TPC-H benchmark over Lance datasets
query_engine benchmark-lance --path ./data/tpch-10gb-lance --iterations 1 \
    --save-csv .scratch/lance_csv

# REPL with TPC-H Lance tables preloaded
query_engine repl --tpch-lance ./data/tpch-1mb-lance
```

Converting Parquet to Lance (needs `.venv` with pylance 0.23.2):

```bash
.venv/bin/python scripts/lance_convert.py \
    --parquet ./data/tpch-10gb --out ./data/tpch-10gb-lance
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
| `.lance <path> <name>` | Load a Lance dataset as table (`--features lance`) |
| `.tpch-lance <path>` | Load all TPC-H tables from a Lance directory (`--features lance`) |
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

## Current Test Status (2026-08-08)

- **All test suites green** (801 tests total), including:
  - **DuckDB-validated suite**: 163 passing (`tests/duckdb_validated.rs`) — requires
    `data/tpch-1mb` generated by the CURRENT generator (CI regenerates it; the
    generator is byte-for-byte deterministic)
  - **SQL Correctness Tests**: 129 passing (`tests/sql_comprehensive.rs`)
  - **Function Validation Tests**: passing (`tests/function_validation_tests.rs`)
- **Outer-join semantics** are pinned by `tests/expected_results/join/{left_on_filter,
  left_on_filter_cols, left_on_not_like, left_on_both_sides, right_on_filter,
  full_on_filter, on_vs_where}.csv`. An ON-clause predicate is a JOIN CONDITION,
  evaluated on candidate (build, probe) pairs inside `HashJoinExec`; only
  Inner/Cross may lower it to a post-join `FilterExec`. Do not "simplify" that
  back — a post-filter deletes the NULL-extended rows and silently makes an
  outer join behave like an inner join.
- **Known aggregate-NULL gap**: `SUM(col)` over a group with no non-NULL rows
  returns `0` instead of `NULL` (`AccumulatorState::Sum` / `raw_sums` carry no
  "seen" bit). Reachable via `LEFT JOIN ... GROUP BY`. Not yet fixed because the
  bare-`(u64, f64)` sum state is a load-bearing optimization.
- **TPC-H queries are spec-compliant** since commit `d84f3a2` for
  Q02/Q04/Q14/Q16/Q18/Q19, and since `6c53559` for Q13 — whose ON-clause
  `o_comment NOT LIKE '%special%requests%'` had been dropped by an earlier
  "simplified version" note that survived the d84f3a2 sweep. Restoring it
  exposed four real engine bugs (see `be575fb`), including a wrong TPC-H
  answer at SF=10 (23 rows instead of 24). **Lesson: a query simplified for
  convenience is a disabled test.** Any future adaptation must be justified
  by the DATA (see below), never by engine capability, and the DuckDB oracle
  in `scripts/generate_expected_results.py` must carry the identical SQL.
  Remaining data adaptations: Q09/Q20 `'Part 1%'`, Q22 2-digit phone codes.
  Q13's spec predicate is a no-op on this data (the generator writes a
  constant `o_comment`), so LEFT-JOIN-ON filter semantics are covered by
  dedicated tests in `tests/expected_results/join/` instead.
- **IMPORTANT — data↔CSV coupling**: `tests/expected_results/*.csv` are only valid
  against parquet produced by the current `src/tpch/generator.rs`. If the generator
  changes, regenerate the data AND the CSVs (`scripts/generate_expected_results.py`)
  in the same commit.
- **Spill tests**: `tests/spill_tests.rs` forces join/agg/sort spills at tiny
  memory limits (uses `data/tpch-10mb`) and compares against unlimited runs.
  The join spill path supports INNER joins only — non-inner joins whose build
  side exceeds the budget fail loudly instead of returning wrong results.
  `SpillableHashJoinExec` still materializes the build side before deciding to
  spill (known hole, fixed by the Phase-5 streaming spill rewrite, see ROADMAP).

## TPC-H Benchmark Status (SF=10, 2026-08-08 night, 48G cgroup)

Log: `logs/safe_benchmark_20260808_222629.log`. Spec queries, spec-generator data.
**22/22 pass; 7.49s total vs native-table DuckDB 2.99s = 2.5x; 22/22 within 10x.**
(Q13 became spec-compliant 2026-08-08 — it now scans/evaluates `o_comment`,
so its 126ms→480ms jump is real added work, not a regression.)
**Like-for-like (DuckDB reading the SAME parquet via views ~4.1-4.2s) ≈ 1.85x.**
Recent additions: dimension-mapped semi-join reduction sources, bare-f64 sum
states, filter-only column pruning in ProjectionPushdown, RT-filter-aware
Semi/Anti probe deferral, VHT-served filtered Semi/Anti, row-native join
build (RowStore), direct-address hash join, deferred decoration joins.
All 22 queries validate CELL-EXACT against DuckDB at SF=10 after every change
(pattern: `benchmark-parquet --save-csv` + `.venv/bin/python` DuckDB views
comparison; see `.scratch/validate22.py` pattern in memory).

| Query | Engine | vs native | | Query | Engine | vs native |
|-------|--------|-------|-|-------|--------|-------|
| Q01 | 342ms | 3.2x | | Q12 | 166ms | 1.8x |
| Q02 | 97ms | 4.6x | | Q13 | 480ms | 4.1x |
| Q03 | 425ms | 5.3x | | Q14 | 194ms | 5.2x |
| Q04 | 222ms | 3.8x | | Q15 | 103ms | 3.0x |
| Q05 | 309ms | 6.4x | | Q16 | 225ms | 5.4x |
| Q06 | 102ms | 4.2x | | Q17 | 303ms | 3.4x |
| Q07 | 352ms | 4.8x | | Q18 | 536ms | 2.2x |
| Q08 | 376ms | 5.4x | | Q19 | 136ms | 1.4x |
| Q09 | 1.37s | **1.0x** | | Q20 | 482ms | 2.8x |
| Q10 | 420ms | 4.7x | | Q21 | 575ms | 2.7x |
| Q11 | 65ms | 4.9x | | Q22 | 213ms | 6.0x |

**What got the engine here (2026-08-08)**: mimalloc global allocator (glibc
free/consolidate stalls cost 550ms on a single aggregate teardown); DPsize CBO
from footer stats with full-correlation composite-key NDV (max of per-column
NDVs); EagerAggregation rule (pre-aggregate duplicated join inputs with packed
int keys — Q09 4.1s -> 1.69s); per-shard HAVING filtering inside the parallel
aggregate merge; build side concatenated once in the join build cache; morsel
parallel aggregation + raw u64 group keys; SemiJoinPushdown; DeriveOrPredicates;
arrow RowFilter pushdown; vectorized SUBSTRING + single-pass string IN-lists;
batch-level parallel Semi/Anti probes; GroupKeyReduction rule (FD-redundant
group columns collapse to the unique key + ANY_VALUE).

**Path to parity**: see `.claude/plans/PARITY-PLAN.md` — three scoped
rewrites (dictionary-aware strings, fused join->aggregate, decode path)
with designs, gates, and expected payoffs from recorded profiling. The
round-by-round evidence log lives in the project memory file.

**Remaining gaps**: Q10/Q11/Q15/Q22 exceed 10x native on small absolute times
(fixed pipeline overhead, double scans, string-heavy group keys). Next levers
in memory: FD-based group-by reduction (Q10's 7-column group key has a unique
c_custkey), radix-partitioned aggregation (hash-probe latency floor), parquet
decode. See the memory file for the full round-by-round log.

## Lance Reader (feature-gated, 2026-08-09)

`src/storage/lance.rs` implements `LanceTable`, a `TableProvider` over Lance
datasets. Enabled with `--features lance`; see Dependencies for protoc/MSRV.

**Design**
- **Projection pushdown** is the point: `Scanner::project(&names)` makes Lance
  read only the requested columns off disk. Column order follows the projection
  list, not the table schema.
- **Fragment-parallel scan**: one `tokio::spawn` per fragment (58 for SF=10
  `lineitem`), collected in fragment order so results are deterministic.
- **Async/sync bridge**: `TableProvider::scan` is sync, Lance is async. A single
  shared `num_cpus`-wide runtime is driven from a dedicated thread
  (`block_on_lance`), mirroring `subquery::subquery_runtime`. Never create a
  runtime per call, and never `block_on` on a thread a runtime already owns.
- **Unsupported types fail loudly** at registration, naming the column.
  `FixedSizeList` (Lance vector/embedding columns) gets a specific message,
  since that is what a real LanceDB table most likely contains.

**Statistics: Lance must SCAN for what Parquet gets from a footer.**
`compute_column_stats` reads the *integer* columns (through Lance's projection,
so wide string columns are never touched) to derive min/max and
`ndv_est = min(non_null_rows, max-min+1)` — the same estimate `ParquetTable`
uses. This is **not** optional: with cardinality but no NDV, the DPsize join
reorderer put TPC-H **Q05 into `supplier ⋈ customer` on `nationkey`, a
~1.2-billion-row intermediate that never finished** (>10 min, killed). With int
stats the Lance plan matches the Parquet plan exactly and Q05 runs in 248ms.
The cost is ~0.9s at SF=10 for all 8 tables; `register_lance_warm` (used by
`benchmark-lance`) pays it at load time instead of inside the first query.
String NDV is still missing — Parquet gets it free from dictionary pages, and
there is no cheap Lance equivalent — so string equality filters fall back to
default selectivity.

**Related fix in shared code**: `PhysicalPlanner::prescan_shared_tables` gated
its 400MB prescan cap on `provider.parquet_files()`, so *any* non-Parquet
provider was exempt and a multi-GB shared table would be decoded into the cache
unconditionally. The gate now falls back to `statistics().total_byte_size`.
Parquet behaviour is bit-identical (same computation); MemoryTable stays ungated.

### Lance vs Parquet vs DuckDB (SF=10, 2026-08-09, serialized, same binary)

All three legs on the same idle machine. Engine legs use ONE binary built with
`--features lance`, so Lance and Parquet differ only in the storage path.

| Configuration | Load | Query total | vs engine/Parquet |
|---|---|---|---|
| Engine over **Parquet** | 0.002s | **7.38s** | 1.00x |
| Engine over **Lance** | 0.89s | **8.04s** | 1.09x slower |
| DuckDB over **Lance** (Arrow interop) | 1.16s | **10.60s** | 1.44x slower |
| DuckDB **native tables** (reference) | — | 2.99s | 0.41x |

**Read the DuckDB/Lance number carefully**: in `materialized` mode DuckDB reads
each dataset into an Arrow table *before* timing, so its per-query times exclude
all Lance decode (paid once, reported as `load=`). The engine's Lance times
*include* decode. Including load on both sides: engine 8.94s vs DuckDB 11.76s.
DuckDB-over-Arrow is also well off DuckDB's native-table pace (2.99s), so this
is not DuckDB at its best — it is the honest cost of its Lance interop path.

All 22 queries are **CELL-EXACT** engine/Lance vs DuckDB/Lance, and row counts
match the Parquet leg exactly. Reproduce with the committed scripts:

```bash
PROTOC=.scratch/tools/protoc/bin/protoc cargo build --release --features lance
./target/release/query_engine benchmark-lance --path ./data/tpch-10gb-lance \
    --iterations 1 --save-csv .scratch/lance_csv
.venv/bin/python scripts/validate_lance.py --lance ./data/tpch-10gb-lance \
    --csv .scratch/lance_csv --sf 10      # -> ALL 22 CELL-EXACT
```

Independently re-measured 2026-08-09 on an idle machine (second run, different
process): engine/Parquet 7.59s, engine/Lance 7.97s (1.05x), DuckDB/Lance 10.80s
query + 1.09s load. Consistent with the table above; run-to-run spread is ~±3%.

`scripts/duckdb_lance_bench.py` and `scripts/validate_lance.py` both divide
Q11's HAVING threshold by the scale factor. Without that DuckDB returns 0 rows
for Q11 while the engine returns 100 — a timing row that looks like a huge
engine loss but is comparing two different queries.

Per-query, the Lance path **wins on join-heavy queries** (Q02 44 vs 64ms, Q05
248 vs 345, Q08 268 vs 346, Q10 285 vs 414, Q03 303 vs 386) and **loses on
filter-heavy single-table scans** (Q19 412 vs 117ms, Q12 372 vs 199, Q09 2088 vs
1431, Q06 131 vs 109). That split is the expected shape: the Parquet path's fast
lanes are exactly the scan-side ones, and none of them are reachable from a
generic `TableProvider`.

**What the Lance path does NOT get** (all keyed off `parquet_files()` /
`ParquetTable`, measured cost above):
- morsel-driven parallel aggregation (`MorselAggregateExec`)
- arrow `RowFilter` predicate pushdown into the decoder
- row-group statistics pruning and dictionary filters
- runtime join-filter bitmaps pushed into the scan
- `scan_with_filter` — `LanceTable` uses the default no-op. Lance's
  `Scanner::filter(&str)` takes a DataFusion SQL string; wiring the engine's
  `Expr` to it risks silent dialect divergence, so it was left out deliberately.
  This is the largest remaining lever for the Lance path. (A *narrow*,
  whitelist-only `Expr -> Lance SQL` renderer now exists in `lance.rs` for the
  k-NN prefilter — see Vector Search — and the dialect divergence fear turned
  out to be justified: see the double-quote finding there.)

## Vector Search (2026-08-09, `--features lance`)

Vector columns (`FixedSizeList<Float32, N>` — how Lance stores embeddings) are
readable, projectable and searchable end-to-end.

```sql
SELECT id, category, text FROM vectors
ORDER BY cosine_distance(embedding, [0.013, -0.041, ...])   -- 384 floats
LIMIT 10;
```

### The three layers

1. **Reading** — `unsupported_reason` in `src/storage/lance.rs` accepts
   `FixedSizeList`/`List`/`LargeList` of scalars. A vector is carried as an
   **opaque value**: selectable, projectable, aliasable, LIMIT-able. It is NOT
   summable, groupable, orderable or comparable — `src/planner/vector_types.rs`
   rejects those in the binder with a message naming the column and its
   dimension. Struct/Map are still rejected.
2. **Distance functions** — `src/physical/vector.rs`. Kernels read the
   `FixedSizeListArray` values buffer directly (one contiguous `&[f32]` per
   batch, no per-row `Vec`). Array literals bind to one `ScalarValue::List`
   (`Binder::bind_array_literal`), so a 384-element vector is a single plan node,
   not a 384-child expression tree.
3. **Index pushdown** — `TableProvider::scan_knn` (default `Ok(None)` = "not
   supported"), `LanceTable::scan_knn` via `Scanner::nearest`, the
   `VectorSearchPushdown` optimizer rule, and `VectorSearchExec`.

### Sign conventions (fixed; the pushdown rule depends on them)

| function | formula | closer = | ORDER BY |
|---|---|---|---|
| `l2_distance` / `euclidean_distance` | `sqrt(sum((a-b)^2))` | smaller | ASC |
| `cosine_distance` | `1 - cos_sim` | smaller | ASC |
| `cosine_similarity` | `cos_sim` | larger | DESC |
| `dot_product` / `inner_product` | `sum(a*b)` | larger | DESC |

`dot_product` is the raw inner product, deliberately not disguised as a distance
(Lance stores "dot distance" as `1 - dot`, pgvector's `<#>` is `-dot`; both
change the number the user sees). `l2_distance` takes the square root; Lance's
internal L2 is *squared*. Ordering is unaffected either way, which is why
pushdown stays valid.

### THE SEMANTIC DECISION: default is EXACT

**`ExecutionConfig::vector_search_mode` defaults to `VectorSearchMode::Exact`.**

Measured on `data/vectors.lance` (200k x 384 real MiniLM embeddings, IVF_PQ /
447 partitions / 48 sub-vectors / cosine), 10 natural-language queries, k=10,
serialized on an idle machine:

| path | median exec | recall@10 vs exact | category precision@10 |
|---|---|---|---|
| engine exact (brute force) | 109 ms | **1.000** | 1.000 |
| indexed, no refine | 5.2 ms | 0.590 | 1.000 |
| indexed, refine=10 (default when opted in) | 5.8 ms | **0.910** | 1.000 |
| indexed, refine=50 | 15 ms | 0.940 | 1.000 |
| indexed, refine=200 | 16 ms | 0.950 (plateau) | 1.000 |
| pylance IVF_PQ refine=10 (reference) | 3.1 ms | — | — |
| pylance flat / exact (reference) | 38.6 ms | 1.000 | — |

**Independently re-verified** (`scripts/verify_vector_search.py`, which drives
the engine BINARY rather than a library call):

| path | distance-exact@10 | id-recall@10 | category precision@10 |
|---|---|---|---|
| engine exact | **1.000** | 0.990 | **1.000** |
| indexed, refine=10 | 0.700 | 0.920 | 1.000 |

**SCORE VECTOR RESULTS BY DISTANCE, NOT BY ROW ID.** This corpus is
template-generated, so many rows share identical text and therefore identical
embeddings. For query 0, **eleven rows fall within the 10th-place distance** and
the exact top-10 contains only **nine distinct distances**. The top-k *set* is
genuinely ambiguous and rank order among equal distances is arbitrary, so
comparing returned ids against ground-truth ids reports failures that are not
failures: it scored the (provably correct) exact path at 0.990 recall and 0.200
"rank-exact". The correct gate is DISTANCE-MULTISET equality — "are the returned
rows as close as the best possible k", which is what the SQL actually asks. By
that gate the exact path is 1.000, and the indexed path's 0.700 is a real,
tie-independent difference.

**Recall never reaches 1.0 at any refine factor.** 21x faster is a real prize,
but silently answering `ORDER BY distance LIMIT 10` from an index that drops
about one true neighbour in ten is a correctness regression dressed as an
optimization. So the fast path is opt-in and explicit:

```bash
QE_VECTOR_SEARCH=indexed      # or ExecutionConfig::vector_search_mode
QE_VECTOR_REFINE=50           # candidates = k * factor, re-ranked exactly
QE_VECTOR_NPROBES=...         # see the warning below
```

`VectorSearchExec` keeps the ORIGINAL plan as `fallback` and executes it
verbatim whenever the mode is `Exact`, the provider has no index, or the
provider declines. Exact is therefore not a re-implementation of the query — it
*is* the query.

### Three measured Lance 0.23.2 defects (all reproduce in raw pylance)

1. **`prefilter(true)` does not prefilter when the vector index is used.**
   `nearest(...) + filter("category = 'books'")` returns **0 rows** on a dataset
   where 40,000 rows match, because the predicate is applied to the index's
   candidate list rather than before it. `use_index(false)` + prefilter returns
   the correct 10. **Workaround**: `LanceTable::scan_knn` forces a flat Lance
   scan whenever a filter is present. Slower than the index (67 vs 11 ms) but
   exact — and filtered searches are therefore exact in *both* modes.
2. **Double-quoted identifiers are parsed as string literals.**
   `"category" = 'footwear'` is a constant FALSE, so the filter matches nothing
   — silently. `expr_to_lance_sql` emits bare identifiers and refuses to push
   any column name that would need quoting.
3. **Raising `nprobes` monotonically *lowers* recall**: 0.91 at Lance's default,
   0.56 at 10, 0.38 at 20, 0.16 at 447 (all with refine=10). Backwards for IVF.
   `vector_nprobes` defaults to `None` (Lance's own default) for that reason.

### The optimizer rule refuses more than it accepts

`VectorSearchPushdown` (`src/optimizer/rules/vector_search.rs`, registered LAST)
matches `Limit(k)` over `Sort([one distance key])` over a chain of *pure column*
projections over a `Scan`. It declines on: multiple sort keys, a direction that
contradicts the metric's sign convention (`ORDER BY cosine_distance(...) DESC`
asks for the k *furthest* rows), no LIMIT, `NULLS FIRST`, any computed
projection, any Filter/Join/Aggregate between Sort and Scan, or a dimension
mismatch. A missed pushdown is slow; a wrong one is wrong answers.

The scan's pushed-down predicate becomes the prefilter. Lance's `_distance`
column is dropped in `VectorSearchExec::shape_output` so it never appears as a
surprise column.

**Known limitation, by design**: putting the distance in the SELECT list —
`SELECT id, cosine_distance(embedding, [...]) AS score FROM ... ORDER BY ...` —
makes the projection a *computed* one, so the rule refuses and the query runs
the exact path. Results are correct, just at exact-path speed. This bit the
test suite first: an early `vector_search_tests.rs` measured "indexed" recall
with that shape and got a meaningless 1.000 because the pushdown never fired.
`pushdown_fires_only_on_the_canonical_shape` now asserts on the optimized plan
so the measurement cannot lie again.

### Related bug found and fixed in shared code

**`ORDER BY <column not in the SELECT list>` failed with `Column not found`.**
`SELECT o_orderkey FROM orders ORDER BY o_totalprice` — valid SQL, and the exact
shape a vector search needs — planned as `Sort(Project([o_orderkey], Scan))`,
whose Sort input has no `o_totalprice`. `extend_projection_for_sort` in
`binder.rs` now widens the projection under the Sort and adds a trimming
projection **above the LIMIT** (above, so Sort+Limit stay adjacent and the
physical planner's top-k fusion still fires). Conservative: only over a plain
`Project`, never over an `Aggregate`, never when the sort key holds a subquery.

### Validation

`tests/vector_search_tests.rs` (feature-gated, skips without
`data/vectors.lance` + `.scratch/vector_gt.json`). Assertions are on the
**distance sequence**, not id lists: the dataset has duplicate texts, hence rows
with byte-identical embeddings and tied distances straddling the k boundary, so
which id lands at rank 10 is arbitrary in any implementation. The exact path
matches the GPU float64 ground truth on all 10 queries at every rank, with
category precision@10 = 1.000.

## Recently Implemented Features

- **Morsel-Driven Aggregation Integration + Vectorization** (2026-01-29)
  - Integrated morsel-driven parallel aggregation into the query engine
  - New `MorselAggregateExec` physical operator in `src/physical/operators/morsel_agg.rs`
  - Automatic routing: PhysicalPlanner detects aggregation over Parquet tables
  - Uses existing morsel infrastructure from `src/physical/morsel.rs` and `src/physical/morsel_agg.rs`
  - Configuration flag: `ExecutionConfig::enable_morsel_execution` (default: true)
  - Extended `TableProvider` trait with `parquet_files()` method for file discovery
  - **Optimizations in `morsel_agg.rs`:**
    - `AggregationState` with dual-mode: perfect hash (fixed array) + HashMap fallback
    - `TypedArrayAccessor` for pre-downcast typed array access
    - `raw_key()` u64 key extraction without ScalarValue allocation
    - `update_f64()` / `update_i64()` direct primitive accumulator updates
    - f64 fast path for all-float aggregate inputs
  - **TPC-H Q1 (SF=10) Performance:**
    - Standard HashAggregateExec: 1830ms
    - With morsel + vectorization: **502ms (3.6x faster)**
    - vs DuckDB (89ms): 5.6x (within 10x target)
    - Remaining gap to DuckDB is primarily Parquet I/O (custom SIMD reader)

- **DelimJoin Infrastructure** (2026-01-27)
  - DuckDB-style deduplication join for efficient correlated subquery execution
  - New logical plan nodes: `DelimJoinNode`, `DelimGetNode` in `src/planner/logical_plan.rs`
  - New join types: `JoinType::Single` (scalar subquery), `JoinType::Mark` (IN subquery)
  - Physical operators: `DelimJoinExec`, `DelimGetExec` in `src/physical/operators/delim_join.rs`
  - Shared `DelimState` for passing distinct correlation values between operators
  - Optimizer rule: `FlattenDependentJoin` in `src/optimizer/rules/flatten_dependent_join.rs`
    - Currently disabled pending column resolution fixes
    - Design: Transform O(n*m) row-by-row execution to O(n+m) set-based execution
  - Physical planner updated to handle DelimJoin/DelimGet nodes
  - Foundation for future Q21 performance improvement at larger scale factors

- **Q21 EXISTS/NOT EXISTS Fix** (2026-01-27 - 400x speedup at SF=0.01)
  - Fixed PredicatePushdown pushing EXISTS predicates to scan nodes
  - SubqueryDecorrelation now properly finds and transforms EXISTS/NOT EXISTS
  - Q21 at SF=0.01: 363 seconds → 921ms (395x faster)
  - At SF=0.1 still slow (61s) due to O(n²) execution - needs DelimJoin
  - Located in `src/optimizer/rules/predicate_pushdown.rs`

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

- **Larger-Than-Memory Dataset Support** (Memory-safe by default)
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
  - Spillable operators are always active (no opt-in flag) in `src/physical/operators/spillable.rs`
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
| Morsel aggregation operator | `src/physical/operators/morsel_agg.rs` |
| Morsel framework | `src/physical/morsel.rs`, `src/physical/morsel_agg.rs` |
| Subquery execution | `src/physical/operators/subquery.rs` |
| DelimJoin operators | `src/physical/operators/delim_join.rs` |
| Flatten dependent join rule | `src/optimizer/rules/flatten_dependent_join.rs` |
| Main entry point | `src/execution/context.rs` |
| Parquet table provider | `src/storage/parquet.rs` |
| Lance table provider | `src/storage/lance.rs` (feature `lance`) |
| Lance tests | `tests/lance_tests.rs` (feature `lance`) |
| Parquet→Lance conversion | `scripts/lance_convert.py` |
| DuckDB-over-Lance oracle/baseline | `scripts/duckdb_lance_bench.py` |
| Streaming Parquet reader | `src/storage/parquet.rs` (StreamingParquetReader) |
| Async Parquet reader | `src/storage/parquet.rs` (AsyncParquetReader) |
| TableProvider trait | `src/physical/operators/scan.rs` |
| Memory pool/config | `src/execution/memory.rs` |
| Spillable operators | `src/physical/operators/spillable.rs` |
| Vector distance kernels | `src/physical/vector.rs` |
| Vector type rules (fail-loudly) | `src/planner/vector_types.rs` |
| k-NN pushdown rule | `src/optimizer/rules/vector_search.rs` |
| k-NN operator | `src/physical/operators/vector_search.rs` |
| Lance k-NN / prefilter SQL | `src/storage/lance.rs` (`scan_knn`, `expr_to_lance_sql`) |
| Vector search tests | `tests/vector_search_tests.rs` |
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
| Development roadmap | `.claude/plans/ROADMAP.md` |
