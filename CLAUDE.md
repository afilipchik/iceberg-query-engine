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

### Sandboxed Build Rule

**EVERY BUILD, TEST, OR BENCHMARK COMMAND MUST RUN THROUGH `scripts/claude-safe-build.sh`.**

```bash
scripts/claude-safe-build.sh cargo test --release --features lance
SAFE_BUILD_MEM=48G SAFE_BUILD_JOBS=8 scripts/claude-safe-build.sh cargo bench --bench tpch
```

The wrapper runs the command in its own transient cgroup scope
(`systemd-run --user --scope`) with a hard 80G memory cap and
CARGO_BUILD_JOBS=8 (the release profile uses fat LTO with codegen-units=1, so each link is a multi-GB job). If a build exceeds the cap, the kernel kills
processes inside that scope only and cargo fails cleanly (exit 137).

Why: on 2026-08-22 a bare `cargo test --release --features lance` peaked
at 105G inside the terminal's cgroup; systemd-oomd killed the whole
terminal scope — including the Claude session and remote-control bridge.
A build must never share a cgroup with the session that launched it.
Never run bare `cargo build/test/bench` for release or heavy-feature
builds; there is no situation where bypassing the wrapper is worth it.

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
├── distributed/              # `query_engine serve` — MILESTONE 1 ONLY
│   ├── mod.rs                # Module exports
│   ├── server.rs             # hyper HTTP server: /healthz /readyz /cluster /sql
│   ├── membership.rs         # Peer discovery (static + DNS), self-id, probes
│   └── http_client.rs        # ~100-line HTTP client used for peer probes
│
└── tpch/
    ├── mod.rs                # TPC-H module exports
    ├── generator.rs          # TpchGenerator for test data + Parquet export
    ├── schema.rs             # TPC-H table schemas
    └── queries.rs            # All 22 TPC-H queries (adapted for generated data)

tests/
├── sql_comprehensive.rs      # 131 SQL correctness tests
└── distributed_cluster.rs    # M1 gate: 3 in-process nodes + 3 real processes

k8s/                          # UNVALIDATED-ON-CLUSTER (no Docker on this box)
├── statefulset.yaml          # 3 nodes, QE_NODE_ID from the pod ordinal
├── service-headless.yaml     # the DNS name --peers-dns resolves
└── service.yaml              # client entry point (NodePort 30777)
Dockerfile                    # multi-stage; data is MOUNTED, not baked
kind-cluster.yaml             # 1 control-plane + 3 workers, host mount for data

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
6. **4KB pages, NOT transparent huge pages**: `main()` calls
   `disable_transparent_hugepages()` (`src/execution/memory.rs`) to opt the
   process out of THP via `prctl(PR_SET_THP_DISABLE)`. This is deliberate and
   counter-intuitive — see the "Transparent huge pages" note below before
   reverting it.

### Transparent huge pages: measured OFF (2026-08-09)

mimalloc calls `madvise(MADV_HUGEPAGE)` on its large regions, so without an
explicit opt-out the engine gets 2MB pages for **97-99% of its RSS** on any
machine whose THP mode is `always` or `madvise`. That is a net **loss** here:

* A standalone random-probe microbenchmark (`.scratch/hugebench/`) confirms 2MB
  pages ARE worth **8-11%** over a 1GB table. The TLB win is real in isolation.
* The engine nonetheless runs **cheaper on 4KB pages**. CORRECTED 2026-08-10
  after independent re-measurement — the original claim here was
  "7.98s -> 7.48s (-6.3%) suite wall", and that **did not reproduce**. Its
  THP-on baseline (7.98s) was inflated by unrelated VM benchmarks running
  concurrently on the box; a clean interleaved suite A/B shows THP-on and
  THP-off within this machine's noise band on WALL time. The defensible,
  reproduced numbers are CPU time and memory, which barely move with noise:
  - **Q01 CPU 29.8s -> 26.5s (-11.3%)**, RSS 2244 -> 2111 MB (-6.3%)
  - **Q09 CPU 83.2s -> 76.3s (-8.3%)**, RSS 5958 -> 5513 MB (-8.1%)
  - **Throughput under load** (4 concurrent engines x 8 pinned cores, the
    shared-nothing shape): **-2.5% wall, 3/3 pairs** for 4KB pages.
  Single-query wall time on an IDLE box is a wash: ~8-11% less CPU is absorbed
  by spare cores, so it only becomes speed once cores are saturated. That is
  precisely why the concurrency measurement, not the idle one, justifies this.
* **Why the microbenchmark does not transfer**: the engine's hot memory is
  *streamed*, not randomly probed. Morsel scans allocate, fill, drain and free
  large Arrow buffers continuously, so sequential prefetch already hides the TLB
  cost. What 2MB pages add is fault-time cost — on Q01 they raised kernel time
  2.64s -> 3.94s and user time 6.27s -> 7.80s, because the kernel zeroes a full
  2MB per fault and the engine ends up touching ~16% more physical memory.
* Peak RSS **drops** with 4KB pages, which is the direction the memory-safety
  rule wants. Two independent measurements agree on the direction and differ
  in magnitude with the run (single-query vs 3-iteration): 1962 -> 1691MB and
  5087 -> 4813MB in the original pass; 2244 -> 2111MB (Q01) and 5958 -> 5513MB
  (Q09) on re-measurement.

Set `QUERY_ENGINE_ALLOW_THP=1` to re-enable huge pages when re-measuring.

**Gotcha for anyone re-measuring this**: the Claude Code CLI sets
`PR_SET_THP_DISABLE`, and child processes inherit it — so any benchmark run
from an agent shell has THP silently unavailable and will show huge pages doing
nothing. Check `grep THP_enabled /proc/self/status` (1 = available) and clear
the flag first; `.scratch/hugebench/src/bin/thprun.rs` is a wrapper that does.
Always confirm `AnonHugePages` in `/proc/<pid>/smaps_rollup` actually rises
before attributing any timing delta to huge pages.

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
| `reqwest` | HTTP client for the older branching-metastore REST API |
| `apache-avro` | Iceberg manifest lists/files (arrow-independent by construction) |
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

# Run as a cluster node (MILESTONE 1: /sql executes LOCALLY, no fan-out yet)
query_engine serve --bind 0.0.0.0:7777 --node-id 0 \
    --peers 10.0.0.1:7777,10.0.0.2:7777 --data ./data/tpch-10mb
# ...or with Kubernetes-style DNS discovery against a headless Service:
query_engine serve --bind 0.0.0.0:7777 --peers-dns query-engine-headless \
    --data /data
```

### Distributed mode (M1 + M2 + M2.5, 2026-08-15)

`serve` turns the process into a cluster node. See
`.claude/plans/DISTRIBUTED-IMPLEMENTATION.md` for the milestone plan and
`.claude/plans/DISTRIBUTED-READINESS.md` for the blockers that must be
fixed before M3 (shuffle).

| Endpoint | Meaning |
|----------|---------|
| `GET /healthz` | Liveness. Touches no tables, peers or disk, so a slow query can never cause a Kubernetes restart. Returns this node's id. |
| `GET /readyz` | Readiness = tables loaded AND discovery resolved AND not draining. 503 with a `reason` otherwise. |
| `GET /cluster` | Membership as JSON. The `members` array is sorted by address so every node in a healthy cluster renders an identical list. |
| `GET /splits` | `?table=<t>&nodes=<n>` — how a table divides, with imbalance. |
| `POST /sql` | Body is the statement. `?format=arrow (default)|json|csv`; `?distributed=auto (default)|1|0`. Headers report `x-qe-distributed`, `x-qe-distribution` (full JSON), `x-qe-imbalance`, `x-qe-shards`. |
| `POST /fragment` | One shard of a distributed query (internal). Digest-checked. |

**Arrow Flight endpoint (2026-08-21, `src/distributed/flight.rs`).** `serve`
also runs an Arrow Flight gRPC server: `--flight-bind <addr>` (default = HTTP
port + 1, ephemeral when HTTP binds `:0`, `none` disables). Both front doors
run the SAME `execute_statement` path extracted from the HTTP handler, so
Flight can never disagree with `/sql` about when a query distributes.

| Flight RPC | Meaning |
|------------|---------|
| `ListFlights` | One `FlightInfo` per registered table (path descriptor + schema, no endpoints). |
| `GetSchema` | Path descriptor = table schema; cmd descriptor = query result schema, PLAN-ONLY (`physical_plan()`, no execution). |
| `GetFlightInfo` | cmd = raw SQL bytes, or JSON `{"sql", "mode": "auto|force|off"}`. Plans, returns schema + ONE endpoint with a stateless v1 JSON ticket `{"v":1,"sql","mode"}` (1MB cap, same as `MAX_SQL_BODY_BYTES`) and EMPTY locations (= fetch from this connection, per spec). |
| `DoGet` | Validates the ticket, executes via `execute_statement`, streams schema + batches + a trailing ZERO-ROW batch whose `app_metadata` is the execution JSON (`rows`, `elapsed_ms`, `distributed`, `shards`, `imbalance`, `skipped_reason`, full `distribution`) — the `x-qe-*` header analogue. |
| `DoAction("cluster")` | The `GET /cluster` JSON, byte-identical (`cluster_view()` shared). |
| everything else | `Status::unimplemented`, naming the RPC. |

Load-bearing details:
- **Error mapping** (`query_error_status`): Parse/Bind/Type/Plan →
  `InvalidArgument`; TableNotFound/ColumnNotFound → `NotFound`;
  NotImplemented → `Unimplemented`; tables-not-loaded → `Unavailable`;
  rest → `Internal`. Message = the engine's error Display, verbatim.
- **The metadata trailer is a zero-row RecordBatch, not a metadata-only
  message, and that is not optional**: arrow-rs 53's `FlightDataDecoder`
  refuses an empty `data_header` while Arrow C++/pyarrow refuses a NONE-typed
  one — a zero-row batch is the only shape both families decode. DoGet
  hand-encodes with `IpcDataGenerator` (batches re-sliced to <=4096 rows for
  gRPC's 4MB default frame limit) instead of `FlightDataEncoderBuilder`.
- **Membership gossips flight addresses**: `/healthz` carries the node's
  advertised flight address, the prober records it, and `/cluster` +
  `DoAction("cluster")` list it per member. Nodes never dial each other's
  Flight ports — internal traffic stays on the hyper transport ("Flight
  semantics, not the Flight crate" holds for the interior; arrow-flight 53
  was added for the client edge only, with an ADD-ONLY Cargo.lock diff).
- Gates: `tests/flight_tests.rs` (8 in-process tests incl. a 3-node real-TCP
  scatter through Flight) and `scripts/flight_validate.py` (pyarrow; all 22
  TPC-H Flight==HTTP at SF=0.01/0.1 single-node and per-node on a 3-process
  cluster; `--quick` runs inside `cluster_local.sh verify` as step 4/5).
  Cluster harness flight ports live at BASE_PORT+100+i.

**Execution paths under `distributed=1`** (`src/distributed/`):

* **Scatter (M2, `plan.rs`+`coordinator.rs`)** — single-table scans/filters/
  projections and exactly-mergeable aggregates (COUNT/SUM/MIN/MAX/AVG, GROUP
  BY/HAVING). Workers run a rewritten partial over their shard, initiator
  merges with generated final SQL. Splits are row ranges inside Parquet row
  groups (`splits.rs`), divided by BYTES with LPT; every node recomputes the
  assignment and a `SplitSet::digest` mismatch FAILS the query.
* **Gather (M2.5, `gather.rs`)** — everything else the local engine can run:
  joins, subqueries, COUNT(DISTINCT), DISTINCT, CTEs, set ops, global ORDER
  BY/LIMIT, STDDEV. Workers stream their shard of every referenced table
  (columns pruned to what the optimized plan reads — NEVER `SELECT *`, whose
  qualified output names cannot be re-bound), the initiator runs the ORIGINAL
  statement over the gathered tables. Memory-bounded: refuses if assigned
  compressed bytes exceed half of `--memory-limit`. All 22 TPC-H queries pass
  distributed, byte-compared against the single-process engine.
* `distributed=auto` scatters exact shapes when >=2 members are up, answers
  everything else locally with the reason in `x-qe-distributed-skipped`.
  `distributed=1` never falls back to local.

Splits are provider-generic (`TableProvider::distributed_splits` /
`shard_by_splits`): Parquet AND Iceberg tables enumerate row-group ranges,
Lance datasets enumerate whole fragments (subset threaded through every
scan path; k-NN pushdown declined on a shard). Still no shuffle — that is M3.

**Testing without Docker.** kind cannot run on the development machine (no
docker/podman, no passwordless sudo, and
`kernel.apparmor_restrict_unprivileged_userns=1` blocks unprivileged user
namespaces). The acceptance testbed is therefore N separate OS processes over
real TCP:

```bash
./scripts/cluster_local.sh start 3     # 3 processes, static peer list
./scripts/cluster_local.sh verify      # the full M1 gate
./scripts/cluster_local.sh stop
cargo test --release --test distributed_cluster
```

The Kubernetes artifacts (`Dockerfile`, `k8s/*.yaml`, `kind-cluster.yaml`) are
**UNVALIDATED-ON-CLUSTER**. `.venv/bin/python scripts/validate_k8s_manifests.py`
checks everything checkable without an API server; `scripts/kind_test.sh` is
what must be run on a Docker-capable machine to change that status.

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

# --- TIME TRAVEL ---
query_engine lance-versions --path ./data/orders.lance
query_engine load-lance --path ./data/orders.lance --name o --version 1 \
    --query "SELECT COUNT(*) FROM o"

# --- WRITING (all in Rust; no Python needed) ---
# Parquet -> Lance, streamed (never materializes either side)
query_engine write-lance --from-parquet ./data/tpch-10gb --out ./data/x.lance
# Append; the version increments and the old version stays readable
query_engine write-lance --from-parquet ./data/more.parquet --out ./data/x.lance \
    --mode append
# CREATE TABLE AS SELECT, in effect: run SQL and write the result
query_engine write-lance --sql "SELECT * FROM orders WHERE o_totalprice > 100000" \
    --tables ./data/tpch-1mb --out ./data/big_orders.lance
# Build the IVF_PQ index the (opt-in) k-NN pushdown uses
query_engine create-lance-index --path ./data/vectors.lance --column embedding \
    --metric cosine --partitions 447 --sub-vectors 48 --replace
```

`scripts/lance_convert.py` (pylance) still exists and produced the committed
`data/*-lance` fixtures, but `write-lance --from-parquet` is the Rust
equivalent and is what the test suite uses.

### Iceberg tables (default build, 2026-08-15)

`src/storage/iceberg.rs` reads REAL Iceberg tables (format v1/v2): latest
`*.metadata.json` (via `version-hint.text` or greatest `last-updated-ms`),
Avro manifest lists + manifest files (apache-avro), entry status handling,
absolute/`file:`-URI resolution, snapshot time travel. The resolved file list
becomes an ordinary `ParquetTable`, so statistics, pruning and distributed
splits apply unchanged. REFUSED by name: delete files, non-Parquet data
files, empty tables, remote URIs. Registration:
`ctx.register_iceberg(name, dir, snapshot_id: Option<i64>)`;
`serve --tables <dir>` auto-detects Iceberg table dirs (checked BEFORE the
parquet-dir test — order is a correctness matter) and `.lance` datasets.
Fixtures: `scripts/iceberg_gen.py` (pyiceberg in `.venv`) regenerates
`data/tpch-{1,10}mb-iceberg`; the 1mb `orders` has TWO snapshots
(1500 rows, then +100 → 1600) for time-travel tests. `data/tpch-10gb-iceberg`
is the SF=10 warehouse (8 tables, 2 snapshots each) used by the benchmark
below.

**Iceberg-table benchmark vs plain parquet (2026-08-23,
`duckdb-parity-2` close-out).** `benchmark-parquet` does NOT auto-detect
Iceberg (it hardcodes `<table>.parquet` lookups); `serve --tables <dir>`
does. `scripts/iceberg_bench_compare.py` (new) drives that HTTP surface —
`serve --tables data/tpch-10gb-iceberg`, POSTs the 22 queries to `/sql` —
against DuckDB's `INSTALL/LOAD iceberg; iceberg_scan(<highest-numbered
metadata.json>)` (the pattern `iceberg_gen.py`'s own `read_back_duckdb()`
uses; pointing `iceberg_scan` at the bare directory fails with a
version-guessing error on this SqlCatalog-layout warehouse, which has no
`version-hint.text`).

| premise | engine total | DuckDB total | ratio |
|---|---|---|---|
| plain parquet, cache-off | 7.03s | 4.22s (like-for-like, `read_parquet` views) | 1.67x |
| plain parquet, cache-on | 5.75s | 4.22s | 1.36x |
| **Iceberg** (`iceberg_scan` / `serve --tables`) | **8.325s** | **6.745s** | **1.23x** |

Row counts match on all 22 queries (engine-Iceberg vs DuckDB-Iceberg).
**Iceberg's manifest/snapshot indirection is real overhead on both
sides, but asymmetric**: it costs the engine only ~+18.5% over its own
plain-parquet cache-off baseline (7.03s→8.325s, since Iceberg resolves to
an ordinary `ParquetTable` and pays little beyond metadata/manifest
parsing), but costs DuckDB's `iceberg_scan` ~+60% over its own
like-for-like baseline (4.22s→6.745s) — so the competitive RATIO actually
narrows under Iceberg (1.23x) versus plain parquet (1.36-1.67x). Report
both premises, matching this doc's own "report multiple premises
separately" convention (cache-on/off, native/like-for-like, now also
iceberg/parquet). Reproduce: `.venv/bin/python
scripts/iceberg_bench_compare.py --iceberg-dir data/tpch-10gb-iceberg
--sf 10 --iterations 2`.

### Native table commands (default build, 2026-08-23)

Full design/capabilities/benchmarks in the "Native Tables" section below;
this is just the CLI/SQL surface.

```bash
# Bulk-load from an existing source (task 003) -- streamed, bounded memory
query_engine write-native --from-parquet data/tpch-10gb/orders.parquet \
    --out data/orders_native --mode create   # or --from-iceberg / --from-lance (--features lance)
# CREATE TABLE ... AS SELECT shape: streams the query's physical plan
# straight into the writer, never materializes it (src/storage/native_write.rs)
query_engine write-native --sql "SELECT * FROM orders WHERE o_totalprice > 100000" \
    --tables data/tpch-10gb --out data/big_orders_native

# Print a native table's manifest, optionally query it (materializing --
# a CLI validation convenience, not the production read path)
query_engine load-native --path data/orders_native --query "SELECT COUNT(*) FROM t"

# `serve --tables <dir>` auto-detects a native table subdirectory
# (`_manifest.json` present) exactly like it does Iceberg/Lance dirs
query_engine serve --tables data/tpch-10gb-native --memory-limit 40G
```

`CREATE TABLE <name> AS SELECT ...` also works through
`ExecutionContext::create_table_as_select` (the REPL calls this
automatically) -- see "Native Tables" below for the precise boundary
(NOT reachable through `sql()` or the distributed HTTP/Flight endpoints).

### Metastore: Apache Gravitino (2026-08-15)

`src/metastore/gravitino.rs` + `serve --metastore <url>
[--metastore-metalake local_lake] [--metastore-catalog lakehouse]
[--metastore-schema tpch]`: every fileset of one Gravitino schema is
registered as a table; the fileset's `format` property (parquet|iceberg|
lance) picks the reader, optional `file` property names a parquet inside a
directory (Gravitino refuses file storageLocations). No sniffing; missing
format is refused by name. A local Gravitino 1.3.0 (+JDK17) lives under
`.scratch/metastore/` — no docker, no sudo:

```bash
scripts/metastore_local.sh start|stop|status|wipe   # the server
scripts/metastore_demo.sh [--with-lance]           # the full gate:
#   populates local_lake/lakehouse with schemas tpch (parquet),
#   tpch_iceberg, tpch_lance; starts a 3-node cluster from NOTHING but
#   --metastore; verifies distributed answers vs the single-process oracle
#   and byte-identical cross-node agreement. PASS for all three formats.
scripts/cluster_local.sh start 3 --metastore http://127.0.0.1:8090 \
    --metastore-schema tpch_iceberg                # harness in metastore mode
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
| `.lance <path> <name> [version]` | Load a Lance dataset as table, optionally at a historical version (`--features lance`) |
| `.lance-versions <path>` | List a Lance dataset's versions (`--features lance`) |
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
- **Iceberg partition pruning + row-level deletes** — the reader
  (`src/storage/iceberg.rs`, DONE 2026-08-15: real metadata.json + Avro
  manifests + snapshot time travel, resolved to a ParquetTable) refuses
  delete files and does no partition pruning yet. The old JSON-manifest stub
  in `src/physical/operators/iceberg.rs` is dead code kept only for its
  stats-pruning scaffolding.
- ~~Window functions~~ — DONE 2026-08-21 (standard-sql-completion epic),
  see the "Window functions" section below
- **Array/Map type support** - Complex nested data types
  - Array functions: array_agg, array_distinct, array_join, filter, transform, etc.
  - Map functions: map_keys, map_values, map_entries, element_at, etc.
  - See plan at `.claude/plans/trino-function-implementation.md` Phases 4-5
- Cost-based optimization (cost.rs exists but is minimal)
- Parallel execution (partition parameter exists but single-threaded)

## Current Test Status (2026-08-15)

- **All test suites green**: **955** on the default build (was 837 on
  2026-08-08; the growth is distributed M2/M2.5, gather, Iceberg reader and
  Gravitino client tests), lance-feature suite green, including:
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

## SF=100: the four-way matrix (2026-08-18, warm, same machine, duckdb 1.4.4)

| storage | engine | DuckDB on the SAME files | ratio |
|---|---|---|---|
| parquet (identical files) | **65.1s** | 40.1s (`read_parquet` views) | **1.62x** |
| parquet + v2 IPC sidecars (auto) | **47.1s** | 65.8s native-premise | **0.70x native** |
| lance (identical files) | ~101s (high variance, see below) | 69.1s (community `lance` ext) | ~1.47x |
| DuckDB native (in-mem) | — | 65.8s (Q9 alone 36.4s) | — |

The 2026-08-18 duckdb-parity epic took parquet 89.3→72.7s (runtime-filter
bitmap cap 64M→2^31 bits; PackedJoinKeys moved after the optimizer
fixpoint loop — this also FIXED a Q5 SF=10 cross-join planning failure
shipped in ad3881a; dictionary-preserving join gather; parallelized
raw-sum merge). Lance SWEEP totals on this box carry ±5-8% variance
(56GB dataset + query RSS exceed what stays page-cached next to the
32GB parquet); warm single-query A/Bs are the honest lance numbers:
Q9 20.8s (−8.2s from dict gather), Q19 3.3s (−2.2s from pushdown),
Q10 2.37s ≈ duck-lance parity. Full evidence: PARITY-PLAN.md epic
section + .claude/epics/duckdb-parity/.

`scripts/duckdb_files_bench_sf100.py` reproduces the DuckDB side. Note the
inversion: DuckDB-native is SLOWER than DuckDB-parquet at SF=100 because
native Q9 is pathological (36.4s vs 9.2s over views) — so "vs native"
ratios (the stored 67.1s baseline ≈ today's 65.8s) understate DuckDB's
parquet reader. The honest like-for-like on identical parquet is 2.21x;
on identical lance it is 1.30x, and the engine beats DuckDB's lance
reader outright on Q1 (3.9 vs 7.9s), Q6 (1.5 vs 11.8s) and Q15
(1.8 vs 3.4s).

## TPC-H Benchmark Status (SF=100 LANCE, updated 2026-08-18)

**Sweep ~101s (±5-8% run variance on this box — see the four-way matrix
note); 22/22 successful, SF=10 lance ALL 22 CELL-EXACT vs
DuckDB-over-Lance.** Dataset: `data/tpch-100gb-lance` (56GB vs 32GB
parquet), written by the engine's own `write-lance --from-parquet`.
Disjoint-agg mode fires through Lance too. The 2026-08-18 epic closed
the two biggest lance-specific gaps: **Q19 5.5→3.3s** (sampled
string/float statistics let the pushdown gate price its OR-of-IN-lists —
the wide-column requirement is retired, the 10% selectivity limit alone
gates) and **Q9 29.0→20.8s** (dictionary-preserving join gather).
Q10/Q18 plans are byte-identical to parquet's; Q10 warm ≈ duck-lance
parity (2.37 vs 2.35s), Q18's remaining ~2s is the runtime-filter
architecture gap (lance scans at planning time, so probe decode can't be
bitmap-pruned). Run: `benchmark-lance --path data/tpch-100gb-lance --sf 100`.

## TPC-H Benchmark Status (SF=100, 2026-08-18)

**With v2 IPC sidecars (2026-08-19, commit 2a9a6b4): 48.3s warm =
0.72x DuckDB native, 1.21x DuckDB on the same parquet; 22/22
cell-valid. Cache-off premise: 65.1s = 0.97x native / 1.62x
like-for-like.** QE_IPC_CACHE now defaults to AUTO (uses fresh
sidecars, never builds; =1 builds ~2.6x parquet's disk; =0 off).
v2 sidecars store low-cardinality strings dictionary-encoded so
dict-coercion scans (Q1/Q13/Q16) no longer fall back to parquet —
that fallback's page-cache contention was the REAL cause of the older
'IPC no benefit at SF=100' verdict. Direct u32 match emission (commit
609509d) added Q9 13.6->12.1s on top of join-output pruning. Join-output pruning (ON-only columns
never gathered, commit 8d2a2b3) took Q9 18.7->13.6s on top of the
duckdb-parity epic's wins; `examples/radix_bench.rs` records why radix
partitioning is REFUTED on this box (probes are MLP-bound at 3.8ns/row). Q6/Q9/Q15/Q19 run faster than native
DuckDB. Worst absolute: Q9 18.7s (saturation-bound; see PARITY-PLAN
residues), Q18 7.6s, Q21 5.5s, Q20 5.0s. Worst like-for-like ratios:
Q1 2.8x, Q16 2.7x, Q10 2.0x. Run via `scripts/sf100_full_benchmark.sh`
(THROUGH `scripts/oomsafe.sh` — systemd-oomd killed a session during an
uncontained sweep); validate VALUES against `data/sf100_duckdb_results`
— a Q11 wrong-answer bug returned exactly the right ROW COUNT (row
counts are not answers). Do NOT set OOMSAFE_MEMHIGH on measurement
runs: MemoryHigh counts page cache and throttled a capped sweep +2.2s.

**Confirmatory re-check (2026-08-23, `duckdb-parity-2` close-out)**: not a
full re-run of the numbers above (none of that epic's fixes target SF=100
scale by design), just a regression check. `.venv/bin/python
scripts/sf100_engine_validate.py`-class sweep, `data/tpch-100gb`, AUTO
cache premise (fresh sidecars, resolves to cache-on-equivalent): **50.66s
total, 22/22 successful, 22/22 CELL-EXACT** (`.scratch/validate22_sf100.py`
against a fresh DuckDB oracle — not row-count-only; Q13's data-dependent
2-row answer at SF=100, down from 24 at SF=10, verified correct rather than
assumed). **Q9 = 11.21s, Q18 = 4.86s** — both inside/better than the
ranges above, unregressed, consistent with tasks 002/003/006's own SF=100
spot-checks during the epic. `finalize_disjoint_states`' single-state fast
path (shipped by task 006) is the only SF=100-relevant change this epic
made; it is additive (Q13 merge-step only), not a suite-wide rewrite.

## TPC-H Benchmark Status (SF=1, 2026-08-21)

**Distributed pushdown (2026-08-22 epic): 2.09s forced-distributed —
2.07x faster; 17/22 scatter (15 two_phase + 2 top_n), 22/22 CELL-EXACT
vs DuckDB.** Scatter now covers joins + subqueries + HAVING/ORDER/LIMIT
via the ClickHouse sharded-fact/replicated-dims model: one elected table
(largest, referenced exactly once, shard-safe join path) is sharded,
everything else reads worker-local replicas; merge stage finishes
HAVING/ORDER BY/LIMIT; TopN pre-truncates per shard. Gather remains for
Q11/Q13/Q15/Q16/Q22 (dup references / nested aggs / COUNT DISTINCT).
Fragment contexts register the FULL catalog (coordinator.rs). Election
and safety rules: src/distributed/plan.rs census. Residual ~30-50ms/query
fan-out overhead is the M3 target.

**Pre-epic record: 4.33s forced-distributed
(distributed=1), 22/22 CELL-EXACT vs DuckDB.** Q06 scatters (two_phase,
imbalance 1.01); the other 21 gather (joins / global ORDER BY). ~3.1x
single-process — gather re-ships shards per query over loopback and 3
nodes share one host's bandwidth, so this measures coordination, not
scaling. `scripts/cluster_local.sh start 3 --data ./data/tpch-1gb` +
`.scratch/sf1/dist_bench.py` pattern.

**1.38s warm vs DuckDB 1.4.4: 0.97s over the same parquet (1.42x) and
0.44s native in-memory tables (3.11x); 22/22 CELL-EXACT.** Best of 3 per
query, 16 duck threads, data `data/tpch-1gb` (current generator,
regenerated same day). Engine WINS like-for-like on Q02/Q09/Q15/Q20/Q21
and beats native DuckDB on Q09 (124 vs 169ms — native Q09 pathology
already seen at SF=100 persists at SF=1). Per-query table in README.
Method: `benchmark-parquet --path data/tpch-1gb --iterations 3` +
`.scratch/sf1/duck_bench.py` pattern (queries extracted from
src/tpch/queries.rs; Q11 threshold needs no adjustment at SF=1).

## TPC-H Benchmark Status (SF=10, updated 2026-08-23)

**Re-baselined again at the close of `duckdb-parity-2` (six tasks: IPC-cache
defaults, Q13 disjoint-threshold + join-pruning, Q16 anti-join parallelism +
hasher swap, dense-group-id Stage 0). Both IPC-cache premises forced and
stated explicitly, as established by the prior (2026-08-22) re-baseline.
22/22 pass in each premise, 995 tests green (default build); 22/22
CELL-EXACT at SF=10 AND SF=100.**

| cache premise | engine total | vs DuckDB native (3.32s) | vs DuckDB on the SAME parquet (4.22s, `read_parquet` views) |
|---|---|---|---|
| `QE_IPC_CACHE=0` (off) | **7.03s** | 2.1x | **1.67x** |
| `QE_IPC_CACHE=1` (build) | **5.75s** | 1.7x | **1.36x** |
| unset (`Mode::Auto`, the default) | same as the `build` row when fresh sidecars already exist (true for every committed `.qeipc` fixture, incl. `data/tpch-10gb`); same as the `off` row on a clean checkout | — | — |

Improved from the 2026-08-22 re-baseline (7.40s/2.23x/1.77x cache-off,
5.88s/1.77x/1.41x cache-on) by `duckdb-parity-2`'s combined Q13+Q16 fixes.
**Q13 and Q16 residue status** (this epic's two named target queries,
PRD bands "Q13 415-500ms", "Q16 153-224ms" depending on premise):

| query | cause | fix | before (band) | after (tight, best-of-8) |
|---|---|---|---|---|
| Q13 | agg-side: `disjoint_group_hint` floor (2M) excluded SF=10's 1.5M `c_custkey` range | floor lowered to 1M | 415-500ms | cache-off 259.9ms avg (min 239.3ms) / cache-on 223.0ms avg (min 206.3ms) — **~37-48% faster** |
| Q13 | join-side: filtered LEFT join excluded from output-pruning + runtime-filter (Inner-only gates) | gates extended to Inner/Left/Right/Full + no-subquery-filter | (bundled above) | shipped, tested, CORRECT — but measured NEGLIGIBLE wall-clock effect on Q13 specifically: `ProjectionPushdown` had already cut its join inputs to 4 columns pre-existing, so this task's own pruning only drops one more (redundant `o_custkey`); Q13's residual join-side cost is a **permanent double-gather of `o_comment`** (the `u32_path` fast-emission stays `filter.is_none()`-gated), out of scope for this epic. Mechanism is now correctly available for OTHER filtered-outer-join queries even though it didn't move Q13. |
| Q16 | `JoinType::Anti` excluded from the batch-parallel-probe gate (oversight, not correctness) — 8M-row NOT-IN probe ran on 1 of 32 threads | gate widened to include `Semi`/`Anti` | 153-224ms | cache-off 131.4ms avg (min 113.0ms) / cache-on 114.9ms avg (min 98.2ms) — **~23-49% faster**, the epic's single largest individual win (anti-probe itself: 41.5ms->6.2ms, 6.7x). Secondary win: **Q22 also ~18% faster** (same VHT-served anti-join shape). |
| Q16 | `distinct_set` used `std::collections::HashSet` (SipHash) instead of `hashbrown` | swapped to `hashbrown::HashSet` | (bundled above) | landed, zero-risk; real but minority contributor as predicted going in |

Dense-group-id remapping (the program's long-standing "next lever," named
since `perf-marathon`): Stage 0 kill-switch microbenchmark **CLEARS**
(24.5-44.5% isolated win, 1M-50M groups, 1-aggregate shape) but Stage 1
correctly **did not proceed** — neither Q10 nor Q20 reaches the boxed
`raw_groups` tier Stage 1 would replace; both already bypass it via the
leaner `raw_sums` tier (`GroupKeyReduction`/`EagerAggregation`). A smaller,
in-scope fix shipped instead: `finalize_disjoint_states`' single-state fast
path (SF=100 Q13 merge step ~205ms/iter -> ~168ms/iter). Full design +
Stage 0 evidence stay in the repo for a future epic to re-open Stage 1
against a freshly-confirmed query.

Like-for-like is the number that was missing here before the 2026-08-22
update — the SF=100 four-way matrix already reported it, this section
didn't, breaking this doc's own "Honesty note" convention (PARITY-PLAN.md).
`Mode::Auto` never BUILDS a sidecar on its own — it only uses
one that is already fresh on disk — because a full sidecar tree costs
~2.6x parquet's footprint; see `storage/ipc_cache.rs`'s module doc for the
tri-state semantics. `benchmark-parquet`'s startup log and
`safe_benchmark.sh`'s header both now print the active cache premise, so a
run's own output states unambiguously what it measured. The cache itself
is an Arrow-IPC-per-row-group sidecar read back mmap zero-copy
(`storage/ipc_cache.rs`, arrow `FileDecoder`). Guards that are
load-bearing: dictionary-coercion scans and string-filtered eager scans
keep the parquet path (decoder evaluates over dictionary values; post-load
walks 60M strings — Q19 132→332ms when that guard was missing). The
2026-08-16 BMAD round in `.claude/plans/PARITY-PLAN.md` has the full
story, including three latent engine bugs it exposed (single-shot spilled-
join build, stream-ending empty row group, capacity-counting batch
estimates).

Reproduce: `QE_IPC_CACHE=0 scripts/safe_benchmark.sh --data
./data/tpch-10gb --iterations 3` / `QE_IPC_CACHE=1 scripts/safe_benchmark.sh
--data ./data/tpch-10gb --iterations 3`; like-for-like DuckDB via
`duckdb_rebaseline.py`'s `tpch_queries()` helper over `read_parquet` views
on the same files, best-of-3 (the `duckdb_files_bench_sf100.py` pattern,
pointed at SF=10). Tight single-query numbers (e.g. Q13/Q16 above): direct
`benchmark-parquet --query N --iterations 8`, which avoids
`safe_benchmark.sh`'s per-query systemd-run/timeout wrapper overhead.

## Previous status (SF=10, 2026-08-08 night, 48G cgroup)

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
- **Unsupported types fail loudly** at registration, naming the column — and
  the check is RECURSIVE, judging a nested type by its leaves, so the rejection
  names the offending leaf (`struct field \`d\`: column type Duration(Second)`).

### Type surface: scalars are EVALUATED, nested values are CARRIED (2026-08-09)

`FixedSizeList` (vectors), `List`/`LargeList`, **`Struct` and `Map`** all read,
project, filter-around, sort-around, UNION ALL and print. None of them can be
ordered, grouped, aggregated or compared — those fail with a message naming the
column, via `crate::planner::vector_types`. Three guards close the holes that
per-expression checks cannot see:

- `is_opaque_nested` covers Struct/Map, arming GROUP BY / ORDER BY / aggregate
  args / binary operators.
- `require_scalar_row` covers **DISTINCT and UNION/INTERSECT/EXCEPT**, which are
  planned as "group by / join on EVERY output column" and so have no `Expr` to
  check. Without it `SELECT DISTINCT *` over a struct column collapsed every row
  into one group (the group-key extractor returns NULL for unknown types) before
  erroring downstream with a type but no column.
- `extend_projection_for_sort` covers `ORDER BY <nested col not in SELECT>`,
  the one shape where the sort key does not resolve against the projected schema
  so `require_scalar` silently passes. The check is on the sort key EXPRESSION,
  not its columns — `ORDER BY cosine_distance(embedding, [...])` names a vector
  but produces a float.

**Field access (`meta.source`) is NOT implemented**, but says so: it parses
identically to `table.column`, so the binder disambiguates against the schema
and reports "field access is not implemented" rather than "column not found".

Fixture: `.venv/bin/python scripts/lance_nested_gen.py` -> `data/nested.lance`.

### Filter pushdown: implemented, gated, and ALWAYS `AllLate` (2026-08-09)

`scan_with_filter` renders a narrow whitelist of `Expr` into `Scanner::filter`.
`LanceTable::plan_pushdown` requires all three of: a **nested (wide) column in
the projection**, **no pushed conjunct touching it**, and **estimated
selectivity known and <= 10%**. TPC-H has no wide columns, so it never fires
there (asserted). On `data/vectors.lance` it does: `SELECT id, category,
embedding FROM vectors WHERE id < 100` goes **33-53ms -> 2.1-5.3ms (15-25x)**.
`QE_LANCE_FILTER_PUSHDOWN=0` reproduces the A/B on the shipped binary.

**`scan_one` sets `MaterializationStyle::AllLate` on every pushed filter, and
that is not optional.** Lance's default is `Heuristic`, which on local storage
late-materializes only columns wider than 10 bytes — i.e. *nothing* in a table
of narrow scalars, so every projected column is decoded for every row and then
thrown away. Leaving it unset is what made pushdown look like a flat
pessimization. Re-measured fragment-parallel (how the engine reads), SF=10
`lineitem`, best of 3:

| shape | no filter | filter, `Heuristic` | filter, **`AllLate`** |
|---|---|---|---|
| Q06, 4 cols, 1.3% out | 192 ms | 1,112 ms | **121 ms (0.63x)** |
| Q12, 5 cols, 2.9% out | 165 ms | 1,707 ms | **134 ms (0.81x)** |
| Q19, 6 cols, 3.6% out | 266 ms | 1,120 ms | 251 ms (0.94x) |

The old "8x slower, scales with rows scanned" numbers were taken through
`Heuristic` **and** through a single scanner instead of one per fragment. Both
were wrong, and together they inverted the sign of the result.

**The gate survives the correction.** `QE_LANCE_PUSH=all` (diagnostic) pushes
every renderable conjunct: with `AllLate` on, the SF=10 suite goes **6.76s ->
10.83s**. The selective queries improve exactly as the table predicts (Q19
405->325ms, Q06 151->133, Q12 241->228) and the non-selective ones collapse
(**Q01 351->1801ms**, Q21 556->1432, Q03 294->811). Capturing the ~110ms of
TPC-H wins would require telling Q06/Q12/Q19 apart from Q01, and the gate
cannot: their selectivity lives in `l_shipmode`, `l_shipinstruct` and
`l_discount`, and `estimate_selectivity` has statistics for neither strings nor
floats. That is a 5x downside wagered on a coin flip.

Correctness rests on the planner ALWAYS re-applying the full predicate in a
`FilterExec` above the scan, so a push can only be wrong by dropping a row the
engine would have kept. Hence: unrenderable AND conjuncts are simply omitted;
unrenderable OR arms refuse the whole predicate; literals are refused unless
their family matches the column's; LIKE is off the whitelist entirely.

### Versioning and time travel (2026-08-09)

`LanceTable::try_new_at_version` / `version()` / `list_versions()`,
`ExecutionContext::register_lance_version`, `load-lance --version`,
`lance-versions`, REPL `.lance <p> <n> [version]` and `.lance-versions`.
An unknown version is an ERROR, never a silent fall back to the latest.
Registering one path twice at two versions diffs snapshots in one query.
**SQL `FOR VERSION AS OF` is NOT implemented** (binder table-factor work).

### Write path, in Rust (2026-08-09)

`src/storage/lance_write.rs`: `write_batches` (CTAS), `write_from_parquet`
(streamed, never materialized), `create_vector_index` (IVF_PQ). Each returns
the version it committed. Verified end to end: converted
`data/tpch-1mb/orders.parquet` cell-exact against the Parquet reader; built a
447-partition/48-sub-vector cosine IVF_PQ index over 200k x 384 embeddings in
**44s**, and `verify_vector_search.py --data .scratch/vecidx.lance` searched it
at distance-exact@10 = 0.800 indexed / 1.000 exact — the whole loop (write,
index, search) with no Python.

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

**...except NOT NULL, which IS free, and which cost Q09 a second (2026-08-09).**
Lance preserves Arrow's `nullable` flag, so a NOT NULL column has zero nulls as
a matter of schema. Every non-nullable column now gets `null_count = Some(0)`
with no scan at all. Restricting statistics to integer columns had left every
**float** column with no entry, and `EagerAggregation` refuses to pre-aggregate
unless each column feeding a SUM factor is provably null-free (a group of all
NULLs must sum to NULL, and a pre-aggregate would make it 0). Q09's factor is
`ps_supplycost`, a Float64: the lookup missed, the rule declined, and the Lance
plan silently diverged from the Parquet one — **2205ms vs 1180ms at SF=10**.

Nothing presented this as a statistics problem. Both plans were valid, both
answers cell-exact; the Lance leg was just mysteriously 44% slower. It took
diffing the two *optimized plans* side by side to see `__ea_key` in one and not
the other. **When a Lance query is slow for no visible reason, diff its plan
against the Parquet plan for the same query before profiling anything** —
`PLAN_DEBUG=1` on both, `diff`. Three of 22 plans still differ (Q10, Q19, Q21);
Q19's is a cosmetic conjunct reordering, and the other two currently favour
Lance.

**Related fix in shared code**: `PhysicalPlanner::prescan_shared_tables` gated
its 400MB prescan cap on `provider.parquet_files()`, so *any* non-Parquet
provider was exempt and a multi-GB shared table would be decoded into the cache
unconditionally. The gate now falls back to `statistics().total_byte_size`.
Parquet behaviour is bit-identical (same computation); MemoryTable stays ungated.

### Diagnostic switches (attribute before optimizing)

Every one of these exists because a plausible optimization had to be priced
before it was written. They are env-gated and cost nothing when unset.

| var | effect | what it answers |
|---|---|---|
| `QE_LANCE_TIMING=1` | prints each Lance scan's wall time, width, row count | is the loss even in the reader? (for Q09: no — 165ms of 2.06s) |
| `QE_LANCE_FILTER_PUSHDOWN=0` | disables Lance filter pushdown | A/B the push on the shipped binary |
| `QE_LANCE_PUSH=all` | pushes every renderable conjunct, ignoring the cost gate | calibrates the gate |
| `QE_MORSEL=0` | forces the generic aggregate path on Parquet | what is `MorselAggregateExec` worth? (35ms Q01, 28ms Q06) — the ceiling for porting it anywhere |
| `RT_DISABLE=1` | disables runtime join-filter bitmaps | what are they worth? (114ms Q19, 0 elsewhere) |
| `PLAN_DEBUG=1` | prints the optimized logical plan | **diff Lance vs Parquet before profiling** |
| `RT_DEBUG`, `HJ_TIMING`, `AGG_TIMING`, `DP_DEBUG` | join/agg/CBO internals | |

### Lance vs Parquet vs DuckDB (SF=10, 2026-08-09, serialized, same binary)

All three legs on the same idle machine. Engine legs use ONE binary built with
`--features lance`, so Lance and Parquet differ only in the storage path.

| Configuration | Load | Query total | vs engine/Parquet |
|---|---|---|---|
| Engine over **Parquet** | 0.002s | **7.39s** | 1.00x |
| Engine over **Lance** | 0.89s | **6.79s** | **0.92x — FASTER** |
| DuckDB over **Lance** (Arrow interop) | 1.16s | **10.60s** | 1.44x slower |
| DuckDB **native tables** (reference) | — | 2.99s | 0.40x |

(The Lance leg was 8.04s / 1.09x before the 2026-08-09 statistics and
materialization work; see "Which Parquet optimizations the Lance path shares".)

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

### Which Parquet optimizations the Lance path shares (2026-08-09)

Head-to-head, **interleaved A/B, 5 alternating pairs, medians**, one binary,
serialized on an idle machine. Ratio = lance/parquet, lower is better:

| Q | ratio | | Q | ratio | | Q | ratio | | Q | ratio |
|---|---|-|---|---|-|---|---|-|---|---|
| Q11 | **0.53** | | Q22 | 0.77 | | Q16 | 0.89 | | Q12 | 1.13 |
| Q02 | **0.59** | | Q17 | 0.79 | | Q08 | 0.89 | | Q18 | 1.16 |
| Q03 | 0.73 | | Q20 | 0.80 | | Q10 | 0.91 | | Q06 | 1.24 |
| Q09 | 0.74 | | Q14 | 0.80 | | Q21 | 0.94 | | Q15 | 1.68 |
| Q05 | 0.76 | | Q13 | 0.86 | | Q07 | 0.95 | | Q19 | **2.89** |
| | | | | | | Q04 | 1.01 | | Q01 | 1.05 |

**Total: Lance 6.79s vs Parquet 7.39s = 0.92x — the Lance path is now 8% FASTER
overall, and wins 15 of 22.** It was 1.06x (7.89 vs 7.45) before this round.

The single change that did it was **statistics, not the reader**: see the NOT
NULL finding above (Q09 2205 -> 1180ms). `QE_LANCE_TIMING=1` shows why the
reader was never the suspect it looked like — Q09 spends **165ms of 2.06s**
inside Lance.

#### Shared

- **Column projection pushdown** — `Scanner::project`, the format's own strength.
- **Fragment-parallel decode** — one task per fragment, the analogue of
  row-group parallelism. Effective: 4 columns x 60M rows in **79ms**.
- **Table and column statistics** — min/max/NDV by scanning integer columns,
  null counts free from `nullable`. Feeds the identical DPsize CBO,
  `EagerAggregation`, `GroupKeyReduction` and `SemiJoinPushdown`. 19 of 22
  optimized plans are now byte-identical to the Parquet ones.
- **Late materialization under a filter** — `Scanner::filter` +
  `MaterializationStyle::AllLate`, gated (see above). 15-25x on wide payloads.
- **Everything above the scan** — vectorized hash join/aggregate, spill,
  semi-join reduction, top-k fusion. These were never Parquet-specific.

#### Cannot transfer, with the mechanism

- **Row-group / zone-map statistics pruning.** Lance 0.23.2's `Fragment` carries
  `{id, files, deletion_file, row_id_meta, physical_rows}` and nothing else;
  `lance-file`'s `ColumnStatistics` is `{num_pages, size_bytes}`. There are no
  per-column min/max at *any* granularity — file, fragment or page — and no zone
  maps anywhere in the read path. This is why `compute_column_stats` scans. Not
  a wiring gap; the data does not exist in the format's public API.
- **Dictionary-level string filters and dictionary NDV.** `lance-encoding` has
  a `DictionaryPageScheduler`, but nothing surfaces a decoded dictionary to a
  reader, so there is no equivalent of reading a Parquet dictionary page to get
  exact NDV or to evaluate a predicate against dictionary codes. Direct
  consequence: string selectivity is unknown, which is what keeps filter
  pushdown gated off the TPC-H shapes it would otherwise win.
- **arrow `RowFilter` decoder pushdown.** Parquet-decoder-specific by
  construction. Its Lance analogues were all measured — and all lost. See below.
- **Runtime join-filter bitmaps.** Not a format limitation but an *architecture*
  one: `LanceTable` scans at physical-planning time, so by the time a build side
  publishes its key set the scan is over. `RT_DEBUG` says it plainly —
  `[rt] no link: probe_leaf=Filter`. Worth **114ms on Q19 and 0ms elsewhere**
  (Parquet `RT_DISABLE=1`: Q19 120->238ms, Q06/Q12/Q09 unchanged). Collecting it
  needs a lazy streaming `LanceScanExec`, and even then Lance could only skip
  the join probe (~10ms wall), not the decode — skipping decode requires
  `take`, measured below to be slower.

#### Tried, measured, REJECTED

| optimization | result | mechanism |
|---|---|---|
| **`take`-based late materialization** (Priority 1 as specified) | **rejected** | Fragment-parallel `take` of Q19's 4 payload columns at its 7.1% selectivity costs **225ms vs 211ms** to scan them outright — a loss *before* paying for the filter-column pass. Lance's sequential decode runs at 10-20 GB/s, so one more narrow column over 60M rows costs 20-45ms while a `take` of even 1% of those rows costs 50-65ms. Uniformly scattered survivors touch every page anyway. |
| **Engine-side filter inside the scan** (the RowFilter analogue that needs no format support: filter each batch as decoded, drop the `FilterExec`) | **rejected** | Implemented fully. **Q19 416->543ms, Q01 353->427ms**, Q06/Q12 flat. The premise was wrong: `MemoryTableExec` holds Arrow batches by reference, so there is no giant intermediate to avoid, and `FilterExec` already parallelizes across 32 rayon partitions while an in-scan filter inherits the *fragment* count — `part` has too few, and its predicate went 4.8ms -> 220ms. |
| **Unconditional filter pushdown** | **rejected (gate kept)** | 6.76s -> **10.83s**, even with `AllLate`. Q01 351 -> 1801ms. |
| **Morsel-driven aggregation for Lance** (Priority 2) | **rejected** | Priced first, via the new `QE_MORSEL=0`: on Parquet, morsel is worth **35ms on Q01 and 28ms on Q06** — that is the ceiling, and it is ~0.9% of the suite. Worse, it is not portable: `MorselAggregateExec` is welded to `ParallelParquetSource` for dictionary-encoded string group keys, decoder `RowFilter` and row-group pruning, all Parquet-decoder features Lance cannot supply. Rewriting the engine's most tuned operator for <0.9%, with regression risk to the Parquet leg, is not "when it makes sense". |

#### The honest remaining gaps

Measure against Parquet with its two format-only fast lanes off
(`QE_MORSEL=0 RT_DISABLE=1`) and the picture is unambiguous:

| Q | lance | parquet | parquet w/o morsel+RT |
|---|---|---|---|
| Q01 | 352 | 336 | 375 — **Lance already wins like-for-like** |
| Q18 | 612 | 527 | 637 — **Lance already wins like-for-like** |
| Q15 | 156 | 93 | 126 |
| Q06 | 131 | 106 | 132 — **Lance already wins like-for-like** |
| Q12 | 234 | 207 | 137 |
| Q19 | 409 | 141 | 238 |

So of the 7 queries Lance still loses, **four are lost purely to morsel
aggregation** (priced at ~35ms, rejected above) and the rest to the decoder
`RowFilter` skipping column materialization — Q19 (167ms) and Q12 (~100ms),
the one mechanism with no Lance analogue that measures positive. Q19 is also
the least representative query on this data: the generator's `p_brand` /
`p_container` values match nothing, so it returns zero rows, and Parquet's
runtime filter therefore eliminates its *entire* probe side. Optimizing for it
would be optimizing for a degenerate case.

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

### Six measured Lance 0.23.2 defects (all reproduce in raw pylance)

Defects 4-6 were found on 2026-08-09 while completing the format support:

4. **A top-level NULL struct does not round-trip.** pyarrow writes `meta = NULL`
   for a row; Lance reads back `{source: '', score: 0.0, active: false}` with
   `null_count == 0`. NULL *lists* and NULL struct *fields* both survive, so it
   is specifically the struct-level validity bitmap that is dropped. The engine
   renders exactly what Lance returns. Reproduce with
   `scripts/lance_nested_gen.py` (row 3 is written NULL).
5. **`Scanner::filter` costs more than decoding the columns**, and scales with
   rows SCANNED not rows RETURNED. SF=10 `lineitem`, 2 columns: 147 ms
   unfiltered vs 1,240 ms for a 1.3%-selective filter (375 ms at the best batch
   size found). 6 columns with `l_shipdate <= '1998-09-02'`: 247 ms vs 1,867 ms.
   This is why pushdown is gated rather than default.
6. **A BTREE scalar index makes filtering SLOWER.** Same query, same data:
   3,323 ms with the index, 1,531 ms with `use_scalar_index=False`, 120 ms with
   no filter at all. Backwards for an index.

The original three:

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

## Catalog Integrations: Gravitino relational + Pulsar (2026-08-22)

Epic `.claude/epics/catalog-integrations/`.

**Gravitino** (`src/metastore/gravitino.rs`): `register_all` now detects the
catalog TYPE. Fileset catalogs keep the exact gated path; RELATIONAL
catalogs list tables via `.../schemas/{s}/tables`, load each and register
`properties.location` through the Iceberg reader (local/file URIs only;
other types/providers refused by name). Hermetic tests run a canned
Gravitino over a real socket against the committed iceberg fixture.
metastore_demo.sh gate re-verified PASS. The hand-rolled HTTP client sends
`Accept: ...gravitino.v1+json, application/json` — Pulsar's Jersey answers
406 without the latter.

**Pulsar** (`src/storage/pulsar.rs`, `--features pulsar`): a namespace is
the catalog. Admin REST lists topics + serves schemas; scans are BOUNDED
SNAPSHOTS (earliest → lastMessageId fetched at scan start, batched
boundaries honored by decoding the WS message id's protobuf varints);
messages arrive over the broker's WebSocket reader API via tungstenite.
**pulsar-rs was REJECTED with evidence**: it requires chrono >=0.4.41 and
the resolver DOWNGRADED arrow to a broken 53.4.0 chasing it — tungstenite
adds exactly two lock entries. JSON + AVRO schema types (both are Avro
record schemas) map to arrow scalars with nullable unions; `__key` +
`__publish_time` metadata columns; refusals BY NAME for schemaless topics,
non-record schemas, unsupported field types, undecodable payloads;
`QE_PULSAR_MAX_MESSAGES` caps snapshots (refuse, never OOM). Pulsar tables
have no splits and no parquet_files, so scatter election and GPU offload
skip them by construction.

Wiring: `serve --pulsar-admin http://host:8085 --pulsar-namespace t/ns`,
REPL `.pulsar <admin> <t/ns>`. Infra: `scripts/pulsar_local.sh`
(standalone 4.0.5, repo JDK17, ports 6650/8085, inactive-topic GC DISABLED
— it deletes idle test topics in ~60s and ate the first gate run);
`scripts/pulsar_demo.sh` = acceptance gate (10k rows/topic through BOTH
decode paths, exact values, discovery + refusal checks): PASS.
`examples/pulsar_produce.rs` is the deterministic producer.

## Dependency Modernization — every crate to latest-or-verdict (2026-08-22)

Staged, gated upgrade of the whole dependency tree (epic
`dependency-modernization`). Final state, with every gate green (default 988 /
lance 1052 / gpu 988 / pulsar 991 tests, M1 + M2 cluster gates, forced-
distributed TPC-H sweep cell-exact vs DuckDB, pyarrow Flight interop,
SF=1 benchmark 1.33–1.35s avg — ~10% faster than pre-epic from the newer
parquet/arrow kernels):

- **Arrow cluster (atomic move)**: arrow/parquet/arrow-flight 53 → 58.4,
  tonic 0.14, chrono 0.4.45, lance 0.23 → 10.0. NOT arrow 59.2: lance 10 is
  built against arrow 58; bumping arrow alone would fork arrow in-tree.
  Re-evaluate when a lance line on arrow 59 ships.
- **sqlparser 0.52 → 0.62**: AST churn absorbed in binder + distributed AST
  rewriter (ValueWithSpan, OrderByKind, LimitClause, CaseWhen,
  ObjectNamePart, JoinOperator variants). New syntax forms the engine does
  not support (ORDER BY ALL, LIMIT ... BY, multi-alias SELECT items) are
  rejected by name, not silently misplanned.
- **Independents**: thiserror 2, itertools 0.15, ordered-float 5, statrs
  0.19, hashbrown 0.17, base64 0.23, tungstenite 0.30, rand 0.10, digest
  family 0.11/0.13, reqwest 0.13, apache-avro 0.22, criterion 0.8.
  Old majors still in Cargo.lock are transitive-only.
- **Deferred with reasons**: rustyline stays 17 (18's `with-fuzzy` pins a
  conflicting dep); libc stays 0.2 by policy (1.0 is an alpha prerelease).
- **Bugs found by the upgrade gates** (both in `hash_agg.rs` partial-state
  merge, both pre-existing): (1) variance/stddev merge applied Chan's
  centered-M2 correction to RAW Σx² states, inflating every merged variance —
  caught because the M2 gate's distributed STDDEV drifted 6.7e-6 off DuckDB;
  (2) SKEWNESS/KURTOSIS merges dropped Σx²/Σx³/Σx⁴ entirely. Both fixed:
  raw power sums now merge by plain addition.
- **Known issue (pre-existing, out of epic scope)**: the morsel-parallel
  path maps SKEWNESS/KURTOSIS to `AccumulatorState::Count` ("default for
  unsupported"), so single-process skew/kurt over parquet-scale data
  returns NULL (`morsel_agg.rs`, `_ => AccumulatorState::Count(0)`).
  Distributed skew/kurt works and matches DuckDB.

## GPU Aggregate Offload — priced, implemented, KEPT (2026-08-22)

`--features gpu` (`src/physical/gpu.rs`, epic `.claude/epics/gpu-acceleration/`).
The research question "can the GPU accelerate this engine" has a hardware-
dependent answer, and this box's RTX 5090 (32GB VRAM ~1.8TB/s, PCIe Gen5)
answers YES for exactly one regime — the one every successful GPU database
(RAPIDS, HeavyDB, Crystal) lives in: **fused aggregates over DEVICE-RESIDENT
columns**. The pricing bench (`examples/gpu_price_bench.rs`) showed warm
kernels 28-33x the 32-thread CPU while a cold PCIe upload LOSES — so the
architecture routes to the GPU only when columns are already resident; the
first query runs the CPU path unchanged and triggers background uploads.
There is no path where the GPU makes a query slower.

Measured warm (best of 5): Q6 shape **39.5x** SF=1 / **58.7x** SF=10; full
Q1 (10 aggregates, 6 groups) **17.0x** / **8.9x**. Equivalence battery vs
CPU: ALL PASS at the distributed 1e-6 float tolerance.

Load-bearing decisions (each forced by a real failure):
- Cache identity = hash of the parquet FILE LIST. Table names collide across
  contexts and raw Arc pointers suffer ABA through allocator reuse (a freed
  MemoryTable's address got recycled and served stale device data). Only
  parquet providers offload.
- Distributed/serve contexts NEVER offload (`config.gpu_offload=false` in
  fragment contexts; serve never opts in): the VRAM cache would alias shards
  with full tables, and the M1/M2 gates demand byte-exact local answers
  while GPU float reduction order differs in the last bits. Offload is
  OPT-IN per context — the single-process CLI paths (repl/sql/load-parquet/
  benchmark-parquet/query) call `ctx.enable_gpu_offload()`.
- Bare COUNT(*) refused (needs no columns => no device length source).
- Kernel: descriptor-driven (preds: conjunctive single-col compares; inputs:
  col, a*b, a*(1-b), a*(1-b)*(1+c); SUM/MIN/MAX/COUNT/AVG), per-thread
  register bins capped at 96 group*slot (Q1's 10 aggregates x 6 groups = 72;
  the cap was 48 and Q1 silently stayed on the CPU — watch for that).
- cudarc 0.19 dynamic-loading + cuda-13000; kernels NVRTC-compiled at
  runtime; **libnvrtc comes from the repo .venv's nvidia pip wheel** — gpu
  builds need `LD_LIBRARY_PATH=$PWD/.venv/lib/python3.12/site-packages/nvidia/cuda_nvrtc/lib`.
- `QE_GPU=0` kills routing at plan time even in a gpu build.
- **VRAM budget/eviction (`QE_GPU_CACHE_MB`) implemented 2026-08-26** —
  see "VRAM budget + LRU eviction" below; superseded the "not done" note
  this bullet used to carry.
- Not done (documented): GPU joins, GPU parquet decode, Lance/Iceberg
  providers, distributed-worker GPU.

**CPU vs GPU split, full SF=10 TPC-H (2026-08-23, `duckdb-parity-2`
close-out).** Ran the SF=10 sweep both with and without GPU routing, as
separate rows, `--features gpu` binary throughout (cache-off premise):

| configuration | full-suite total | note |
|---|---|---|
| default build (no `gpu` feature) | 7.03s | true CPU-only binary |
| `gpu` build, `QE_GPU=0` | 7.17s | CPU-only path inside a gpu build — within ~2% of the row above, confirms gpu-build harness overhead is negligible when routing is off |
| `gpu` build, GPU enabled, single cold pass | 7.87s | **WORSE** — expected: first touch of any column is always CPU + triggers an async upload, and a single un-repeated 22-query pass never amortizes it (Q01 706ms vs 399ms CPU, Q06 352ms vs 102ms CPU, Q15 415ms vs 168ms CPU — all cold-upload artifacts) |

Per this doc's own honest-expectations rule: most of `duckdb-parity-2`'s
target queries (Q13/Q16/Q20 — join- or DISTINCT-heavy) were NOT separately
GPU-measured, since the mechanism structurally cannot engage for them
(no joins, no DISTINCT) — forcing that comparison would be measuring
nothing. Q1/Q6/Q14/Q15 (the aggregate-eligible shapes) WERE measured warm
(6 iterations, iteration 1 discarded as cold; GPU engagement independently
CONFIRMED via `nvidia-smi`, VRAM 1066→1572 MiB during a run, not assumed):

| query | CPU steady (avg) | GPU warm (avg) | delta |
|---|---|---|---|
| Q1 | ~302ms | ~315-430ms | none (flat to slightly worse) |
| Q6 | ~93ms | ~94-100ms | **none** — essentially identical |
| Q14 | ~132ms | ~136ms | none (also structurally ineligible: JOINs `part`) |
| Q15 | ~127ms | ~132ms (1 outlier at 305ms) | inconclusive/flat |

**Correcting finding**: the "Q6 shape 39.5x/58.7x, full Q1 17.0x/8.9x"
numbers above are from `examples/gpu_price_bench.rs` — an ISOLATED KERNEL
microbenchmark over synthetic, already-VRAM-resident columns, with NO
scan/decode/plan overhead. That result is real and correctly measured at
the kernel level. But at the FULL QUERY level (`benchmark-parquet` over
real SF=10 parquet), scan+decode+filter — not the final SUM/aggregate
reduction — dominates Q1/Q6's total wall time, so an even much-faster
reduction kernel doesn't move total wall time measurably. Do not read the
kernel-level numbers as full-query TPC-H speedups; they answer a narrower,
still-useful question (is the reduction itself faster on the GPU — yes)
than "does this query run faster end-to-end" (no, at this scale, because
the reduction was never the bottleneck once scan/decode is included).
Reproduce: `LD_LIBRARY_PATH=$PWD/.venv/lib/python3.12/site-packages/nvidia/cuda_nvrtc/lib
QE_IPC_CACHE=0 [QE_GPU=0] scripts/safe_benchmark.sh --data ./data/tpch-10gb
--binary <gpu-featured binary> --iterations 3` for the full suite; direct
`benchmark-parquet --query N --iterations 6` (same env) for the warm
per-query numbers.

**CPU/GPU split on NATIVE TABLES (2026-08-23, native-tables-foundation
task 008): the parquet "no full-query win" verdict above does NOT
transfer — Q6's shape shows a real, reproducible, order-of-magnitude
win.** Task 007 generalized GPU eligibility to native tables
(`TableProvider::identity()`) but left a live end-to-end query
unexercised; task 008 ran one. Methodology note first: `serve` (and
therefore the HTTP-driven benchmark harness `native_bench_compare.py`
uses) is a "distributed context," where GPU offload is intentionally
never enabled (same reasoning as always) — so measuring this needed a
single-process path, and `load-native --query` turned out to be the
wrong tool too (it materializes through `native_write::read_back` into a
plain `MemoryTable`, which has no `identity()` override and could never
pass the eligibility gate regardless of `enable_gpu_offload()`). Neither
gap was fixed (both predate this task, out of scope); `examples/
native_gpu_check.rs` (new, permanent) calls `register_native_table` (the
real provider) + `enable_gpu_offload()` directly instead. VRAM growth
confirmed independently (`nvidia-smi` sampled every 0.3s, RTX 5090):
**1048 → 3858 MiB** the moment the run touches `lineitem`'s columns —
offload genuinely engages for a real `NativeTable`, not assumed:

| query | CPU steady (avg) | GPU cold (iter 1) | GPU warm (iters 2-6 avg) | verdict |
|---|---|---|---|---|
| Q6 (single SUM, no GROUP BY) | ~140ms | ~2.0-2.2s | **~7-8ms** | **~18-20x faster end to end** |
| Q1 (10 aggs, GROUP BY x2) | ~597ms | ~0.8-1.1s | ~505-623ms | flat (matches parquet's own Q1 result) |

Reproduced twice, Q6's warm number consistent both times (7-8ms). The
mechanism is the SAME kernel as the parquet path; what changed is the
denominator — a native table has NO decode step (mmap-resident Arrow,
not parquet row-group/dictionary decode), so for a shape simple enough
that the reduction kernel is a meaningful share of total wall time (Q6:
one ungrouped SUM), the kernel's own speedup finally shows up end to end
instead of being hidden behind scan/decode cost. Q1's multi-aggregate/
GROUP BY shape stays flat, same as it did over parquet — that shape's
bottleneck is something other than scan/decode or the reduction kernel
(not investigated further). Reproduce: `LD_LIBRARY_PATH=$PWD/.venv/
lib/python3.12/site-packages/nvidia/cuda_nvrtc/lib NATIVE_DIR=data/
tpch-10gb-native/lineitem cargo run --release --features gpu --example
native_gpu_check` (add `QE_GPU=0` for the CPU leg).

### VRAM budget + LRU eviction + the mutation-driven leak, FIXED (native-tables-tiering epic, task 001, 2026-08-26)

The gap named above ("Not done: eviction/QE_GPU_CACHE_MB cap") is
closed. `GpuEngine`'s resident-column cache (`src/physical/gpu.rs`) was
insert-only with zero VRAM byte accounting — and because a native
table's `identity()` is `table_id ++ version` (`native_table.rs`),
every INSERT/DELETE/UPDATE against a GPU-queried native table produced
a new cache key, permanently leaking the old version's columns. Both
are now fixed by ONE mechanism, entirely inside the worker thread's
existing single-consumer loop (no new concurrency surface): every
resident column/group-codes buffer is byte-accounted (`GpuCache`,
`ColumnEntry`/`CodesEntry`) and tagged with an LRU tick; before each
upload, `GpuCache::reserve` evicts the globally least-recently-used
entries until the upload fits under **`QE_GPU_CACHE_MB`** (now actually
implemented — default 24576 MiB, re-read from the environment on every
check rather than cached at startup). No native-table-specific code
exists in the fix: a superseded version's columns are never touched
again by any future query, so under plain LRU they are always the
coldest entries and are evicted first — the mutation leak closes as a
direct consequence of the general eviction policy, not a special case.

**Leak confirmed empirically first** (`examples/
gpu_cache_tiering_check.rs`, 2,000,000-row native table, 15 mutation
cycles, real `nvidia-smi` sampling): pre-fix, VRAM grew **1864 → 2088
MiB (+224 MiB)** while the table's own row count oscillated within
0.05% of constant. **Post-fix, same repro with `QE_GPU_CACHE_MB=24`**:
VRAM stayed **perfectly flat at 1767 MiB across all 16 table
versions**, with the query's answer independently verified cell-exact
every single cycle (correctness and the leak fix demonstrated
together, in the same run). A dedicated hardware-backed test
(`tests/gpu_cache_tests.rs`) confirms real eviction under an 8 MiB
budget (`eviction_count=13`, `resident_bytes` bounded at ~7.03 MiB vs.
~16 MB unbounded) and that a column evicted then re-requested
transparently re-uploads and answers cell-exact — the exact same
"not yet resident" path a never-uploaded column already took, no new
re-upload logic. A necessary related fix: the pre-existing `queued`
upload-dedup `HashSet` was never cleared, which — once eviction
existed — would have silently and permanently blocked re-upload of any
evicted column forever; fixed alongside (`GpuEngine::unmark_queued`).

**No regression to the Q6-shape win, measured same-session before/after**
(`git stash` isolated this task's own diff, rebuilt, ran
`native_gpu_check` on both sides of the SAME session against SF=10
`lineitem`): Q6 warm 5.405ms → 5.479ms (+1.4%), Q1 warm 482.5ms →
501.8ms (+4.0%) — both within this program's normal run-to-run noise
band, confirming the budget/eviction bookkeeping adds no measurable
cost to the already-resident (no-eviction-pressure) warm path. A fresh
CPU baseline taken in the same session (~87ms Q6) puts today's
GPU-vs-CPU ratio at ~16x — below the "~18-20x" figure recorded above,
but that shift traces to the CPU baseline itself moving (140ms → 87ms
across sessions, on this shared, multi-agent development machine, in a
code path this task's diff never touches), not to anything this task
changed; the before/after-fix GPU numbers agree with each other far
more tightly than either does with the older session's CPU number.

Full task detail, every command, and the complete Outcome section:
`.claude/epics/native-tables-tiering/001.md`.

### Per-column failure isolation + observability (native-tables-tiering epic, task 002, 2026-08-26)

The other pre-existing gap named above ("one failed upload permanently
disables GPU offload for the rest of the process") is closed.
`GpuEngine::mark_unhealthy()` — a single process-wide `healthy:
AtomicBool`, set `false` on any upload failure and never reset, gating
BOTH `request()` (stopped queuing new uploads at all) and `ready()`
(stopped serving even already-resident columns from the GPU) — is
DELETED, not replaced with a new global mechanism. Per-column isolation
was already structurally present in task 001's `resident`/`codes`/
`queued` maps (all keyed per `(pid, column)`/`codes_key`); `healthy` was
the only thing overriding it. **Design decision: a failed upload is
RETRIED on a later query, never permanently blacklisted** — matches the
pre-existing "not cacheable" (`Ok(None)`) handling exactly (never
blacklisted either, `unmark_queued` from task 001 already clears the
dedup key unconditionally), and avoids fighting task 001's own eviction
mechanism (a permanent blacklist would keep a column CPU-only forever
even after eviction frees the exact room a transient VRAM-pressure
failure needed).

**Validated with two independent real-hardware failure constructions**
(`tests/gpu_failure_isolation_tests.rs`, one hardware-backed
`#[tokio::test]`): (1) a deterministic Int64-column-out-of-2^52-range
"not cacheable" refusal — zero risk to the shared GPU, confirms
per-column isolation; (2) a GENUINELY INDUCED real VRAM-exhaustion
failure — a second handle onto the SAME primary CUDA context (`cudarc::
driver::CudaContext::new` retains the device's primary context) ate
real VRAM from 31.6GB down to 271MB free (confirmed via `mem_get_info()`
before/after, not assumed), forcing a genuine `stream.memcpy_stod`
failure for a 480MB victim column upload — the EXACT call site
`mark_unhealthy()` used to gate on. That query correctly fell back to
CPU with a numerically correct answer; critically, a DIFFERENT,
unrelated table's column immediately uploaded and became GPU-resident
right after (the key proof the process isn't poisoned), and the
ORIGINAL victim column also eventually became resident once retried
after VRAM pressure cleared (real evidence for the retry design, not
just isolation). Honest finding along the way: `mem_get_info()` read
IDENTICALLY before and after releasing the ~31GB eater buffer,
consistent with CUDA's stream-ordered memory-pool allocator retaining
freed pages rather than returning them to the driver-global free count
immediately — this did not prevent the pool from correctly servicing
later real allocations, confirming the test's pass/fail signal correctly
rests on real allocation attempts through `GpuEngine`, not a raw
post-release byte count. Task 001's own hardware-backed eviction test
(`tests/gpu_cache_tests.rs`) re-confirmed passing, unmodified.

**Observability**: two new `AtomicU64` counters — `upload_failures`
(every upload/build-codes attempt that did not result in a resident
entry, for any reason) and `run_fallbacks` (a fully-`ready()` plan whose
device `run()` itself still failed) — plus `GpuEngine::snapshot() ->
GpuCacheSnapshot` (bundles resident columns, VRAM used, budget, eviction
count, both new counters into one `Display`-able struct) and a new
**`QE_GPU_DEBUG`** env var (confirmed absent from the codebase before
adding it — checked, not assumed), matching `QE_SPILL_DEBUG`'s exact
established convention (checked fresh via `std::env::var(...).is_ok()`
on every relevant call, no `OnceLock` caching): traces every upload/
build-codes/run outcome plus a live snapshot to stderr, prefixed
`[gpu-trace]`. Confirmed working on real hardware via `examples/
gpu_cache_tiering_check.rs`.

Full suite green, all four feature combinations (default 1252, lance
1317, gpu 1261 = task 001's 1259 baseline + 2 new tests, pulsar 1255,
zero failures anywhere); `cargo fmt --all -- --check` clean. Sole file
changed: `src/physical/gpu.rs` (373 insertions, 24 deletions) plus one
new test file. Full task detail and the complete Outcome section:
`.claude/epics/native-tables-tiering/002.md`.

## Expression Compilation — researched, priced, narrowly adopted (2026-08-22)

The "modern engines compile queries" question, answered with measurements
(`examples/expr_compile_bench.rs`, epic `.claude/epics/expression-compilation/`):

- **Full JIT (LLVM/Cranelift) REFUTED for this engine.** Arrow's SIMD kernels
  already beat a hand-fused loop on standalone arithmetic (0.544 vs 0.638ms /
  524k rows). The compilation camp's real wins are pipeline FUSION, and the
  memory-bound side of this engine (decode, MLP-bound probes) is where
  Kersten et al. (VLDB'18) show compilation does not pay. Photon and DuckDB
  reached the same conclusion for the same architecture.
- **What survived measurement**: temporaries in predicate masks (a Q6-shaped
  predicate = 5 kernel passes + 5 intermediates, 4.4x worse than fused).
  `src/physical/compiled_expr.rs` compiles in-subset predicates ONCE per
  operator into a flat register program over 1024-row L1 slabs: 2.6x the
  interpreter on the mask microbench (80% of the hand-fused ceiling).
  Subset: numeric cols, f64 arithmetic, same-type comparisons, strict
  AND/OR/NOT, BETWEEN; null-strict validity = AND of leaf validities
  (bit-identical to kernels — equivalence tests). Everything else keeps the
  interpreter; `QE_COMPILE=0` kills it. Wired into FilterExec + both parquet
  RowFilter sites.
- **Honest suite-level result: ~0% at SF=1** (1.53 vs 1.50s avg, inside this
  box's noise); best-of-5 per query: Q6 -7%, Q19 -4.4%, Q12 -1.4% — real but
  small, because TPC-H predicates ride the decoder RowFilter where DECODE
  dominates. Kept because it is never slower, wins where predicates actually
  bite (post-join filters, gathered distributed tables, memory tables), and
  costs zero dependencies. Getting the register machine from 2.2x SLOWER
  than the interpreter to 2.6x faster took three measured iterations:
  hoist per-row operand dispatch, monomorphize comparison loops (enum match
  per element blocks vectorization), pack mask bits per chunk
  (append_packed_range) instead of per-bit appends.

## Window Functions & Standard-SQL Completion (2026-08-21)

The standard-sql-completion epic (`.claude/epics/standard-sql-completion/`)
closed the gaps a 59-probe battery found (`scripts/sql_feature_probe.py`,
55/59 green; the 4 remaining are RECURSIVE CTE, BETWEEN SYMMETRIC
(sqlparser 0.52 cannot parse it), NATURAL JOIN, LATERAL — all refused, not
mis-answered).

**Window functions** — all of: ROW_NUMBER, RANK, DENSE_RANK, PERCENT_RANK,
CUME_DIST, NTILE(n), LAG/LEAD(x[,off[,default]]), FIRST/LAST/NTH_VALUE, and
COUNT/SUM/AVG/MIN/MAX over windows. PARTITION BY, multi-key ORDER BY
(default NULLS LAST for ASC, Postgres-style), ROWS frames exact, RANGE
frames for UNBOUNDED/CURRENT bounds plus numeric offsets over ONE
numeric/date key, named `WINDOW w AS (...)` clauses, windows inside scalar
expressions, several windows per SELECT.

- Architecture: binder extracts window exprs post-HAVING into a
  `Window` logical node (`__wN` output columns) below the final projection
  (`binder.rs::bind_window_function` / `extract_windows`);
  `WindowExec` (`src/physical/operators/window.rs`) sorts a permutation per
  window spec, walks partitions/peer groups (arrow partition kernel),
  resolves frames per row, scatters results back to input order.
  COUNT/SUM/AVG are O(1)-per-row via prefix sums; MIN/MAX recompute frames.
- Refused BY NAME (never silently wrong): STDDEV etc. OVER, GROUPS frames,
  EXCLUDE, IGNORE NULLS, non-literal frame offsets/NTILE/LAG args, RANGE
  offsets over unsupported key types, windows outside the SELECT list.
- Optimizer: predicate pushdown stops at Window (barrier); NO column
  pruning beneath it (its bind-time schema carries every input column —
  lifting this is a perf follow-up); scatter planning rejects windows by
  name, so distributed execution uses the GATHER path (verified on a
  3-node cluster via HTTP + Flight, `window_gather` in flight_validate.py).
- Perf status: single-threaded, input fully materialized (same class as
  SortExec); morsel/spill treatment is a future epic.

**Grouping extensions** — GROUP BY GROUPING SETS/ROLLUP/CUBE desugar in the
binder to UNION ALL of plain aggregates (typed-NULL padding, GROUPING()
bitmask constants per branch). Refused by name: mixing with plain GROUP BY
items, HAVING, wildcard/complex projections.

**Expression forms** — IS [NOT] DISTINCT FROM (CASE desugar); ANY/SOME/ALL
(= ANY -> IN, <> ALL -> NOT IN, ordering ops -> MIN/MAX scalar subquery
with emptiness guard; NULL-element corner semantics follow MIN/MAX skip
behavior); OVERLAY (SUBSTRING/CONCAT desugar); `expr ± INTERVAL 'n' unit`
-> DATE_ADD (calendar-correct months); GROUP BY <ordinal> resolves into the
SELECT list.

Gates: `scripts/window_validate.py` (63 DuckDB-compared cases; QE_BINARY
env selects the binary) and `tests/window_functions.rs` (9 hermetic
semantics tests). Both must stay green.

## Native Tables (native-tables-foundation epic, phase 1 of 4, 2026-08-23)

A first-class, writable, independently-persistent table format — phase 1
("foundation") of the four-phase `native-tables` PRD
(`.claude/prds/native-tables.md`; phases 2-4 are mutation, GPU/RAM/disk
tiering, and materialized rollups — as of this writing, phase 2
(mutation) and phase 4 (rollups) are BOTH fully shipped and archived
(matching/substitution mechanism, SQL DDL surface, and automatic
refresh-on-write, plus a QA close-out that broadened validation and
found/fixed one general, non-rollup-specific SQL binder bug — see
"Materialized rollups" below); phase 3 (tiering) is the only phase not
built. Generalizes the
engine's existing Arrow-IPC sidecar cache (`src/storage/ipc_cache.rs`,
already measured faster than DuckDB's own native storage at SF=100) from
an opportunistic read-through cache tied to shadowing a specific parquet
file into a table type a user can `CREATE`/bulk-load/query independently.
Epic tasks 001-008, `.claude/epics/archived/native-tables-foundation/`.

**Modules** (three new sibling files under `src/storage/`, mirroring the
existing `lance.rs`/`lance_write.rs` split): `native_manifest.rs` (task
002 — manifest format, identity/versioning, zone-map statistics),
`native_write.rs` (task 003 — the write path), `native_table.rs` (task
004 — the `TableProvider` implementation). `ipc_cache.rs` itself is
UNCHANGED — its `read_row_group`/`sidecar_dict_cols` functions were
already parameterized purely by directory + segment index, so native
tables call them directly with zero refactor.

### Format

One `_manifest.json` per table directory (JSON, not Arrow IPC/Parquet or
Iceberg's Avro format — chosen because the manifest is metadata, KBs to
low MBs even for a large table, read once at open time, never on the
per-row hot path, so JSON's zero-copy cost never materializes; see task
001's Outcome for the full tradeoff analysis): a stable `table_id` (UUID,
generated once at creation, independent of any source file's path/mtime/
size — the decoupled-identity model this epic exists to build), the
Arrow schema, a `snapshot { version, row_count, created_at_ms }` marker
bumped on every full-table replace, and a `segments[]` list each naming
one Arrow IPC file (`rg_{id:05}.arrow` — NOT a free choice; `ipc_cache`'s
reader hard-codes this name) with its own row count/byte size/per-column
min-max-null-count statistics, plus a table-level rollup of the same
shape computed once at write time (O(1) for `TableProvider::
statistics()`). Segments themselves are unchanged from the existing IPC
sidecar: mmap zero-copy via `Buffer::from_custom_allocation`, low-
cardinality Utf8 columns dictionary-coerced — decided ONCE from the first
segment written and applied uniformly to every later segment (a
deliberate difference from the parquet-shadowing sidecar, which
re-decides per row group; forced by the manifest declaring one Arrow type
per column for the whole table).

### Write path

`write_batches`/`write_from_parquet`/`write_from_iceberg`/`write_from_lance`
(`--features lance`), modeled on `lance_write.rs`: streams a
`RecordBatchStream` straight through, batch by batch, computing each
segment's statistics from the already-buffered batch — never a second
pass, never materializing the source. **Measured bounded memory,
independent of source scale**: converting SF=10 `lineitem` (60,000,000
rows, 2.8GB compressed parquet) peaked at ~406MB RSS in 12.3s (58
segments, 5.3GB on disk); the full SF=10 warehouse (8 tables) writes in
23.5s to 6.5GB total (SMALLER than the 9.6GB parquet source — dictionary
coercion wins); the full SF=100 warehouse (600,000,000-row `lineitem`
alone) writes in 209.6s to 65GB (again smaller than the 97GB parquet
source). `Create` mode refuses an existing destination; `Overwrite`
refuses a non-empty, non-native destination (protects against a wrong
`--out` path) — every write stages into a temp directory and publishes
atomically, so a failure mid-write leaves the destination untouched.

### SQL DDL: `CREATE TABLE <name> AS SELECT ...`

Landed as a NEW `&mut self` method, `ExecutionContext::
create_table_as_select` — deliberately NOT a change to `sql()` (which
takes `&self`, cannot register a table, and fully materializes results
before returning). `sql()` explicitly refuses a `CREATE TABLE` statement
with a message pointing at `create_table_as_select`, so a misdirected
call fails loudly rather than silently running only the inner `SELECT`
and writing/registering nothing. Reachable from: the REPL (dispatches
automatically) and directly via `ExecutionContext`. **NOT reachable from
`sql()`, the distributed HTTP `/sql` endpoint, or the Arrow Flight
endpoint** — wiring those is unclaimed follow-up work, not attempted this
epic. The query itself can be anything `bind_query()` already handles
(joins, aggregates, subqueries, CTEs, window functions) — `sqlparser`
0.62 already parses the full `CreateTable{ query: Some(Box<Query>) }`
shape; a columns-only `CREATE TABLE t (a INT)` (no `AS SELECT`, `query:
None`) and all other `CreateTable` clauses (`IF NOT EXISTS`, `TEMPORARY`,
partitioning, etc.) are refused BY NAME, not silently ignored.

### Registration and querying

`ExecutionContext::register_native_table(name, path)`; `serve --tables
<dir>` auto-detects a native-table subdirectory (`_manifest.json`
present) exactly like it does Iceberg/Lance directories, structurally
disjoint from both so detection order doesn't matter. Once registered, a
native table is queried through the SAME generic physical-planner path
every non-Parquet provider uses (no special-cased scan operator) — joins,
filters, aggregates, GROUP BY all apply unchanged, and `TableProvider::
statistics()` (never `None` — a direct O(1) copy of the manifest's
rollup) feeds the same cost-based join reordering and `disjoint_group_hint`
machinery every other table type does.

### Dense-direct-address fast path (task 005)

`try_execute_dense_direct` (`morsel_agg.rs` — the engine's fastest
aggregation tier) previously read its key-range bounds from PARQUET FILE
FOOTERS specifically, which would have made it silently unreachable for
native tables (a real regression vs. parquet, not just a missed
optimization). Generalized: `MorselAggregateExec` gained a
`native_provider: Option<Arc<dyn TableProvider>>` field alongside the
existing parquet `files: Vec<PathBuf>` field (the parquet code path is
byte-for-byte unchanged), and a new planner extractor
(`try_extract_native_dense_source`) routes eligible native-table scans
into it, re-validating eligibility with the SAME shared functions the
executor itself uses (so a planner/executor mismatch is structurally
unreachable, not just untested). **Measured, not assumed**: a Q18-shaped
`GROUP BY l_orderkey` over a native table (SF=1, 1,498,929 groups) took
**6.1ms** for the dense-direct scan+accumulate step vs the parquet
source's **41.7ms** in the same run (`AGG_TIMING=1`, `examples/
native_dense_direct_check.rs`) — both legs agree on group count exactly.
Eligibility is identical to parquet's (single unfiltered integer/date
GROUP BY column, plain COUNT/SUM/AVG, key range ≤64,000,000) minus filter
support: `NativeTable::scan_with_filter` has no pushdown at all (see
Limitations below), so a filtered GROUP BY correctly falls through to the
generic aggregate tier rather than silently aggregating unfiltered rows.

### Memory safety

**Write path**: safe by construction (streaming, bounded ~400MB
regardless of source scale — measured above, not just designed for).
**Read path**: `NativeTable::scan()` is NOT incremental — it materializes
every active segment into one `Vec<RecordBatch>` (the same
`MemoryTable`-shaped gap `LanceTable::scan()` also has; a genuinely
streaming native-table scan comparable to `ParallelParquetSource` remains
open follow-up work, materially larger than this epic's scope). Task 006
added a hard admission-control check instead:
`register_native_table` computes `memory_limit * spill_threshold` (the
SAME formula `spillable.rs` already applies at 7 call sites) and
`NativeTable::scan()` refuses BEFORE touching a single segment if the
active segment set's on-disk size exceeds it, naming the exact byte
counts. **Verified end to end, not just unit-tested**: the identical
60,000,000-row full-table query that SIGKILL'd (exit 137, ~1.6GB peak
RSS) under a bare 1GiB cgroup cap before this fix now returns a clean
HTTP 400 in ~20ms citing the exact budget; raising `--memory-limit`
appropriately lets the same query run correctly (588ms, matching every
other run's values). **Consequence for anyone benchmarking native tables
at scale**: size `--memory-limit` generously and explicitly — unlike
Parquet's streaming path, which needs no such cap, a native-table query
against SF=10/SF=100-scale data WILL refuse cleanly under the engine's
1GiB default. This doc's own benchmark commands below always pass one.

### GPU-offload eligibility (task 007)

`TableProvider::identity() -> Option<Vec<u8>>` (new trait method,
default `None`) is the single mechanism `GpuAggPlan::pid()`'s cache key
and `plan_gpu_agg`'s eligibility gate both now use, replacing an inlined
`parquet_files()`-only hash. A provider opts in by overriding either
`parquet_files()` (the common case — Parquet/Iceberg tables get a
correct `identity()` for free from the trait's own default) or
`identity()` directly (native tables: `table_id` bytes ++ little-endian
`version` bytes — stable across repeated queries against the same loaded
table, changes on every full-table replace, exactly the cache-
invalidation behavior the mechanism needs). A SHARDED native-table
provider (`only_segments: Some(_)`) returns `None`, mirroring
`ShardedParquetTable`'s own reasoning — a GPU cache keyed on the whole
table's identity must never alias a worker's partial shard. Distributed/
`serve` contexts still never enable GPU offload at all
(`ExecutionConfig::gpu_offload` defaults `false`, forced `false` again in
fragment contexts), independent of and unaffected by this change.

### Mutation: INSERT (native-tables-mutation epic, phase 2, task 002, 2026-08-23)

`INSERT INTO <native table> SELECT ...` / `INSERT INTO <native table>
VALUES (...)` — the first mutation capability, extending a native table
incrementally rather than only full-replacing it. Task 001's design spike
(`.claude/epics/native-tables-mutation/001.md`'s Outcome) decided the
mechanism ahead of implementation: reusing phase 1's `publish_table_dir`
(whole-directory `remove_dir_all` + `rename`) unchanged would delete every
pre-existing segment on an incremental write, so `Append` uses a
DIFFERENT, single-FILE atomic-publish model instead.

- **`NativeWriteMode::Append`** (new variant, `src/storage/
  native_write.rs`) writes new segment(s) DIRECTLY into the LIVE table
  directory (never a staging directory) under fresh, non-colliding
  segment ids continuing from the existing maximum (never restarting at
  0), casts every batch to the target's ALREADY-DECLARED schema and
  dictionary encoding (inherited, never rediscovered from the new data's
  own cardinality — a schema/type mismatch is a clean, named
  `QueryError::Type`, never silent coercion), then publishes ONE new
  manifest via `native_manifest::write_manifest_atomic`'s single-file
  `rename()` (a NEW sibling primitive to `publish_table_dir`, additive,
  zero changes to Create/Overwrite's own directory-level publish).
  `NativeManifest::build` is reused COMPLETELY UNCHANGED to roll up
  `row_count`/`table_stats` from the full (old + new) segment list — no
  separate merge function needed. A zero-row source is a legitimate
  no-op (no version bump, manifest untouched), unlike Create/Overwrite's
  own zero-row refusal.
- **Single-writer enforcement**: `std::fs::File::try_lock()` (stable std
  since Rust 1.89, zero new dependency) on a sibling `<table>.lock` file,
  held for the whole read-modify-write-publish span via an RAII guard
  (`native_write::TableWriteLock`). A concurrent writer gets an
  immediate, named `QueryError::Storage` — never blocks, never corrupts.
- **Reusable, non-publishing building blocks** (task 001's Decision 2,
  for task 003/004's DELETE/UPDATE to compose into their own single
  atomic publish rather than two sequential self-publishing calls):
  `native_write::write_append_segments` (writes segments, returns
  `Vec<Segment>`, touches no manifest) and `native_write::
  publish_manifest_update` (given a caller-assembled full `Vec<Segment>`,
  builds + atomically publishes one manifest, does no locking itself).
  `native_write::append_to_native_table` composes lock + both blocks into
  the self-publishing entrypoint.
- **SQL surface**: `Binder::bind()` gained a `Statement::Insert` arm
  (`src/planner/binder.rs`, mirrors the `CreateTable` arm's shape exactly
  — binds and returns only the source query's `LogicalPlan`, no new DML
  node) plus `require_supported_insert_shape`, mechanically checked
  against sqlparser 0.62's real 26-field `ast::Insert` struct (explicit
  column lists, Hive `INSERT OVERWRITE`, `ON CONFLICT`/`ON DUPLICATE KEY
  UPDATE`, MySQL `INSERT...SET`, multi-table `INSERT ALL/FIRST`, and more
  are all refused by name). `INSERT ... VALUES` is SUPPORTED (not
  refused) — task 001 confirmed it binds through the identical
  `bind_query()` path with zero extra binder work.
- **`ExecutionContext::insert_into_native_table`** (`&mut self`, mirrors
  `create_table_as_select`'s shape): the target must already be a
  REGISTERED native table (no auto-discovery — matches every other
  table's "register before you reference it" contract); streams the
  source query's `RecordBatchStream` directly into `append_to_native_table`
  (never through `sql()`'s materializing path — `sql()` itself now
  refuses a bare `INSERT` with a pointer to this method, mirroring its
  pre-existing CREATE TABLE guard). Re-registers the table afterward so
  its new rows are immediately queryable in the same session. Wired into
  the REPL alongside `CREATE TABLE`'s existing dispatch.
- **A real, pre-existing bug found and fixed while wiring `INSERT ...
  VALUES` end to end**: `LogicalPlan::Values`'s physical planning
  (`src/physical/planner.rs`) was an unimplemented stub that always
  returned an EMPTY batch regardless of the actual literal rows — no
  existing test anywhere in the suite exercised literal `VALUES` SQL
  text, so this was silently dead code until this task's own integration
  test caught it. Fixed by evaluating each row's expressions via the
  existing `evaluate_expr` against a 1-row/0-column dummy batch — the
  SAME trick `LogicalPlan::EmptyRelation` already uses one arm above for
  a table-less `SELECT <literal>` — then concatenating per column. Fixes
  `VALUES (...)` everywhere in the engine, not just for INSERT.
- **Cell-exact validated** (`tests/native_insert_tests.rs`): inserting
  into a table with existing CTAS data reassembles byte-identically to
  an independently-computed reference (the same combined query against
  the original, never-split parquet source); a `SELECT *`-wildcard
  source (table-qualified field names) still lands positionally correct;
  an empty result set is a no-op, not an error; a column-count schema
  mismatch (two real, differently-shaped TPC-H fixtures) is a clean
  error that leaves the table completely untouched; `statistics()`/
  `distributed_splits()` correctly widen/add a split after an INSERT.
- **Memory**: verified, not assumed, the same way phase 1 task 003 did —
  measured two DIFFERENT things separately (`examples/
  native_append_memory_check.rs`, `QE_MEM_CHECK_MODE=direct|sql`),
  reported as separate premises per this doc's own standing convention:
  - **`direct`** (apples-to-apples with task 003's own methodology:
    `native_write::append_to_native_table` called directly, fed by the
    SAME `StreamingParquetReader` construction `write_from_parquet`
    uses, bypassing the SQL engine entirely) — appending SF=10
    `lineitem` (60,000,000 rows, 2.8GB compressed) to an existing native
    table peaked at **328MB** (`/usr/bin/time -v`, kernel `VmHWM`),
    **better than** task 003's own 406MB CREATE-mode baseline for the
    identical row count. This is the direct, load-bearing confirmation
    of this task's own acceptance criterion: Append's write core is
    genuinely bounded, not assumed to inherit Create/Overwrite's
    discipline just because it reuses the same buffered-flush shape.
  - **`sql`** (the full, realistic `INSERT INTO t SELECT * FROM
    lineitem_src` statement via `ExecutionContext::
    insert_into_native_table`) — peaked at **~5.3GB**, MUCH higher.
    Root-caused, not just observed: `StreamingParquetScanExec::
    output_partitions()` returns one partition per row-group work item
    (tens of partitions for a 60M-row file), and `insert_into_native_table`
    (like `create_table_as_select` before it — confirmed by reading both,
    IDENTICAL structure) drives every partition's stream via
    `physical.execute(partition_id)` and merges them ALL with
    `futures::stream::select_all` before the single-threaded Append
    writer drains them — so many partitions' worth of decoded parquet
    data can be "in flight" concurrently with no backpressure. This is a
    PRE-EXISTING, CTAS-shared characteristic (confirmed identical code
    shape in `create_table_as_select`, not introduced by this task's own
    Append write core) — named honestly as a residual risk for a future
    task (bound concurrently-polled partitions, or give the merge real
    backpressure), not fixed here: closing it would mean changing the
    engine's generic multi-partition scan/stream-merge pattern, shared
    by CTAS, well outside this task's own charter.

### Mutation: DELETE (native-tables-mutation epic, phase 2, task 003, 2026-08-23)

`DELETE FROM <native table> [WHERE ...]` — the deletion-vector mechanism
task 001's design spike decided (Outcome, Decision 1) plus the SQL surface
that exercises it. Unlike CTAS/INSERT, this is NOT a thin wrapper around
`bind_query()`/the generic `LogicalPlan`/`PhysicalOperator` pipeline —
that pipeline has no way to carry a matched row's (segment id, local
position) back out (`TableProvider::scan()` returns only
`Vec<RecordBatch>`), so DELETE's row-identification is a genuinely new,
bespoke loop.

- **`Segment::deleted_rows: Vec<u32>`** (new field, `src/storage/
  native_manifest.rs`, `#[serde(default)]`): sorted, deduplicated LOCAL
  row positions within that segment's own on-disk row order that a
  DELETE has tombstoned. Every manifest phase 1 or task 002 ever wrote
  deserializes with an empty `Vec` here — zero behavior or performance
  change for a never-deleted-from table. `Segment.row_count`/
  `Snapshot.row_count` are DELIBERATELY unaffected by `deleted_rows` —
  they keep meaning the PHYSICAL (write-time) count, so
  `NativeManifest::validate`'s existing `row_count == sum(segments[].
  row_count)` invariant holds with zero changes and `NativeManifest::
  build` stays callable completely unchanged after a DELETE. A NEW method,
  `Segment::live_row_count()` (`row_count - deleted_rows.len()`), is the
  LOGICAL (post-delete, visible) count; `NativeManifest::validate` also
  now bounds-checks `deleted_rows` (sorted, strictly increasing, every
  entry `< row_count`).
- **`src/storage/native_delete.rs`** (new sibling module to
  `native_write.rs`/`native_manifest.rs`/`native_table.rs`): owns
  row-identification and deletion-vector editing. Two reusable,
  non-publishing building blocks (task 001's Decision 2, for task 004's
  UPDATE to compose into ONE atomic publish rather than two sequential
  self-publishing calls):
  - `identify_matching_rows(dir, target, predicate: Option<&Expr>,
    materialize_rows: bool) -> Result<MatchedRows>` — opens the target's
    CURRENT manifest's segments directly (in id order), reads each via
    `ipc_cache::read_row_group` (the SAME mmap-backed reader `NativeTable
    ::scan` uses), evaluates the predicate batch-by-batch via
    `physical::operators::evaluate_expr` (the SAME function `FilterExec`
    itself uses), and tracks a running local-row offset across a
    segment's (possibly many) batches to convert matches into
    segment-relative positions. `predicate: None` matches every row
    (`DELETE FROM t` with no WHERE) without ever building an all-true
    mask. `materialize_rows` controls whether matched rows' ACTUAL COLUMN
    DATA is also gathered (`MatchedRows::per_segment[].rows: Option<
    RecordBatch>`) — DELETE itself always passes `false` (it only needs
    WHICH rows, never their values, so a broad `DELETE FROM t` doesn't
    pay to materialize row data it will never use); task 004 (UPDATE) is
    expected to pass `true` to get matched rows' CURRENT values for
    evaluating its SET expressions against, reusing this exact function
    rather than re-deriving an equivalent scan.
  - `apply_deletions(target, matches) -> Vec<Segment>` — unions each
    segment's newly matched positions into its EXISTING `deleted_rows` via
    a `BTreeSet` (insert + re-serialize sorted) — idempotent BY
    CONSTRUCTION, so re-matching an already-deleted row via a second,
    overlapping DELETE is naturally a no-op with no special-casing. Drops
    a `Segment` entirely once `deleted_rows.len() >= row_count` (every row
    tombstoned) — task 001's Decision 3's narrow, in-scope compaction
    exception; NOT full compaction (deferred to a future epic).
  - `delete_from_native_table(table_dir, predicate) ->
    Result<NativeDeleteResult>` — the self-publishing entrypoint:
    `native_write::lock_table_for_write` (held for the whole span) →
    `native_manifest::read_manifest` → `identify_matching_rows` →
    `apply_deletions` → `native_write::publish_manifest_update` (task
    002's single-file atomic-rename primitive, reused completely
    UNCHANGED — DELETE never writes new segment `.arrow` files, only an
    edited manifest, so no second publish path was needed). **Two
    distinct no-op cases**, both leave the manifest byte-for-byte
    untouched and never bump the version: the predicate matches zero
    physical rows, OR it matches rows but EVERY one was already
    tombstoned by a prior DELETE (detected via the LOGICAL row-count
    delta, `rows_deleted == 0`, not a segment-by-segment equality check)
    — `NativeDeleteResult::rows_deleted` is the NET newly-tombstoned
    count, never the gross predicate-match count, so a fully-redundant
    repeat DELETE reports `0` rather than the same nonzero number every
    time it runs.
- **Read-path consultation — the single choke point**: `NativeTable::scan`
  (`src/storage/native_table.rs`) gained a deletion-filtering step: for
  each segment with a non-empty `deleted_rows`, a new free function
  (`filter_deleted_rows`) tracks a running local-row offset across the
  segment's (possibly many) batches and applies `arrow::compute::filter`
  — with a per-batch fast path that skips the mask/filter call entirely
  when NO deleted position falls in that batch's range (the common case
  for a segment with only a few scattered deletions). A segment with an
  EMPTY `deleted_rows` (every table phase 1/task 002 ever wrote, and any
  untouched segment of a mutated table) takes a zero-allocation fast path
  straight through — confirmed zero behavior/performance change, not just
  designed for it. Because `TableProvider::scan_with_filter`'s DEFAULT
  implementation is `self.scan(projection)` and `NativeTable` does not
  override it, EVERY read path funnels through this one fix with zero
  further changes — confirmed by reading every call site, not assumed:
  `morsel_agg.rs`'s dense-direct-address fast path
  (`try_execute_dense_direct` calls `provider.scan_with_filter(...)`),
  `physical/planner.rs`'s generic `MemoryTableExec` path, and its 400MB
  prescan cache all reach the same `scan()`. Dense-direct-address needed
  literally ZERO code changes: it only emits a group whose `presence` bit
  was actually set during accumulation, and deleted rows now simply never
  reach the accumulator (the same sparse-key mechanism that already
  handles ordinary gapped key ranges) — task 001's prior analysis is
  reconfirmed against the ACTUAL implementation, not just carried over
  unchanged from the design doc.
- **`NativeTable::statistics()` row count**: extended to sum
  `segment.live_row_count()` (the NEW logical count) instead of raw
  `segment.row_count`, for BOTH the whole-table and sharded-provider
  branches — a one-line change to an already-existing "compute row_count
  from active_segments()" pattern. `total_byte_size` and the column-stats
  rollup (min/max/NDV) are DELIBERATELY left untouched (task 001: deletion
  vectors never shrink a segment's physical bytes, and a wider stats
  bound is always safe) — this is also why `check_scan_budget`'s existing
  `memory_limit * spill_threshold` formula (phase 1 task 006) needed ZERO
  changes: it estimates `total_byte_size`, which DELETE never changes,
  and `scan()` still must decode a segment's full physical content before
  the new per-batch filter can drop rows.
- **SQL surface**: `delete_target_name(stmt) -> Option<String>`
  (extraction, mirrors `insert_target_name`) plus a `Statement::Delete`
  arm in `Binder::bind()` that validates shape
  (`require_supported_delete_shape`, mechanically checked against
  sqlparser 0.62's real 10-field `ast::Delete` struct plus
  `TableFactor::Table`'s 10 fields — multi-table FROM lists, JOINs,
  `USING`, `RETURNING`/`OUTPUT`, `ORDER BY`/`LIMIT` all refused by name)
  and then ALWAYS returns `Err` pointing at the real entrypoint — DELETE
  has no `LogicalPlan` to give back, so unlike the Insert/CreateTable
  arms this one is not a small wrapper around `bind_query()`. The real
  binding work is `pub fn bind_delete(&mut self, stmt: &ast::Delete) ->
  Result<(String, Option<Expr>)>`: validates shape, then binds the WHERE
  predicate (if any) via the SAME `bind_expr` method `bind_select`'s own
  WHERE-clause binding uses, against the target table's own schema
  (aliased if the DELETE names one, mirroring `bind_table_factor`'s
  convention). `selection: None` binds to predicate `None` (match every
  row), not a trivial `TRUE` literal or an error. A subquery in the WHERE
  clause (`DELETE FROM t WHERE id IN (SELECT ...)`) BINDS successfully
  (confirmed by task 001's spike — the identical `bind_expr` path
  `bind_select` uses) but is refused right after, by name, at bind time:
  evaluating it needs a `SubqueryExecutor`, which the bespoke
  `identify_matching_rows` loop (deliberately not the generic
  `PhysicalOperator` pipeline `SubqueryExecutor` is normally wired
  through) does not have — refusing here, rather than letting
  `physical::operators::evaluate_expr` fail deep inside the
  identification loop with a less specific error, matches this codebase's
  "refuse cleanly and early" discipline.
- **`ExecutionContext::delete_from_native_table`** (`&mut self`, mirrors
  `insert_into_native_table`'s shape): target must already be a
  REGISTERED native table; calls `Binder::bind_delete` directly (NOT
  `bind()` + a physical plan — there is no `LogicalPlan`/
  `PhysicalOperator` pipeline involved at all for DELETE) then drives
  `native_delete::delete_from_native_table`. Re-registers the table
  afterward so the deletion is visible to a subsequent query in the same
  session. `sql()` gained the same DELETE-refusal guard it already has
  for CREATE TABLE/INSERT. Wired into the REPL alongside CREATE
  TABLE/INSERT's existing dispatch.
- **Cell-exact validated** (`tests/native_delete_tests.rs`): a subset
  DELETE matches the independently-computed complement of the original
  source cell-exact; deleting ALL rows leaves the table existing but
  logically empty (subsequent queries return zero rows cleanly, not an
  error); deleting ZERO rows is a clean no-op (manifest byte-for-byte
  unchanged); a DELETE spanning a segment boundary in a real two-segment
  table (CTAS + INSERT) applies to the correct segments only; repeated
  overlapping DELETEs (fully AND partially redundant) never corrupt or
  double-count, with the NET-vs-gross `rows_deleted` distinction
  explicitly asserted; `statistics()` reflects the post-delete logical row
  count; `sql()`'s refusal guard; unregistered-table and
  non-native-table error paths; INSERT and a fresh CTAS both still work
  correctly in a session that has already performed a DELETE (no
  regression).

### Mutation: UPDATE (native-tables-mutation epic, phase 2, task 004, 2026-08-24)

`UPDATE <native table> SET <col> = <expr>, ... [WHERE ...]` — per task
001's Decision 2 (DELETE + INSERT, composed as ONE atomically-published
operation, never two sequential self-publishing calls). Genuinely NEW
mechanism (not just a thin wrapper): reads matched rows' CURRENT column
values, evaluates every SET assignment against them, and publishes the
tombstoned-old + recomputed-new segments in a SINGLE manifest rename.

- **`src/storage/native_update.rs`** (new sibling module to
  `native_write.rs`/`native_delete.rs`/`native_manifest.rs`): owns the
  whole composition. `pub async fn update_native_table(table_dir,
  predicate: Option<&Expr>, assignments: &[(String, Expr)]) ->
  Result<NativeUpdateResult>` — the ONLY entrypoint, sequence: (1)
  `native_write::lock_table_for_write` ONCE, held for the WHOLE span; (2)
  `native_manifest::read_manifest` ONCE; (3)
  `native_delete::identify_matching_rows(dir, &existing, predicate,
  materialize_rows: true)` ONCE — reused completely UNCHANGED from task
  003, called with `materialize_rows: true` (task 003's own Outcome names
  this exact call shape as task 004's need) to get BOTH tombstone-candidate
  positions AND matched rows' current values in one pass; (4) evaluate
  every assignment's bound value expression against each matched row's
  PRE-update values (`assemble_updated_batch` — evaluates ALL assignments
  first against the untouched input batch, THEN overwrites columns, so
  `SET x = x + 1` and multi-assignment SET lists never read a
  partially-updated value, regardless of assignment order); (5)
  `native_delete::apply_deletions` (task 003's function, reused
  UNCHANGED) for the tombstoning half; (6)
  `native_write::write_append_segments` (task 002's function, reused
  UNCHANGED — schema/dictionary inheritance and by-position type
  validation come for free) for the recomputed-rows half, against the
  SAME already-read manifest; (7) fold both segment lists into ONE
  `Vec<Segment>`; (8) `native_write::publish_manifest_update` (task 002's
  single-file atomic-rename primitive) EXACTLY ONCE; (9) release the lock
  (guard `Drop`). This is the concrete mechanism — not just a design
  argument — behind "no partial-state visibility": exactly one
  `rename(2)` publishes the whole statement, so a reader can only ever
  observe the fully-pre-update or fully-post-update manifest.
- **A real correctness gap found and fixed, beyond task 001/003's own
  analysis**: `identify_matching_rows` deliberately does NOT consult
  `Segment::deleted_rows` (harmless for DELETE — re-matching an
  already-deleted position is a no-op union). UPDATE is different: a
  match means "read this row's CURRENT value and write a BRAND-NEW live
  row from it," so a match that is ALREADY tombstoned (e.g. a second
  UPDATE, or an UPDATE after a DELETE, whose predicate still covers an
  already-removed row) must NOT be resurrected. `live_matched_rows`
  filters each segment's materialized matches down to only the positions
  NOT already in that segment's OWN `deleted_rows` BEFORE any SET
  expression ever sees them — without this filter, two overlapping
  UPDATEs (or an UPDATE following a DELETE) touching the same rows would
  silently DUPLICATE them. Caught by this task's own required "a second
  UPDATE that overlaps a first" adversarial test, which fails loudly
  (wrong row count / duplicate ids) without the fix and passes with it —
  confirmed by writing the test first, seeing it fail against a
  naive (unfiltered) composition, then adding the fix.
  `NativeUpdateResult::rows_updated` is therefore the NET count of
  actually-live matched rows recomputed, never the gross predicate-match
  count (mirrors `NativeDeleteResult::rows_deleted`'s own NET-vs-gross
  discipline) — a predicate matching only already-dead rows is a TRUE
  no-op (no version bump, manifest untouched), exactly like DELETE's own
  fully-redundant-repeat case.
- **Dictionary round-trip**: a matched row's NEW value for a
  dictionary-coerced column (e.g. `SET category = 'new-value'`) evaluates
  to a plain `Utf8` array (`evaluate_expr`'s literal/arithmetic/string
  paths never re-encode dictionaries); an UNTOUCHED dictionary column
  passes through unchanged (still `Dictionary(Int32, Utf8)`, since
  `assemble_updated_batch` only overwrites assigned column positions).
  Both land correctly because `write_append_segments`'s own by-position
  `cast_batch_to_target` (task 002, reused completely UNCHANGED) already
  handles exactly these two shapes: an exact type match passes through
  as-is, and a plain-`Utf8`-into-declared-`Dictionary` mismatch is the
  ONE sanctioned coercion. The manifest's declared schema for the column
  is NEVER re-derived from the new data — `publish_manifest_update` is
  always called with the SAME `target_schema` read at the start of the
  operation — so a regression back to plain `Utf8` is structurally
  impossible, not just tested against. Verified end to end (not just by
  this argument) via a real dictionary-encoded TPC-H `o_orderstatus`
  column, cell-exact.
- **SQL surface**: `update_target_name(stmt) -> Option<String>`
  (extraction, mirrors `delete_target_name`) plus a `Statement::Update`
  arm in `Binder::bind()` that validates shape
  (`require_supported_update_shape`, mechanically checked against
  sqlparser 0.62's real 11-field `ast::Update` struct —
  `src/ast/dml.rs` — plus `TableFactor::Table`'s 10 fields, identical
  check to `require_supported_delete_shape`'s own: JOINs, `FROM`
  [Postgres/Snowflake/MySQL `UPDATE ... FROM`, a materially different
  join-shaped feature this epic's "recompute against the row's OWN
  current values only" model does not support], `AssignmentTarget::Tuple`
  [`SET (a,b) = (1,2)`], `RETURNING`/`OUTPUT`, SQLite `OR` conflict
  resolution, `ORDER BY`/`LIMIT` all refused by name) and then ALWAYS
  returns `Err` pointing at the real entrypoint — UPDATE has no
  `LogicalPlan` to give back, same reason DELETE doesn't. The real
  binding work is `pub fn bind_update(&mut self, stmt: &ast::Update) ->
  Result<(String, Vec<(String, Expr)>, Option<Expr>)>`: validates shape,
  binds EVERY SET assignment's value expression (`assignment_target_
  column_name` extracts the target column's unqualified name — the LAST
  identifier of its `ObjectName`, so both `col` and `alias.col` resolve
  the same way — and its existence is validated against the table's
  schema at BIND TIME, a clean `ColumnNotFound` rather than a confusing
  failure deep in the storage layer) plus the WHERE predicate (if any),
  all via the SAME `bind_expr` method `bind_delete` uses, against the
  target's schema (aliased if the UPDATE names one). A subquery ANYWHERE
  — in the WHERE clause OR a SET value (task 001's spike confirmed even a
  correlated scalar subquery as a SET value parses to a plain `Expr`) —
  BINDS successfully but is refused right after, by name: this epic's
  subquery-free evaluator (`physical::operators::evaluate_expr`, no
  `SubqueryExecutor`) cannot execute it, mirroring `bind_delete`'s own
  "refuse cleanly and early" discipline exactly.
- **`ExecutionContext::update_native_table`** (`&mut self`, mirrors
  `delete_from_native_table`'s shape): target must already be a
  REGISTERED native table; calls `Binder::bind_update` directly (NOT
  `bind()` + a physical plan — no `LogicalPlan`/`PhysicalOperator`
  pipeline involved at all) then drives `native_update::
  update_native_table`. Re-registers the table afterward so the update is
  visible to a subsequent query in the same session. `sql()` gained the
  same UPDATE-refusal guard it already has for CREATE TABLE/INSERT/
  DELETE. Wired into the REPL alongside the other three's existing
  dispatch.
- **Verified — not just designed for — no partial-state visibility**
  (this task's own highest-priority acceptance criterion): a real
  concurrent-reader test (`tests/native_update_tests.rs`) races 60
  back-to-back UPDATEs (alternating a marker column between two values,
  matching every one of a 1500-row table's rows each time) against a
  tight polling loop using the REAL `TableProvider::scan` read path (task
  003's own deletion-aware `NativeTable::scan`, not the non-production
  `native_write::read_back` dump) on a genuine multi-threaded tokio
  runtime (`flavor = "multi_thread"`, needed because neither side's I/O
  is truly async — both are blocking `std::fs` work wrapped in async
  fns, so a default single-threaded runtime would let the reader's tight
  loop starve the writer task entirely; confirmed empirically — the test
  hung indefinitely under the default flavor before this fix). Every
  single poll (many hundreds across 5 repeated runs, 0 flakes) observed
  EITHER exactly 1500 rows of the pre-update marker OR exactly 1500 of
  the post-update one — never a row-count dip, never a mix. A second,
  independent, non-timing-dependent angle on the same property: each
  UPDATE call is asserted to bump `snapshot.version` by EXACTLY 1 — a
  regression to two sequential self-publishing calls (the exact
  anti-pattern task 001's design spike forbids) would deterministically
  show as a jump of 2 for one statement, caught with zero timing
  dependence at all.
- **Cell-exact validated** (`tests/native_update_tests.rs`): a
  self-referential `SET total = total * 1.1` matches an independently-
  computed CASE-expression reference over the ORIGINAL source, including
  for rows the SET did NOT touch; a zero-match UPDATE is a byte-for-byte
  no-op; an all-rows UPDATE (no WHERE) via a real dictionary-encoded
  TPC-H column round-trips correctly (manifest type unregressed); two
  overlapping sequential UPDATEs compose exactly as if run in order (no
  duplicate/lost rows — the `live_matched_rows` fix above, exercised for
  real); a cross-segment UPDATE (CTAS + INSERT-built two-segment table)
  applies to the correct segments only; `sql()`'s refusal guard;
  unregistered-table and non-native-table error paths; INSERT, DELETE and
  a fresh CTAS all still work correctly in a session that has already
  performed an UPDATE (no regression).

A live example (real result, captured 2026-08-24, `repl --tpch
data/tpch-1mb`, 1500-row `orders` table):
```
Created table 'orders_native' (1500 rows, 1 segment(s), now at version 1) in 5.140ms
SELECT o_orderkey, o_totalprice FROM orders_native WHERE o_orderkey <= 3 ORDER BY o_orderkey;
-- 1 | 360828.2497833599
-- 2 | 362430.2868490354
-- 3 | 350094.35363558686
UPDATE orders_native SET o_totalprice = o_totalprice * 1.1 WHERE o_orderkey <= 500;
-- Updated 500 row(s) in 'orders_native' (0 segment(s) dropped, 1 segment(s) added, now 1500 row(s) total, version 2) in 0.974ms
SELECT o_orderkey, o_totalprice FROM orders_native WHERE o_orderkey <= 3 ORDER BY o_orderkey;
-- 1 | 396911.07476169587   (== 360828.2497833599 * 1.1)
-- 2 | 398673.315533939
-- 3 | 385103.78899914556
SELECT COUNT(*) FROM orders_native;  -- 1500 (unchanged)
```

### Mutation: memory safety + concurrency/crash-safety adversarial verification (native-tables-mutation epic, task 005, 2026-08-24)

Six adversarial scenarios, each given a REAL, evidenced verdict (not design
review) per `.claude/epics/native-tables-mutation/005.md`'s Outcome section
— full numbers, methodology, and code pointers there; summary here.

- **Deletion vector growth**: a single large DELETE stays cheap (bounded by
  the per-segment `Vec<u32>` + the empty-segment-drop exception). The
  named "many segments, each lightly touched" shape (task 001's own
  residual-risk worry) was built for real (2573 segments via 3000
  separate Append/Delete/Update calls) and hit with one broad ~1% DELETE:
  **~13.1-13.7 bytes per `deleted_rows` JSON entry, stable across a
  313-segment and a 2573-segment run** — extrapolated (not re-measured at
  literal scale) to task 001's own "1000 segments x 1,000,000 rows x 1%
  deleted" scenario: **~131MB**, larger than task 001's "tens of MB"
  guess. Judged a real-but-not-urgent, well-quantified residual risk
  (current realistic scales stay in the sub-2MB range) rather than fixed
  in this task — task 001's own named forward-compatible escape hatch
  (compact `deleted_rows` on-disk encoding) needs a backward-compat
  migration story big enough to warrant its own task, not a same-task fix.
- **Sequential-mutation growth**: a REAL mixed sequence (Append/Delete/
  Update interleaved, not just repeated inserts) up to 2573 segments/4.6M
  rows stays fully correct throughout (`scan()` and `statistics()` agree
  exactly at every checkpoint) and manifest size grows perfectly linearly
  (~413 bytes/segment, no super-linear blowup). `scan()`/`statistics()`
  wall time stay roughly linear in segment count (no cliff) — **but the
  cumulative cost of a LONG mutation SEQUENCE is O(N²)**, a genuine new
  finding beyond the letter of the acceptance criteria: per-mutation cost
  grew from ~0.44ms/op near-empty to ~8.8ms/op at 2500+ segments (every
  mutation re-reads + re-writes the WHOLE `_manifest.json`), because
  compaction is out of this epic's scope by design (task 001 Decision 3).
  Named as a residual risk with concrete numbers for a future compaction
  task, not fixed here (fixing it for real means building compaction).
  Open file descriptors stayed flat (10) for the entire 3000-op run —
  confirmed no handle leak.
- **Empty-segment-drop exception effectiveness**: measured for real, not
  assumed — 5 fully-tombstoned segments were confirmed dropped from the
  manifest, and the counterfactual "what it would have cost had the
  exception not fired" was computed by literally serializing the
  actual retained-shape `Segment` value: **~9,114 bytes/segment that
  would have persisted, vs. 0 actual** (~45.6KB saved across the 5
  segments tested).
- **Single-writer lock under a REAL `kill -9`**: a genuine two-process
  test (`examples/native_crash_kill_check.rs`) drives the REAL
  `native_write::write_append_segments`/`publish_manifest_update`
  building blocks (not a lock-only synthetic harness): a child writes new
  segment file(s), signals readiness, and gets a real external `kill -9
  <pid>` subprocess sent at it (confirmed reaped with `signal=Some(9)`).
  **PASS, 6/6 repeated runs, zero flakes**: the lock is immediately
  re-acquirable (kernel auto-release), `_manifest.json` is byte-for-byte
  untouched, the new segment file is a harmless unreferenced orphan, the
  table reads back as EXACTLY its pre-crash state via the real
  `TableProvider::scan()`/`statistics()` path, and the table is still
  normally writable afterward. A separate `QE_CRASH_MODE=concurrent` run
  (two real child processes racing a real Append against the same table)
  confirms exactly one succeeds and the other gets a clean, named
  `QueryError::Storage` ("another writer already holds the lock...") —
  never silent data loss, never corruption — also 3/3 clean runs.
- **Crash safety mid-mutation**: the SAME `kill -9` test above IS this
  verdict — a writer killed strictly between writing new segment files
  and the final manifest rename leaves the table in EXACTLY its
  pre-mutation state, never a partial one.
- **Carried-forward 5.3GB SQL-path finding (from task 002)** — given a
  real verdict, not left silent: (a) **CONFIRMED BOUNDED, not unbounded
  in source size** — SF=10 (60M rows) measured 5.38GB peak RSS, SF=100
  (600M rows, 10x) measured 5.86GB (+9% for 10x data): the partition
  COUNT (capped at `rayon::current_num_threads()`), not row count, drives
  it, since `StreamingParquetScanExec`'s row-group size is ~constant
  (~1,048,576 rows) regardless of scale. (b) **CONFIRMED it does NOT fail
  safely before this task's fix**: a configured `--memory-limit` had
  ZERO effect on actual usage (proved by setting it to 1GB while the
  query still used 5.4GB), and under a real tight cgroup cap the process
  was OOM-killed by the KERNEL (`journalctl -k`: "Memory cgroup out of
  memory: Killed process ... (native_append_m)") — the identical failure
  class phase 1 task 006 found and fixed for `NativeTable::scan()`. (c)
  **FIXED with a small, scoped, root-cause mitigation**: `ExecutionContext
  ::create_table_as_select`/`insert_into_native_table` (`src/execution/
  context.rs`) now merge their per-partition streams via
  `bounded_partition_merge` (`futures::stream::iter(streams).
  flatten_unordered(Some(limit))`, `limit` from `QE_INSERT_MERGE_
  CONCURRENCY`, default 8) instead of unconditionally-concurrent
  `futures::stream::select_all` — bounding how many partition readers'
  decoded-row-group working sets can be resident at once. **Measured
  effect: SF=10 5.38GB -> 1.63GB (-70%), SF=100 5.86GB -> 1.67GB (-71%,
  and now even MORE tightly scale-invariant), wall time NEUTRAL TO
  FASTER (SF=10 6.54s -> 6.29s; SF=100 100.3s -> 77.8s, -22%)** — a real
  win, not a slow-but-safe tradeoff. Zero correctness regression (all 21
  pre-existing INSERT/CTAS integration tests unchanged and green, plus 3
  new unit tests for the merge helper itself). A 2GB and even a 1GB cgroup
  cap now complete successfully (were both OOM-kills before); a 512MB cap
  still gets SIGKILL'd — a real but much narrower residual gap (this path
  still has no formal pre-flight admission check consulting
  `--memory-limit`, unlike `NativeTable::scan()`'s `check_scan_budget` —
  named as a follow-up, not attempted here: a reliable estimate for THIS
  path's true need is harder to derive than the read-path's clean
  "total on-disk bytes" proxy, and a wrong heuristic risks false
  refusals, a new kind of bug).

Reproduce: `scripts/claude-safe-build.sh cargo build --release --example
native_mutation_growth_check --example native_crash_kill_check --example
native_append_memory_check`, then run each (`QE_MEM_CHECK_MODE=sql
QE_MEM_CHECK_SOURCE=sf100 QE_MEM_CHECK_LIMIT_GB=60 /usr/bin/time -v
./target/release/examples/native_append_memory_check` for the SF=100 SQL
path leg; `SAFE_BUILD_MEM=2G` prefix for the tight-cap legs;
`QE_CRASH_MODE=concurrent` for the two-writer leg).

### Mutation: QA close-out (native-tables-mutation epic, task 006, 2026-08-24)

Final task of the epic. Full suite green in all four feature
combinations, cell-exact validation of INSERT/DELETE/UPDATE composed
together at REAL scale (SF=10 `orders`, not the ~1500-row fixtures
tasks 002/003/004 individually used), M1/M2 distributed gates
re-confirmed, never-mutated-table performance parity re-confirmed
against phase 1's own recorded numbers, and a mutated table's read-
performance cost measured and explained. Also found and FIXED three
real, pre-existing bugs (none mutation-specific — all three reproduce
on phase 1's own never-mutated fixtures) and found one more, deeper bug
that was deliberately NOT fixed (see below). Full detail, including
per-bug code pointers and every reproduction command:
`.claude/epics/archived/native-tables-mutation/006.md`'s Outcome
section.

**Three real bugs found and fixed, one root cause, one fix pattern.**
`SELECT ... FROM <table with a Dictionary-coerced string column>
ORDER BY ...` (or a large-enough JOIN carrying one) failed outright —
`Arrow error: Invalid argument error: column types must match schema
types, expected Utf8 but found Dictionary(Int32, Utf8)` — whenever
`ExternalSortExec`'s or `SpillableHashJoinExec`'s SPILL path engaged
(the ALWAYS-used spillable operators, per this engine's memory-safety
rule). Root cause: their declared output schema comes from
`plan_schema_to_arrow`, which has no Dictionary representation (a
string column is always reported as plain `Utf8`) — three OTHER call
sites in this codebase already carry an established fix for exactly
this "declared vs actual type" mismatch (`ProjectExec::project_batch`,
`MemoryTableExec::execute`'s `rewrap`, `hash_join.rs`'s
`batch_with_actual_types`), but the SPILL-specific code in
`src/physical/operators/spillable.rs` had none of them — its in-memory
counterpart was already safe (it delegates to the already-fixed
operators). Never triggered before because no existing test/benchmark
combined "large enough to spill" with "carries a Dictionary-coerced
column" — this task's real-scale validation was the first time both
conditions held at once, for BOTH the sort operator (a 15M-row `ORDER
BY`) and the join operator (Q12's spilling INNER join, a pre-existing,
already-documented characteristic — see Current limitations below).
Fixed at all three call sites it was missing from
(`ExternalSortExec::flush_run`, `ExternalSortExec::
{build_merged_batch,build_merged_batch_final}`, `SpillableHashJoinExec`
's `create_joined_batch`) with a new local `batch_with_actual_types`
helper (mirrors `hash_join.rs`'s function of the same name — this file
follows the SAME local-duplication convention that function and
`scan.rs`'s `rewrap` already established, rather than a new cross-
module dependency for a three-line function). A SEPARATE, genuinely
distinct bug in the same file was found and fixed alongside:
`ExternalSortExec`'s k-way spill-merge held `(run_idx, row_idx)`
references into a run's CURRENT in-memory Parquet batch that went
stale — silently wrong data, or an out-of-bounds panic ("the len is N
but the index is N") — the instant that batch was reloaded (any real
spill run over `MERGE_BUFFER_ROWS` = 8192 rows, the ordinary case, not
an edge case) before pending references were flushed; fixed by
flushing `output_rows` before every buffer transition, not only the
pre-existing periodic size-based flush. Four new regression tests pin
all of this: `external_sort_spill_path_handles_dictionary_encoded_columns`,
`k_way_merge_survives_a_run_needing_more_than_one_buffer_load`,
`create_joined_batch_handles_dictionary_encoded_columns` (all in
`spillable.rs`'s own test module).

**A fourth, deeper bug found — NOT fixed, documented instead (a
deliberate "stop and document" judgment call, not an oversight).** Once
the crash above was fixed, TPC-H Q12 at SF=10 against a native table
completes but returns a WRONG ANSWER (`high_line_count` exactly
2x-inflated: engine 707644 vs an independent DuckDB oracle's 353822)
and takes ~320 SECONDS (vs ~150-350ms for every other query). This
SUPERSEDES this doc's own prior claim (native-tables-foundation task
008) that a spilling native-table join is "always a safe, clean refusal
or a slow-but-correct completion — never wrong data": that was only
true because the schema-mismatch crash above always fired FIRST, before
the join's own spill/partition/probe/merge sequence ever ran far enough
to expose this SECOND, independent, more severe bug. Read
`SpillableHashJoinExec::execute_spill_path`/`build_with_partitioning`/
`probe_with_spilling`/`process_spilled_partition` end to end looking for
a duplicate-counting mechanism (the clean ~2x ratio is diagnostic); the
build/probe partition-vs-spill bookkeeping reads as mutually-exclusive-
by-construction on inspection, and nothing in it is Dictionary-specific
— meaning this is a genuinely SEPARATE, deeper bug in the partition/
spill algorithm itself (or possibly its caller), not another instance of
the same "declared vs actual type" class, and root-causing it safely
would need materially more investigation than the three fixes above
(each was a single missing schema-reconciliation call). Deliberately NOT
attempted under time pressure — an unconfident fix risks leaving wrong
answers that no longer even crash to reveal themselves, which is worse
than the status quo. Also deliberately did NOT revert the three schema
fixes to "restore" the crash as an accidental safety net: the
duplication mechanism is not Dictionary-specific, so it is reachable by
ANY sufficiently large spilling INNER join, native table or not —
reverting would only narrow, not close, the exposure, while also
regressing the (independently verified, cell-exact-correct) 15M-row
`ORDER BY` case this task's own validation depends on. Recommendation
for whoever picks this up: the native-tables-foundation epic's own
"streaming rewrite of the join spill path" future-work item should be
treated as a P0 correctness bug now, not merely a performance one.

**Independent re-verification (2026-08-24, orchestrating session, before
merge to main).** Reproduced this exact query twice from scratch against
freshly-built native tables (`CREATE TABLE ... AS SELECT` from
`data/tpch-10gb` orders/lineitem via `serve --tables --memory-limit 40G`,
bypassing the small REPL default budget): once against a never-mutated
table, once against the same table after a real `DELETE FROM lineitem_v
WHERE l_orderkey % 1000 = 0` (60,266 rows removed, no segments dropped).
**The extreme slowness reproduced identically both times** (150.0s and
150.5s — same order of magnitude as the original ~320s, vs. 150-350ms for
every other query; this part of the finding is solid). **The 2x-inflated
wrong answer did NOT reproduce either time** — both runs returned
`high_line_count` 353822/352224, cell-exact against a freshly-computed
DuckDB oracle (the mutated run's `low_line_count` shifted by exactly the
expected amount for the rows actually deleted — itself a small additional
correctness confirmation of the DELETE mechanism under this exact join
shape). This does **not** disprove the wrong-answer finding — the
reported ratio was a suspiciously exact 2.0000x, which reads as a real,
systematic duplication mechanism under SOME condition, not measurement
noise — but it narrows where to look: a plain post-CREATE `DELETE` was
not sufficient to trigger it in two independent attempts, so the trigger
(if the original run's exact condition wasn't itself a fluke of that run's
specific partition-count/memory-pressure state) more likely depends on
the full `CREATE→INSERT→DELETE→UPDATE` segment/deletion-vector shape the
original finding's own cell-exact validation used (not yet re-attempted),
or on non-deterministic partition-count selection under spill pressure.
**Verdict: treat both halves as a live, P0, open risk** — the slowness is
confirmed-severe on its own regardless of the correctness question, and
the wrong-answer risk is unconfirmed-but-uncleared, not stood down.
Whoever picks up the P0 follow-up should start from exactly this
narrowed reproduction matrix rather than re-deriving it.

**CORRECTION + resolution of the trigger question (`spill-join-
correctness` epic, task 001, 2026-08-24).** The paragraph immediately
above is WRONG about the trigger: re-reading the *raw* original
investigation log (not just this doc's own summary of it) shows the
original finding came from a plain benchmark run against the **pristine,
never-mutated** warehouse — the `CREATE→INSERT→DELETE→UPDATE` sequence
mentioned above never touched `lineitem` and never ran Q12. Mutation
history is not part of the trigger; this doc's own prior speculation
sent the wrong signal. The real picture, now precisely quantified: **21
repro runs against the pristine warehouse, 1 wrong (4.8%) — a real,
non-deterministic, intermittent bug**, not a deterministic one gated by
data shape. All 21 runs showed the extreme slowness (140-291s). The one
wrong run's ratio was ~1.9925-1.9927x, not an exact 2.0000x — consistent
with a racy partial overlap rather than a clean "every row counted
twice" mechanism. Real, adversarial root-cause work was done: the
code's own comment names the leading suspect (`execute_fused_streaming`
aborting and re-executing its non-idempotent join child); a controlled
test FORCED exactly that path after a correct first computation and got
the CORRECT final answer back (283s, matching the real wrong run's 291s
almost exactly) — this **disproves** the leading hypothesis for
wrongness while fully confirming it explains the slowness. Sort
re-executing the aggregate, per-process hash-seed randomization, and
build/probe double-collection were also ruled out with direct evidence,
not assumption. Root cause for the wrongness specifically: **still not
confirmed** — best remaining hypothesis is a genuine mid-computation
failure (not a clean post-completion discard) inside the spill path.
Separately found, not the root cause but real and actionable: a likely
O(n²) read-rewrite-rename-per-append pattern in the spill path's
Parquet-file handling that plausibly explains most of the deterministic
slowness on its own. Full details, the reusable repro script, and the
complete chronological investigation: `.claude/epics/archived/
spill-join-correctness/001.md` and its `updates/001/stream-A.md`.

**Epic close-out (`spill-join-correctness` epic, tasks 002-004, closed
2026-08-25).** This is NOT a "bug fixed" epic — say so plainly, since
the paragraphs above could otherwise read that way in isolation. Two
real, separate things landed. **The O(n²) `append_to_parquet`
spill-write slowness named just above IS FIXED** (task 002 — replaced
the read-rewrite-rename-per-append pattern with a single `ArrowWriter`
kept open per partition for the whole build/probe phase, appending
each batch as one more row group instead of re-reading and rewriting
everything already spilled): **140-291s -> 3-6s on this exact Q12
repro, a ~40-90x speedup**, 40/40 trials cell-exact, zero regression,
full suite green in all four feature combinations. This was always a
mechanism SEPARATE from the wrongness (task 001's own chaos-test
experiment had already shown a forced ~2x-retry-and-discard does not
reproduce the wrong answer even though it fully explains the
slowness), so fixing it neither touched nor was expected to touch the
duplicate-counting bug. **The wrong-answer bug itself remains OPEN —
root cause still UNCONFIRMED.** Real, adversarial root-cause work was
attempted (task 001: runtime instrumentation plus a controlled
chaos/fault-injection test) and reached a genuine, evidenced negative
result — the leading hypothesis (non-idempotent join-child
re-execution) was directly disproven for the wrongness specifically —
but no mechanism was ever caught in the act. Per this epic's own
"no-guess-fixes" gate, no fix was attempted. Rate estimate, reported as
a range because two real estimates exist at different sample sizes and
the larger one does not supersede the smaller one's validity, it just
bounds it more tightly: task 001's own standalone 21-trial estimate is
**4.8% (1/21, 95% CI [0.12%, 23.82%])**; pooling every trial run
across the whole epic (tasks 001+002+003, 290 trials total, still only
ever the same one wrong observation) tightens this to **0.34% (1/290,
95% CI [0.01%, 1.91%])**. The lower pooled number is explicitly NOT
evidence of an accidental fix — nothing in tasks 002 or 003 touched the
duplicate-counting mechanism — it is a tighter bound on what was
always a real, low, still-live rate, from a larger sample. **Not
native-table-specific — no evidence found either way that it's
confined there**: task 003 forced plain parquet into the IDENTICAL
spill code path (`--memory-limit 1M`, matched to native's own natural
spill shape — same `build_batches=916 build_rows=1765881` once both
spill) and got 0/80 wrong, statistically indistinguishable from
native's own 0/80 at equal trial counts. **Confirmed also reachable via
distributed execution**: Q12's shape qualifies for the SCATTER
`two_phase` election, so each of 3 nodes independently runs the
identical `execute_spill_path` over its own shard of `lineitem`
(confirmed directly, not inferred — per-node `build_rows` summed
exactly to the single-process total); 40/40 distributed trials came
back correct, so this is a confirmed EXPOSURE, not a confirmed-safe
path — the absence of a wrong distributed trial in 40 tries is
consistent with, not proof against, the same low rate applying there
too.

Task 003 also found **three new, distinct residue bugs** while
characterizing blast radius, none fixed (per the epic's own
no-guess-fixes gate) — tracked here as known, open issues:

1. **Concurrent same-host `serve` processes collide on the default
   spill directory.** `spill_path`'s default (`$TMPDIR/
   query_engine_spill`) disambiguates only via a PER-PROCESS-LOCAL
   `SPILL_COUNTER` starting at 0 in every process, so multiple `serve`
   processes sharing one host's `$TMPDIR` (this repo's own local
   multi-process cluster test harness included) can compute the
   identical `spill_id` and write to the same directory concurrently.
   Trigger condition, stated precisely: a REALISTIC single-node
   memory-limit (40G — native Q12's own natural spill) was enough to
   surface this; the adversarial ingredient is co-located `serve`
   processes sharing a temp filesystem, NOT an extreme memory-limit —
   a real, practical risk for any multi-node-per-host deployment, not
   just a synthetic stress condition. Severity: fails LOUDLY (`Parquet
   error: Required field schema is missing`, HTTP 400) — confirmed
   never silently wrong (giving each node its own `TMPDIR` fixed it,
   cell-exact) — but a real availability risk. Not fixed; needs
   per-process disambiguation added to the spill-path default (e.g.
   include the PID).
2. **`LIMIT` not enforced under spill for `ORDER BY ... LIMIT`
   queries** (Q2/Q3-shaped). Trigger condition: a deliberately
   adversarial, unrealistic `--memory-limit 1M` sweep that forced 13
   queries' joins into the spill path at once — NONE of these 13 were
   ever observed spilling at any realistic memory-limit anywhere in
   this epic. Symptom: row count blew up (2085 vs 100; 755880 vs 10)
   while every compared row's VALUES were byte-for-byte correct
   against DuckDB's own LIMIT-ed oracle — a "correct top-K prefix,
   LIMIT never applied to truncate it" bug, likely in
   `ExternalSortExec`'s spill path or the top-k fusion rule losing
   track of the LIMIT under a spilled input. Severity: wrong ROW COUNT
   under an adversarial, non-default setting only — a different
   symptom shape than the target bug (which inflates VALUES, not row
   count) and not reproduced at any realistic setting.
3. **Sort-spill run-file-not-found crash** (Q10-shaped): `Failed to
   open run file ".../sort_0_27/merged_pass0_48.parquet": No such file
   or directory`. Same artificial `--memory-limit 1M` sweep as #2.
   Severity: crashes loudly, never silently wrong; not reproduced at
   any realistic setting.

None of the three was investigated further or fixed — each needs its
own root-cause + fix task. Full evidence, exact trial counts, and every
reproduction command: `.claude/epics/archived/spill-join-correctness/
003.md`'s Outcome section and `updates/003/stream-A.md`.

**Bottom line, stated plainly per this epic's own honesty
requirement**: the wrong-answer bug described in this section is real,
reliably reproducible (at a real, if low, rate), and STILL UNFIXED,
with its root cause still UNCONFIRMED — this is the epic's own honest
headline, not a footnote. A real, separate performance win landed (the
O(n²) fix, ~40-90x). A real, valuable characterization landed
(native/parquet parity, confirmed distributed exposure, three
newly-found bugs). Full epic close-out, including G1-G6 verdicts and
every commit: `.claude/epics/archived/spill-join-correctness/epic.md`.

**Full suite, all four feature combinations**, final state (all fixes +
all new tests included), through `scripts/claude-safe-build.sh`:

| combo | passed | failed | ignored |
|---|---|---|---|
| default | 1190 | 0 | 1 |
| lance | 1255 | 0 | 2 |
| gpu | 1190 | 0 | 1 |
| pulsar | 1193 | 0 | 1 |

(Updated post-`spill-join-correctness` epic: +2 over the prior baseline
in every combination, the two tests task 002 added for the O(n²)
spill-append fix; zero regressions, re-confirmed at merge time.)

**Cell-exact validation at real scale** (`examples/
native_mutation_cell_exact_check.rs` + `scripts/
native_mutation_cell_exact_check.py`): CREATE (12,000,000 rows) →
INSERT +3,000,000 (15,000,000) → DELETE −492,202 (14,507,798) → UPDATE
2,071,620 rows recomputed in place (14,507,798, unchanged by design,
matching UPDATE's own semantics) against real SF=10 `orders` (not the
~1500-row fixtures individual tasks used) → final `SELECT * ... ORDER
BY o_orderkey` (the exact shape that hit the schema-mismatch bug
above). `scripts/native_mutation_cell_exact_check.py` independently
recomputed the IDENTICAL 4-statement sequence as REAL DuckDB DML
against the SAME source parquet (row counts agreed exactly at every
step) and compared every cell via a DuckDB `EXCEPT` set difference in
both directions: **PASS — 0 rows different either direction,
14,507,798 rows × 9 columns, cell-exact.**

**Never-mutated-table performance parity** (`data/tpch-10gb-native`,
pristine, `scripts/native_bench_compare.py`, Q12 excluded — the fourth
bug above, unrelated to mutation or this check): **21/21 cell-exact,
5.667s total, 1.27x vs DuckDB-parquet** — matches phase 1's own recorded
5.324s/1.23x within this program's established run-to-run noise band.
Confirms the new deletion-vector-consultation code path costs nothing
when a segment has nothing to filter (the empty-`deleted_rows` fast
path holds).

**Mutated-table (non-empty deletion vector) regression, reported
honestly, not hidden.** Same 21 queries against a hardlink-mutated copy
of the SAME warehouse (`examples/native_post_mutation_checks.rs`:
`lineitem`'s deletion vector non-empty via `l_discount > 0.09`, ~10% /
5,997,226 rows, spread near-uniformly across all 58 segments — DELETE
never touches the other 7 tables' hardlinked copies): **15.017s total,
3.35x vs DuckDB-parquet — a real 2.65x slowdown vs the SAME
never-mutated warehouse.** Root-caused, not just measured: Q06's own
filter (`l_discount BETWEEN 0.05 AND 0.07`) is DISJOINT from the delete
predicate (`> 0.09`), so no deleted row could ever have mattered to
Q06's ANSWER — yet its wall time still ~4.6x'd (111ms → 506ms),
isolating the cost to the deletion-vector CONSULTATION itself (paid
per-batch, on every scanned row, at the single choke point inside
`scan()`/`scan_with_filter()`), not to any change in query selectivity.
Because deletions are spread near-uniformly (not concentrated), the
per-batch "no deleted position in this batch's range" fast-path skip
almost never fires. Queries that barely touch `lineitem` (Q02, Q11,
Q16, Q22) show flat-to-negligible regression, confirming the cost
scales with rows actually scanned from the mutated table, not a fixed
per-query tax — a REAL, BOUNDED, well-explained cost, exactly the
honest-reporting bar this task set for itself.

**Dense-direct-address and GPU offload, post-mutation** (`examples/
native_post_mutation_checks.rs`, `examples/native_gpu_check.rs`): both
still fire correctly. Dense-direct: native post-delete group count
(14,594,694) exactly matches an independent parquet+equivalent-filter
cross-check computed via the engine's generic/parquet code path (a
materially different path from the native dense-direct one);
`AGG_TIMING=1` confirms the `(native)` fast-path tag fires on both
native legs (47.5ms / 303.5ms scan+accumulate). GPU offload: still
engages (correct row counts, VRAM growth confirmed on the pristine
leg — cold iter1 ~2.3s, warm iters ~5-7ms, exactly matching task 008's
own documented numbers) but its warm speedup is FULLY MASKED by the
same deletion-vector overhead once the table is mutated — a clean 2×2
CPU/GPU × pristine/mutated matrix (Q6, warm, ms):

| table | CPU | GPU | speedup |
|---|---|---|---|
| pristine (never-mutated) | ~106 | ~6 | **~18x**, matches task 008's own documented finding exactly |
| mutated (10% deletion vector) | ~445 | ~415 | **~1.07x — effectively none** |

A never-mutated table pays none of this (its dense-direct and GPU
numbers match already-published pristine numbers exactly) — the cost is
real but confined to tables that have actually been mutated, exactly as
this epic's own design intends.

**M1/M2 distributed gates**: both PASS (`scripts/cluster_local.sh
verify` / `verify-m2`, re-confirmed with the FINAL default binary, all
four fixes and all new tests included) — nothing this epic touched
broke existing distributed behavior for parquet/Iceberg/Lance tables.

**G1-G5 (this epic's own success criteria) — verdicts with evidence**:
- **G1** (INSERT/DELETE/UPDATE work end-to-end through SQL, cell-exact
  vs an independently computed reference) — **MET**. Real SF=10 scale
  (14,507,798 rows), independently verified against DuckDB DML over the
  same source parquet, 0 mismatches.
- **G2** (no performance cliff for the still-dominant read-only query
  shapes, for a table that has never been mutated) — **MET**. 1.27x
  matches phase 1's own 1.23x within noise; dense-direct-address and GPU
  offload both confirmed still firing at their pre-epic numbers.
- **G3** (memory safety holds under adversarial testing) — **MET**
  (task 005: two real findings quantified with concrete numbers —
  deletion-vector JSON density at very large segment counts, O(N²)
  cumulative manifest-rewrite cost across a long mutation sequence —
  both named residual risks for a future compaction epic, not fixed
  here by design; one real SQL-path OOM found AND fixed, 70-71% RSS
  reduction).
- **G4** (full suite green in all feature combinations; M1/M2 gates
  unaffected) — **MET**. All 4 combinations green (table above, 0
  failures anywhere); M1 + M2 PASS via real 3-process clusters.
- **G5** (single-writer assumption enforced, not just documented; a
  concurrent write fails cleanly and namedly) — **MET** (task 001/005:
  `std::fs::File::try_lock()`, verified live with a real cross-process
  test AND a real `kill -9` mid-mutation, 6/6 runs, zero flakes).

Reproduce: `scripts/claude-safe-build.sh cargo build --release --example
native_mutation_cell_exact_check --example native_post_mutation_checks`
then run `native_mutation_cell_exact_check` + `.venv/bin/python
scripts/native_mutation_cell_exact_check.py` (cell-exact validation);
`native_post_mutation_checks` (builds the mutated warehouse the
benchmark commands below need, and runs the dense-direct cross-check);
`.venv/bin/python scripts/native_bench_compare.py --native-dir
data/tpch-10gb-native --source-dir data/tpch-10gb --iceberg-dir
data/tpch-10gb-iceberg --sf 10 --memory-limit 40G --iterations 2
--queries 1,2,3,4,5,6,7,8,9,10,11,13,14,15,16,17,18,19,20,21,22`
(never-mutated leg; Q12 excluded per the fourth-bug note above) and the
same command pointed at the mutated warehouse directory with
`--no-cell-exact --no-iceberg` (mutated leg — cell-exact is skipped
there because the DuckDB oracle reads the ORIGINAL un-mutated parquet,
so it would legitimately mismatch; correctness of mutation itself is
what the cell-exact validation above already established).

### Materialized rollups: matching/substitution mechanism (native-tables-rollups epic, task 001, 2026-08-26)

Phase 4 of the native-tables PRD, task 1 of 4. Proves the epic's own
real risk — can a query be matched against a registered rollup
definition and transparently, correctly answered from it — against a
PROGRAMMATIC registration API (`ExecutionContext::register_rollup`,
proven before any SQL DDL was built on top of it; task 002, immediately
below, wires `CREATE MATERIALIZED VIEW` onto this same API unchanged).
Genuinely new-algorithm-class
work: a dedicated research pass confirmed nothing in this codebase
previously reasoned about equivalence between two independently-planned
queries.

**Data model.** `NativeManifest` (`src/storage/native_manifest.rs`)
gains an additive, `#[serde(default)]` `rollup: Option<RollupMeta>`
field — every manifest the foundation/mutation epics ever wrote
deserializes with `None`, unchanged behavior (mirrors `Segment::
deleted_rows`'s own precedent exactly). `RollupMeta` records:
`base_table` (the registered name), `defining_sql` (verbatim, for
provenance/a future DDL surface, NOT used for matching), `base_table_id`/
`base_table_version` (staleness bookkeeping — see below), and
`columns: Vec<RollupColumn>` (each GROUP BY/aggregate's canonical shape
key paired with its physical column name in the rollup's own native
table).

**The matching mechanism** (`src/storage/native_rollup.rs`, new sibling
module to `native_write.rs`/`native_delete.rs`/`native_update.rs`):
`canonical_expr_key` renders an `Expr` into an alias-blind, table-
qualification-blind string key (strips every `Expr::Alias` wrapper and
every `Column.relation` before falling back to a Debug-based rendering
of each node's own deterministic "tag" — this task's own answer to a
question the codebase's two existing plan-comparison precedents
(`optimizer/mod.rs`'s fixpoint-loop Debug-string hash, `subquery.rs`'s
`plan_hash`) leave open: both compare for EXACT identity, never semantic
equivalence). `recognize()` matches the `Project(Aggregate(Scan))` shape
— exactly what `Binder::bind_select` produces, unmodified, for a plain
single-table GROUP BY query — straight off the RAW BOUND plan, BEFORE
the ordinary optimizer pipeline runs (a deliberate choice: several rules
rewrite an `AggregateNode`'s own fields or push work into the `Scan`,
which would make an incoming query's OPTIMIZED shape diverge from a
rollup's recorded shape for reasons having nothing to do with whether
the two queries are equivalent).

**Matching semantics, decided explicitly (not left implicit, per the
task's own gotcha warning)**:
- GROUP BY and aggregate SETs are compared order-independently (`GROUP
  BY a, b` matches `GROUP BY b, a`) via SORTED-MULTISET equality (element
  COUNT still matters — nothing is silently collapsed the way a true set
  would).
- Column aliasing never affects matching (`SUM(x) AS a` and `SUM(x) AS
  b` canonicalize identically); the substituted plan's OUTPUT columns
  always reflect the INCOMING query's own aliases, never the rollup's.
- Table qualification is stripped (`l_returnflag`/`lineitem.l_returnflag`/
  `l.l_returnflag` all canonicalize the same) — safe only because this
  task's scope is single-base-table, no-JOIN rollups (structurally
  guaranteed by `recognize`).
- The requested aggregate SET must match EXACTLY, not be a subset in
  either direction — coarser/subsumption matching is explicitly out of
  scope (the epic's own "narrow slice first" decision).
- A query's SELECT list may reorder/alias/omit trailing columns freely
  (e.g. `SELECT l_returnflag, SUM(l_quantity) FROM lineitem GROUP BY
  l_returnflag, l_linestatus`, omitting `l_linestatus` from the output
  while still grouping by it) — matching is against the `Aggregate`
  node's OWN `group_by`/`aggregates` fields, independent of what the
  outer `Project` exposes — but every SELECT list item itself must be a
  bare (optionally aliased) column reference, never a computed
  expression.
- A WHERE clause anywhere (rollup definition OR incoming query) always
  misses the shape and falls back correctly — this epic ships
  unfiltered, exact-match rollups only.

**Where this plugs in — NOT an `OptimizerRule`, per the epic's own
architecture decision**: `OptimizerRule::optimize(&self, plan) ->
Result<LogicalPlan>` has no catalog/registry parameter by design, so
`ExecutionContext` (`src/execution/context.rs`) itself owns
`rollup_candidates()` (builds a `&[RollupCandidate]` snapshot from
`self.tables`, EXCLUDING any rollup whose recorded base-table identity
no longer matches the base table's CURRENT `(table_id, version)` — this
IS task 001's staleness enforcement) and `substitute_rollups()` (calls
`native_rollup::substitute`, a `VectorSearchPushdown`-style recursive
plan rewrite), wired into `sql()` and `optimized_plan()` BEFORE
`Optimizer::optimize()` runs. Zero-cost when no rollup is registered:
`rollup_candidates()` returns empty and substitution short-circuits
before walking the plan tree at all — a rollup is additive, confirmed by
the full pre-existing suite staying green with zero regression.

**Registration** (`ExecutionContext::register_rollup(name, base_table,
defining_sql)`, `&mut self`, async): the base table must already be a
REGISTERED NATIVE table (staleness bookkeeping needs a real (table_id,
version) pair, which only a native table's manifest provides — a
rollup over plain parquet/Iceberg/Lance is out of scope). Binds the
defining SQL, validates its shape via the SAME `recognize()` an
incoming query is later matched against, then reuses `create_table_as_
select`'s own plan/optimize/execute/stream pipeline UNCHANGED to write
the rollup as an ordinary native table via `native_write::write_batches`
(a rollup's row data IS a native table) — then attaches `RollupMeta` to
the just-published manifest via a SECOND, small, manifest-only atomic
patch (`native_manifest::write_manifest_atomic`, the SAME single-file
atomic-rename primitive the mutation epic's Append/DELETE/UPDATE paths
already established for exactly this "patch just the manifest" shape).

**Provenance** (G3/PRD G5): `QueryMetrics::rollup_answered: Vec<String>`
— empty when no rollup was involved, else the name(s) of every rollup
that answered some subtree, checked directly by every integration test
(never left implicit). `QE_DEBUG_ROLLUP=1` traces every match/no-match/
staleness decision to stderr, matching this codebase's established
diagnostic-switch convention.

**Cell-exact validated**: `tests/native_rollup_tests.rs` (15 tests, real
SQL-level path, `data/tpch-1mb`) — the PRD's own worked example
(`lineitem` grouped by `l_returnflag`/`l_linestatus`, SUM/COUNT)
cell-exact vs. direct base-table computation; order-independence;
alias-blindness; subset-projection; THREE explicit non-matching-shape
fallback tests (different GROUP BY set, added filter, different
aggregate) each asserting the FALLBACK ANSWER stays cell-exact, not just
"doesn't crash"; staleness after a real base-table DELETE, fallback
answer verified cell-exact vs. an independently-mutated reference
context; 5 `register_rollup` validation/error-path tests.
`examples/native_rollup_cell_exact_check.rs` + `scripts/
native_rollup_cell_exact_check.py`: real SF=1 scale (`data/tpch-1gb`'s
`lineitem`, 6,000,000 rows), a query that deliberately differs from the
defining query in GROUP BY order, aliases AND SELECT-list order,
independently verified against a fresh DuckDB computation over the same
source parquet — PASS, 6/6 groups, within this repo's own PRE-EXISTING
float-SUM tolerance (`scripts/native_bench_compare.py`'s `cell_compare`:
`tol = max(0.02, |v|*1e-9)`; a first pass using flat 6-decimal rounding
reported spurious ~1e-15-relative mismatches, exactly the
float64-summation-order noise class that tolerance already exists to
absorb — not a rollup-specific issue).

**Full suite green, all four feature combinations** (default/lance/gpu/
pulsar), zero regression: 1224/1289/1224/1227 passed respectively (each
exactly the prior baseline + this task's 34 new tests), 0 failed
anywhere.

**What's explicitly NOT done, per the epic's own task breakdown** (not
silently narrowed — named here): SQL DDL was task 002's job (now shipped
— see the next section); the refresh-on-write model that keeps
`RollupMeta` current automatically on base-table INSERT/DELETE/UPDATE
(task 003 — today, a mutated base table's rollup simply goes stale and
is excluded from matching until manually re-registered/re-`CREATE
MATERIALIZED VIEW`-ed); subsumption/coarser-grouping matching
(explicitly out of scope for this epic, named by the epic itself as "a
genuinely new algorithm class"); rollups over a non-native base table; a
subquery-embedded aggregate opportunity (`LogicalPlan::children()` does
not expose plans inside `Expr::ScalarSubquery`/`Exists`/`InSubquery`, so
`substitute`'s recursive walk never visits them); distributed rollups.

Reproduce: `scripts/claude-safe-build.sh cargo test native_rollup` (35
unit+integration tests); `scripts/claude-safe-build.sh cargo build
--release --example native_rollup_cell_exact_check && scripts/
claude-safe-build.sh ./target/release/examples/
native_rollup_cell_exact_check && .venv/bin/python scripts/
native_rollup_cell_exact_check.py` (DuckDB oracle, real SF=1 scale).

### Materialized rollups: SQL DDL surface (native-tables-rollups epic, task 002, 2026-08-26)

`CREATE MATERIALIZED VIEW <name> AS SELECT ...` wired onto task 001's
`register_rollup`, with ZERO sqlparser grammar work (confirmed by the
research: `Statement::CreateView { materialized: true, query, .. }`
already parsed this shape natively at the pinned sqlparser 0.62) and
ZERO changes to task 001's own matching/substitution mechanism
(`native_rollup.rs` — confirmed by an empty diff against task 001's
head, not just "we didn't mean to touch it"). Example:

```sql
CREATE MATERIALIZED VIEW lineitem_rollup AS
  SELECT l_returnflag, l_linestatus,
         SUM(l_quantity) AS sum_qty, SUM(l_extendedprice) AS sum_base_price,
         COUNT(*) AS count_order
  FROM lineitem_native GROUP BY l_returnflag, l_linestatus;
```

**`Binder::bind()` gains a `Statement::CreateView` arm**
(`src/planner/binder.rs`), mirroring `Statement::CreateTable`'s existing
shape exactly (validate, then bind the inner query).
`require_supported_create_view_shape` refuses, BY NAME, every one of
sqlparser 0.62's real 17 `CreateView` struct fields this epic does not
implement (re-read directly from the vendored `ast/ddl.rs`, not a
paraphrase) — `or_alter`, `or_replace`, `secure`, an explicit view
column list, `options`, `cluster_by`, `comment`,
`with_no_schema_binding`, `if_not_exists`, `temporary`, `copy_grants`,
`to`, `params`. `materialized` is required `true` — a plain `CREATE
VIEW` is refused explicitly (out of scope entirely, never silently
treated as a rollup), checked separately from the shape validator since
its required sense (must be ON) is the opposite of every other field
(must be OFF).

**`IF NOT EXISTS` — decided explicitly: ERROR**, matching `CREATE
TABLE`'s own `ct.if_not_exists` precedent exactly: `register_rollup`
always recomputes/replaces wholesale, so silently accepting and ignoring
the flag would change what the statement means rather than doing what
it says.

**`ExecutionContext::create_materialized_view(&mut self, sql: &str)`**
(new, placed beside `register_rollup`): extracts the target name,
`Binder::bind()`s the whole statement (validates shape, binds the inner
query), recovers the defining query's base table from that SAME bound
plan via `native_rollup::recognize` — task 001's OWN recognition
function, reused unchanged from a new call site, so this layer and
`register_rollup`'s own internal re-derivation of the same query can
never disagree about which table a rollup is defined against — then
calls `register_rollup(name, base_table, defining_sql)`, which does all
the real work. This method never itself touches the plan/execute/write
pipeline: purely a DDL front end, per the task's own scope. `sql()`
gained a fifth DDL/DML redirect case (mirrors CREATE TABLE/INSERT/
DELETE/UPDATE's identical pattern); a plain `CREATE VIEW` is
deliberately NOT caught by that guard (no entrypoint to redirect it to)
and instead falls through to `Binder::bind()`'s own direct refusal,
mirroring `Statement::Delete`/`Update`'s precedent. REPL wiring mirrors
the other four DDL/DML statements' existing dispatch exactly.

**Checked, per the task's own instruction, whether REPL/CLI share the
`sql()` path — finding**: `src/distributed/server.rs`'s
`execute_statement` (used by both HTTP `/sql` and Arrow Flight) calls
`ctx.sql(&statement)` directly, so CREATE MATERIALIZED VIEW's
REGISTRATION step is **NOT reachable via HTTP `/sql` or Flight**,
exactly mirroring CTAS's own already-documented boundary above — only
the SUBSEQUENT matching/answering step (an ordinary query) runs through
the real, shared `sql()` path. The REPL has its own dedicated dispatch
for registration, matching CREATE TABLE/INSERT/DELETE/UPDATE's identical
pattern; no other CLI subcommand runs arbitrary DDL text.

**Validated end to end** (`tests/native_materialized_view_tests.rs`, 11
new tests): `CREATE MATERIALIZED VIEW` populates a rollup and a
subsequent `ctx.sql()` query — the exact method HTTP `/sql`/the REPL's
catch-all call — is transparently answered from it, provenance-confirmed
and cell-exact vs. an independent reference context; `sql()`'s redirect
and the plain-`CREATE VIEW`/`IF NOT EXISTS`/`OR REPLACE`/view-column-list
refusals; the defining query's own shape requirements (no WHERE, no
computed projection, no JOIN, native base table only) are not loosened
by the DDL layer, reaching `register_rollup`'s own already-tested
validation correctly through the new glue code.

**Full suite, all four feature combinations, zero regression**: default
1235 (+11), lance 1300 (+11), gpu 1235 (+11), pulsar 1238 (+11) — each
exactly task 001's own baseline plus this task's 11 new tests, 0 failed
anywhere. `cargo fmt --all -- --check` clean.

Reproduce: `scripts/claude-safe-build.sh cargo test --test
native_materialized_view_tests`.

### Materialized rollups: refresh-on-write (native-tables-rollups epic, task 003, 2026-08-26)

Task 001 built the staleness BOOKKEEPING (a rollup whose recorded base-
table `(table_id, version)` no longer matches the base table's CURRENT
one is excluded from matching); this task makes it real by wiring
AUTOMATIC refresh into the mutation epic's existing INSERT/DELETE/UPDATE
entrypoints, per the epic's own Architecture Decision ("refresh-on-write,
not a new scheduler").

**Model chosen: EAGER, decided explicitly, not defaulted into.**
Recompute inline, synchronously, as part of the mutation call itself
returning — not LAZY ("mark stale, recompute on next match attempt").
The deciding factor: LAZY has no viable call site in this codebase as it
stands. The ONLY place a rollup is ever MATCHED is `ExecutionContext::
substitute_rollups`, called from `sql()`/`optimized_plan()` — both
`&self`. The only refresh mechanism (`register_rollup`, reused UNCHANGED
by this task) is `&mut self` (mutates `self.tables`/`self.catalog`).
Making LAZY possible would mean either changing `sql()`'s own signature
to `&mut self` — reaching the HTTP `/sql` handler, Arrow Flight, the
REPL, and every existing caller, far outside an S-M task's risk budget —
or wrapping `self.tables` in interior mutability (`Arc<RwLock<..>>`), a
genuinely NEW concurrency-control surface this codebase does not have
today, exactly the kind of new infrastructure this epic's own
Architecture Decisions steer away from. EAGER, by contrast, has a
natural, minimal-risk home: `insert_into_native_table`/
`delete_from_native_table`/`update_native_table` (native-tables-mutation
epic) are ALREADY `&mut self`, ALREADY `async fn`, and ALREADY
re-register the just-mutated base table before returning — calling
`register_rollup` again right there is a direct extension of an existing
pattern, not new infrastructure, and matches "always correct, even if
that means not fast" most literally: by the time a mutation call
RETURNS, every dependent rollup is either fresh or safely excluded from
matching — no window where a rollup sits in a "known-stale, not yet
being recomputed" limbo. Full reasoning:
`ExecutionContext::refresh_dependent_rollups`'s own doc comment
(`src/execution/context.rs`).

**Where this is wired — a deliberate layering choice worth stating
explicitly, since the task's own naming could be read as pointing
elsewhere**: `ExecutionContext::insert_into_native_table`/
`delete_from_native_table`/`update_native_table` (the SQL/REPL-reachable
entrypoints), immediately after each re-registers the mutated base
table — NOT inside `src/storage/native_write.rs`/`native_delete.rs`/
`native_update.rs` themselves. Those storage-layer modules have zero
SQL/registry awareness by design (no `Binder`, no `Optimizer`, no
`ExecutionContext` — confirmed by reading their imports, not assumed):
refreshing a rollup needs to know WHICH rollups depend on a table (only
`ExecutionContext::tables` is that registry) and re-parse/re-bind/
re-plan/re-execute SQL (`Binder`/`Optimizer`/`PhysicalPlanner`, none of
which the storage layer touches). This is the EXACT SAME reasoning task
001 already used to place the MATCHING mechanism in `ExecutionContext`
rather than an `OptimizerRule` — reapplied here to refresh. Zero changes
to `native_write.rs`/`native_delete.rs`/`native_update.rs` were needed or
made.

**Mechanism** (`ExecutionContext::refresh_dependent_rollups`, new,
private, called from all three mutation entrypoints — but SKIPPED
entirely when a mutation was itself a genuine no-op, since a mutation
that changed zero rows cannot have made any rollup stale): scans
`self.tables` for every `NativeTable` whose `manifest().rollup.base_table`
matches the just-mutated table (a cheap, no-recompute scan when there are
no dependents — the common case), then calls `Self::register_rollup`
again for each, using that rollup's OWN already-recorded `(name,
base_table, defining_sql)` — the SAME full-recompute mechanism a manual
`CREATE MATERIALIZED VIEW` re-run performs, reused completely UNCHANGED.
Multiple dependents are refreshed SEQUENTIALLY, not concurrently — a
deliberate memory-bounding choice (running N rollups' recomputes
concurrently would let N physical-execution working sets be resident at
once; sequential caps peak memory to ONE recompute's footprint regardless
of rollup count, at the cost of wall-clock time scaling with count,
measured below).

**Failure handling — never escalated into a mutation failure.** A
rollup's own write path (`native_write::write_batches` in `Overwrite`
mode, since the rollup directory already exists) stages a complete
replacement in a sibling directory and only atomically publishes it as
the LAST step, so any error during a refresh (I/O error, disk full,
permission denied, ...) leaves the rollup's existing manifest — still
recording its OLD, now-mismatched `base_table_version` — completely
untouched, which is exactly what keeps it correctly excluded from
matching by task 001's own staleness check. The base table's own
mutation, already published atomically BEFORE refresh is ever attempted,
is never rolled back or reported as failed just because a derived,
secondary artifact could not be recomputed.

**Provenance for the write side** (extending G3/PRD G5 from the read
side, which task 001 already covered via `QueryMetrics::rollup_answered`):
`InsertResult`/`DeleteResult`/`UpdateResult` all gained
`rollups_refreshed: Vec<RollupRefreshOutcome>` (`{ rollup_name,
error: Option<String> }`) — empty when the table has no dependents or the
mutation was a no-op, one entry per dependent otherwise, `error: Some(..)`
naming exactly why a refresh failed. The REPL prints a one-line summary
("refreshed rollup(s): ...") and a named `WARNING` for any failure —
never silently different from a plain mutation.

**Cell-exact validated** (`tests/native_rollup_refresh_tests.rs`, 7 new
tests, plus 1 renamed + 1 new test in `tests/native_rollup_tests.rs`,
`data/tpch-1mb`): each of INSERT/DELETE/UPDATE (through the real
`ExecutionContext` entrypoint) eagerly refreshes a dependent rollup and
the SAME query is STILL rollup-answered immediately afterward
(provenance-confirmed), cell-exact vs. an independently-mutated reference
context; TWO differently-shaped rollups on the same base table are BOTH
refreshed by ONE mutation, both cell-exact; a mutation against a table
with zero dependent rollups reports none refreshed; a genuine no-op
mutation does not touch the rollup's manifest at all (byte-for-byte
unchanged, asserted directly); and — the acceptance criterion's own
explicit "if refresh somehow fails" case — a REAL induced failure
(`native_table_root` made read-only mid-test, via `chmod`, after warming
up the base table's own lock file so ONLY the rollup's own Overwrite-mode
staging-directory creation fails, not the base table's mutation itself)
leaves the base table's own mutation successful, the rollup correctly
left stale (`rollups_refreshed[0].error.is_some()`), and the SAME query
correctly falling back to the base table with a cell-exact answer — never
silently serving stale rollup data. A separate test proves task 001's
staleness BOOKKEEPING itself is unaffected by this task: mutating the
base table via the LOW-LEVEL `storage::native_delete::
delete_from_native_table` function directly (bypassing `ExecutionContext`
and therefore this task's refresh wiring entirely) still correctly
leaves a dependent rollup stale and falling back — the mechanism task 001
built is a property of the manifest comparison, not of which code path
triggered a mutation.

One PRE-EXISTING test's assertion was intentionally changed, not
regressed: `native_rollup_tests.rs`'s staleness test asserted a mutated
base table's rollup "falls back and stays correct" — that was task 001's
own explicitly-named TEMPORARY behavior ("stays stale... until
register_rollup is called again manually... task 003's job"), and this
task closes exactly that gap for the `ExecutionContext` entrypoint the
test exercises. The test now asserts the CORRECT new behavior (the
SAME query is STILL rollup-answered, now with fresh data) and is
renamed accordingly; the ORIGINAL scenario/coverage (staleness
bookkeeping holding when nothing refreshes a rollup) is preserved by the
new low-level test described above, not silently dropped.

**Performance, measured, not assumed** (`examples/
native_rollup_refresh_perf_check.rs`, real SF=1 scale — `data/tpch-1gb`'s
`lineitem`, 6,000,000 rows, same fixture task 001's own cell-exact check
uses): a tiny (1-row) `INSERT` repeated 3x per premise, `InsertResult::
elapsed` (which now includes any eager refresh) reported for 0/1/3
registered rollups (three distinct low-cardinality GROUP BY shapes, each
a real, valid, exact-match rollup):

| rollups registered | avg `InsertResult::elapsed` | vs. 0-rollup baseline |
|---|---|---|
| 0 (baseline) | 8.01ms | 1.0x |
| 1 | 23.78ms | 3.0x (+15.76ms) |
| 3 | 45.14ms | 5.6x (+37.13ms) |

**Answer: YES, attaching a rollup meaningfully changes mutation
performance, in RELATIVE terms (3-5.6x), while staying small in
ABSOLUTE terms (tens of milliseconds) at this scale.** Cost scales
roughly LINEARLY with the NUMBER of dependent rollups (~12.4ms/rollup
here, consistent with sequential-not-concurrent refresh) and is
DOMINATED BY A FULL BASE-TABLE RESCAN PER ROLLUP, not by the mutation's
own delta size — this INSERT added exactly 1 row each time, yet paid
almost the identical refresh cost a much larger INSERT would, because
`register_rollup` has no concept of "what changed," it always
recomputes the WHOLE defining query from scratch. This is the direct,
honest cost of choosing EAGER, full-recompute refresh over a
(materially harder, out of this task's S-M scope) incremental/delta
merge — named plainly, not hidden. Extrapolating (not literally
re-measured, matching this doc's own established "extrapolated" framing
elsewhere): at a much larger base-table scale (e.g. SF=100), each
rollup's refresh cost would grow roughly with table size too, since the
mechanism is a full rescan regardless of scale — a real consideration
for anyone attaching MANY rollups to a FREQUENTLY-mutated, VERY LARGE
base table, though squarely inside the target use case this epic's own
PRD names ("many concurrent dashboard viewers hitting a known query
set") the absolute costs measured here are unlikely to matter in
practice.

**Memory safety, reasoned explicitly**: eager refresh briefly holds
"old and new" rollup data — but only ON DISK (the rollup's OLD live
directory and a NEW staging directory coexisting until
`publish_table_dir`'s atomic rename swaps them), never in memory at
once, and this is the SAME pre-existing bounded-overlap behavior every
Create/Overwrite/manual `register_rollup` re-run already has, not a new
pattern. The write side reuses task 003 of native-tables-foundation's
already-measured ~400MB-peak-RSS bounded streaming writer, UNCHANGED.
Sequential (not concurrent) multi-rollup refresh caps peak memory to
ONE recompute's footprint regardless of how many rollups are attached
(see "Mechanism" above) — no new unbounded-with-N-rollups path exists.

**Full suite, all four feature combinations, zero regression**: default
1243 (+8), lance 1308 (+8), gpu 1243 (+8), pulsar 1246 (+8) — each
exactly task 002's own baseline plus this task's 8 net-new tests (7 in
the new `native_rollup_refresh_tests.rs` + 1 net in
`native_rollup_tests.rs`, which gained one new test and had one existing
test's assertion updated per the note above), 0 failed anywhere.
`cargo fmt --all -- --check` clean. The mutation epic's own crash/
k-way-merge/resurrection-bug fix regression tests (`spill_tests.rs`,
`native_delete_tests.rs`, `native_update_tests.rs`) are unaffected —
confirmed by their continued 100% pass rate, not merely assumed safe
because the diff looks additive.

Reproduce: `scripts/claude-safe-build.sh cargo test --test
native_rollup_refresh_tests --test native_rollup_tests`;
`scripts/claude-safe-build.sh cargo build --release --example
native_rollup_refresh_perf_check && scripts/claude-safe-build.sh
./target/release/examples/native_rollup_refresh_perf_check` for the
performance table above.

### Materialized rollups: QA close-out (native-tables-rollups epic, task 004, 2026-08-26)

Final task of the epic. Headline: this is a real, working, cell-exact-
validated feature — not a partial or honest-negative-result epic like
`spill-join-correctness`. All three build tasks (001-003) met their own
gates and shipped real capability; this task's own broader validation
independently re-confirms that verdict at real scale rather than merely
repeating it, and closes two genuine coverage gaps tasks 001-003 left.

**Broader validation sweep** (`examples/native_rollup_multi_shape_check.rs`
+ `scripts/native_rollup_multi_shape_check.py`): task 001's own DuckDB-
oracle check validated exactly ONE rollup shape; task 003's own multi-
rollup test validated TWO rollups but only against a direct-computation
reference, never DuckDB. This task registers THREE distinctly-shaped
rollups (2/1/3 GROUP BY columns respectively, varied order; aggregate
sets from 2 to 4 functions, including MIN/MAX on a DATE column — new
coverage) simultaneously against one `lineitem_native` table (real SF=1
scale, `data/tpch-1gb`, 6,000,000 rows), all via the real `CREATE
MATERIALIZED VIEW` DDL text — deliberately exercising "a rollup
depending on a table that also has other rollups" at real scale. Each is
queried with a differently-phrased query (GROUP BY order/aliases/
SELECT-list order all varied) through ordinary `sql()`, provenance-
confirmed, and compared THREE ways with this repo's own established
float tolerance (`tol = max(0.02, |v|*1e-9)`): rollup-answered vs.
DuckDB, direct base-table computation vs. DuckDB, and rollup-answered
vs. direct computation directly. **Result: PASS, all 3 shapes, all 3
comparisons, real SF=1 scale.**

**Fallback-correctness sweep** (`tests/native_rollup_qa_closeout_tests.rs`,
8 tests): task 002's own tests never touch mutation; task 003's own
refresh tests register every rollup via `register_rollup`, never via the
DDL — no prior test combined "rollup registered via real DDL text" with
"base table mutated via real DML text, refresh fires, a subsequent
ordinary query is still correctly answered." This file closes that gap:
a `CREATE MATERIALIZED VIEW`-registered rollup correctly falls back for
4 distinct non-matching shapes (different GROUP BY set, added filter,
different aggregate, different base table), and correctly survives an
INSERT-, a DELETE-, and an UPDATE-triggered eager refresh — each through
the real mutation SQL entrypoints (`insert_into_native_table`/etc.) and
the ORDINARY `sql()` path afterward, cell-exact vs. an independently-
mutated reference context. A final test registers TWO DDL-created
rollups on one table and confirms one mutation refreshes both. Per task
002's own established precedent (no real HTTP-server round-trip test
there either, validated at the `ExecutionContext::sql()` level — the
exact function the HTTP handler calls), this file follows the identical,
already-established test depth rather than standing up a real `serve`
process. **Result: PASS, 8/8.**

**A real, pre-existing, general SQL binder bug found and fixed** —
unrelated to rollups or native tables specifically, but surfaced by this
task's own broader sweep (query B's `ORDER BY l_shipmode` after `l_shipmode
AS mode` placed LAST in the SELECT list crashed with "Column not found").
Root cause: `extend_projection_for_sort` (`src/planner/binder.rs`) had a
blanket bailout for any `Project` whose child is an `Aggregate`, added
2026-08-09 alongside the function itself (the vector-search feature) as a
conservative, never-revisited scope limit — `bind_order_by` never
validates a bare identifier against the schema at bind time, so with no
rescue the Sort node's own input silently lacked its sort key and physical
execution failed. **Confirmed general, not rollup-specific**: reproduces
on plain in-memory TPC-H data with zero rollup involved
(`query_engine sql "SELECT COUNT(*) AS cnt, ..., l_shipmode AS mode FROM
lineitem GROUP BY l_shipmode ORDER BY l_shipmode"`). Fixed by removing the
blanket bailout: the function's own resolve-based check (a few lines
below it, unconditionally applied) already safely gates every widened
column — for an Aggregate input that check can only ever succeed for an
already-computed per-group scalar (a GROUP BY key or an aggregate's own
output), never a raw pre-aggregation column, so GROUP BY semantics cannot
be violated by removing the bailout. Regression test:
`test_order_by_group_key_under_its_original_name_when_aliased_in_select`
in `tests/sql_comprehensive.rs`, asserting both that the query no longer
crashes AND that the sort order/values are correct, not merely "doesn't
panic."

**Full suite, all four feature combinations, through
`scripts/claude-safe-build.sh`, re-confirmed at HEAD (not merely trusted
from tasks 001-003's own prior reports)**:

| combo | task 003 baseline | this task | delta | failed |
|---|---|---|---|---|
| default | 1243 | **1252** | +9 | 0 |
| lance | 1308 | **1317** | +9 | 0 |
| gpu | 1243 | **1252** | +9 | 0 |
| pulsar | 1246 | **1255** | +9 | 0 |

Every combination is exactly task 003's own baseline plus this task's 9
new tests (8 in `native_rollup_qa_closeout_tests.rs` + 1 in
`sql_comprehensive.rs`) — zero regression anywhere, confirmed by exact
arithmetic. `cargo fmt --all -- --check` clean. **M1 and M2 distributed
gates re-run** (`scripts/cluster_local.sh verify` / `verify-m2`) — not
skipped this time, unlike tasks 001-003 (which had zero `src/distributed/`
changes and correctly didn't need to): this task's own `binder.rs` fix
touches shared planning code every query path uses, including gather/
scatter, so both gates were re-confirmed PASS with a real 3-process
cluster (5/5 M1 checks incl. Flight==HTTP parity and mid-query SIGTERM
survival; 4/4 M2 checks incl. 13 gather-path queries covering joins/
subqueries/DISTINCT/ORDER BY+LIMIT/STDDEV/CTE, all cell-exact vs. DuckDB).

**G1-G5 (this epic's own Success Criteria) — verdicts with evidence**:
- **G1** (a rollup can be defined, populated, and an exact-matching query
  is transparently answered from it — cell-exact vs. both direct
  base-table computation and an independent DuckDB oracle) — **MET**.
  Task 001's own DuckDB-oracle check (SF=1, one shape) plus this task's
  own broader 3-shape sweep (also SF=1, via the real DDL surface) both
  independently confirm this at real scale.
- **G2** (a non-matching query or a stale rollup correctly falls back to
  the normal base-table plan — never silently wrong, never silently
  stale) — **MET**. Task 001's 3 non-matching-shape tests + 1 staleness
  test; this task's own 4 additional DDL-registered non-matching-shape
  tests plus 3 mutation-triggered-refresh tests, all cell-exact.
- **G3** (provenance is always visible when a rollup answers a query —
  never indistinguishable from the base-table path) — **MET**.
  `QueryMetrics::rollup_answered` (read side, task 001) and
  `RollupRefreshOutcome`/`{Insert,Delete,Update}Result::rollups_refreshed`
  (write side, task 003) are structured, directly-checkable fields —
  every test in the epic (34+11+8+9) asserts on them directly, never
  infers provenance from timing or side effects.
- **G4** (refresh model is explicit and documented; validated against the
  mutation epic's existing INSERT/DELETE/UPDATE paths with no
  regression) — **MET**. EAGER chosen and justified in
  `refresh_dependent_rollups`'s own doc comment (task 003); this task's
  own full-suite run re-confirms the mutation epic's own regression tests
  (`spill_tests.rs`, `native_delete_tests.rs`, `native_update_tests.rs`)
  are still 100% green, and this task's new multi-rollup DDL test adds
  one more independent confirmation of the multi-rollup case.
- **G5** (full suite green; PRD status updated) — **MET**. Table above;
  `.claude/prds/native-tables.md`'s status note updated by this task.

**Bottom line**: the epic is complete. Scope out of this epic, named
consistently across all four tasks: subsumption/coarser-grouping matching
(only exact-shape rollups match), Gravitino metastore discoverability
(native tables aren't reachable through Gravitino at all — pre-existing
gap), distributed rollups, any new background scheduler, and non-native
base tables. Full epic close-out: `.claude/epics/archived/
native-tables-rollups/epic.md`.

Reproduce: `scripts/claude-safe-build.sh cargo build --release --example
native_rollup_multi_shape_check && scripts/claude-safe-build.sh
./target/release/examples/native_rollup_multi_shape_check && .venv/bin/
python scripts/native_rollup_multi_shape_check.py` (broader sweep);
`scripts/claude-safe-build.sh cargo test --test
native_rollup_qa_closeout_tests` (fallback sweep); `scripts/
cluster_local.sh start 3 && scripts/cluster_local.sh verify && scripts/
cluster_local.sh verify-m2 && scripts/cluster_local.sh stop` (M1/M2).

### Current limitations (explicit, matching this epic's own G5 boundary and the PRD's phase plan)

- **No filter/row-group pruning at scan level.** `NativeTable::
  scan_with_filter` has no predicate pushdown at all; every query reads
  every active segment in full and relies on a post-scan `FilterExec` for
  correctness. This has a real, measured cost at scale, found by phase
  1's own QA close-out (task 008): parquet's row-group statistics let it
  skip most of the work for date-range-filtered queries before a join
  ever sees those rows; native tables cannot, so their post-filter join
  inputs are larger. For 3 of 22 TPC-H queries (Q4, Q12, Q13 — at
  scale-dependent thresholds: only Q12 at SF=10, all three at SF=100)
  this pushes a join's build side across the `SpillableHashJoinExec`
  spill threshold, which (a pre-existing characteristic of that
  operator, not introduced by either epic — see its own doc comment)
  fully materializes the build side before deciding to spill, then
  spills as many small Parquet files. Root-caused with a live `gdb`
  thread dump (caught a thread inside `parquet::column::writer`
  mid-query — direct evidence, not inference) and filesystem evidence
  (`/tmp/query_engine_spill/join_*/build_*.parquet`, hundreds of files,
  actively growing).
  **CORRECTION (native-tables-mutation epic, task 006, 2026-08-24):
  phase 1's own claim directly above this line — "always a safe, clean
  refusal or a slow-but-correct completion — never wrong data" — is NOT
  always true, and is superseded by this finding.** Task 006 found and
  fixed a real, previously-masked crash in this exact spill path
  (Dictionary-vs-declared-schema mismatch — see "Mutation: QA close-out"
  above for the full story); once that crash was fixed, Q12 at SF=10
  was revealed to complete but return a WRONG ANSWER (`high_line_count`
  exactly 2x-inflated) after ~320 SECONDS — a SEPARATE, deeper,
  NOT-YET-ROOT-CAUSED bug in `SpillableHashJoinExec`'s partition/spill
  algorithm itself, found but deliberately NOT fixed (judged too large
  for a same-task fix; see task 006's Outcome for the full investigation
  and reasoning). Closing this for real now has three real levers, not
  two: scan-level pruning for native tables, a streaming rewrite of the
  join spill path (both already-named, separately-scoped future work),
  or — now the more urgent one — root-causing and fixing the
  duplicate-counting bug in the EXISTING partition/spill algorithm,
  which affects ANY sufficiently large spilling INNER join
  (Parquet/Iceberg/Lance too, not native-table-specific) and should be
  treated as a P0 correctness bug by whichever of the three a future
  epic picks up first.
  **Further update (`spill-join-correctness` epic, closed 2026-08-25):**
  the O(n²) read-rewrite-rename-per-append pattern that was the
  dominant cost of "spills as many small Parquet files" above is now
  **FIXED** — see "Mutation: QA close-out"'s own epic-close-out
  paragraph above for the full number (**~40-90x** on the Q12 repro).
  This was always a separate mechanism from the wrong answer, confirmed
  not assumed, and fixing it left the duplicate-counting bug untouched.
  That bug remains **OPEN, root cause still unconfirmed**, now with a
  tighter rate estimate (0.34%, 1/290 trials epic-wide, vs. this
  section's own earlier 4.8%/1/21 standalone estimate — see "Mutation:
  QA close-out" for the full range and why both numbers are reported)
  and **empirically confirmed NOT to be native-table-specific** (plain
  parquet forced into the identical spill path: 0/80 wrong,
  statistically indistinguishable from native's own 0/80) and
  **confirmed also reachable via distributed (scatter) execution**. Of
  the three levers named just above, native-table scan-level pruning
  would therefore NOT close this bug even if built — only the
  join-spill streaming rewrite or directly root-causing the
  duplicate-counting mechanism can. Full detail: `.claude/epics/
  archived/spill-join-correctness/epic.md`.
- **No compaction** (native-tables-mutation epic, confirmed OUT OF SCOPE
  by design — task 001's Decision 3, not merely deferred incidentally). A
  deletion vector is correctness-preserving indefinitely — reads always
  apply the filter; they just do increasing filtering work as deletes
  accumulate. Named, honest cost: segment count grows by at least one
  per `Append`/`INSERT` statement forever (no merging of small
  segments), and disk space from partially-deleted rows is never
  physically reclaimed. One narrow, IN-SCOPE exception, not full
  compaction: a segment tombstoned to 100%
  (`deleted_rows.len() == row_count`) is dropped from the manifest
  outright (task 003) — measured effective, not just designed for
  (5/5 fully-tombstoned segments actually dropped in task 005's
  adversarial run, ~9.1KB/segment saved vs. what would have persisted
  without the exception). Task 005's adversarial testing found and
  quantified two residual risks a future compaction epic should size
  against — real, but neither urgent at this program's current scale
  nor fixed in this epic:
  - **Deletion-vector encoding density at very large segment counts.**
    A broad, shallow DELETE (many segments, each lightly touched) costs
    ~13.1-13.7 bytes per `deleted_rows` JSON entry (measured, stable
    across a 313-segment and a 2573-segment run). Extrapolated — not
    literally re-measured at this scale — to task 001's own original
    "1000 segments x 1,000,000 rows x 1% deleted" worry: ~131MB, larger
    than that design-time "tens of MB" guess. Every scale this program's
    own fixtures/benchmarks actually reach today stays sub-2MB.
  - **O(N²) cumulative manifest-rewrite cost across a long mutation
    sequence.** Every single Append/Delete/Update unconditionally
    re-reads AND re-writes the WHOLE `_manifest.json`, so per-mutation
    latency grows roughly linearly with segment count — measured
    ~0.44ms/op near-empty to ~8.8ms/op at 2500+ segments across a real
    3000-operation mixed Append/Delete/Update sequence. Fine at the
    scales exercised so far; a real ceiling for a table that
    accumulates thousands of small mutations over its lifetime without
    ever compacting.
- **Single-writer only** (native-tables-mutation epic task 001's
  Decision 5). No lock manager, no WAL, no MVCC — a mutation
  (INSERT/DELETE/UPDATE) holds an exclusive `std::fs::File::try_lock()`
  on a sibling `<table>.lock` file for its whole
  read-identify-write-publish span. A concurrent writer gets an
  immediate, clean, named `QueryError::Storage` — never blocks, never
  corrupts (verified live: two real OS processes racing an Append,
  exactly one succeeds, 3/3 runs). Process-crash-safe: the lock is a
  kernel-managed `flock`, released automatically the instant a holder
  dies for ANY reason including `SIGKILL` — verified with a real
  external `kill -9` sent mid-mutation, 6/6 runs, zero flakes, manifest
  byte-for-byte unchanged and the table immediately writable again
  afterward. Explicit scope boundary, matching — not narrowing — phase
  1's own: this is NOT `fsync`/power-loss durability beyond the OS's own
  page-cache behavior (neither this epic's atomic single-file manifest
  rename nor phase 1's own directory-level `publish_table_dir` ever call
  `fsync`/`sync_all`). Readers never lock (writer-vs-writer only).
  **Two distinct atomic-publish mechanisms, not one** — worth stating
  plainly so a future reader doesn't assume a single mechanism covers
  every write mode: `Create`/`Overwrite` still use phase 1's
  whole-DIRECTORY `rename()` (`native_manifest::publish_table_dir`,
  unchanged); `Append`/`DELETE`/`UPDATE` use this epic's single-FILE
  `rename()` of a freshly-written manifest onto the live
  `_manifest.json` (`native_write::publish_manifest_update` /
  `write_manifest_atomic`) — chosen because a directory-level replace
  would silently delete every pre-existing segment an incremental write
  doesn't re-copy.
- **No distributed participation yet — for reads OR mutation.** A
  native table registers and reads correctly on a single `serve` node
  (including via `--tables` auto-detection), and `distributed_splits`/
  `shard_by_splits` are real, non-`None` implementations (one `Split`
  per segment) — but multi-node SCATTER/GATHER planning for native
  tables is explicitly out of scope for the foundation epic's own G5
  criterion (only requires NOT breaking existing parquet/Iceberg/Lance
  distributed behavior, confirmed by the M1/M2 gates) and has not been
  validated on a real cluster. `INSERT`/`DELETE`/`UPDATE` (this epic)
  inherit the identical boundary and go further: every mutation
  entrypoint (`ExecutionContext::insert_into_native_table` etc.) is a
  single-process, single-`ExecutionContext`-session operation with no
  distributed-write story of any kind — not reachable from `serve`'s
  HTTP/Flight surface at all, matching CREATE TABLE's own pre-existing
  boundary from phase 1.
- **GPU/RAM/disk tiering is not built** — phase 3 of the PRD. This epic
  only kept the GPU-offload identity hook open (above) so that work
  isn't blocked from zero; native-tables-mutation task 006 confirmed
  offload still engages correctly (VRAM growth + correct post-mutation
  values) against a table with a non-empty deletion vector — see
  Benchmarks below.
- **Materialized rollups (phase 4 of the PRD) have their core matching/
  substitution mechanism built and cell-exact validated, a SQL DDL
  surface on top of it, AND automatic refresh-on-write** (native-tables-
  rollups epic, ALL FOUR tasks 001-004, now complete and archived — see
  above and the epic's own close-out at `.claude/epics/archived/
  native-tables-rollups/epic.md`) — a mutated base table's
  dependent rollup(s) are now EAGERLY, automatically refreshed as part of
  the INSERT/DELETE/UPDATE call itself, no manual re-registration needed
  (task 003). Still only single-base-table/unfiltered/exact-match shapes;
  still no `ALTER`/`DROP`/`REFRESH MATERIALIZED VIEW` (re-running `CREATE
  MATERIALIZED VIEW` under the same name, or letting a mutation's own
  automatic refresh do it, are the only "refresh" available today);
  registration is not reachable via HTTP `/sql`/Flight (mirrors CTAS's
  own identical boundary — only the subsequent matching/answering step
  is, and now also the automatic refresh, since that lives inside the
  same `ExecutionContext::insert_into_native_table`/etc. entrypoints CTAS
  already established this boundary for); no subsumption/coarser-
  grouping matching (explicitly out of scope for the whole epic); refresh
  cost is a FULL base-table rescan per dependent rollup, not an
  incremental/delta merge (measured 3-5.6x mutation latency at SF=1 with
  1-3 rollups registered — see task 003's own section above for the full
  numbers and reasoning).

### Benchmarks (2026-08-23, task 008 close-out)

Both scales load `data/tpch-{10gb,100gb}` (plain parquet) into native
tables via `write-native --from-parquet`, then compare the ENGINE
reading its OWN native tables against DuckDB reading the SAME source
data two ways — plain parquet (`read_parquet` views) and, at SF=10 only
(no SF=100 Iceberg fixture exists), the engine's own Iceberg tables
(`data/tpch-10gb-iceberg`, `iceberg_scan`) — per this program's standing
"report every premise separately" convention.

| scale | queries | engine total | vs DuckDB-parquet | vs DuckDB-iceberg |
|---|---|---|---|---|
| SF=10 | 22/22 cell-exact | **5.324s** | 4.321s → **1.23x** | 6.888s → **0.77x (engine faster)** |
| SF=100 | 19/22 cell-exact + successful (Q4/Q12/Q13: see Limitations) | **75.17s** | 50.02s (same 19) → **1.50x** | n/a (no SF=100 Iceberg fixture) |

Disk footprint, both scales: smaller than the parquet source it was
loaded from (SF=10: 6.5GB vs 9.6GB; SF=100: 65GB vs 97GB) — dictionary
coercion of low-cardinality strings outweighs parquet's own compression
here. Reproduce: `.venv/bin/python scripts/native_bench_compare.py
--write --source-dir data/tpch-10gb --native-dir data/tpch-10gb-native
--binary target/release/query_engine` then `scripts/
native_bench_compare.py --native-dir data/tpch-10gb-native --source-dir
data/tpch-10gb --iceberg-dir data/tpch-10gb-iceberg --sf 10
--memory-limit 40G --iterations 2` (SF=100: `--memory-limit 100G`, no
`--iceberg-dir`, and exclude Q4/Q12/Q13 via `--queries` if a bounded run
is wanted — see Limitations for why).

**CPU vs GPU split** (`--features gpu`, per this program's standing
convention of reporting these as separate rows): see the "GPU Aggregate
Offload" section's own CPU/GPU-split subsection below for the combined
parquet+native-table finding — task 007 confirmed the offload path is
reachable for native tables (identity plumbing unit-tested end to end);
task 008 measured whether it produces a full-query win at native-table
scale.

## Recently Implemented Features

- **Hardware Topology Awareness** (2026-08-09) — `src/execution/topology.rs`
  - Startup sysfs probe: NUMA nodes + cpulists + SLIT distances, SMT siblings,
    per-CPU performance weight from `cpuinfo_max_freq` (ARM `cpu_capacity`
    fallback). Classes are DERIVED — no CPU model numbers anywhere. Intersected
    with the process affinity mask, so `taskset` shrinks the topology.
  - `query_engine topology` prints the detected layout and placement.
  - API: `Topology::get()`, `num_numa_nodes()`, `cpus_for_node()`, `distance()`,
    `total_weight()`, `is_uniform()`, `preferred_cpu_order()`, `fast_cpus()`,
    `core_siblings()`; free functions `init_global_pool()`, `node_pool()`,
    `preferred_node_for()`, `first_touch()`, `workers_for()`,
    `pin_current_thread_to()`, `set_thread_affinity()`, `current_cpu()`.
  - Env: `QE_TOPOLOGY=0` disables the placed pool; `QE_PLACEMENT=cpu|core|node`
    selects binding tightness (default `node`).
  - **THIS DEV BOX HAS NO NUMA** (`numactl`: 1 node). It is a hybrid
    i9-13900KF: CPUs 0-15 = 8 P-cores with SMT (5.5-5.8 GHz), CPUs 16-31 = 16
    E-cores (4.3 GHz). Every NUMA branch is a no-op at one node, is unit-tested
    against synthetic sysfs fixtures (fake 2-socket / hybrid / empty container),
    and **has never run on real multi-socket silicon**.
  - Measured and REJECTED as defaults: per-CPU pinning (Q02 -16.2% but Q11
    +22.7%, Q06 +8.2%) and per-core pinning (Q06 +16.2%). Default `node`
    placement A/Bs flat (-0.2% over 22 queries) — it is free here and is the
    right rule on a real server.
  - `workers_for(work_units, max)` sizes fan-outs to available work; used by
    the morsel aggregate drivers and `ParallelParquetSource` (`total_work()`).
    Merge shard count now scales with group count (`merge_shard_count`).
    Suite effect at SF=10: -1.0% (Q02 -16%, Q12 -9%, Q19/Q11 -7%, Q14 -7%).
  - **What does NOT work on a hybrid box**: any static core-class policy. The
    E-cores help long CPU-bound queries (Q17/Q20/Q07/Q03/Q08 gain 16-29% from
    them) and hurt short ones (Q02/Q11/Q14 lose 12-25%). Restricting to
    P-cores wins some and loses more. Reducing the global thread count does
    nothing (`RAYON_NUM_THREADS=16` on 32 CPUs: +-1%).

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
| Window execution | `src/physical/operators/window.rs` |
| Window/grouping DuckDB gate | `scripts/window_validate.py` |
| SQL feature probe | `scripts/sql_feature_probe.py` |
| Morsel aggregation operator | `src/physical/operators/morsel_agg.rs` |
| Morsel framework | `src/physical/morsel.rs`, `src/physical/morsel_agg.rs` |
| Subquery execution | `src/physical/operators/subquery.rs` |
| DelimJoin operators | `src/physical/operators/delim_join.rs` |
| Flatten dependent join rule | `src/optimizer/rules/flatten_dependent_join.rs` |
| Main entry point | `src/execution/context.rs` |
| Parquet table provider | `src/storage/parquet.rs` |
| Lance table provider | `src/storage/lance.rs` (feature `lance`) |
| Lance writer (CTAS, parquet→lance, index) | `src/storage/lance_write.rs` (feature `lance`) |
| Lance tests | `tests/lance_tests.rs` (feature `lance`) |
| Parquet→Lance conversion (Rust) | `write-lance --from-parquet` |
| Parquet→Lance conversion (Python) | `scripts/lance_convert.py` |
| Nested-column Lance fixture | `scripts/lance_nested_gen.py` |
| Versioned Lance fixture | `scripts/lance_versions_gen.py` |
| DuckDB-over-Lance oracle/baseline | `scripts/duckdb_lance_bench.py` |
| Streaming Parquet reader | `src/storage/parquet.rs` (StreamingParquetReader) |
| Async Parquet reader | `src/storage/parquet.rs` (AsyncParquetReader) |
| Server mode (`serve`) | `src/distributed/server.rs` |
| Arrow Flight endpoint | `src/distributed/flight.rs` |
| Flight integration tests | `tests/flight_tests.rs` |
| Flight acceptance gate (pyarrow) | `scripts/flight_validate.py` |
| Peer discovery / membership | `src/distributed/membership.rs` |
| M1 cluster tests | `tests/distributed_cluster.rs` |
| Local N-process cluster harness | `scripts/cluster_local.sh` |
| Kubernetes manifests (unvalidated) | `k8s/`, `Dockerfile`, `kind-cluster.yaml` |
| Manifest checks without a cluster | `scripts/validate_k8s_manifests.py` |
| kind end-to-end test (needs Docker) | `scripts/kind_test.sh` |
| TableProvider trait | `src/physical/operators/scan.rs` |
| Memory pool/config | `src/execution/memory.rs` |
| Hardware topology / NUMA / core classes | `src/execution/topology.rs` |
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
