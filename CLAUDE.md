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
- Not done (documented): eviction/QE_GPU_CACHE_MB cap, GPU joins, GPU
  parquet decode, Lance/Iceberg providers, distributed-worker GPU.

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
tiering, and materialized rollups, none built yet). Generalizes the
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

### Current limitations (explicit, matching this epic's own G5 boundary and the PRD's phase plan)

- **No filter/row-group pruning at scan level.** `NativeTable::
  scan_with_filter` has no predicate pushdown at all; every query reads
  every active segment in full and relies on a post-scan `FilterExec` for
  correctness (always cell-exact — never a wrong answer). This has a
  real, measured cost at scale, found by this epic's own QA close-out
  (task 008): parquet's row-group statistics let it skip most of the
  work for date-range-filtered queries before a join ever sees those
  rows; native tables cannot, so their post-filter join inputs are
  larger. For 3 of 22 TPC-H queries (Q4, Q12, Q13 — at scale-dependent
  thresholds: only Q12 at SF=10, all three at SF=100) this pushes a
  join's build side across the `SpillableHashJoinExec` spill threshold,
  which (a pre-existing characteristic of that operator, not introduced
  by this epic — see its own doc comment) fully materializes the build
  side before deciding to spill, then spills as many small Parquet
  files, and refuses outright rather than guessing when a non-INNER
  join's build side is the one that's oversized. Root-caused with a live
  `gdb` thread dump (caught a thread inside `parquet::column::writer`
  mid-query — direct evidence, not inference) and filesystem evidence
  (`/tmp/query_engine_spill/join_*/build_*.parquet`, hundreds of files,
  actively growing). Always a safe, clean refusal or a slow-but-correct
  completion — never wrong data. Closing this for real means either
  scan-level pruning for native tables or a streaming rewrite of the
  join spill path — both real, separately-scoped future work, not a
  same-task fix.
- **No distributed participation yet.** A native table registers and
  reads correctly on a single `serve` node (including via `--tables`
  auto-detection), and `distributed_splits`/`shard_by_splits` are real,
  non-`None` implementations (one `Split` per segment) — but multi-node
  SCATTER/GATHER planning for native tables is explicitly out of scope
  for this epic (its own G5 criterion only requires NOT breaking existing
  parquet/Iceberg/Lance distributed behavior, confirmed by the M1/M2
  gates) and has not been validated on a real cluster.
- **GPU/RAM/disk tiering and materialized rollups are not built** —
  phases 3 and 4 of the PRD. This epic only kept the GPU-offload identity
  hook open (above) so that work isn't blocked from zero.

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
