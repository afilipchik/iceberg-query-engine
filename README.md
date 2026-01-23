# Query Engine

A high-performance SQL query engine built from scratch in Rust, designed for analytical workloads on columnar data formats.

## Features

- **Full SQL Support**: SELECT, JOIN, GROUP BY, ORDER BY, LIMIT, subqueries, and more
- **TPC-H Benchmark**: All 22 TPC-H queries supported and tested
- **Parquet Support**: Read Parquet files and directories directly
- **Interactive REPL**: SQL shell with history and tab completion
- **Streaming Execution**: Memory-efficient processing via Arrow RecordBatch streams
- **Query Optimization**: Predicate pushdown, projection pushdown, constant folding

## Building

### Prerequisites

- Rust 1.70+ (install via [rustup](https://rustup.rs/))

### Build

```bash
# Debug build
cargo build

# Release build (recommended for benchmarks)
cargo build --release
```

The binary will be at `./target/release/query_engine`.

## Testing

```bash
# Run all tests
cargo test

# Run specific module tests
cargo test parser
cargo test planner
cargo test optimizer
cargo test physical
cargo test tpch

# Run with output
cargo test -- --nocapture
```

## Quick Start

### 1. Generate Test Data

Generate TPC-H benchmark data in Parquet format:

```bash
# Small dataset (~10MB)
./target/release/query_engine generate-parquet --sf 0.01 --output ./data/tpch-10mb

# Medium dataset (~100MB)
./target/release/query_engine generate-parquet --sf 0.1 --output ./data/tpch-100mb

# Large dataset (~1GB)
./target/release/query_engine generate-parquet --sf 1.0 --output ./data/tpch-1gb
```

### 2. Start Interactive SQL Shell

```bash
# Start REPL with TPC-H tables preloaded
./target/release/query_engine repl --tpch ./data/tpch-10mb
```

Once in the REPL:

```sql
sql> .tables
Registered tables:
  nation (4 columns)
  region (3 columns)
  customer (8 columns)
  orders (9 columns)
  lineitem (16 columns)
  ...

sql> SELECT COUNT(*) FROM lineitem;
+----------+
| COUNT(*) |
+----------+
| 60175    |
+----------+
(1 rows in 12.345ms)

sql> SELECT l_returnflag, SUM(l_quantity) as total_qty
     FROM lineitem
     GROUP BY l_returnflag
     ORDER BY l_returnflag;

sql> .quit
```

### REPL Commands

| Command | Description |
|---------|-------------|
| `.help` | Show help message |
| `.quit` | Exit the shell |
| `.tables` | List registered tables |
| `.schema <table>` | Show table schema |
| `.load <path> <name>` | Load Parquet file as table |
| `.tpch <path>` | Load all TPC-H tables |

## CLI Commands

### Run Single Query

```bash
# Run TPC-H query #1
./target/release/query_engine query --num 1 --sf 0.01

# Show query plan
./target/release/query_engine query --num 1 --sf 0.01 --plan
```

### Run Custom SQL

```bash
./target/release/query_engine sql "SELECT * FROM lineitem LIMIT 10" --sf 0.01
```

### Run Benchmark

```bash
# Benchmark with in-memory data
./target/release/query_engine benchmark --sf 0.01 --iterations 3

# Benchmark with Parquet files
./target/release/query_engine benchmark-parquet --path ./data/tpch-100mb --iterations 3
```

### Load Parquet Files

```bash
# Load single file
./target/release/query_engine load-parquet \
    --path ./data/orders.parquet \
    --name orders \
    --query "SELECT COUNT(*) FROM orders"

# Load directory
./target/release/query_engine load-parquet \
    --path ./data/tpch-10mb \
    --name lineitem \
    --query "SELECT * FROM lineitem LIMIT 5"
```

## Architecture

```
SQL Query
    │
    ▼
┌─────────────┐
│   Parser    │  SQL text → AST
└─────────────┘
    │
    ▼
┌─────────────┐
│   Binder    │  AST → Logical Plan
└─────────────┘
    │
    ▼
┌─────────────┐
│  Optimizer  │  Rule-based optimization
└─────────────┘
    │
    ▼
┌─────────────┐
│  Physical   │  Logical → Physical Plan
│   Planner   │
└─────────────┘
    │
    ▼
┌─────────────┐
│  Execution  │  Stream<RecordBatch>
└─────────────┘
    │
    ▼
  Results
```

## Project Structure

```
src/
├── parser/          # SQL parsing (sqlparser-rs)
├── planner/         # Query planning and binding
├── optimizer/       # Query optimization rules
├── physical/        # Physical operators (scan, filter, join, etc.)
├── storage/         # Table providers (Parquet, Iceberg planned)
├── execution/       # Execution context and utilities
└── tpch/            # TPC-H benchmark queries and data generator
```

## Dependencies

| Crate | Purpose |
|-------|---------|
| `arrow` | Columnar data format |
| `parquet` | Parquet file I/O |
| `sqlparser` | SQL parsing |
| `tokio` | Async runtime |
| `clap` | CLI framework |
| `rustyline` | REPL line editing |

## Performance

Benchmark results on TPC-H SF=0.1 (100MB dataset):

```
All 22 TPC-H queries: ~70 seconds total
Average query time: ~3 seconds
```

*Results measured on Apple M1, single-threaded execution*

## Roadmap

- [x] Parquet file support
- [x] Interactive SQL REPL
- [ ] Apache Iceberg table support
- [ ] Parallel execution
- [ ] Cost-based optimization
- [ ] Window functions

## License

Apache-2.0
