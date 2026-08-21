# Query Engine

A high-performance SQL query engine built from scratch in Rust, designed for analytical workloads on columnar data formats.

## Features

- **Full SQL Support**: SELECT, JOIN, GROUP BY, ORDER BY, LIMIT, UNION, and more
- **Trino SQL Compatibility**: 100+ functions including math, string, date/time, regex, JSON, and aggregates
- **Correlated Subqueries**: EXISTS, NOT EXISTS, IN, NOT IN, scalar subqueries
- **TPC-H Benchmark**: All 22 TPC-H queries passing (160+ SQL tests total)
- **Parquet Support**: Read Parquet files and directories directly
- **Lance Support** (feature `lance`): Read and write Lance datasets, with version time travel and vector search
- **Iceberg Support**: Read Apache Iceberg tables (v1/v2 metadata, Avro manifests, snapshot time travel)
- **Vector Search**: k-NN over embedding columns (`cosine_distance`, `l2_distance`, …), exact by default with opt-in IVF_PQ index pushdown
- **Larger-Than-Memory**: Spillable operators for datasets exceeding available RAM
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

### Optional: Lance support

The Lance reader/writer is behind the `lance` feature and needs `protoc`
(lance-table's build script compiles `.proto` files):

```bash
# e.g. apt install protobuf-compiler / brew install protobuf
cargo build --release --features lance
# or point at a local protoc:
PROTOC=/path/to/protoc cargo build --release --features lance
```

The `lance` crate is pinned to 0.23.x deliberately — it is the last version
built against arrow 53, the arrow major this engine uses. Do not bump it.

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
| `.lance <path> <name> [version]` | Load Lance dataset, optionally at a historical version (feature `lance`) |
| `.lance-versions <path>` | List a Lance dataset's versions (feature `lance`) |
| `.tpch-lance <path>` | Load all TPC-H tables from Lance datasets (feature `lance`) |
| `.mode <format>` | Set output format (table, csv, json, vertical) |

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

### Lance Datasets (feature `lance`)

Read, write, time-travel and benchmark Lance datasets:

```bash
# Load a Lance dataset and query it
./target/release/query_engine load-lance \
    --path ./data/orders.lance --name orders \
    --query "SELECT COUNT(*) FROM orders"

# Convert Parquet to Lance, streamed (never materializes either side)
./target/release/query_engine write-lance \
    --from-parquet ./data/tpch-10mb --out ./data/tpch-10mb-lance

# CREATE TABLE AS SELECT, in effect: run SQL and write the result
./target/release/query_engine write-lance \
    --sql "SELECT * FROM orders WHERE o_totalprice > 100000" \
    --tables ./data/tpch-10mb --out ./data/big_orders.lance

# Time travel: list versions, query an old one
./target/release/query_engine lance-versions --path ./data/orders.lance
./target/release/query_engine load-lance --path ./data/orders.lance \
    --name o --version 1 --query "SELECT COUNT(*) FROM o"

# TPC-H benchmark over Lance datasets
./target/release/query_engine benchmark-lance \
    --path ./data/tpch-10mb-lance --iterations 3
```

Vector columns (`FixedSizeList<Float32, N>` embeddings) are searchable in SQL:

```sql
SELECT id, category, text FROM vectors
ORDER BY cosine_distance(embedding, [0.013, -0.041, ...])
LIMIT 10;
```

Results are exact (brute force) by default. Build an IVF_PQ index with
`create-lance-index` and set `QE_VECTOR_SEARCH=indexed` to opt into
approximate index-backed search (~20x faster, recall ≈ 0.9).

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
├── physical/        # Physical operators (scan, filter, join, aggregate, subquery, etc.)
├── storage/         # Table providers (Parquet, Lance, Iceberg)
├── execution/       # Execution context and utilities
├── metastore/       # Branching metastore REST API client
└── tpch/            # TPC-H benchmark queries and data generator

tests/
└── sql_comprehensive.rs  # 131 SQL correctness tests
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
| `reqwest` | HTTP client for metastore |
| `apache-avro` | Iceberg manifest lists/files |
| `lance` | Lance dataset reader/writer (optional, `--features lance`) |

## Performance

TPC-H SF=100 (100GB), warm, all 22 queries, validated cell-exact against
DuckDB:

```
Parquet:                  65.1s  (DuckDB on the same files: 40.1s)
Parquet + IPC sidecars:   47.1s  (0.70x DuckDB native tables)
Lance:                   ~101s   (DuckDB's lance extension: 69.1s)
```

*Measured 2026-08 on an i9-13900KF (8P+16E cores), parallel execution.
See `CLAUDE.md` for the full benchmark history and methodology.*

## Supported Functions

### Math Functions (28)
`ABS`, `CEIL`, `CEILING`, `FLOOR`, `ROUND`, `POWER`, `POW`, `SQRT`, `CBRT`, `MOD`, `SIGN`, `TRUNCATE`, `LN`, `LOG`, `LOG2`, `LOG10`, `EXP`, `RANDOM`, `RAND`, `SIN`, `COS`, `TAN`, `ASIN`, `ACOS`, `ATAN`, `ATAN2`, `DEGREES`, `RADIANS`, `PI`, `E`, `INFINITY`, `NAN`, `IS_FINITE`, `IS_INFINITE`, `IS_NAN`, `COSH`, `SINH`, `TANH`, `FROM_BASE`, `TO_BASE`

### String Functions (29)
`UPPER`, `LOWER`, `TRIM`, `LTRIM`, `RTRIM`, `LENGTH`, `CHAR_LENGTH`, `SUBSTRING`, `SUBSTR`, `CONCAT`, `CONCAT_WS`, `REPLACE`, `POSITION`, `STRPOS`, `REVERSE`, `LPAD`, `RPAD`, `SPLIT_PART`, `STARTS_WITH`, `ENDS_WITH`, `CHR`, `CODEPOINT`, `ASCII`, `LEFT`, `RIGHT`, `REPEAT`, `TRANSLATE`, `LEVENSHTEIN_DISTANCE`, `HAMMING_DISTANCE`, `SOUNDEX`, `NORMALIZE`, `TO_UTF8`, `FROM_UTF8`, `LUHN_CHECK`, `WORD_STEM`

### Date/Time Functions (25)
`YEAR`, `MONTH`, `DAY`, `HOUR`, `MINUTE`, `SECOND`, `MILLISECOND`, `DAY_OF_WEEK`, `DOW`, `DAY_OF_YEAR`, `DOY`, `WEEK`, `WEEK_OF_YEAR`, `QUARTER`, `DATE_TRUNC`, `DATE_PART`, `EXTRACT`, `DATE_ADD`, `DATE_DIFF`, `CURRENT_DATE`, `CURRENT_TIMESTAMP`, `CURRENT_TIME`, `NOW`, `LOCALTIME`, `LOCALTIMESTAMP`, `LAST_DAY_OF_MONTH`, `FROM_UNIXTIME`, `TO_UNIXTIME`, `DATE_FORMAT`, `DATE_PARSE`

### Aggregate Functions (22)
`COUNT`, `COUNT(DISTINCT)`, `SUM`, `AVG`, `MIN`, `MAX`, `STDDEV`, `STDDEV_POP`, `STDDEV_SAMP`, `VARIANCE`, `VAR_POP`, `VAR_SAMP`, `BOOL_AND`, `BOOL_OR`, `EVERY`, `COUNT_IF`, `ANY_VALUE`, `ARBITRARY`, `APPROX_DISTINCT`, `APPROX_PERCENTILE`, `CORR`, `COVAR_POP`, `COVAR_SAMP`, `REGR_SLOPE`, `REGR_INTERCEPT`, `KURTOSIS`, `SKEWNESS`, `GEOMETRIC_MEAN`, `BITWISE_AND_AGG`, `BITWISE_OR_AGG`

### Conditional Functions (6)
`COALESCE`, `NULLIF`, `CASE`, `IF`, `GREATEST`, `LEAST`

### JSON Functions (14)
`JSON_EXTRACT`, `JSON_EXTRACT_SCALAR`, `JSON_ARRAY_LENGTH`, `JSON_ARRAY_GET`, `JSON_ARRAY_CONTAINS`, `JSON_SIZE`, `JSON_PARSE`, `JSON_FORMAT`, `JSON_KEYS`, `IS_JSON_SCALAR`, `JSON_QUERY`, `JSON_VALUE`, `JSON_EXISTS`

### Regex Functions (6)
`REGEXP_LIKE`, `REGEXP_EXTRACT`, `REGEXP_EXTRACT_ALL`, `REGEXP_REPLACE`, `REGEXP_COUNT`, `REGEXP_SPLIT`

### Binary/Encoding Functions (12)
`TO_HEX`, `FROM_HEX`, `TO_BASE64`, `FROM_BASE64`, `MD5`, `SHA1`, `SHA256`, `SHA512`, `HMAC_MD5`, `HMAC_SHA1`, `HMAC_SHA256`, `HMAC_SHA512`, `CRC32`, `XXHASH64`

### Bitwise Functions (8)
`BITWISE_AND`, `BITWISE_OR`, `BITWISE_XOR`, `BITWISE_NOT`, `BIT_COUNT`, `BITWISE_LEFT_SHIFT`, `BITWISE_RIGHT_SHIFT`, `BITWISE_RIGHT_SHIFT_ARITHMETIC`

### URL Functions (9)
`URL_EXTRACT_HOST`, `URL_EXTRACT_PATH`, `URL_EXTRACT_PROTOCOL`, `URL_EXTRACT_PORT`, `URL_EXTRACT_QUERY`, `URL_EXTRACT_FRAGMENT`, `URL_EXTRACT_PARAMETER`, `URL_ENCODE`, `URL_DECODE`

### Other Functions
`TYPEOF`, `UUID`, `TRY`, `TRY_CAST`, `FORMAT`, `FORMAT_NUMBER`

## Roadmap

- [x] Parquet file support
- [x] Interactive SQL REPL
- [x] Correlated subqueries (EXISTS, IN, scalar)
- [x] All 22 TPC-H queries passing
- [x] Trino-compatible SQL functions (100+)
- [x] Larger-than-memory dataset support
- [x] Lance dataset support (read/write, time travel, vector search)
- [x] Apache Iceberg table support (read, snapshot time travel)
- [x] Parallel execution (morsel-driven, NUMA/topology-aware)
- [x] Cost-based join ordering (DPsize from file statistics)
- [x] Distributed execution (`serve` mode: scatter/gather over N nodes)
- [ ] Window functions (ROW_NUMBER, RANK, etc.)
- [ ] Array/Map type support
- [ ] Iceberg partition pruning + row-level deletes

## License

Apache-2.0
