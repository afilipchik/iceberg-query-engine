# ADR: Standalone TPC-H/TPC-DS Benchmark Harness

## Status: Accepted

## Context

We need a reproducible way to compare iceberg-query-engine performance against DuckDB,
Trino, and Presto on standard TPC-H and TPC-DS benchmarks. This addresses GitHub issue #9
(performance vs DuckDB) and provides ongoing regression tracking.

## Decision

Create a standalone `benchmark-harness/` directory (sibling to `iceberg-query-engine/`)
with shell-based orchestration, DuckDB-based data generation, and JSONL output.

## Alternatives Considered

### 1. Rust benchmark binary (like DataFusion)
- **Pro**: Type-safe, fast, integrates with cargo bench
- **Con**: Requires compilation, harder to add new engines, couples to Rust ecosystem
- **Verdict**: Rejected — we need to benchmark non-Rust engines (Trino/Presto = JVM)

### 2. Python orchestration (like ClickBench)
- **Pro**: Rich libraries (pandas for analysis), easy string manipulation
- **Con**: Extra dependency, slower startup, overkill for shell command wrapping
- **Verdict**: Partially adopted — Python used only for result comparison, not orchestration

### 3. Docker-only approach (all engines in Docker)
- **Pro**: Fully reproducible, no host dependencies
- **Con**: DuckDB in Docker adds overhead for a native binary; iceberg-query-engine
  needs Rust toolchain either way
- **Verdict**: Rejected — DuckDB and iceberg-engine run natively, Trino/Presto use Docker

### 4. Built-in cargo bench in iceberg-query-engine
- **Pro**: Already exists (`cargo run --release -- benchmark`)
- **Con**: Only benchmarks our engine, not comparative
- **Verdict**: Complementary — the harness wraps this existing command

## Research: How Other Systems Benchmark

### DuckDB (benchmark/tpch.cpp + scripts/)
- C++ benchmark binary generates data internally
- Shell scripts wrap CLI for comparative benchmarks
- Outputs CSV/TSV tables
- Key insight: Separate data generation from query execution

### DataFusion (benchmarks/tpch.rs)
- Rust binary with `--benchmark tpch --sf 1 --iterations 3`
- Uses DuckDB to generate Parquet data (same approach we chose)
- Criterion-based microbenchmarks for operators
- Key insight: DuckDB as data generator is standard practice

### Velox (scripts/benchmark/)
- Shell scripts orchestrating C++ binaries
- Parquet as input format
- JSON output for results
- Key insight: Shell orchestration is production-proven even at Meta scale

### ClickBench (https://github.com/ClickHouse/ClickBench)
- Shell scripts per engine (run_duckdb.sh, run_clickhouse.sh, etc.)
- JSONL output, Python comparison
- Key insight: One runner script per engine is the proven pattern

## Trade-offs

| Aspect | Our Choice | Trade-off |
|--------|------------|-----------|
| Orchestration | Bash | Simpler but less type-safe than Rust |
| Data gen | DuckDB tpch extension | Standard practice, single binary |
| Interchange | Parquet | Universal but adds I/O overhead for in-memory engines |
| Trino/Presto | Docker | Reproducible but requires Docker daemon |
| DuckDB | Native binary | Fastest but requires download per platform |
| Output | JSONL | Simple but no built-in visualization |

## Disk Safety

- SF > 10 requires explicit `--allow-large` flag
- Pre-generation disk space check via `df`
- Default SF=0.01 (~10MB) for smoke tests
- SF=1 (~1GB) for meaningful benchmarks

## Consequences

- Easy to add new engines (write a runner script)
- Reproducible across machines (Docker + pinned versions)
- Low maintenance burden (no compilation step for the harness itself)
- Results are portable JSONL that can feed dashboards later
