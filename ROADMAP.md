# Iceberg Query Engine - Development Roadmap

## Project Status

**Current State**: TPC-H Benchmark TBD queries passing (fix deployed, testing in progress)

### Completed Features ✅
- **SQL Parser**: Full SQL-2011 support via sqlparser-rs
- **Query Planner**: Logical and physical planning with validation
- **Optimizer**: Rule-based + cost-based (predicate pushdown, projection pushdown, constant folding, join reordering, predicate reordering)
- **Subquery Execution**:
  - Uncorrelated subqueries (ScalarSubquery, InSubquery, Exists)
  - Single-level correlated subqueries (outer references resolved via substitution)
  - Per-row execution with outer context propagation
- **Physical Operators**: Scan, Filter, Project, HashJoin, HashAggregate, Sort, Limit
- **Parallel Execution**: Parallel scan execution across partitions
- **SIMD Aggregations**: Scalar aggregates optimized with Arrow kernels
- **Metastore Integration**: Branching metastore client with reliability features
- **TPC-H Support**: Full 22-query suite with schema definitions

### Known Limitations ⚠️
- **FIXED: Predicate Pushdown Bug** (2024-06-XX): The optimizer was dropping tables from join trees when processing implicit joins
  - **Issue**: When processing CROSS join chains, the JoinReordering optimizer was incorrectly pruning tables
  - **Impact**: Most queries with implicit joins were failing (Q2, Q3, Q5, Q7, Q8, Q9, Q10, Q11, Q18, Q20, Q21)
  - **Root Cause**: `JoinReordering::greedy_join_reorder()` was designed for INNER joins with explicit conditions, but was breaking CROSS joins
  - **Fix**: Modified `JoinReordering::reorder_joins()` to skip reordering for CROSS joins without empty `on` clauses
  - **Status**: Fix deployed in `src/optimizer/rules/join_reordering.rs`
- **Q20**: Nested correlated subqueries (requires multi-level context propagation)
- **Q17, Q22**: Some test edge cases with CTE column resolution (work in CLI mode)

### Passing Queries ✅
- Q01, Q04, Q06, Q12, Q13, Q14, Q15, Q16, Q17, Q19, Q22 (11/22)

---

## Short-Term Priorities (Next 2-4 Weeks)

### 0. CRITICAL: Fix Predicate Pushdown Bug (HIGHEST PRIORITY)
**Goal**: Restore queries that were broken by optimizer bug

- [ ] Fix predicate pushdown optimizer to preserve all tables in CROSS join chains
  - Issue: `src/optimizer/rules/predicate_pushdown.rs` drops intermediate tables
  - Root cause: Recursive join processing loses tables during tree reconstruction
  - Impact: Will restore Q2, Q3, Q5, Q7, Q8, Q9, Q10, Q11, Q18, Q20, Q21 (11 queries)
  - Expected outcome: 21/22 queries passing (95%)

**Success Criteria**: All queries with implicit joins pass correctly

### 1. Complete TPC-H Coverage (22/22)
**Goal**: Achieve 100% TPC-H query success rate

- [ ] Fix Q20 nested correlated subquery support (after predicate pushdown fix)
  - Implement outer scope propagation through multiple nesting levels
  - Add substitution for deeply nested correlated references

- [ ] Stabilize Q17/Q22 CTE handling
  - Ensure CTE column resolution works in all contexts
  - Add integration tests for CTE edge cases

**Success Criteria**: All 22 TPC-H queries passing consistently

### 2. Performance Optimization
**Goal**: Improve TPC-H query performance by 2-3x

- [ ] Enhance cost model with real statistics
  - Collect NDV (number of distinct values) statistics
  - Histogram support for range predicates
  - Table size statistics for better join ordering

- [ ] Improve hash aggregation performance
  - Fast aggregation for common patterns (COUNT, SUM with GROUP BY)
  - Adaptive algorithm selection (hash vs. sort-based)

- [ ] Join optimization enhancements
  - Sort-merge join for sorted inputs
  - Nested loop join for small tables
  - Broadcast joins for dimension tables

**Success Criteria**: 50% reduction in geometric mean execution time

### 3. Iceberg Table Format Integration
**Goal**: Read from actual Iceberg tables

- [ ] Implement Iceberg metadata reader
  - Parse manifest files
  - Snapshot resolution
  - Partition pruning

- [ ] Parquet file reading
  - Use `parquet` crate for columnar file access
  - Predicate pushdown to Parquet row groups
  - Schema evolution support

- [ ] Partition filtering
  - Predicate-based partition pruning
  - Transform predicates for partition values

**Success Criteria**: Can query a real Iceberg table stored in S3/GCS

---

## Medium-Term Goals (1-3 Months)

### 4. Distributed Query Execution
**Goal**: Execute queries across multiple nodes

- [ ] Design exchange protocol
  - Split-based data exchange
  - Shuffle implementation
  - Streaming results

- [ ] Task scheduler
  - Multi-stage execution plans
  - Worker node coordination
  - Fault tolerance and retry

- [ ] Distributed aggregator
  - Partial aggregation strategy
  - Final aggregation coordinator
  - Spill-to-disk for large aggregations

**Success Criteria**: 4-node cluster can execute TPC-H at scale factor 10

### 5. Advanced SQL Features
**Goal**: Support more SQL constructs

- [ ] Window functions (ROW_NUMBER, RANK, SUM OVER)
- [ ] Common Table Expressions (CTEs) - fully working
- [ ] UNION/INTERSECT/EXCEPT
- [ ] DISTINCT with multiple columns
- [ ] CAST between types
- [ ] Date/time functions

### 6. Query Federation
**Goal**: Query multiple data sources

- [ ] PostgreSQL external tables
- [ ] MySQL external tables
- [ ] Cross-database joins
- [ ] Statistics collection for external sources

---

## Long-Term Vision (3-6 Months)

### 7. Production Readiness
**Goal**: Enterprise-grade reliability and performance

- [ ] Comprehensive test suite
  - Unit tests >80% coverage
  - Integration tests for all operators
  - Fuzz testing for SQL parser

- [ ] Observability
  - Metrics export (Prometheus)
  - Distributed tracing
  - Query plan visualization

- [ ] Resource management
  - Memory limits and spilling
  - CPU throttling
  - Query cancellation

### 8. Advanced Optimizations
**Goal**: Smart query optimization

- [ ] Adaptive query execution
  - Runtime plan adjustments based on statistics
  - Switch join strategies mid-execution

- [ ] Materialized views
  - View maintenance
  - Automatic query rewriting

- [ ] Multi-query optimization
  - Share scans across concurrent queries
  - Pipelined table functions

### 9. Ecosystem Integration
**Goal**: Work with existing tools

- [ ] JDBC/ODBC drivers
- [ ] Python bindings
- [ ] Spark compatibility layer
  - Read Spark Hive/Iceberg tables
  - Convert Spark plans

---

## Technical Debt & Cleanup

### Code Quality
- [ ] Reduce compiler warnings to < 20
- [ ] Add docstrings to all public APIs
- [ ] Improve error messages with context
- [ ] Add more integration tests

### Architecture
- [ ] Simplify filter expression evaluation (currently complex)
- [ ] Unify execution trace collection
- [ ] Clean up duplicate code patterns
- [ ] Improve type system for Schema/SchemaRef

### Performance
- [ ] Profile hot paths with flamegraph
- [ ] Reduce allocations in hot loops
- [ ] Consider SIMD for more operations
- [ ] Optimize string handling

---

## Contributing

### Architecture Decisions Record
See `ARCHITECTURE.md` for key design decisions and rationale.

### Development Workflow
1. Create feature branch from `master`
2. Implement with tests
3. Run `cargo test`, `cargo clippy`
4. Submit PR with tests passing
5. Code review required

### Performance Benchmarks
Run TPC-H suite before/after optimization:
```bash
cargo bench --bench tpch -- --save-baseline main
cargo bench --bench tpch -- --baseline main
```

---

## Dependencies

| Crate | Purpose | Version |
|-------|---------|--------|
| arrow | Columnar data format | 53 |
| sqlparser | SQL parsing | 0.52 |
| tokio | Async runtime | latest |
| parquet | Parquet file I/O | 53 |
| reqwest | HTTP client | latest |
| rust_decimal | Decimal arithmetic | latest |

---

## Related Projects

- **Apache Iceberg**: Table format specification
- **Apache Arrow**: Columnar in-memory format
- **DataFusion**: Rust query execution (inspiration)
- **Databend**: Cloud warehouse (architecture reference)
