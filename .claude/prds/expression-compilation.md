---
name: expression-compilation
description: Closure-compiled fused expression evaluation - the form of query compilation that survives measurement in a vectorized engine
status: completed
created: 2026-08-22T05:55:00Z
---

# PRD: expression-compilation

## Executive Summary

Modern engines split into two camps on query compilation. The compilation
camp (HyPer/Umbra: whole-pipeline LLVM/adaptive codegen; Trino: per-query JVM
bytecode; Spark Tungsten: whole-stage Java codegen; ClickHouse: opt-in LLVM
JIT for hot expression chains) generates code per query. The vectorized camp
(DuckDB, Velox, Databricks Photon — which explicitly REPLACED Spark's codegen
— and this engine) interprets over batches, letting SIMD kernels amortize
dispatch. Kersten et al. (VLDB 2018, "Compiled and Vectorized Queries")
measured the camps as roughly equal overall: compilation wins compute-heavy
expression chains; vectorization wins memory-bound joins/scans and offers
instant "compile" times and debuggability.

We priced it on THIS engine before proposing anything
(`examples/expr_compile_bench.rs`, 524k rows/iter, release build):

| shape | interpreted | perfect fused loop | verdict |
|---|---|---|---|
| Q1 arithmetic chain, standalone | 0.544ms | 0.638ms | **arrow SIMD already wins — standalone codegen REFUTED** |
| same chain fused INTO the SUM | 0.605ms | 0.213ms | 2.8x — killing the output vector is the win |
| Q6 3-term predicate to mask | 1.417ms | 0.320ms | 4.4x — five kernel passes + five temporaries vs one pass |

Conclusion: **applicable, narrowly.** Not JIT — closure-compiled FUSED
evaluation for the two consumers where temporaries dominate: filter
predicates (expression -> selection mask in one pass) and aggregate input
expressions (expression -> accumulator without materializing). Zero new
dependencies, nanosecond "compile" times (which also suits distributed
workers re-planning each fragment — Umbra's adaptive-compilation lesson,
solved by never being slow to compile).

## Problem Statement

`evaluate_expr` walks the Expr tree per batch, calling one arrow kernel per
node and materializing an ArrayRef per intermediate. For a 3-comparison
predicate that is 5 full passes over the batch plus 5 temporary allocations;
the mask alone costs 4.4x its fused equivalent. Every FilterExec predicate
and every computed aggregate input pays this on every batch of every query —
locally and on every distributed worker.

## Research Notes (what the practice actually is)

- **HyPer** (Neumann 2011): compile pipelines to LLVM IR; tuples stay in
  registers; the celebrated wins come from FUSION (no operator boundaries),
  not from compiling isolated expressions.
- **Umbra**: adaptive — start on a bytecode interpreter, JIT in the
  background, swap in when ready; exists because LLVM compile latency hurt
  short queries. Closure compilation sidesteps the problem entirely.
- **ClickHouse**: vectorized interpreter; `compile_expressions` LLVM-fuses
  hot scalar chains after `min_count_to_compile_expression` executions —
  precisely scoped to expression fusion, not whole plans.
- **Trino/Presto**: per-query JVM bytecode for projections/filters/joins;
  the JVM is the JIT they already ship.
- **Photon** (SIGMOD 2022): Databricks REPLACED Tungsten codegen with a
  vectorized C++ engine, citing maintainability, observability and
  incremental coverage; **DuckDB**: no JIT on principle; **Velox/DataFusion**:
  vectorized, no JIT.
- This engine's own ledger points the same way: probes are MLP-bound at
  3.8ns/row (radix REFUTED), scans are decode-bound — the memory-bound side
  of Kersten's split, where compilation does not pay. What remains
  interpretation-bound is exactly the temporaries the bench measured.

## User Stories

1. **Anyone running filtered queries**: predicates evaluate in one fused
   pass; results identical to the interpreter, bit for bit.
2. **Operator**: `QE_COMPILE=0` restores the interpreter everywhere (the
   established diagnostic-switch pattern), and unsupported expression shapes
   fall back silently-correctly, never wrongly.
3. **Distributed user**: workers get the same fused evaluation on partial
   queries with no compile-latency tax.

## Functional Requirements

- A `CompiledExpr` built once at operator construction from an `Expr` tree:
  a type-specialized fused evaluator for the supported subset —
  Float64/Int64/Int32/Date32/Boolean columns, literals, arithmetic
  (+,-,*,/), comparisons, AND/OR/NOT, null-free fast path with a
  null-tracking general path. Unsupported nodes (strings, CASE, functions,
  subqueries, dictionary columns...) -> the whole expression falls back to
  `evaluate_expr`.
- Consumers wired: `FilterExec` (predicate -> BooleanArray mask, one pass)
  and the aggregate input evaluation in the batch aggregate paths where a
  computed (non-column) expression feeds SUM/AVG/MIN/MAX/COUNT.
- Fallback correctness: compiled and interpreted paths produce IDENTICAL
  arrays (asserted in tests, including NULL propagation and division).
- `QE_COMPILE=0` kill switch; default on.

## Non-Functional Requirements

- Zero new dependencies (no cranelift/LLVM — refuted by measurement above).
- No regression on expressions the compiler declines (fallback adds one
  match at build time, nothing per batch).
- All existing suites cell-exact; TPC-H plans unchanged.

## Success Criteria

1. Microbench: compiled path within 1.3x of the hand-fused ceiling on the
   Q1-chain-into-SUM and Q6-predicate shapes (i.e. capture >= 70% of the
   measured headroom).
2. Suite: measurable improvement on predicate/expression-heavy TPC-H
   queries at SF=1+ with `QE_COMPILE` A/B; honest publication either way —
   if the suite-level win is <1%, that finding is published with the same
   prominence (the refutation ledger tradition).
3. 984+ tests green; DuckDB-validated suites cell-exact with compilation on.
4. Research + verdict recorded (this PRD + design-doc note + README note).

## Constraints & Assumptions

- Filters pushed into the parquet decoder (RowFilter) bypass FilterExec;
  the fused path helps post-join filters, memory tables and gathered
  distributed tables — attribution must be honest about that.

## Out of Scope

- Cranelift/LLVM JIT (refuted: arrow kernels already saturate standalone
  expression evaluation; dependency and correctness surface unjustified).
- Whole-pipeline fusion across operators (M3-era architecture change).
- String/dictionary expression fusion (the dict-literal fast path already
  covers the hot case).
- Join key expression fusion (keys are near-always bare columns in TPC-H).

## Dependencies

- `examples/expr_compile_bench.rs` (the pricing bench, committed).
- Existing evaluate_expr semantics as the oracle.
