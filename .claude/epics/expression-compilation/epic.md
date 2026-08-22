---
name: expression-compilation
status: backlog
created: 2026-08-22T05:55:00Z
updated: 2026-08-22T05:55:00Z
progress: 0%
prd: .claude/prds/expression-compilation.md
github: (will be set on sync)
---

# Epic: expression-compilation

## Overview

Closure-compiled fused expression evaluation: build a `CompiledExpr` once
per operator, evaluate supported expression trees in ONE pass per batch
(no intermediate ArrayRefs), wired into filter masks and aggregate inputs.
Fallback to the interpreter for everything else; `QE_COMPILE=0` kill
switch. No new dependencies. The full-JIT alternative is REFUTED by the
committed pricing bench and stays refuted in the docs.

## Architecture Decisions

1. **Closure DAG, not bytecode, not JIT.** Compilation = recursively
   translating `Expr` into a tree of enum-dispatched typed ops evaluated in
   a single row-chunk loop:
   `enum FusedOp { ColF64(usize), LitF64(f64), Add(Box,Box), ... CmpGe, And }`
   executed per 1024-row chunk into stack buffers, or (simpler and chosen)
   a recursive `eval_scalar(row)`-free VECTOR form: each node computes into
   a reusable `&mut [f64]` slab with fused arithmetic where profitable.
   Decision: implement the chunked evaluator writing into per-node slabs
   allocated ONCE per operator (not per batch) — kills allocation and
   temporaries; the compiler collapses comparison+logic chains into a
   single fused mask loop (the 4.4x shape).
2. **Two consumers only** (measured): FilterExec predicate -> mask;
   aggregate input expressions in the batch-aggregate paths -> f64 slab
   handed to accumulators (kills the output vector + extra sum pass).
3. **Support matrix v1**: numeric columns (F64/I64/I32/Date32), numeric
   literals, + - * /, all comparisons, AND/OR/NOT. Nulls: general path
   carries a validity mask through the fused loop; null-free columns take
   the branchless fast path. ANY unsupported node => whole expression
   interpreted (never mixed).
4. **Equivalence is the gate**: property tests compare compiled vs
   interpreted on randomized batches (with nulls, division, overflow-ish
   values) bit for bit.

## Task Breakdown Preview

1. Research capture + pricing bench commit (bench already written).
2. `src/physical/compiled_expr.rs`: the compiler + fused evaluator +
   equivalence property tests.
3. Wire FilterExec + batch aggregate input paths, QE_COMPILE switch.
4. Validate (suites, DuckDB gates) + benchmark micro and SF=1 suite A/B +
   publish honestly (README perf note, CLAUDE.md, refutation note for JIT).

## Success Criteria (Technical)

- >= 70% of fused-ceiling captured on the two bench shapes.
- Suites green, cell-exact; A/B published even if ~0.

## Estimated Effort

4 tasks. Risk: null-path subtleties — the property tests carry it.
