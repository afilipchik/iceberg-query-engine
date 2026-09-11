# Expression and output identity: systemic correctness findings

## Outcome and scope

Independent small regressions exposed several different identity contracts that the engine had conflated. These defects affect general SQL and plan transformations, not particular benchmark queries. The fixes use structural/physical identity and explicit intermediate output symbols; no SQL-text recognition or data-dependent semantic proof is introduced.

1. **Projection composition:** equal expression trees are not idempotent. Applying x+1 twice must produce x+2. Collapse now requires a complete unambiguous positional passthrough with identical output schema. Three baseline failures and one positive control become four passes.
2. **Typed expression substitution:** numeric decimal equality does not preserve physical scale. The shared Expr equality now compares exact coefficient/scale, float bits and recursive list representations. Every AST/scalar variant is covered exhaustively. Scalar numeric equality is unchanged. Four direct binder/HAVING failures and one positive control become five passes. Aggregate root reuse uses this shared contract.
3. **Display-name substitution:** a name such as SUM(x) is presentation metadata, not evidence that a computed expression equals its aggregate child. Binder fallbacks by display name are removed; missing aggregates fail binding instead of silently selecting an output.
4. **Aggregate output lookup:** distinct expressions can have the same generated label. On input1,1,2,NULL, SUM(x),SUM(DISTINCT x) returned4,4 instead of4,3; two CASE sums returned2,2 instead of2,1. Collision-checked internal aggregate names now distinguish physical fields, with final SELECT labels preserved. Ordinary and GROUPING SETS paths are covered.
5. **Hidden sort projection:** widening and trimming by repeated display labels can select the first output twice. Widened visible outputs now have unique intermediate symbols and trimming restores the known original positions. A grouped CASE/HAVING/hidden-order regression passes after this additional fix. Sort+LIMIT adjacency is preserved.
6. **Empty DISTINCT SUM:** added GROUPING SETS coverage finds zero instead of NULL for the all-NULL group. The shared integer/float finalizer now returns NULL for empty sets and preserves valid nonempty zero. Direct tests and ordinary/actual-spill typed oracles pass.

## Evidence boundaries

See [full logs, patches and source snapshots](benchmarks/2026-09-06-expression-substitution/README.md). The pre-name-collision combined gate passed740unique selected tests including an explicit real-IPC process, with one pre-existing ignored test. Additional tests subsequently demonstrated more failures: that gate is not proof of closure. The earlier dotted-SQL decimal reproducer was invalid because those literals bind Float64; its log and original fixture are retained and explicitly disqualified. Explicit CAST SQL controls passed before the typed AST change.

No benchmark measurement belongs to the latest source yet. The previous frozen prepared-pipeline binary remains performance-rejected across five CPU modes despite typed correctness. The source582 release was deliberately cancelled before publishing a new binary when these defects were discovered.

## Architectural path forward

Internal output identity should ultimately be a stable bound slot, separate from public labels and scalar comparison semantics. The current engine represents many references as names, so collision-safe generated fields are a bounded repair within that architecture. Positional ORDER BY currently lowers an ordinal to a name-only column reference and needs its own audit for repeated public labels. Do not claim that the aggregate/sort repair certifies all name-based consumption boundaries.

The final frozen-source gate passes750unique tests across library, semantic, spill, partition, ownership and explicit real-IPC coverage; one pre-existing test remains ignored. Formatting passes and all585 source hashes match. Release1693 and all six actual-spill cap cases passed. The five CPU mode screens passed880typed/time comparisons but retained protected performance regressions; see [complete outcome](frozen585-benchmark-outcome-2026-09-06.md). Preserve the already-failed performance comparisons; new correctness fixes do not establish a speedup.
