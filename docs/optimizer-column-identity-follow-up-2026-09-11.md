# Optimizer column identity follow-up

The LEFT COUNT regression exposed two different binding failures: a discarded
qualifier and a generated field name colliding with an input. The local repair
preserves bound key fields and makes the generated count name fresh across the
participating schemas. This closes the executed shape, not every optimizer rule.

Local DuckDB source at1c27c54f27bea397d18d86f7c0d6a9f1d4aa85f8 provides a useful
contrast. `src/optimizer/common_aggregate_optimizer.cpp`, especially
`VisitReplace` and `ExtractCommonAggregates`, remaps references using
`ColumnBinding(aggregate_index, output_index)`. Erasing or moving an aggregate
explicitly remaps these bindings. Projection/CTE/set-operation scopes use a fresh
optimizer visitor. This source was read locally; no DuckDB build or performance
measurement supports this comparison.

This engine's `planner::Column` still identifies columns by optional relation
and name. Its Arrow field metadata preserves those bound components separately
from displayed field names, and existing identity regressions protect that
boundary. A longer-term migration to relation/output IDs should preserve that
work, not remove metadata or reintroduce suffix-based ambiguous lookup.

A bounded migration would first add stable bound output identities after binding,
with a display name carried separately. Each logical rewrite would then supply
an explicit old-to-new output map and validate all consumer references against
its new child schema. Physical expression binding, provider schemas, CTE aliases,
decorrelation and serialized plans must migrate together in reviewed stages.
Tests should rename display fields without changing results, collide aliases,
reorder/prune outputs and repeat optimizer iterations. Until that migration is
complete, every synthetic-field rewrite needs freshness and qualifier tests.

No such migration is implemented by the current count-preaggregation candidate.
It should not be mixed into the frozen benchmark comparison. The immediate
performance question remains whether reducing generic outer-join fanout lowers
query time without resource or protected-query regressions.
