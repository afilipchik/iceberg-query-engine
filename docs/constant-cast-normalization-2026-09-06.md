# Constant cast normalization and provider filtering

The canonical Lance Q14 optimized filter retained a constant upper-date CAST.
The provider could render only the lower bound. This exposed a shared optimizer
omission: `ConstantFolding` recursively visited CAST inputs but never evaluated
the conversion itself. It is not proof of the older binary-to-binary regression.

The current change evaluates literal casts with the same `numeric::cast_array`
kernel and strict/TRY mode as execution. It replaces only successful, non-NULL
values whose scalar representation has exactly the target Arrow type. Invalid
casts stay executable expressions, preserving unused-branch and empty-input
behavior. Typed NULLs, unsupported scalar encodings, decimal precision not
representable by ScalarValue, remain casts. No volatile
functions are evaluated. Nested casts fold from the inside out when representable.

Traversal now also covers scan filters, sort expressions and IN/BETWEEN operands.
Integer constant arithmetic uses checked operations; overflow and division errors
remain runtime expressions rather than panicking or wrapping during optimization.
No query IDs, date constants, provider policy thresholds or memory limits change.

## Evidence motivating the implementation

The earlier frozen release, using the equivalent typed DATE literal SQL, returns
identical independently verified results. In a short sequential diagnostic, warm
Lance full-query times were 202.8/194.9 ms by default, versus 316.3/316.6 ms with
forced complete-range pushdown. Forced scans return 749,223 lineitem rows but cost
about 244 ms, compared with ~103 ms for the earlier unfiltered scans. Thus fewer
rows alone does not justify enabling pushdown. This altered SQL diagnostic is not
canonical acceptance and does not establish the new binary's performance.

The preceding forced partial-range diagnostic returned 28,117,026 rows and was
also slower. Keep default pushdown policy. Same-column interval selectivity
multiplication remains a costing inconsistency, but changing it to activate the
slower path would not meet the performance goal.

Artifacts: `.scratch/lance-range-literal-diagnostic-01/`,
`.scratch/constant-cast-repair/trace-lance-literal.py`, and the prior archived
[attribution evidence](ipc-scalar-subquery-attribution-2026-09-06.md).

## Validation checkpoint

Final default source: 677 library tests and 23 selected integration tests pass,
plus the separately invoked IPC regression: **701 selected executions pass**.
The historical flatten-dependent-join test remains ignored. Earlier feature-Lance
provider checks passed 22 tests; their `data/tpch-1mb-lance/orders.lance` fixture
exists (the five fixture-dependent checks did not take their silent early-return
branch). The last non-finite guard was added after that feature test build.
A separate earlier focused run passed 30 integrations including semantic proofs
and exact numerics. Final fmt and whitespace checks pass.

Optimized release build and canonical provider performance validation are next.
No leadership or resource-certification claim is made.

## Acceptance remains pending: floating comparison domain

Review after the passing checks reproduced an existing signed-zero SQL discrepancy
on the control and identified a possible new constant-versus-row mismatch when
finite float casts fold. See [reproducer and next gate](float-comparison-domain-findings-2026-09-06.md).
The candidate release is building; do not run or publish its performance acceptance
before checking and resolving this semantic boundary. This is not covered by the
non-finite guard or the existing selected passing tests.

Current follow-up source also repairs SQL float comparison in the shared numeric
module, interpreter and compiled predicates. Non-finite casts may now fold when
their type is representable; comparisons use the same SQL rule as row execution.
See the floating-domain report for the failed intermediate candidate and current
validation status; do not attribute its earlier passing counts to this newer source.
