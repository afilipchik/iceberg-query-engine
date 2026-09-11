# Floating extrema: update and merge must share SQL ordering

Local engine source review exposed a reproduced wrong-result bug in floating
MIN/MAX. The typed morsel update used primitive IEEE `<`/`>`, while scalar update
and partial-state merge used SQL ordering: NaN equals NaN and sorts above every
non-NaN; signed zeros compare equal. Thus MIN(NaN, -7) could return NaN, and
changing partition/batch boundaries could change the answer.

The new independent oracle fails before the repair in ordinary, shared fused
and disjoint fused operators. A state-level test also fails before the repair.
This is a reproduced semantic divergence, not a performance hypothesis.

## Repair

`planner/numeric.rs` supplies float extrema selection through the existing SQL
comparison contract. The typed morsel loop now uses that comparator. Vectorized
hash extrema and legacy floating state update/merge routines use the same
selection helpers. Ties retain the existing value, including signed zero and
NaN payload ties; SQL does not require a particular tied payload.

No allocation, dependency, planner eligibility or spill routing change was
introduced. Existing generic scalar/merge ordering remains equivalent. This
does not certify every aggregate family or GPU reduction, and does not change
the existing GPU nonfinite-input refusal/fallback policy.

## What the reference sources show

The local DuckDB revision `1c27c54f27bea397d18d86f7c0d6a9f1d4aa85f8`
defines NaN-above-numbers in
`src/common/vector_operations/comparison_operators.cpp::GreaterThanFloat`.
`src/function/aggregate/distributive/minmax.cpp` uses those comparison operations
in both `Execute` and `Combine`. The relevant design lesson is a shared semantic
contract across update and merge, not a copied engine-specific optimization.

The local ClickHouse revision `a1b25f3f4beb3ba49aa3b73671cc244185331b86`
uses primitive comparison in `SingleValueDataFixed<T>::setIfSmaller` and
`setIfGreater`; its batch extrema also dispatch through `findExtremeMin/Max`.
Those inspected routines are not proof of equivalent SQL NaN behavior. No
ClickHouse binary was run, and no claim about its complete runtime semantics is
made. Both repositories were read only. File hashes and exact local paths are
preserved in `external-sources.json` in the evidence archive.

## Validation and limits

Final contained command and source hashes are in
[the evidence archive](benchmarks/2026-09-07-float-extrema/). The full library
passes **808 tests**, with **10 ignored**. Eight affected integration targets
pass **33 tests**, with no ignores or failures. These **841 unique passes**
include the five new tests; earlier selected runs are not additional coverage.
The ignores remain flatten-EXISTS, eight isolated CUDA tests and the separate
IPC-cache test. No dedicated CUDA, cap or optimized performance run occurred.
Formatting passes.

The new state oracle exercises both scalar and typed ingestion, every split
point and both merge orders. Explicit expected values cover NaNs with different
payloads/signs, infinities, finite duplicates and signed zeros. Operator tests
cover opposite NaN positions, a NULL group, all-NULL values, and one batch versus
ten slices. A separate test directly exercises vectorized hash aggregation,
so ordinary operator routing cannot accidentally bypass that validation.

The original three operator tests and one state test failed before the fix;
the direct vectorized test was added afterward and has only post-fix evidence.
The archive preserves the isolated patch against the already-dirty working tree,
both failure logs and final results. No current DuckDB benchmark result is
inferred from source review or debug tests.

The fused group-budget path still needs reservation-owned selected values and
keys, a complete partial-state layout, bounded file flush/merge, and cursor
integration. The two input-replay regressions and separate 256 KiB completion
failure remain open and were not rerun for this ordering repair. Resume that
[implementation contract](../.claude/epics/realistic-benchmarks-duckdb-leadership/updates/2026-09-07-fused-spill-implementation.md).
