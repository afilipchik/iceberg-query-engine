# Atomic partial aggregate rows

`physical/morsel_agg/state_rows.rs` composes fixed and selected aggregate states
into contiguous row storage with reusable preparation scratch. It is **not yet
connected to query ingestion or the spill controller**. Canonical key ownership,
complete spill serialization/framing, file ownership and bounded partition merge
remain required before the input-replay branch can be replaced.

## Representation and admission

`StateRowLayout` binds physical slots, supported input domains and fixed aggregate
functions. In particular, the final function remains available to distinguish
variance/standard-deviation variants sharing one physical state. Unsupported
shapes and DISTINCT return no binding before consumption. Complete expression,
output-schema and on-disk layout identity are still outside this component.

`StateRows` uses two flat reservation-owned vectors for fixed and selected slots.
Groups do not allocate individual accumulator vectors. Both arrays admit growth
before a logical row is added. If an array's capacity grows but later admission
fails, the extra capacity can remain reserved, while row count and initialized
state remain unchanged. The vector helper now supports truncating an initialized
suffix while retaining its admitted capacity.

`RowWorkspace` admits fixed scratch and selected-replacement slots before use.
Preparation does not grow those arrays. Winning variable-width values separately
admit their payloads with old and new storage simultaneously alive. Numeric
states and selected numeric payloads remain inline. Actual state footprint and
hot-path performance are unmeasured; the current AccumulatorState enum and live
operator routing were not enlarged or replaced by this component.

## Whole-row preparation and commit

Preparation copies fixed state into admitted scratch, applies updates there,
and prepares selected replacements. An exclusive row/workspace token prevents
stale commits or scratch reuse. Any later error drops pending payloads and leaves
all fixed and selected destination fields unchanged. Dropping a successful token
has the same rollback behavior. Commit copies the fixed scratch and moves the
selected replacements into their slots without further admission.

This closes the component-level gap where a failed MIN/MAX string replacement
could follow an already-applied COUNT. The ingestion caller must still prepare
the complete row before advancing the applied-row cursor. That caller integration
has not been implemented.

COUNT, integer SUM and AVG/variance counters check arithmetic overflow during
both update and merge before committing any destination field. Overflow is a
named execution error, distinct from recoverable pool pressure. These checks
belong to the new row path; existing live accumulator paths have not acquired
them through this change.

Merge requires the same bound layout and preserves partial algebra: COUNT adds
counts, decimal SUM retains exact coefficients/scale/seen/overflow, and AVG
retains sum and count. Selected winners share their original leases. Final-value
access uses the bound function for fixed states and borrows selected payloads;
it does not clone retained strings. Output builders must admit their own storage
and perform final SQL output-type/precision validation.

## Verification

Final gates pass **48 morsel tests and 11 ownership tests**, with zero failures
or ignores. Four tests are new. Earlier runs are preserved but not added to the
59 unique final passes. Formatting passes.

The new row tests verify a late string-admission failure after COUNT and another
selection have been prepared, complete rollback and retry, and explicit token
discard. They merge seven aggregate states over differently weighted partials,
including a decimal coefficient `(1 << 100) + 17`, and check independent final
COUNT/SUM/AVG/MIN/MAX/ANY_VALUE/ARBITRARY values. All-NULL rows preserve zero
COUNT and NULL non-COUNT outputs. Merge succeeds with a fully reserved pool,
and selected string output is borrowed after the source state is destroyed.

Additional cases cover invalid row/slot/arity/workspace identity, denied row
growth, and checked update/merge overflow for COUNT, integer SUM, AVG and variance
counters. A NULL input does not increment a full counter. Reservation cleanup is
asserted after every fixture.

[The evidence archive](benchmarks/2026-09-07-aggregate-state-rows/) contains the
component, isolated helper patches, commands, logs and645 source-input hashes.
No full library/integration, end-to-end spill, dedicated GPU/cap or optimized
benchmark gate was rerun. The small component fixture's256KiB pool is not the
existing full-query256KiB spill-completion test. The two consuming-source replay
regressions and that query test remain open and were not rerun here.

Next implement canonical key ownership and complete state-row serialization,
then bounded file flush/merge and oversized-partition handling. Connect this
storage and row preparation to the existing ingestion cursor and worker
controller, and rerun the unchanged end-to-end gates before performance work.
Continue the existing
[implementation contract](../.claude/epics/realistic-benchmarks-duckdb-leadership/updates/2026-09-07-fused-spill-implementation.md).
