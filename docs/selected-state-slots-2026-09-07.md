# Bound selected-state replacement and merge

`physical/morsel_agg/selected_state.rs` now connects the admitted scalar payload
owner to selected partial-state slots. This is a **component integration**, not
integration into current aggregate ingestion or the spill controller. Existing
AccumulatorState layout and all operator routing remain unchanged.

## Bound ownership and replacement

`BoundSelection` binds the selected operation and input type before ingestion.
New `ReservedDataType` admits metadata before cloning; one shared bound layout
retains that owner across groups and workers. Binding rejects DISTINCT and
unsupported types. MIN/MAX list ordering is explicitly unsupported by this
component; ANY_VALUE/ARBITRARY support lists of representable scalar types.
Dictionary/large/view encodings require the caller's prior normalization and
explicit normalized type binding. No fallback routing has been added here.

Numeric scalar payloads remain inline. Strings, lists and timestamps use an
Arc of the reservation-owned scalar. The caller must admit the enclosing slot
storage separately. Keeping payloads inline does not establish that this new
slot representation has the same footprint or performance as the old one; it
has not been added to the existing hot accumulator enum or benchmarked.

`prepare` checks the bound type, decides whether a value wins, and admits a
replacement before mutation. Its token exclusively borrows the destination.
Dropping the token destroys only the uncommitted replacement; `commit` consumes
the token and installs the replacement. A later slot's preparation failure can
therefore discard all earlier tokens without changing any old selected value.
The complete row controller must perform this preparation before changing
COUNT/SUM/AVG or other fixed states. That controller is not implemented yet.

NULL inputs are ignored. Ties retain the current value; MIN/MAX use the existing
SQL comparator, including NaN-above-finite ordering. Decimal scale and timestamp
unit/timezone must match the binding. Lists check child metadata and actual
element types/validity. Exact scalar bits are preserved; this component does not
perform final SQL decimal precision validation.

## Merge

`prepare_merge` requires pointer identity of the shared bound layout, preserving
operation/type and budget provenance. Identical schema text or pool names are
insufficient. Sharing a winning owned payload requires no new payload copy or
reservation, and its lease survives destruction of the source state. Cross-layout
transfer must explicitly re-admit values rather than borrowing another query's
budget. An in-process Arc identity is not an on-disk layout identifier; complete
spill framing and file identity are still required.

## Verification

Final focused gates pass **44 morsel tests and 5 scalar-ownership tests**, with
zero failures or ignores. Four tests are new; the earlier four-test run is not
additional coverage. Formatting passes. The new tests verify:

- A later selected slot's admission failure, rollback of an earlier prepared
  replacement, unchanged old values and pool use, and successful retry.
- Merge with zero available pool capacity; the target retains the source's
  payload lease, and a separately bound query with the same name is rejected.
- Inline floating updates and merges with a full pool, SQL NaN extrema,
  exact decimal coefficient bits, and rejection of mismatched numeric types/scales.
- First-value NULL behavior, nested payload retention, malformed child values,
  unsupported list ordering/DISTINCT, and timestamp domain checks.

[The evidence archive](benchmarks/2026-09-07-selected-state-slots/) preserves
the module, type-owner delta, commands, logs and644 source-input hashes. These
are component tests. No full library/integration, actual spill, cap, GPU or
optimized performance gate was rerun, and no performance improvement is claimed.

## Next

Compose fixed and selected slots into a bound partial-state row with canonical
key ownership and pre-admitted preparation scratch. Commit a row only after all
its growth succeeds, using the existing applied-row cursor. Then finish bounded
file flush/merge and oversized-partition handling, and connect the worker
controller without source replay. The two consuming-source spill-transition
tests and separate 256 KiB completion failure remain open and were not rerun.
Continue the existing
[implementation contract](../.claude/epics/realistic-benchmarks-duckdb-leadership/updates/2026-09-07-fused-spill-implementation.md).
