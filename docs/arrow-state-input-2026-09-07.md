# Transactional evaluated-array ingestion into partial group rows

`GroupRows::process_evaluated_from` now composes the Arrow key adapter and complete
aggregate row preparation. It validates bound column types/arity even for empty
batches, consumes retained evaluated arrays, commits one whole key/state/index row
at a time and returns the exact first unapplied row on failure. New unpublished
groups and pending selected values roll back together. It never evaluates an
expression or opens original input. The caller owns and must account for the
retained RecordBatch; this API supplies neither queues nor worker admission.

Fixed inputs use stack scalar bytes and existing checked update semantics. COUNT
reads logical validity without materializing its payload. Numeric selections stay
inline. String and timestamp extrema compare borrowed input before admission;
ANY_VALUE/ARBITRARY skip later non-NULL candidates after a selection exists. Only
variable winners use reusable admitted encoding scratch and decode once into their
final reservation owner. Lists use that path for first non-NULL selection. Timestamp
unit/zone and decimal scale must match the bound schema exactly. Raw dictionary,
view and large encodings still require normalization before this bound route.

The shared Arrow visitor has distinct key and scalar modes: key equality canonicalizes
NaN/signed zero, while selected state preserves original float bits. RowWorkspace
now has a reusable admitted byte buffer for variable winner encoding; it can grow
and cause typed memory denial. Pending earlier slot changes then roll back. Losing
string candidates do not copy payloads or require fresh winner admission. This
implementation still dispatches by type per row and is not a measured CPU improvement.

Three new tests cover:

- Direct evaluated input with duplicate/NULL groups, COUNT over nullable strings,
  exact large decimal SUM, AVG, string MIN and nullable list ANY_VALUE, checked
  against independent values.
- A retained batch whose first row commits and second row fails under real pool
  pressure. Both existing and new destination groups roll back correctly; resuming
  cursor one produces COUNT 3 or a separate COUNT 1, without applying the prefix twice.
- Empty-batch schema validation and exact selected NaN/signed-zero bits.

Final gate: **101 aggregate component tests passed**, no failures or ignores;
formatting passes. The intermediate 100-pass run is preserved but not additional
coverage. [Evidence](benchmarks/2026-09-07-arrow-state-input/) contains 14 verified
members and 655 current source-input hashes. Manifest SHA256:
`ea1f5516a85787804c9f1faf64e5df8008106fb05692b62c15d1208e7d9566b6`.

Live planner/worker routing is unchanged. A controller must still prepare flushing
before filling resident state, stop pulling on pressure, spill partial rows, resume
the retained batch, bound worker working sets and emit admitted Arrow results.
Global empty-aggregate output is also the enclosing operator's responsibility.
Both original consuming-source replay failures and the original live 256KiB decimal
completion case remain open and were not rerun. No full integration, resource cap,
GPU or benchmark gate ran for this unconnected adapter.
