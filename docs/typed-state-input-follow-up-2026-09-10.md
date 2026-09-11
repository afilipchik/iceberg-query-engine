# Typed aggregate input binding: conditional follow-up

This is a source-grounded design hypothesis, not an implemented optimization or
measured speedup. The direct-state comparison retained a Q18 regression. The borrowed-input fde271b1
comparison recovered it and its full provider screen is complete: raw still trails
DuckDB and native/Lance remain incomplete. The retained-input progress-credit
resource fix is now undergoing release validation; finish that frozen measurement
before introducing this separate input-dispatch change.
Do not mask a regression by comparing only with the slower compact prototype.

## Observed dispatch boundary

`StateRows::bind_arrays` checks logical types, arity, lengths and exact layout
identity once. It retains the original ArrayRef slice. `prepare_arrays_indexed`
now calls the shared `key_rows::arrow_input::with_inline` consumer or `resolve`
per input cell. The borrowed consumer removes wide returned scalar copies, but
still resolves dictionaries/validity, matches physical DataType and downcasts the
Arrow array on every row. Removing the owning state
roundtrip does not remove that input dispatch.

Local DuckDB source was inspected at revision
`1c27c54f27bea397d18d86f7c0d6a9f1d4aa85f8` under
`/media/afilipchik/nvme6tb/src/duckdb/duckdb`:

- `src/execution/aggregate_hashtable.cpp`, UpdateAggregates: dispatches one
  aggregate across the payload vector and its resolved state-address vector.
- `src/include/duckdb/common/vector_operations/aggregate_executor.hpp`,
  UnaryScatter and UnaryScatterLoop: selects flat/constant/unified representation
  before a typed loop, with separate selection and validity cases.

This supports moving stable dispatch out of the row loop. It does not establish
which fraction of this engine's measured ingestion time comes from dispatch, nor
justify copying DuckDB's allocation or error behavior without this engine's budget
and row-publication contracts.

## Bounded implementation sequence for a later candidate

1. Bind fixed aggregate input columns to typed borrowed Arrow references once per
   retained batch. Keep logical decimal scale, NULL buffers, slice offsets and
   source lifetime. Count must handle any admitted input domain, not just numeric
   arrays. Keep selected-value columns on their existing path initially.
2. Charge binding metadata to the query pool before reading any input. The current
   binding allocates nothing; replacing it with an untracked Vec would regress the
   memory contract. Prefer reusable reserved metadata with an explicit lifetime
   tied to the retained batch. A preparation refusal must report the original
   ingestion cursor, before any group row changes.
3. Initially keep dictionaries on the checked generic route. Later dictionary
   binding needs checked codes and logical NULL handling, including NULL dictionary
   values and changing codebooks. Never assume matching physical codes mean
   matching logical values across batches.
4. Preserve the existing row transaction and input order. Binding alone can remove
   downcasts without changing group ownership, reduction order, spill policy or
   selected-payload admission. A vector-wide scatter implementation is a separate
   design: repeated group addresses, mid-vector refusals and partial spill retries
   need their own publication/cursor proof.
5. Add focused coverage for sliced primitive arrays, all numeric widths, decimal
   scale, signed zero and NaN payloads, NULL Count, duplicate row selections,
   dictionary fallback, empty batches, foreign layouts and binding-memory denial.
   Retain independent typed spill oracles and both ownership gates.
6. Freeze and measure the same raw/native/resident 16/4-thread shapes, then the
   complete provider/resource/concurrency gates if the diagnostic result warrants
   promotion. Record failures and all samples; no automatic repeat-until-pass.

## Important representation constraint

The existing `key_rows/bound_arrays.rs` demonstrates reserved borrowed bindings,
but its float output canonicalizes signed zero and NaN for SQL key equality.
Aggregate numeric input must retain the original bits and values. Do not feed
canonical key bytes into SUM, AVG or variance or broaden representation equality.

This proposal leaves native decoder admission, ownership costing and canonical
GPU execution as separate open work. It introduces no new source module today.
