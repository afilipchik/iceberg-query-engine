# General aggregate update boundary — September 8, 2026

The prepared-key change improves canonical Q10 by3.97% and Q19 by1.67% in the
completed paired component, but does not solve memory-resident Q1 or canonical
Q12/Q13. The earlier memory-input diagnostic already had four input slots and
four aggregate workers. More threads and faster key preparation alone therefore
have not established the required completion/performance outcome.

## Verified source distinction

Our `physical/morsel_agg/group_rows.rs::process_rows_with_limit` iterates rows,
resolves each group, prepares every aggregate slot, then commits that row.
`state_rows.rs::prepare_arrays_indexed` repeats argument type/extent checks and
resolves fixed Arrow inputs into ScalarValue for each row. `StateRows::begin`
copies fixed states into transactional scratch; `PreparedRow::commit` copies them
back. This protects exact retry semantics, but an Arrow batch at the operator
boundary does not make this inner update loop a batch-specialized kernel.

Local DuckDB snapshot `1c27c54f27bea397d18d86f7c0d6a9f1d4aa85f8`,
`src/execution/aggregate_hashtable.cpp`: `AddChunk` obtains group addresses with
`FindOrCreateGroups`; `UpdateAggregates` loops over aggregates and invokes
`RowOperations::UpdateStates` or its filtered variant over the payload batch.
It advances the state-address vector by each aggregate's payload size. This is
an explicit group-address/update-kernel boundary, not proof of a particular SIMD
instruction or a comparison against the installed benchmark wheel's source.

Local ClickHouse snapshot `a1b25f3f4beb3ba49aa3b73671cc244185331b86`,
`src/Interpreters/Aggregator.cpp::executeImplBatch`: after constructing group
places, it loops over aggregate instructions and invokes `addBatch` or a
single-place batch operation. `addBatch` dispatches ordinary, array and sparse
argument forms. Compiled aggregate functions are a separate capability in this
source. Neither source observation establishes that copying these implementations
would satisfy our memory or SQL contracts. No third-party code was copied.

## Implementation path and safety constraints

First bind stable Arrow argument types and typed accessors once per retained
evaluated batch. A private token must carry exact layout/array ownership and
extents; it cannot be reused for an unrelated batch. Preserve per-cell validity,
dictionary code/value checks, decimal precision and signed-scale behavior. Keep
the existing per-row transaction and first-unapplied-selection-position cursor
during this change so type binding can be measured independently.

Then evaluate a group-address plus fixed-aggregate batch kernel. Dispatching each
aggregate across all rows changes the failure boundary: if one aggregate updates
the whole batch and another fails halfway, the old row cursor cannot safely retry
the batch. Do not transpose these loops without a new transaction contract.
Admit address vectors, new groups/index growth and required scratch before updates;
either prove the update phase allocation-free or retain complete rollback/delta
state. Errors must poison/abort before results are visible. Spill must resume from
an exact unapplied prefix, not replay a partially updated aggregate column.

Retain general fallbacks for selected/variable states and unsupported encodings.
Do not use estimated cardinality or dictionary IDs as semantic proofs. Bound
typed access first across primitive, decimal and dictionary/NULL test shapes;
compare independent SUM/AVG/count results with duplicate selections, empty batches,
multiple partitions, forced spill, allocation denial and cleanup. Measure key
preparation and state updates separately, then paired complete queries and the
full provider/resource gates. Per-row validation is a source-level cost hypothesis;
its isolated CPU contribution is not yet measured.

This read-only source inspection occurred during the four-provider screen on
frozen1a0ece71, which has since completed; see the prepared-key provider report.
No engine code changed during its measurements.
