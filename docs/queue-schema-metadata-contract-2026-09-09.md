# Dictionary output metadata accounting — September 9, 2026

Projection and inner-join output bound calculations now preserve field metadata
when adjusting the logical schema to the actual dictionary array type. Execution
already cloned the field and changed its type/nullability. Bounds instead built
new fields, discarding metadata and underestimating copied queue storage. The
runtime admission check correctly refused those batches; removing that check or
adding a fixed padding constant would conceal the broken contract.

Four existing native mutation tests reproduced errors after multi-segment insert,
delete and dictionary/self-referential update. Actual/bound bytes were
27364/27177,25748/25561,17210/17023 and35348/35161. The same exact pairs occurred
with the preceding IPC repair temporarily removed (control26497), proving their
independence from the reader change. See [IPC evidence](ipc-extent-validation-2026-09-08.md).

New independent regressions cover a metadata-rich dictionary projection with
exact duplicate/NULL/string values, and dictionary join copied-output bounds in
both output orientations. Projection51135 and join87055 each failed before the
repair. Both now pass. Source review checked both schema adaptation sites against
ProjectExec and hash_join::batch_with_actual_types. Queue admission, exact runtime
charge checking and semantic output remain unchanged. No dependency change.

Green73489 terminal0:1003 library passes,11 ignored; IPC3,native delete10,insert9,
streaming scans10,table validation12,update12 all pass (56 integrations). The final
follow-up only scopes the now-test-only Field import to cfg(test). Spill99630
terminal101 recompiles that source:6pass/7same failed names as before. This is
not full resource certification or a measured performance improvement.

Commands ran through scripts/claude-safe-build.sh with TMPDIR=$PWD/.scratch,
RAYON_NUM_THREADS=4, SAFE_BUILD_MEM=48G, SAFE_BUILD_JOBS=1 on HEAD88849c4 plus the
existing dirty tree, --locked --offline --features lance,gpu:

```
cargo test --locked --offline --features lance,gpu --lib dictionary_projection_bound_preserves_field_metadata_and_values
cargo test --locked --offline --features lance,gpu --lib dictionary_join_bound_preserves_metadata_in_both_orientations
cargo test --locked --offline --features lance,gpu --no-fail-fast --lib --test ipc_extent_contract --test native_insert_tests --test native_delete_tests --test native_update_tests --test native_streaming_scan_tests --test native_table_validation
cargo test --locked --offline --features lance,gpu --test spill_tests
```

[Immutable before/after, red/green and debugger evidence](benchmarks/2026-09-09-queue-metadata-tests/sha256.json)
contains11 verified files. Existing performance archives remain frozen; these
source repairs have not yet received a new release benchmark certification.

## Next resource boundary: decoded page admission

Contained GDB86984 terminal0 stopped at the actual denied PoolState::grow call
for count_distinct_spill_matches_in_memory. Request120512, used167901, limit262144.
The stack is ReservedVec<u8>::with_capacity(120000) → ReservedBufferBuilder →
admitted_page_body::decode_page_body → AdmittedColumnPages → AdmittedFlatColumn →
AdmittedBatchReader → StreamingParquetScan. This is a decoded Parquet page body,
not a queued-batch copy. The64KiB value-byte/8192-row output quantum does not bound
this whole-page decompression allocation. The named refusal is safe; completion
at this budget remains failed. The debugger intentionally stops before query
completion, so its exit0 is diagnostic success only.

Next inspect which encoded page, reader state and downstream reservations remain
live at decompression. Reproduce with a small independent page fixture and a
bounded pool. Choose a shared page/decoder contract that either reserves the
required simultaneous working set before pulling or supports bounded decoding
without replay, dropping live state, or bypassing admission. A smaller output
batch or a larger benchmark budget alone does not establish that contract.
The other six spill failures require their own attribution; no universal cause
is claimed from this one stack.
