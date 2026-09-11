# Live admitted Parquet pressure path, September 8

The production `StreamingParquetScanExec` now selects the admitted decoder for
raw scans under planner-estimated memory pressure when no fixed-width copy bound
applies. The physical planner supplies its existing shared query pool; directly
constructed scans default to the process pool. Ordinary fixed-width and IPC paths
retain their existing routing. This is a production routing change, not merely a
standalone decoder test.

`streaming_parquet_scan/admitted.rs` retains a row-group reader, uses the union of
output/static-filter/runtime-key columns, reserves numeric/string predicate
compilation and evaluation, combines WHERE-valid-true and runtime membership in
an admitted bitmap, and gathers survivors into admitted buffers. Output order,
repeated projections and logical schema names are restored with reserved handoff
metadata. Runtime filters are resolved anew at each row-group open; absent slots
are skipped, populated slots are AND-combined. Signed Int32/Int64 keys preserve
exact integer membership. Empty filtered batches/groups advance through remaining
work. Errors terminate the stream; there is no legacy decoder replay.

The route uses an 8192-row maximum and a preferred 64 KiB string-values target
per column output. Actual page bodies, levels, dictionary IDs, output buffers,
mask scratch and handoff owners use the existing admission components. A single
large value may exceed the preferred value target only after actual admission.
Whole pages/dictionaries must fit or refuse; compressed pages are not internally
streamed. The plan now exposes `decoder=admitted_flat` and the actual row quantum.
It continues to report unknown copied-output bound for variable-width data.

The 39-row nullable-string live fixture now emits one admitted batch rather than
39 legacy single-row batches, preserving exact values. This is a dispatch result,
not a throughput claim. The full SF10 low-budget Q12 improvement is unmeasured.

## Verification

- Focused17514: 5 passed, no skips.
- Focused42508: 7 passed, no skips, including the new live V1/V2 × dictionary
  on/off matrix and query-pool denial regression.
- Full library83054: 968 passed, zero failures, 10 existing ignored, 30.95 seconds.
  This includes an additional unsupported-ZSTD refusal/no-replay test.
- Integration78413: partition_contract17, runtime_filter_domain_contract2,
  shared_prescan_errors3, systemic_numeric_tests12, typed_memory_pressure2:
  36 passed, zero failures, zero skips.
- Tests use the memory-capped wrapper, repository TMPDIR, Rayon4, one build job,
  48 GiB scope, locked/offline lance,gpu features. Formatting/whitespace pass.

The live matrix checks filter-only tag and runtime-key columns, static LIKE and
numeric comparison, two runtime filters, NULL keys/strings, duplicate values and
repeated output columns, fourteen row groups across every partition, empty
filtered prefixes and independently calculated exact outputs. Retained output
buffers keep pool charges alive; dropping outputs and scan returns usage to zero.
The 128-byte denial test requires a typed memory error, terminal stream and zero
leaked reservation. These are component/live routing gates, not full provider,
concurrency or RSS certification.

## Required next step: ZSTD

Canonical SF10 generation uses COMPRESSION ZSTD (`scripts/benchmark/prepare.py`,
line124). The current admitted page path supports UNCOMPRESSED/SNAPPY and therefore
explicitly refuses this dataset under variable-width memory pressure. This is a
known intermediate capability regression relative to the slow legacy reader,
not a performance pass. Do not publish or freeze a performance candidate yet.
Implement bounded ZSTD destination and workspace ownership next; then inspect the
actual field encodings and rerun Q12's independent query/process-budget cases.
Do not transcode benchmark inputs to conceal missing codec support.

The lockfile pins zstd-sys2.0.13+zstd1.5.6. Its primary C source exposes
`ZSTD_estimateDCtxSize` (sizeof context), `ZSTD_initStaticDCtx` (caller-owned,
8-byte-aligned workspace), and single-pass `ZSTD_decompressDCtx`. These are a
candidate interface, not an implemented or verified contract here. Inspect the
pinned version and legacy/dictionary/error paths before introducing unsafe FFI;
pre-admit destination and workspace and test corrupt/truncated/oversized bodies.

Footer parsing and Arrow metadata/schema construction remain outside decoder
admission. Query-owned metadata avoids footer reparsing, but the admitted adapter
currently reconstructs the plain Arrow metadata view per group to remove legacy
dictionary overrides. Query/plan metadata and runtime-payload ownership still need
complete admission. In-place file mutation, IPC sidecar lifecycle and full provider
certification remain open. The fixed-width/ordinary legacy decoder has not been
made globally allocation-safe by this route. No new release binary, SF10 ratio,
GPU or engine-leadership claim applies to this checkpoint.


ZSTD implementation follow-up: the intermediate ZSTD refusal above is superseded
by admitted static-context decoding and a fixed destination. The direct dependency
reuses the pinned zstd-sys2.0.13+zstd1.5.6 with experimental bindings. Full library
24107 passes970,0 failures,10 existing ignores, including the expanded live matrix.
Actual SF10 footers use supported ZSTD/PLAIN/dictionary encodings. Release/Q12
measurement remains open. See `admitted-zstd-2026-09-08.md`.
