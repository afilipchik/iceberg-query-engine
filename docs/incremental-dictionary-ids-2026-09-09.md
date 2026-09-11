# Incremental dictionary ID consumption — September 9, 2026

The shared flat Parquet reader now consumes dictionary IDs in output-sized
prefixes instead of requesting every dense ID in a data page. The existing
HybridDecoder already supported bounded chunks; its caller requested the full
page count. The new admitted_flat_column/dictionary_page.rs retains the encoded
ID cursor, dictionary and definition owners and at most one prepared ID prefix.
It serves both fixed-width and UTF8 dictionary expansion.

Preparation uses a provisional hybrid cursor. Memory refusal can reduce the ID
quantum without committing that cursor; semantic errors do not retry. A prepared
prefix is committed once, then retained through output refusal. Remaining fixed
or string output resumes from its own cursor. Page source reads are not repeated.
Whole decoded page bodies, dictionaries and definition bitmaps remain retained;
this is bounded ID expansion, not a fully bounded page/dictionary working set.
The new state is inline in the admitted column structure, with no uncharged Box.

A12,000-row independent writer fixture failed before the change: request41652,
used7182, limit16384 (red67950). It now passes for Int64/UTF8, V1/V2 pages, NULLs
and duplicates, checking every output value and unchanged file-read count across
all subsequent chunks and an injected budget refusal. Green95416 passes. Separate
regressions verify prepared IDs survive output refusal and later out-of-domain
IDs do not commit a bad prefix. Validation is incremental: a later malformed ID
may be discovered after earlier valid chunks, but the public column reader then
poisons and fails the query rather than accepting the malformed suffix.

Focused57253:53 admitted-storage tests pass. Broad70736 terminal101:1008 library
passes/11ignored,43 typed aggregate/ownership/IPC/native integrations pass. Spill
remains6pass/7fail; changed reader metadata and retention move denial boundaries.
No new release benchmark or full resource certification is claimed yet.

Commands ran through scripts/claude-safe-build.sh with TMPDIR=$PWD/.scratch,
RAYON_NUM_THREADS=4, SAFE_BUILD_MEM=48G, SAFE_BUILD_JOBS=1 on HEAD88849c4 and the
existing dirty tree:

```
cargo test --locked --offline --features lance,gpu --lib large_dictionary_pages_emit_bounded_ids_with_exact_values
cargo test --locked --offline --features lance,gpu --lib storage::admitted_
cargo test --locked --offline --features lance,gpu --no-fail-fast --lib --test ipc_extent_contract --test native_streaming_scan_tests --test spill_tests --test aggregate_binding_transparency --test aggregate_encoding_contract --test aggregate_expression_quantum --test decimal_root_reuse_aggregate_contract --test fused_aggregate_input_errors --test fused_aggregate_evaluated_keys --test fused_aggregate_budget_transition --test parallel_input_spill_contract
```

[Immutable before/after and test evidence](benchmarks/2026-09-09-dictionary-id-chunks/sha256.json)
contains8 verified files. Formatting/whitespace checks pass. No dependency changes.

Next freeze a release candidate containing the cumulative IPC, metadata, page
lifetime, output quantum and ID-prefix repairs, then compare with frozena023079f
using the existing matched canonical/provider/residency boundaries. Any measured
change belongs to this cumulative candidate, not solely to ID chunking. Full
page/dictionary working-set coordination and seven tiny-budget spill failures
remain open; do not certify leadership from the passing component fixtures.

Release15551 completed in8m48s. Frozen candidate11d16e73975eef6055cc06330533de1c6786ef79ce42566f035004e80470168a,
source manifest verified by the build driver. Matched four-block comparisons
against a023079f are now queued sequentially: custom memory Q1/Q6 and canonical
raw SF10 Q1/Q2/Q5/Q9/Q10/Q12/Q13/Q19/Q20. Same six measured pairs per block, typed
oracles, fresh DuckDB10x gates and balanced startup/execution order as preceding
measures. No performance result until terminal evidence is audited.
