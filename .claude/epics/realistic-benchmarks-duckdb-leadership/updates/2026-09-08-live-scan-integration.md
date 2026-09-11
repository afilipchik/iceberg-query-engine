# Live scan integration checkpoint, September8

The admitted page/column/batch path now has a validated row-group adapter. It
accepts parsed metadata, file handle, explicit projection and output schema,
checks flat counts/types/annotations/codecs/encodings before page construction,
and preserves repeated/reordered output. The live scan does not yet select it.

Source evidence: `physical/operators/streaming_parquet_scan.rs` uses cached
metadata builders and an unfold of Parquet/IPC readers; it has no query pool.
`physical/planner.rs` constructs it in lower_scan and has SharedMemoryPool.
`storage/metadata_cache.rs` retains global parsed footer caches outside query
reservations. The separate public StreamingParquetReader is not the measured path.

Required implementation sequence:

1. Carry the planner's SharedMemoryPool into the live scan and reader state.
   Retain parsed metadata ownership and account for decoder/metadata work; do not
   call a new footer parser per row group or claim the global cache is admitted.
2. At each row-group open, resolve existing runtime filters and form the union
   of output, static-predicate and runtime-key columns. Validate this read set
   through the adapter before reading any pages. Restore exact output order,
   repeated columns and logical names after filtering. Keep IPC residency distinct.
3. Implement/reuse admitted predicate masks and flat survivor gather. NULL masks
   mean false for WHERE; runtime filters combine as in the existing path. Do not
   replace the scalar decode cliff with one batch per surviving row/run, or use
   an unadmitted Arrow take/filter allocation under a new budget-safe label.
4. Introduce one retained live reader state with explicit capability selection.
   Unknown schemas/encodings keep an always-correct supported policy or refuse
   before consumption. Never restart through the old reader after output/errors.
   Preserve all partitions, empty filtered row groups and source-error propagation.
5. Test mixed projection/filter-only/runtime-only columns, duplicate projections,
   NULLs, dictionaries, multiple files/groups/partitions, source failure and
   admission denial. Check actual displayed/observed reader quantum and owners.
6. Build/freeze a release candidate, rerun independent-limit Q12 (1/12 and4/4GiB)
   and the contained resource matrix with exact outputs; then fresh matched SF10
   provider gates. Retain all failures and compare with the frozen control.

No production performance claim applies to the current adapter checkpoint.

Row-group checkpoint validation: library62226 is terminal0,948 passed,0 failed,
10 existing ignored,30.03s. The ignores remain eight dedicated CUDA tests, one
IPC-sidecar test and one dependent-join test, not successful hardware/provider
coverage. Focused suite76733 passed42 with no skips before the final preflight
assertion. Both use `TMPDIR="$PWD/.scratch" RAYON_NUM_THREADS=4 SAFE_BUILD_MEM=48G
SAFE_BUILD_JOBS=1 scripts/claude-safe-build.sh cargo test --locked --offline
--features lance,gpu`, selecting `--lib storage::admitted_ -- --nocapture` or
`--lib -- --test-threads=4`. Formatting/whitespace pass. Evidence is preserved in
`docs/benchmarks/2026-09-08-admitted-row-group/`. No new benchmark binary or SF10
result was produced.

Survivor gather implementation: `admitted_gather` now copies exact flat typed
values, strings and NULL masks into pre-admitted buffers; `admitted_batch::finish`
retains handoff metadata on output buffers. Three focused tests24354 pass against
independent Arrow take/filter oracles, including refusal after partial output
construction and extracted-owner lifetime. The real Parquet fixture now composes
this gather with aligned decoding. Step3 survivor copying is implemented;
predicate-mask evaluation remains, as do query-pool/metadata and live routing.

Gather checkpoint validation: library45937 is terminal0,951 passed,0 failed,
10 existing ignored,29.45s. The ignores remain eight dedicated CUDA tests,
one IPC-sidecar test and one dependent-join test. A subsequent test-only assertion
checks empty fixed-width typed-clone/ArrayData metadata retention; focused30367
passes3 tests with zero skips. Production source did not change after the library
gate. Commands use `TMPDIR="$PWD/.scratch" RAYON_NUM_THREADS=4 SAFE_BUILD_MEM=48G
SAFE_BUILD_JOBS=1 scripts/claude-safe-build.sh cargo test --locked --offline
--features lance,gpu`, with `--lib -- --test-threads=4` or
`--lib storage::admitted_gather -- --nocapture`. Formatting/whitespace pass.
Evidence: `docs/benchmarks/2026-09-08-admitted-gather/`. No new SF10 result.

Numeric predicate-mask follow-up: `CompiledPredicate::evaluate_admitted` now
reserves column handles, register slabs and output bitmaps; allocation refusal
is an error distinct from runtime type decline. A per-register validity program
supports nullable arithmetic/comparison/AND/OR/NOT directly. The real Parquet
fixture composes admitted decode, numeric/date mask and survivor gather against
independent values. Full library77670 passes954,0 failures,10 existing ignores;
fmt/whitespace pass. Strings/IN/LIKE and other unsupported compiler shapes,
compilation metadata admission, live query-pool/metadata and routing remain.
Evidence: `2026-09-08-admitted-compiled-predicate`. No SF10 claim.

String predicate follow-up: the opt-in admitted compiler now supports UTF8
comparisons, constant IN/NOT IN including NULL members, and constant LIKE/NOT LIKE
using the existing allocation-free matcher. Ordinary compiler selection remains
unchanged. Real Parquet decode/mixed numeric-date-string masks/gather validate
against independent values. Full library15375 passes957,0 failures,10 existing
ignores; focused1570 passes9 with no skips. Program/literal metadata admission,
query-pool/footer/cache ownership and live scan selection remain before Q12/SF10.
Evidence: `2026-09-08-admitted-string-predicate`; fmt/whitespace pass. No SF10 claim.

Reserved compilation follow-up: `compile_reserved` now bounds AST depth/nodes,
admits construction before cloning, shrinks to retained vector/string capacities
and owns that lease for the program lifetime. The real Parquet fixture now uses
this API with a1MiB component pool. Preflight review also removed temporary
qualified-name String formatting from the shared resolver. Focused47358 passes11;
qualified integration98795 passes12; full library71312 passes959,0 failures,
10 existing ignores. Formatting/whitespace pass. Evidence: reserved-compilation.
Footer/cache ownership and live query-pool/scan selection remain before Q12/SF10;
construction admission is conservative and does not prove exact RSS. No SF10 claim.

Metadata identity follow-up: reproduced stale cached row counts after preserved-
mtime replacement (red60780:2 vs5). Cache now keys opened-file identity, retains
schema owners, drops obsolete variants and builds readers from that same handle.
Final focused85952 passes3, full library91595 passes962 with10 existing ignores,
and integrations35500 pass32 with no skips. This fixes live cache correctness;
footer/cache memory admission and later in-place mutation remain separate open
contracts. Evidence: metadata-cache-identity; see the dedicated report. No SF10
claim; admitted live scan routing remains pending.


Metadata ownership follow-up: live raw scan work shares `ParquetSnapshot`, the
metadata/version used for pruning. Every row-group reader opens independently,
checks that version and reuses the retained metadata. Cache eviction does not
force reparsing; pathname replacement explicitly refuses before that group's
output. A shared cloned File cursor was deliberately avoided because Parquet's
File ChunkReader uses seek. Unix identity is required; later in-place mutation,
IPC lifecycle, parser/query-wide metadata admission remain separate. Global
retention now caps 256 variants/256MiB reported estimates; all eight SF10 footers
are reused in the contained probe. Library17707 passes965 with10 existing ignores;
integrations89712 pass34 without skips. Evidence: scan-metadata-ownership and
metadata-retention. Live admitted decoder/query-pool selection remains pending;
no new benchmark result applies to these changes.


Live pressure routing implemented: variable-width raw scans receive planner query
pool and select the admitted row-group/mask/gather pipeline. Exact projection,
filter-only columns, repeated output, runtime updates at group open, empty groups
and terminal denial are covered. Library83054:968 pass/10 existing ignores;
integrations78413:36 pass/no skips. Canonical SF10 uses ZSTD and now explicitly
refuses in this route; implement bounded ZSTD next before release/Q12 measurement.
No workload transcoding, no fallback replay, no performance claim. Footer/schema
and query metadata admission, fixed-width legacy and IPC routes remain separate.
See the live-admitted-Parquet report and `2026-09-08-live-admitted-scan` archive.


ZSTD complete at component/live level: admitted static DCtx workspace and fixed
output slice, exact lengths and typed refusal; pinned zstd-sys2.0.13+zstd1.5.6 now
direct with experimental bindings. Focused94037 passes2; full library24107 passes970
with10 existing ignores; integrations1592 pass36/no skips. Real live matrix covers
SNAPPY/ZSTD. Actual SF10 codecs/encodings are supported. Build/freeze release next
and run the prepared hash-checked Q12 isolation driver. No performance claim yet.
Evidence: admitted-zstd; footer/query metadata admission and broader gates remain.
