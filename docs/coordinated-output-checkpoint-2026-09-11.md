# Coordinated reader intermediate checkpoint — 2026-09-11

Frozen binary `7ddbf9ce` contains531verified source inputs with Lance/GPU features,
based on pushed commit`de7605f`. The reader now prepares missing columns' page
state before output and admits reversible decoder checkpoints. Typed memory denial
discards provisional output and retries a smaller common row target without source
reads. Existing pending prefixes and final output ownership remain intact. Other
errors stay terminal. No dependency, ownership default or budget was changed.

The independent160KiB reproduction now completes all4096rows for both one-row and
larger targets; the larger target retains multi-row output. Plain/dictionary SNAPPY
fault tests verify cursor/ID rollback, zero source reads during trials, NULLs,
duplicates, unequal prefixes, retained leases and terminal errors. The oversized
page control still refuses cleanly. Full Lance/GPU validation passes1131library
checks (11ignored),125contracts and28spill/numeric checks in each ownership mode.
Native/IPC passes63disjoint and62with1failurepartial. The same6spill failures remain
each; complete executable/count/exit comparison finds no added or removed failures.

This is retained provisionally as a memory-progress repair, not a certified CPU
optimization. The42-output paired diagnostic validates all results,792join traces
and14equal logical/physical plan groups. Generic rawQ6 is1.030694times the previous
1bba20b3 checkpoint, slower in both reverse-order blocks; nativeQ9 is1.018964 and
resident4threadQ9 is1.020718, also slower in both. NativeQ6 improves to0.875128 in
both blocks, but this does not establish a broad gain. Generic rawQ6 remains1.779016
times pre-coercion13210a20. Two blocks do not establish confidence or neutrality.
All per-query samples and route records are preserved.

Canonical SF10 uses one session, three measured samples per query,16threads and
matched4/12GiB query/process caps for raw/native/Iceberg/Lance. Providers produce
337independently typed-correct outputs and252/264valid measured pairs: raw66,
native60,Iceberg63,Lance63. Raw completes22queries at geometric mean2.373308 and
suite2.528335 versus DuckDB,zero wins. NativeQ1 times out in warmup; Q6 passes warmup
then times out on measured sample1. IcebergQ13 calibration0 refuses a32MiB DuckDB
allocation. LanceQ9 calibration0 refuses128MiB in DuckDB; its engineQ9 is NOTRUN,
so the earlier engine join-index failure is not cleared. No incomplete provider
receives a full-suite score. Lance retains the recorded extension-optimizer
compatibility setting for decimal AVG correctness; this is not stock extension
pushdown performance.

Residency produces256typed-correct outputs and209valid measured pairs out of278
planned (212emitted). Decoded IPC completes66/66. CPUcontrol completes63/66 because
Q1 warmup times out; this blocks all66canonical mixed-GPU measurements. Planned
and emitted counts are both reported. The separate custom float smoke completes
40CPU and40required-GPU pairs, with all40measured device proofs validated. It is
not canonical SF10 GPU coverage. Canonical residency uses32/48GiB with preload
excluded and does not clear the16GiB preload gate.

Decoded IPC geometric mean is0.839134 and suite ratio1.393701,14query wins. Its
engine suite time is18108.629434ms versus18120.118760ms in the previous screen;
DuckDB's changes from18069.815889ms to12993.196972ms. The higher ratio mainly reflects
the reference-time change. Separate screens cannot isolate a candidate regression,
and stable aggregate engine time does not prove per-query neutrality.

Checkpoint98921 is terminal1; independent evidence72426 is terminal0. Archives
verify452diagnostic,1425provider and1097residency files, plus source/red/validation/
refusal evidence. Eight empty temporary payloads are omitted. The diagnostic/screen
scope peaks at39497113600bytes under48GiB,swap0,zeroOOM/max. Separately, the combined
compile/test scope reached48GiB and103max events withzeroOOM; preserve that distinct
qualification. No DuckDB leadership or full resource/concurrency acceptance.

The remaining COUNT(DISTINCT) spill refusal is whole-page decompression before
output trials, already at a one-row target and with zero collected join batches.
A complete dictionary/page/metadata ownership ledger remains open. Native IPC
admission, unnecessary dictionary decoding and possible alignment copies are
additional source-verified boundaries. Reference initialization/provider overhead
is a separate, unproven explanation for its allocation refusals.

Commit/push this completed cycle, then run the documented same-cap reference
initialization diagnostic and continue source-level native/page ownership work.
Do not change budgets, skip validation, or treat missing comparisons as success.

- [Implementation and remaining refusal](coordinated-reader-output-2026-09-11.md)
- [Paired diagnostic](coordinated-output-triage-2026-09-11.md)
- [Provider screen](coordinated-output-providers-screen-2026-09-11.md)
- [Residency screen and ratio supplement](coordinated-output-residency-screen-2026-09-11.md)
- [Terminal records](benchmarks/2026-09-11-coordinated-output-checkpoint/manifest.json)
- [Native admission follow-up](native-admission-follow-up-2026-09-11.md)
- [Reference initialization follow-up](reference-worker-initialization-follow-up-2026-09-11.md)
