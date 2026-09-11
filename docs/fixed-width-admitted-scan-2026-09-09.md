# Fixed-width scans can offer admitted decoding

The previous frozen e4608ccf improves resident Q9 through admitted projection and
join composition, but raw Q9 remains serial. Its lineitem scan offers a compact
copied-output bound and `streaming_parquet_scan/admitted.rs::prepare` explicitly
excluded every scan with that certificate, even though the existing admitted
decoder supports its flat numeric/decimal columns.

## Current change

The source now permits fixed-width scans to offer a separately prepared admitted
decoder. This reuses the existing checked row-group opener and budgeted page,
dictionary, validity and output construction. It does not wrap legacy output in
an ownership claim or relax byte accounting. Metadata/encoding/type checks still
run for every selected row group before any encoded page or output is consumed.
IPC sidecars still decline; unsupported formats decline before decoding.
Ordinary execute and its compact copied-output certificate remain unchanged.

Preparation is optional for a fixed-width scan that already has a certified
copied route. If descriptor/reader metadata admission fails, all partially built
owners are dropped before the factory returns None. There has been no page or
source-output consumption. This preserves an existing two-slot copied queue that
fits a1,364-byte query pool but cannot afford4,608bytes of decoder metadata.
I/O errors and non-fixed-route memory failures remain errors. Once an admitted
stream is selected, every runtime failure is terminal with no fallback or replay,
even if memory is subsequently released or a missing file restored.

No dependencies, ownership defaults, query-specific heuristics or native scan
code changed. Native input still needs its own admitted preparation contract.
Unlike a new adapter over legacy copied buffers, this route accounts for decoder
scratch as well as output. Its CPU cost must be measured: capability availability
alone does not prove it is faster than the previous Parquet implementation.

## Reproductions and tests

All test commands use `TMPDIR=$PWD/.scratch`, the repository capped wrapper,
48GiB/one build job, locked/offline dependencies and features `lance,gpu`.

- Red63061 reproduces None for an otherwise supported fixed-width scan.
- Green88469 passes the independent decimal/NULL/duplicate test across three
  partitions, Parquet1/2, plain/dictionary encodings and detached-array ownership.
- Initial broad51852:1,074pass/11ignored, one regression. A1,364-byte copied queue
  refused4,608bytes of optional preparation metadata. Integration targets were
  not reached after the library failure; this result is preserved.
- Corrected broad90721: **1,075library passes/11ignored and37integrations**.
  The existing tiny copied-queue regression passes. New lifecycle checks verify
  unsupported GZIP fallback before decode, preparation refusal cleanup, and
  terminal memory/I/O errors after selection.
- Both-mode5005 is terminal1: partial library1,075/11ignored; native10default
  passes and6partial passes/4failures; aggregate-memory10passes each; spill8/6
  and systemic numeric11/1 each. The failed names remain, but the partial-native
  deletion-vector request changes137328→137344bytes. This does not clear full
  resource acceptance.

Next freeze a source-verified optimized binary, repeat raw/native/resident Q9
routing and typed diagnostics, and compare against e4608ccf in reversed-order
blocks. Broader protected/provider/resource/concurrency acceptance remains required.

[Prior pipeline and measured resident gain](admitted-pipeline-q09-measurement-2026-09-09.md),
[admitted Parquet decoder](live-admitted-parquet-scan-2026-09-08.md),
[architecture](architecture.md).

## Native follow-up found during source review

The missing native capability cannot safely be filled by delegating ordinary
`execute`. NativeStreamingScanExec calls `read_segment_batches` in spawn_blocking,
which first collects IPC batches and then calls `filter_deleted_rows`. That helper
builds a Vec of all survivors, allocating a boolean mask and Arrow filtered columns
for each affected batch. The scan holds this complete VecDeque before yielding
its first batch. Thus deletion filtering can retain anonymous copies for an entire
segment; the module's "one batch in flight" prose is too strong. This is confirmed
source routing, not a new measured memory failure or a repaired native contract.

A native admitted reader should preserve the immutable manifest/segment snapshot,
checked IPC footer/block extents and Arrow validation, while decoding/validating
batch metadata and selecting deletion survivors under reservations. Preparation
must own bounded metadata, and output owners must survive detached arrays. Every
declared partition and deletion offset must be checked; no replacement of the
existing SQL/pruning proof with estimates is allowed. A regression should spread
deletions across several large batches in one segment and prove bounded first
output plus exact results after dropping earlier batches. Native's current
collected API has no query pool parameter, so merely adding an admitted factory
that wraps it would be an unsupported ownership claim.

## First frozen diagnostic: intended raw path still not reached

Release44979 completed in8m54s and froze2f9ad9f1, verifying518source inputs.
Diagnostic38254 is terminal0 with3independent typed-correct outputs and no OOM/max
(scope peak19656007680bytes). Raw6506.438463ms and native3474.093097ms remain
one-slot; resident1816.008924ms remains16slots. These isolated samples do not
prove a regression or improvement. The new raw capability did not compose into
Q9, so the prepared two-block comparison has not been launched.

The raw file's selected columns use supported INT64/DECIMAL annotations, ZSTD and
PLAIN/PLAIN_DICTIONARY encodings. The next investigation must locate the actual
higher-level capability rejection, rather than assuming decoder selection from
its availability. Preserve this negative evidence and the frozen binary.

The subsequent probes identified the higher-level barrier: join build materialization
substituted physical dictionary schema for declared UTF8 schema. That separate
repair, its dictionary regressions and full-projection preparation evidence are
recorded in [build schema preservation](build-schema-admission-2026-09-09.md).
The fixed-width decoder candidate's negative benchmark remains unchanged.
