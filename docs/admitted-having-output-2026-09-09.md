# Admitted HAVING output and exact decimal predicates — 2026-09-09

## Reproduction and change

The [matched frozen attribution](input-retention-attribution-2026-09-09.md)
reproduces Lance Q18 completing twice on b61ce3d3 and refusing twice on acdb8c51.
The aggregate fallback retains raw source batches under a transient input domain
that fills before its query-level spill threshold. HAVING previously forced this
fallback without attempting the streaming partial-state path.

A generic16MiB regression reproduces the same problem without benchmark SQL:
131,072 rows over256 groups, exact wide decimal sums, nullable values/keys, and a
small HAVING result. Red36427 fails when the2MiB input domain fills. The current
candidate streams accumulation and filters completed groups instead.

`filter/admitted_batch.rs` binds a shared `AdmittedBatchFilter` before consuming
input. It owns admitted program, column-map and metadata storage. Ordinary
admitted FilterExec now uses the same evaluator as aggregate finalization.
Supported masks are admitted, NULL predicate values discard rows, all-true batches
reuse their buffers, all-false batches drop immediately, and partial outputs use
admitted selection. Masks must match input length; output leases survive slices.
Unsupported or unavailable optional binding declines before source consumption.
Once evaluation starts, failure is terminal: no expression/source replay.

`morsel_agg/live_spill.rs` binds HAVING before opening the input frontier, then
applies it only inside controller finalization after partial/spilled group states
have merged. It retains only surviving output batches. It does not apply HAVING
to partial sums, change aggregation order, or raise any memory limit. Empty scalar
aggregation keeps its existing ordinary route. Unsupported HAVING/aggregate
capabilities still fall back; other collecting consumers remain an open contract.

## Decimal comparison capability

The first shared-filter implementation passed integer SQL/spill tests but the
wide-decimal case still declined. The admitted predicate compiler now supports
Decimal128 comparisons against Decimal128, Int64 and Int32 columns/literals.
Precision differences do not change numeric comparison; full runtime column types
must still match the bound schema. Float/decimal coercion is not added.

`compiled_expr/decimal.rs` binds the power-of-ten scale factor once. Each comparison
uses integer coefficients and checked multiplication. If scaling a nonzero
coefficient exceeds i128, its sign proves ordering against every representable
coefficient on the other side. Zero is handled separately; reversing the scaling
direction reverses the comparison. This covers negative scales and large scale
separations without float conversion, truncation, or per-row allocation. SQL NULL
validity propagates through the existing Boolean program. Expression identity
and representation-aware literal equality are unchanged. No dependency change.

## Validation and limits

Commands use locked/offline lance,gpu builds through the required wrapper,
TMPDIR=.scratch,48GiB scope, one build job and four Rayon threads.

- Red36427: generic input-domain refusal reproduced.
- Initial93962: compile visibility error; corrected with a crate-local re-export.
- Green96220:2 tests pass; decimal capability remains red.
- Green46392:all3 integration tests pass after exact decimal support.
- Broad56714 terminal0:1,029 library tests pass (11 ignored),35 integrations pass.
- Spill82440 terminal101:6 pass,7 preceding failed names remain. These are not
  cleared by the new actual-spill regression.
- Formatting and whitespace checks pass.

Coverage includes wide coefficients beyond f64 precision, NULLs, aliases, empty
and all-true/all-false/partial output, output ownership, and actual spill merging.
The spill test filters final sums only after four batches contribute to each group
and asserts positive spilled bytes. Decimal tests cover all comparison operators,
mixed scales/integers, bit-offset slices and chunk boundaries, sign/extreme-scale
cases and an independent small-domain scaled-integer oracle.

Eight changed source/test inputs versus acdb8c51;506 total frozen inputs.
Release1709 completed successfully in8m51s, binary `fe3cc8fe`. Its sampled build
scope peak was10,631,086,080 bytes with no memory-limit/OOM events.
The correctness archive has8 verified files:
[manifest](benchmarks/2026-09-09-having-output/manifest.json).

## Frozen Lance Q18 result: correct, too slow

Matched diagnostic67286 is terminal0: the preceding acdb8c51 refuses both requests
under its512MiB transient source domain; fe3cc8fe completes both and matches the
independent typed oracle. Reverse block ordering, data/provider/setup,4GiB query
and12GiB process limits,16 threads and CPU affinity0–15 are preserved. All506
source hashes, both binaries and provider inputs verify after execution. The48GiB
scope peaks at14,406,021,120 bytes with no max/OOM/kill events.

This repairs completion, but **fails the performance objective**. The first
candidate request takes98,735ms. Both traces spend about22.2s ingesting and76.1–76.3s
finalizing; output construction/filtering accounts for only1.33–1.35s of that
finalization. Each spills about1.59GB and retains624 qualifying groups. These
instrumented180s-watchdog diagnostics do not satisfy the matched10× timing gate.

During the second candidate request, `/proc/PID/io` records70,350,157 read calls
and95,973,068 write calls, despite only45,056 physical bytes read at that snapshot.
This is a process-wide partial sample, not an exclusive operator measurement.
Source inspection confirms `RunReader` passes an unbuffered File to three frame
reads, and `RunWriter` passes an unbuffered File to individual serialized field
writes. Frame reads additionally request stream position for exact admission retry.
The scheduler may scan/repartition/remerge these row-framed runs repeatedly.
The next shared correction should batch I/O with admitted buffers, preserve exact
retry positions and checked framing, and propagate buffered flush failures before
publishing a run or releasing input state. Buffer ownership must obey the same
query budget, with optional buffering declining before any I/O if unavailable.
No query-specific memory-limit change or relaxed framing is warranted.

The189-file [frozen archive](benchmarks/2026-09-09-having-lance-attribution/manifest.json)
preserves both results, source, harness, oracle, build records and the I/O sample.
Raw/native/preloaded diagnostics47517 completed successfully with one request per
mode, chosen before launch after the slow Lance replication. All three outputs
match their independent typed oracles. All506 source inputs, binary, dataset and
provider manifests verify after execution. The64GiB scope peaks at21,550,297,088
bytes with no max/OOM/kill events.

| Mode | Query/process budget | Instrumented Q18 | Input accumulation | Finalization | Spill bytes |
| --- | --- | ---: | ---: | ---: | ---: |
| Raw Parquet | 4/12GiB | 969.975ms | Different scan aggregate route | Different scan aggregate route | See preserved response |
| Native | 4/12GiB | 98,977.688ms | 22,299.827ms | 76,120.895ms | 1,590,000,742 |
| Preloaded CPU | 32/48GiB | 5,757.286ms | 3,775.920ms | 1,412.709ms | 0 |

These are single-request diagnostic timings, not paired DuckDB ratios. The
resident result is a capacity experiment and does not clear lower-budget
residency acceptance. It cannot justify raising default budgets. Native reproduces
the same spill-heavy cost as Lance despite916 rather than7,323 input batches;
small source batches alone cannot explain the roughly99s result. All five repaired
candidate outputs across both studies are correct, but this is not a full suite.
The30-file [provider diagnostic archive](benchmarks/2026-09-09-having-provider-diagnostics/manifest.json)
references the frozen source/harness archive above. Full protected, provider,
residency, resource and concurrency acceptance remain open.

## Next implementation gate: bounded spill I/O

Keep fe3cc8fe as the immutable control. Change the shared spill-file I/O boundary,
not Q18 planning or query memory limits. Reserve optional read/write buffers before
allocation; retain leases for their complete lifetime and use a correct unbuffered
fallback if optional admission is unavailable before I/O starts. Do not use an
unaccounted standard buffered allocation. Start with a bounded byte quantum and
measure its benefit before exploring partition or state-format changes.

The reader must preserve logical stream position even when it reads ahead.
`FrameCursor::read` takes a position before each header and rewinds exactly on
scratch admission denial. Returning the underlying descriptor position would
skip data. Retain ordinal, identity, CRC, truncated-frame and trailing-byte checks,
including across reader reset and buffered boundaries.

The writer must surface a failed buffered flush before `RunWriter::finish` can
publish a `SpillRun`, and before a prepared flush clears aggregate source state.
Partial write failures poison the writer; never retry a partially written buffer
from its beginning. Drop must not silently publish or flush failed work. Preserve
owned-file cleanup and foreign-file protection. Existing tests that inject direct
file truncation must flush pending bytes before the injection so they still test
actual corruption rather than an empty physical file awaiting buffered output.

Add independent counting-I/O tests that show many small logical reads/writes
collapse into bounded underlying operations, with exact byte equality. Cover
short/interrupted reads/writes, partial-write failure, admission refusal, buffer
boundaries, seek/rewind and owner release. Run the existing framing, spill-file,
repartition, merge, actual-spill and HAVING gates; preserve the seven preceding
integration failures as failures. Then freeze a new release and repeat this
matched Lance experiment plus native/low-budget and protected query gates.
Buffering can remove syscall overhead; it does not by itself repair repeated
partition scans, serial finalization or compact-state cost. Measure what remains.
