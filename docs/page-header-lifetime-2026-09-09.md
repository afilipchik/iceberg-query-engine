# Parquet header lifetime and decoded output budget — September 9, 2026

AdmittedColumnPages now releases the consumed header-read window before admitting
the decoded body. Typed fields, the validated encoded body and captured decoded
length own everything decompression needs. CRC and header validation still finish
before release; cursor updates still occur only after successful decoding. No
codec replacement, budget increase, source replay or admission bypass was added.

GDB96905 identified SNAPPY at the original120512-byte refusal. The cursor was at
file offset4, chunk end86405, decoded size120000. Inspection showed the full
64KiB header read window remained live through decompression. It is not required
once typed metadata and the CRC-checked body have been obtained.

Independent pseudo-random ASCII fixture85490 failed before the repair: requested
120516, used186573, limit262144. The fixture produces a110–125KB encoded page
and checks every decoded byte, EOF, owner lifetime and final pool cleanup.
Focused40548 terminal0:5page-reader tests pass. The existing post-read refusal
fixture now uses a compressible1024-byte value so its decoded body really exceeds
remaining capacity; its retry still verifies two positional reads per attempt,
unchanged cursor on denial and full reservation cleanup. This avoids testing a
refusal that only existed because an unnecessary header allocation was retained.

Broad50491 terminal101:1004 library tests pass/11ignored, IPC3 and native scans10
pass. Spill remains6pass/7fail, with changed failure boundaries. All commands used
scripts/claude-safe-build.sh, TMPDIR=$PWD/.scratch, RAYON_NUM_THREADS=4,
SAFE_BUILD_MEM=48G, SAFE_BUILD_JOBS=1, HEAD88849c4 plus the existing dirty tree:

```
cargo test --locked --offline --features lance,gpu --lib decoded_page_does_not_retain_consumed_header_window
cargo test --locked --offline --features lance,gpu --lib storage::admitted_column_pages::tests
cargo test --locked --offline --features lance,gpu --no-fail-fast --lib --test ipc_extent_contract --test native_streaming_scan_tests --test spill_tests
```

[Immutable source, patch, red/green and debugger logs](benchmarks/2026-09-09-page-header-lifetime/sha256.json)
contains10 SHA256-verified files. Formatting/whitespace checks pass. This repair
has no new release performance measurement and does not certify spill completion.

## Next coordinated working-set contract

GDB82310 stopped at the next count-distinct spill refusal:66048 requested,
222320 used,262144 limit. It is ReservedBufferBuilder<i64> capacity8192 inside
PlainFixedDecoder::primitive, reached through AdmittedFlatColumn/AdmittedBatchReader.
It is an Arrow output buffer, not a new header or queued copy. The earlier decoded
page allocation now succeeds. Join/outer-join tests now refuse60512 bytes instead
of120512; those new boundaries have not yet been independently attributed.

AdmittedBatchReader fills pending arrays column by column. Decoded pages,
dictionaries and earlier pending columns can remain live while later columns
allocate pages/output. The fixed8192-row output request does not account for this
combined working set. Merely dropping the header cannot guarantee completion.
Next reproduce multi-column coexistence and inspect pending/decoder owners, then
coordinate a shared output quantum with page/dictionary reservations. Preserve
row alignment, exact retry position and no replay. Where one complete page or
combined required pages cannot fit, bounded decode/retention needs a separate
contract; reducing output rows alone is not sufficient. The8KiB semi/anti and
aggregate-state refusals remain separately open.
