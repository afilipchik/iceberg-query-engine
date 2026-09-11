# Admitted PLAIN UTF8 output decoder

Added `storage/admitted_plain_utf8.rs`, a component for flat, decompressed PLAIN
BYTE_ARRAY bodies and decoded validity. It validates complete encoded extents
and UTF8 before output allocation; exact-sized Arrow buffers use existing
pre-admitted builders. Byte/row chunking preserves a retryable encoded cursor;
output slices retain charges. It is not yet routed into production scans.

Four tests pass, zero skips, under the48GiB wrapper with Rayon4 and one build
job, `--locked --features lance,gpu`: typed NULL/duplicate/Unicode chunks,
prefix refusal/retry and slice ownership, malformed/empty input, and actual
writer-produced PLAIN pages. Initial compile fixture error and successful rerun
logs are preserved. Formatting and whitespace checks pass. No dependency change.

This closes the output-buffer construction part only. Compressed/decompressed
page ownership, definition levels, dictionary IDs and scan integration remain
open. The low-budget scalar fallback is unchanged, so no SF10 gain is claimed.
The next step is pre-admitted page construction with retained ownership, followed
by definition/dictionary integration and the existing independent-limit gates.

[Contract and evidence](../../../../docs/scan-budget-batching-cliff-2026-09-08.md).

Follow-up: added `admitted_page_body` for fixed-destination UNCOMPRESSED/SNAPPY
decoding, checked decoded/prefix extents and reservation-owned page buffers.
V2 level-only pages are covered. Root now directly depends on the already-pinned
snap1.1.1; no package version changed. Combined gate75621 passes9 tests, no skips,
with `--locked --offline --features lance,gpu`. Page headers and encoded input
are still caller-owned; admitted reads/bounded parsing precede scan integration.
No production routing or SF10 performance claim yet.

Encoded-read follow-up: `admitted_page_read` admits a full validated range before
payload I/O and uses positional reads. Short/interrupted reads, partial errors,
EOF after source truncation and retained slices are covered. The Unix file
fixture composes read → Snappy decode → PLAIN strings with separate owners.
Gate62164 passes14 combined tests, no skips; fmt/whitespace pass. Bounded header
parsing, snapshot validation, levels/dictionaries and routing remain open.

Header follow-up: `bounded_page_header` adds allocation-free Compact envelope
parsing with bounded nesting/byte work, checked core and column extents, admitted
immutable header windows and encoded-body CRC checks. Gate3273 passes7 new tests,
zero skips, including actual writer-produced Parquet pages and a deterministic
malformed corpus. Typed page subheaders and an owned retry-safe page cursor are
next; levels/dictionaries/routing remain open. No production scan speedup claimed.

Typed/cursor follow-up: V1/V2/dictionary headers validate required fields, counts,
level extents and compression semantics. `admitted_column_pages` owns its source
and advances only after validation/CRC/admitted decompression; budget denial is
retryable, hard failures poison, dictionaries precede data and column counts
must match. Nine focused header and four cursor tests pass. Full library80733
is terminal0:924 pass,10 existing ignores; fmt/whitespace pass. Actual-file tests
now use our parsed value counts and owned cursor, with library decoding only as
oracle. Next: admitted levels/dictionary IDs and aligned column assembly, then
production routing and the independent-limit performance gate. No SF10 claim.

Hybrid follow-up: `admitted_hybrid` provides pre-admitted UInt32 chunks for
RLE/bit-packed levels and dictionary IDs, widths0–32, exact domain checks and
retry-safe chunk commits. Gate58699 passes5 focused tests, zero skips, including
257 independently expected nullable dictionary values from three actual Parquet
row groups. Test assembly vectors are ordinary allocations, not a claim of
production output admission. Next: admitted NULL masks/dictionary expansion,
retained current-page state and aligned column assembly before scan routing.

Retained-decoder follow-up: admitted flat NULL masks and dictionary UTF8 output
are implemented, including exact ID/cardinality checks and retry-safe chunk
commits. Dictionary/ID/validity handles and optional owned PLAIN/hybrid pages
retain input reservations across pulls. The actual nullable dictionary-file
fixture now uses admitted intermediate/output buffers. All29 admitted-component
tests pass with zero skips (58315); formatting/whitespace pass. Retained flat
column composition, fixed-width decoding, aligned batches and production routing
remain. Evidence: `docs/benchmarks/2026-09-08-retained-decoders/`. No new SF10 gain.

Full library gate17456 is terminal0:935 passed,0 failed,10 existing ignored,
29.53s, using the same contained feature/environment settings and four test
threads. Ignored coverage comprises eight dedicated CUDA tests, one dedicated
IPC-sidecar process test and one existing dependent-join test. These are not
successful hardware/provider coverage. Log: `owned-decoder-library.log`.

Retained UTF8 column composition is now implemented in `admitted_utf8_column`.
It supports flat required/optional PLAIN/dictionary V1/V2 pages, retains pending
page owners across downstream admission failure, preserves chunk position, checks
V2 decoded NULL counts and poisons unsupported/hard failures without replay.
All31 admitted-component tests pass (24387), zero skips, including12 actual-file
combinations and an explicit delta-encoding refusal test. Formatting/whitespace
pass. Fixed-width decoding, aligned batches and production scan routing remain.

Column checkpoint validation: full library81145 is terminal0,937 passed,0 failed,
10 existing ignored,29.68s. The ignored cases remain eight dedicated CUDA tests,
one dedicated IPC-sidecar test and one dependent-join test; no hardware coverage
is implied. All31 admitted tests pass separately (24387). Commands use
`TMPDIR="$PWD/.scratch" RAYON_NUM_THREADS=4 SAFE_BUILD_MEM=48G SAFE_BUILD_JOBS=1
scripts/claude-safe-build.sh cargo test --locked --offline --features lance,gpu`,
with `--lib storage::admitted_ -- --nocapture` or `--lib -- --test-threads=4`.
Formatting/whitespace pass. Source snapshots, hashes and all attempted test logs
are preserved in `docs/benchmarks/2026-09-08-admitted-utf8-column/`.

Fixed-width follow-up: `admitted_plain_fixed` provides exact/endian-correct typed
PLAIN and dictionary output with NULL-aware dense positions and retry-safe
admission. `admitted_flat_column` replaces the UTF8-only module and shares page,
level and pending-conversion state across both families. The actual-file matrix
checks eight independent typed columns across V1/V2, dictionary on/off and three
row groups. Aligned multi-column batch assembly and production routing remain;
no SF10 performance claim.

Fixed-column full library gate27567 is terminal0:943 passed,0 failed,10 existing
ignored,29.39s. It includes the additional numeric dictionary prefix/refusal test.
The10 ignores remain eight dedicated CUDA tests, one dedicated IPC-sidecar test
and one dependent-join test, not successful provider/hardware coverage. Commands:
`TMPDIR="$PWD/.scratch" RAYON_NUM_THREADS=4 SAFE_BUILD_MEM=48G SAFE_BUILD_JOBS=1
scripts/claude-safe-build.sh cargo test --locked --offline --features lance,gpu
--lib -- --test-threads=4`. Formatting and whitespace checks pass. Evidence and
source provenance: `docs/benchmarks/2026-09-08-admitted-fixed-column/`.

Aligned batch follow-up: `admitted_batch` retains per-column pending chunks,
commits a shared row range after successful handoff, and preserves prefixes on
source or metadata admission denial. Output buffer owners retain handoff leases
through typed clones/ArrayData. The real-file matrix now assembles nine mixed
columns across unequal boundaries. Type/required-NULL/declared-count errors poison
without replay. Production integration must target the live physical scan with
its cached metadata, predicate pushdown and IPC routing, carrying the query pool;
the separate public storage reader is not the measured path.

Batch checkpoint validation: full library92639 is terminal0:947 passed,0 failed,
10 existing ignored,29.56s. Ignored cases remain eight dedicated CUDA tests,
one dedicated IPC-sidecar test and one dependent-join test. Commands use
`TMPDIR="$PWD/.scratch" RAYON_NUM_THREADS=4 SAFE_BUILD_MEM=48G SAFE_BUILD_JOBS=1
scripts/claude-safe-build.sh cargo test --locked --offline --features lance,gpu`,
with `--lib storage::admitted_ -- --nocapture` for the40-test intermediate gate or
`--lib -- --test-threads=4` for the final library gate including the added type/
required-NULL test. Formatting and whitespace pass. Source/evidence archive:
`docs/benchmarks/2026-09-08-admitted-batch/`. No new SF10 performance claim.
