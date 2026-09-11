# Decoded output quantum and handoff reservations — September 9, 2026

PlainFixedDecoder now treats the requested row count as an upper bound. On a
memory refusal it halves the output quantum down to one row. next_chunk commits
row/dense cursors only after successful output construction; failed provisional
output drops before retry. Non-memory errors are never retried. No page read,
dictionary-ID decode or volatile expression is repeated by this loop.

AdmittedBatchReader reserves its output vector and handoff metadata before
pulling columns. This prevents column output from consuming the capacity already
known to be required for returning a batch. The same reservations are transferred
to the returned buffer owners. Error exits release provisional handoff state;
pending column prefixes and row alignment remain intact. End-of-input and
zero-column paths avoid unnecessary handoff reservations.

GDB8419 established that the earlier66048-byte output refusal expands an Int64
dictionary: width8, row/dense0, full UInt32 IDs buffer60000 bytes, no NULL mask.
The retained value page is120000 bytes. A direct primitive view cannot implement
that indexed expansion; no zero-copy bypass was introduced.

New independent dictionary regression26603 failed with requested8704, used0,
limit4096. It includes1024 logical rows, duplicates and NULLs, verifies a fully
occupied pool leaves both cursors unchanged, then reconstructs all expected values
from smaller chunks. Focused93750 terminal0:50 admitted-storage tests pass.
The existing unequal-column-prefix test now verifies handoff refusal does not
pull the next column (one call instead of two); typed alignment/owner checks remain.

Broad96184 terminal101:1005 library passes/11ignored, IPC3 and native scans10 pass;
spill remains6pass/7fail. Count-distinct now fails a later32117-byte request with
252064 used. Joins refuse120512 with145343/145448 used. These changed boundaries
are not proof of equivalent failure causes or complete query progress. They show
that per-output adaptation and early handoff reservation alone do not bound all
simultaneous pages/dictionaries. No performance result is claimed.

Commands ran on HEAD88849c4 plus the existing dirty tree through
scripts/claude-safe-build.sh with TMPDIR=$PWD/.scratch, RAYON_NUM_THREADS=4,
SAFE_BUILD_MEM=48G and SAFE_BUILD_JOBS=1:

```
cargo test --locked --offline --features lance,gpu --lib dictionary_output_shrinks_to_budget_without_losing_rows
cargo test --locked --offline --features lance,gpu --lib storage::admitted_
cargo test --locked --offline --features lance,gpu --no-fail-fast --lib --test ipc_extent_contract --test native_streaming_scan_tests --test spill_tests
```

[Immutable sources, patches, debugger and test logs](benchmarks/2026-09-09-output-quantum/sha256.json)
contains11 verified files. Formatting/whitespace checks pass.

The next structural task is to inspect whole-page dictionary IDs and pending
columns as one reader working set. A bounded output request cannot make a decoder
that first materializes all IDs/pages fully incremental. Reproduce that minimum
working set independently, distinguish reducible retention from genuinely required
codec state, and design incremental ID/page consumption or explicit coordinated
reservation before changing defaults again. Full provider/resource/concurrency and
new frozen release performance validation remain open in the existing epic.
