# Preallocated cast traversal and strategy experiment — 2026-09-07

**Neither scratch prototype is accepted for production.** Valid-index traversal
reduces nullable construction cost, but no strategy tested here is uniformly
competitive. Production remains635 and still fails its protected performance gate.
These measurements do not establish casts as the dominant SF10 cost.

## Scope and method

Starting from the preserved whole-buffer prototype, the preallocated variant
initializes admitted value/validity extents and copies the actual offset input
bitmap. It visits only valid indexes through pinned Arrow's valid_indices API.
All output updates use safe indexing. The converter is a pure value operation;
all buffer admission happens before conversion. Strict and infallible type-domain
paths retain their prior logic. This tests the traversal contract inside the cast
module; a reusable production kernel interface has not been introduced.

The adaptive variant samples at most32 evenly spaced values for non-null input
only. If a majority fail conversion, it uses an append-and-set-validity traversal;
otherwise it uses the preallocated traversal. The sample controls cost only: both
paths perform complete per-value conversion. The sample_miss fixture puts valid
values exactly at sampled positions and failures elsewhere; it exposes a poor
strategy choice without causing incorrect output. No query identifiers are used.
No claim is made that this heuristic generalizes to unseen data.

Three independent, sequential12GiB scopes, CPU0 affinity,8GiB process cap and120s
watchdog. Each process has28 cases: two sizes, seven patterns and two target types;
each side retains70 construction/release samples after four warmups. All84 typed
case comparisons match pinned Arrow; all observed memory.events remain zero.
The driver retained70 rather than the planned60 samples because its generated
iteration bound was74; all70 are preserved and used consistently across sides.
Seven additional scratch tests pass with no skips: the two candidate modules each
run the existing2,704-combination type/bitmap oracle and failure-message test;
a separate352-combination adversarial sample/offset/strict-TRY oracle passes.
The first oracle build failed on an ambiguous test integer type and was corrected;
both logs are preserved. No production Rust integration suite or full provider,
GPU, cap or leadership acceptance is inferred from these scratch tests.

## Results

Ranges of three process medians in microseconds,65,536 rows, Int64→Int8:

| Pattern | Arrow | 632 | 635 | Preallocated | Adaptive |
|---|---:|---:|---:|---:|---:|
| all_valid | 12.823–12.835 | 47.927–47.953 | 35.853–35.873 | 13.191–13.204 | 25.100–25.106 |
| all_invalid | 49.005–49.029 | 19.709–19.719 | 36.290–36.300 | 49.542–49.550 | 24.487–24.507 |
| mixed | 34.606–34.761 | 37.255–37.614 | 36.297–36.328 | 34.053–34.056 | 36.566–36.779 |
| nullable | 19.541–19.628 | 93.730–96.880 | 53.915–55.147 | 35.760–35.892 | 39.533–39.788 |
| sparse_null | 3.123–3.135 | 66.448–84.875 | 8.187–8.531 | 6.619–6.651 | 6.077–6.133 |
| sample_miss | 49.189–49.206 | 19.975–20.296 | 36.309–36.337 | 49.727–49.733 | 49.702–49.712 |

The preallocated valid-index path improves nullable and sparse-null construction
versus635, but remains slower than Arrow. The adaptive path fails to retain the
all-valid speed of its non-adaptive counterpart and is slower than632 on invalid
input. The sample_miss input forces its slower choice. These are direct timings;
compiler/code-layout effects are hypotheses, not measured instruction attribution.
Do not compare absolute control timings across separately compiled diagnostics.

## Reproduction and disposition

Exact source/rlib hashes, rustc command and binary identities accompany raw output.
From repository root, retaining fresh output paths when replicating:

```bash
TMPDIR="$PWD/.scratch" SAFE_BUILD_MEM=12G SAFE_BUILD_JOBS=1 scripts/claude-safe-build.sh python3 .scratch/cast-preallocated-profile/build.py
TMPDIR="$PWD/.scratch" SAFE_BUILD_MEM=12G PYTHONPATH="$PWD/scripts" scripts/claude-safe-build.sh taskset -c 0 .scratch/venv-lance/bin/python .scratch/cast-preallocated-profile/run-observed.py 1
TMPDIR="$PWD/.scratch" SAFE_BUILD_MEM=12G SAFE_BUILD_JOBS=1 scripts/claude-safe-build.sh python3 .scratch/cast-preallocated-profile/test.py
```

Repeat the measurement command with2 and3, sequentially. The test driver uses
opt1; timing uses opt3/fatLTO. Build linkage requires the recorded635 release
artifacts or regeneration against its frozen source and features.

Stop production cast-loop adoption from these results. Preserve the admission
contract and the regression as open; resolving it requires stronger kernel/codegen
attribution or an audited library interface, not another unsupported speed claim.
The next substantive implementation work is the known timestamp-domain defect:
represent unit/timezone with exact ticks across binder, scalar extraction and
expansion together, then validate scalar subqueries, negative epochs and aggregates.
Do not merely change the binder metadata or count unsupported results as feature
parity. The existing broader CPU/provider/resource/workload tasks remain open.

## Verified evidence

[Archive and manifest](benchmarks/2026-09-07-cast-preallocated/README.md):31
members verified; archive SHA256
`bec063a902e06cffc5754036c44d02af1d69db1a616c06c9168091e2fca8e80a`.
The archived report predates this link paragraph.
