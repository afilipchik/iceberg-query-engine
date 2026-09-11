# Cast construction cost attribution — 2026-09-07

The custom conversion loop, rather than reservation bookkeeping, is the stronger
measured optimization target in this isolated experiment. This does not establish
casts as the dominant remaining SF10 cost. Frozen635 remains rejected for repeated
SQL component regressions; production source has not changed during this diagnostic.

## Reproducible experiment

The diagnostic links exact632 and635 cast source, each extended only by a two-type
entry adapter, into one executable with pinned Arrow58.4.0. Shared memory/buffer
source and Cargo.lock are identical between these snapshots. Source, dependency
artifact and compiler hashes are recorded in kernel-sources.json,
linked-dependencies.json and build-provenance.json. The standalone release compiler
context differs from the SQL executable. Four fresh processes each run20 cases:
65,536/262,144 rows, five input patterns, Int8/Float64 targets. Each side has four
warmups and60 retained samples, with rotating order. Typed results match Arrow in
all80 cases; reservations return to zero after result destruction.

Construction and release are timed separately. Reservation-only and admitted
zero-fill controls measure cost, not a proof of another kernel's allocation bound.
All runs use a12GiB cgroup, CPU0 affinity, an8GiB process cap and a120-second watchdog.
The fourth run additionally records actual cgroup limits and memory.events before
and after: zero max/OOM/kill events; peak41,189,376 bytes. Hardware perf events are
unavailable (perf_event_paranoid=4); no instruction-level profile is claimed.

Build through the required wrapper, from repository root:

```bash
TMPDIR="$PWD/.scratch" SAFE_BUILD_MEM=12G SAFE_BUILD_JOBS=1 scripts/claude-safe-build.sh python3 .scratch/cast-construction-profile/build.py
TMPDIR="$PWD/.scratch" SAFE_BUILD_MEM=12G PYTHONPATH="$PWD/scripts" scripts/claude-safe-build.sh taskset -c 0 .scratch/venv-lance/bin/python .scratch/cast-construction-profile/run-observed.py
```

The build uses the exact local release rlibs listed in linked-dependencies.json.
Rebuilding dependencies may change artifact paths; regenerate linkage against the
frozen635 source/features before rerunning. The initial source-extraction shell
command was not retained; extracted source, derivation description and hashes are.
The run script writes run04 outputs: use a fresh output location for replication.

## Results

Ranges of four process medians, construction microseconds at65,536 rows, Int64→Int8.
These ranges are not confidence intervals.

| Pattern | Arrow | 632 | 635 | Reservation only | Admitted zero fill |
|---|---:|---:|---:|---:|---:|
| all_valid | 12.822–12.834 | 47.992–48.039 | 35.850–35.863 | 0.047–0.047 | 0.757–0.760 |
| all_invalid | 49.255–49.267 | 19.748–19.932 | 36.285–36.321 | 0.047–0.047 | 0.758–0.759 |
| mixed | 33.656–33.672 | 38.080–38.311 | 36.286–36.307 | 0.047–0.047 | 0.758–0.762 |
| nullable | 25.087–25.204 | 90.103–90.151 | 49.608–49.745 | 0.047–0.048 | 0.761–0.766 |

Arrow wins valid and nullable input, while632 wins invalid-heavy input. Replacing
all cases with raw Arrow would introduce a different regression and would bypass
the existing pre-admission/escaped-buffer ownership contract.

## Next experiment and decision rule

Pinned arrow-array58.4.0 primitive_array.rs::unary_opt initializes values and
validity once, then visits valid input indexes and clears validity only on failure.
In contrast635 repeatedly extends output within64-value chunks. This is a source
observation and a testable hypothesis, not proof of instruction-level causality.
Test whole-buffer initialization and indexed updates in a scratch-only adapter
using ReservedBufferBuilder; retain safe indexing and full lease lifetime. Compare
all patterns and both sizes against unchanged controls in the same executable.
Only a consistently competitive result justifies production integration and full
semantic/ownership/component gates. Never infer data validity from a sample.

If the isolated strategy remains uncompetitive, stop hand tuning this loop and
assess an audited kernel adapter or defer adoption of the regression-bearing change.
Any Arrow adapter requires a proven allocation envelope including aligned buffers,
temporary bitmap allocations and escaped-buffer ownership; reserving logical
output size alone is insufficient. Neither outcome closes equal-weight provider
attribution, timestamp-domain correctness, resource/concurrency gates or leadership.

## Preserved evidence

[Archive and manifest](benchmarks/2026-09-07-cast-construction/README.md):
28 members verified, archive SHA256
`739547a866c0af773f2c015ab244ab03d54650c2c42bba31a6ec549abb73dfb5`.
The archived report predates this archive-link paragraph.

The planned scratch experiment is now complete and was not integrated. See
[its results and allocation-interface audit](cast-kernel-allocation-interface-2026-09-07.md).
