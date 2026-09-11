# Whole-buffer cast experiment and allocation interface — 2026-09-07

**Do not integrate this prototype.** It restores all-valid speed but loses on
invalid-heavy and nullable input. Production remains frozen635, which is itself
not performance-accepted. This is a negative experiment with a narrower causal
finding, not an engine optimization or a leadership result.

## Experiment

The scratch prototype initializes admitted values and validity once, then writes
through safe slices. All-valid input avoids input-null checks; nullable input uses
Arrow's option iterator. Failed values clear validity. Strict casts, conversion
functions and output leases are inherited unchanged from635. No unsafe code or
production/dependency changes were introduced. This is not a faithful copy of
Arrow's valid-index traversal: that distinction matters for nullable costs.

Three fresh processes, CPU0,12GiB cgroup,8GiB process cap,20 cases per process,
60 samples after four warmups per side. All60 case oracles match Arrow; all
reservations return to zero. All observed memory.events stay at zero. These tests
cover Int64→Int8/Float64 only; no general SQL/offset/strict-mode certification is
claimed. Six sides rotate within each process. Both controls are compiled into
this executable;632 timing changed from the previous five-side executable, so
cross-executable medians must not be substituted for within-process comparisons.

Ranges of three process medians, microseconds,65,536 rows, Int64→Int8:

| Input | Arrow | 632 | 635 | Whole buffer |
|---|---:|---:|---:|---:|
| all_valid | 12.809–12.823 | 42.786–42.797 | 35.844–35.853 | 12.693–12.702 |
| all_invalid | 49.119–49.171 | 16.556–16.587 | 36.279–36.286 | 48.901–48.903 |
| mixed | 33.642–33.662 | 34.892–34.903 | 36.285–36.302 | 35.252–35.635 |
| nullable | 25.873–25.940 | 90.150–90.346 | 50.178–50.382 | 72.769–84.064 |

The same trade-off appears at262,144 rows; complete raw samples are preserved.
The first build contained an unreachable timing-driver arm, corrected before any
measurement. Its log/provenance are retained; the measured binary is identified
in build-provenance.json and every observed-run-NN.json.

Reproduction from repository root, using the frozen dependency paths in the
recorded linkage (use fresh destinations to preserve these outputs):

```bash
TMPDIR="$PWD/.scratch" SAFE_BUILD_MEM=12G SAFE_BUILD_JOBS=1 scripts/claude-safe-build.sh python3 .scratch/cast-whole-buffer-profile/build.py
TMPDIR="$PWD/.scratch" SAFE_BUILD_MEM=12G PYTHONPATH="$PWD/scripts" scripts/claude-safe-build.sh taskset -c 0 .scratch/venv-lance/bin/python .scratch/cast-whole-buffer-profile/run-observed.py 1
```

Repeat the second command with2 and3, sequentially. Build settings are opt3,
fat LTO, one codegen unit, with exact release rlibs from source635's lance+gpu build.

## Systemic finding and next implementation boundary

The resource integration replaced library buffer construction and traversal at
once. The experiments show that preserving pre-admission does not itself explain
the slowdown: a budgeted path can match Arrow on valid input. Traversal/failure
handling and compiler context still matter. Avoid another blanket replacement of
all primitive cast kernels based on a single favorable input distribution.

A concrete dependency audit rules out simply enabling Arrow's pool feature as
the memory-safety fix. In pinned58.4.0, MemoryPool::reserve and reservation::resize
are infallible; the pool may overfill. MutableBuffer::with_capacity allocates first
and begins with no reservation. It rounds requested capacity to64-byte multiples
and calls handle_alloc_error on failure. Those APIs cannot provide the engine's
required named, pre-allocation refusal merely through a tracking-pool adapter.
Exact local source paths and SHA256 values are in arrow-source-audit.json.

The next bounded work item is a **fallible, preallocated kernel interface**:

1. Define primitive cast kernel input/output contracts independently of allocation:
   caller supplies admitted values and validity extents; kernel may not grow them
   or retain references. Keep NULL validity and conversion failure separate.
2. Audit the pinned Arrow valid-index traversal and conversion routines for reuse
   with caller-owned slices. Preserve upstream license/attribution if code is
   adapted. Avoid a generic raw-cast fallback whose temporary allocations are
   outside the reservation. This needs a design review before production edits.
3. Establish empty/all-null/sliced bitmap and strict/TRY oracles, integer extremes,
   floating-point boundaries, decimal metadata, admission refusal before conversion,
   and escaped-buffer lease tests. The scratch20-case timing fixture is insufficient.
4. Compare valid/invalid/mixed/nullable distributions at both sizes; require no
   repeated protected regression before the expensive production build/provider
   screen. Do not use samples to establish semantic validity or query identifiers
   to choose a path.
5. Resume equal-weight attribution across required providers after the cast
   regression is resolved. Casts have not been shown to account for30% of remaining
   end-to-end excess; this work does not trigger the broad package9 engine rewrite.

The temporal representation defect and wider memory/resource/workload gates remain
open. No fullSF10/GPU/cap run was repeated for this scratch-only experiment.

## Verified archive

[Evidence and manifest](benchmarks/2026-09-07-cast-whole-buffer/README.md):
28 members verified; archive SHA256
`ab13f68fb4065a5c795c91444e02085ec620b561fcc7c33597021211eed99373`.
The archived report predates this link paragraph.

The traversal/strategy experiment has now completed without production integration:
[results and disposition](preallocated-cast-traversal-2026-09-07.md).
