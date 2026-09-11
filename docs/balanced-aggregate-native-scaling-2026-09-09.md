# Balanced ownership: native SF10 scaling — 2026-09-09

Frozen `cd8098d5` completes all24 diagnostic outputs correctly with the requested
ownership evidence. At16 query threads, partial ownership observes about47% lower
Q1 time, but about41% higher Q18 time. It must not become the blanket default.
The experiment exposes a second structural bottleneck: serial cross-owner final
merging reverses the gain from faster local ingestion on high-cardinality input.

## Conditions and observed paired results

Both modes use the identical lance,gpu release binary, with GPU explicitly off,
canonical SF10 native tables,4GiB query/12GiB process limits and fixed CPU0–15
affinity. Two fresh blocks reverse thread/query/mode order. Each case starts one
worker process, executes one query, validates its complete typed output against
the existing matched oracle and reaps the process before the next case.

| Query | Query threads | Disjoint times, ms | Partial times, ms | Geometric mean paired ratio |
|---|---:|---:|---:|---:|
| Q1 | 1 | 21497.244 / 21610.404 | 21577.718 / 21740.618 | 1.00488 |
| Q1 | 4 | 11567.454 / 11688.799 | 8183.953 / 8214.279 | 0.70512 |
| Q1 | 16 | 11707.116 / 11735.967 | 6356.117 / 6075.230 | 0.53014 |
| Q18 | 1 | 10420.625 / 10413.648 | 10360.144 / 10331.788 | 0.99317 |
| Q18 | 4 | 4996.412 / 5075.476 | 8068.060 / 7953.006 | 1.59068 |
| Q18 | 16 | 5043.694 / 5271.142 | 7258.604 / 7261.003 | 1.40799 |

The one-thread partial request executes the same ordinary one-owner algorithm.
At4 threads this compares ownership with the same worker count. At16 query
threads disjoint still has4 owners while partial has16, so the latter comparison
also includes removal of that cap. Affinity includes the same physical/SMT layout
in both modes;16 query threads must not be described as16 dedicated physical
cores. No control or candidate query spilled in these completed diagnostics.

These are two-block instrumented observations, not confidence intervals or the
ordinary10× DuckDB acceptance gate. The180-second watchdog permits attribution
of already-failing queries. The experiment neither establishes DuckDB leadership
nor replaces protected/provider/residency/resource/concurrency acceptance.

## Phase evidence changes the next action

Second-block,16-thread Q1 state processing falls from6914.633 to1676.191ms.
Routing remains2218.543ms and expression evaluation1579.843ms in the partial
case. Thus splitting hot keys addresses the measured ownership limit, but leaves
substantial input preparation/evaluation cost. Finish remains below0.2ms here.

Second-block Q18's dominant inner grouped stage has59,986,052 input rows:

| Stage, ms | Disjoint | Partial |
|---|---:|---:|
| Key preparation/routing | 1168.195 | 795.172 |
| State processing wall time | 2213.756 | 1001.974 |
| Whole ingestion | 3388.936 | 1807.623 |
| Whole finish | 1290.899 | 4888.790 |
| Output callback, included in finish | 1250.551 | 1246.690 |

Finish time outside the output callback grows from40.348 to3642.100ms. Output
construction itself is nearly unchanged. Source agrees with this attribution:
`ParallelControllers::finish` feeds all local partial states sequentially into
one `PartialMerge`, constructing one global group table. Existing disjoint owners
need no such global union. These are diagnostic wall intervals, not exclusive
CPU-cycle samples. They support fixing final-merge parallelism rather than
calling ingestion speedup whole-query success.

## Required next implementation

1. Keep cd8098d5 and both mode results as controls. Do not enable partial ownership
   globally or hide Q18 with query-ID rules.
2. Partition partial states by their checked canonical key into independent
   reduction owners, then merge owners in parallel. Equal keys must reach the
   same reducer; full equality still decides grouping. Only complete disjoint
   reducer outputs may concatenate. Final values and HAVING stay downstream.
3. Pre-admit reducer metadata, route windows, spill writers and progress resources
   from the same query budget. Preserve exact source-row cursors and borrowed
   source lifetimes. Do not allocate a second full set of routing arrays or scan
   every spill run once per reducer. A bounded resident parallel path may retain
   the existing correct spill fallback during development, but that does not
   certify spill-parallel performance.
4. Record partial-state row counts, final group counts, routing time, merge wall
   time and output time. Cardinality/skew observations may choose algorithms;
   they must never prove uniqueness or allow skipping a required merge.
5. Validate independent results under1/4/16 workers, hot/balanced/high-cardinality
   keys, NULLs, dictionaries, decimal/AVG semantics, actual spill, admission denial
   and consumer failure. Keep the current startup/poisoning/owner-release tests.
6. Rerun matched scaling and protected queries before default cost-policy selection.
   Follow with separate provider/residency/resource gates. After the reduction
   issue is resolved, Q1's remaining preparation/evaluation floor needs its own
   measured shared-kernel work.

## Provenance

Release76858 terminates0 in8m53s, freezes509 source inputs, and produces SHA256
`cd8098d5b9d82a1a80b84056dce4e65cb665129dd36938d496279a465d75fff7`.
The live build snapshot verifies48GiB MemoryMax/zero swap; its10,633,940,992-byte
peak is a live snapshot, not a terminal build peak.
Diagnostic4049 terminates0. All24 outputs and expected ownership traces pass;
source, binary, provider, dataset, driver and harness hashes verify afterward.
The48GiB scope peaks at7,741,480,960 bytes, with zero OOM/max events. There is no
GPU execution. The [verified archive](benchmarks/2026-09-09-balanced-ownership-native-scaling/manifest.json)
preserves every case, raw trace/output, both oracles, harness, prespec and release
provenance; source is linked to the preceding correctness archive.

Command: `TMPDIR="$PWD/.scratch" PYTHONPATH="$PWD/scripts" SAFE_BUILD_MEM=48G
SAFE_BUILD_JOBS=1 scripts/claude-safe-build.sh taskset -c 0-15
.scratch/venv-lance/bin/python
.scratch/parallel-aggregate-input/run_balanced_ownership_scaling.py`.
