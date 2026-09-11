# Shared Lance scanner candidate — September 11, 2026

The candidate replaces one scanner per fragment with one ordered scanner across
selected fragments. The objective is to reduce duplicated I/O and decode
scheduling. Sustained memory improvement and performance remain unproven until
the current SF10 screen and repeated-worker diagnostic finish.

## Why change this boundary?

Each Lance scanner schedules concurrent reads and decoding internally. The
provider also spawned every fragment independently, multiplying these scheduling
windows. The previous binary failed canonical Lance Q9 after 80 successful
requests, although the same Q9 completed in a fresh process. Allocator controls
failed sustained acceptance: lazy commitment failed after 82 requests; disabling
arena reservation completed one 88-request sequence and then aborted on the next.
Neither setting is promoted. See the [allocation investigation](join-index-allocation-follow-up-2026-09-11.md)
and its 500 independently validated outputs.

Local reads in pinned Lance can use Tokio blocking tasks. Recorded runs reached
hundreds of I/O threads. This supports testing shared scheduling, but thread count
alone does not explain all retained mapping growth. Scanner I/O allowance also
excludes returned output and is not a hard total memory limit.

## Implementation and contracts

`scan_fragments_inner` now calls one `scan_ordered_fragments` scanner. Selected
fragment metadata follows dataset order. An explicit empty subset returns no
rows. The helper also supports supplied fragment-vector order, as promised by
Lance's `with_fragments` and `scan_in_order(true)` API. Projection, filters,
8192-row batches and AllLate materialization remain. Runtime size, allocator,
process caps, query budgets and default ownership are unchanged.

The former provider-level tasks, `FragmentTasks` guard and four collector tests
are removed together. No provider-spawned fragment tasks remain to detach.
Internal Lance cancellation and blocking work remain dependency contracts;
collected results still need resource accounting. Fewer scanners do not establish
query-wide memory admission.

## Validation

The independent fixture creates three appended fragments, each larger than one
output batch. Expected rows are generated independently. It covers reordered
fragments, subsets, empty subsets, reordered projection, NULLs, duplicates,
filters, deletion files, multiple batches and a no-match predicate. The production
routing check additionally verifies dataset order for subsets.

The fixture passed before production routing changed (job 38877). The production
Lance library passed 28 tests (76256), and SQL integration passed 31 with fixtures
present (52325). Broad validation 39700 preserved the known failure inventory:

| Gate | Default disjoint | Experimental partial |
|---|---:|---:|
| Library | 1,132 passed; 11 ignored | 1,132 passed; 11 ignored |
| Contracts | 125 passed | 125 passed |
| Native / IPC | 63 passed | 62 passed; 1 failed |
| Spill / numeric | 28 passed; 6 failed | 28 passed; 6 failed |

Executable counts, exits, failure names and retained test names were compared
against the parent. Exactly four obsolete collector tests were retired and one
fixture added. No new failures were found. The existing failures remain open.
The compile/test scope peaked at 43,907,133,440 bytes under a 48 GiB cap, with zero
OOM/max events and swap disabled; this is not query-only memory telemetry.

The [validation archive](benchmarks/2026-09-11-ordered-scanner-validation/manifest.json)
contains 25 files and verifies all 531 archived source inputs. Lance, Lance-IO and
Lance-Core match all 364 files in their cached crates, whose hashes match
Cargo.lock. No dependency source was modified.

## Frozen SF10 screen

Release 44289 completed successfully in 8m48s, freezing binary `835ae7ad` from
531 source inputs with Lance/GPU features. The provider screen uses raw Parquet,
native, Iceberg and Lance at 16 threads, a 4 GiB query budget and 12 GiB process
cap, three samples and one session, with default disjoint ownership. Reference-only
Lance I/O quota 16 is explicit. Source remains frozen during measurement.

The screen is terminal (pipeline 44289: release 0, providers 1). Independent audit
1801 validates all 341 completed outputs and 255/264 measured pairs:

| Provider | Typed outputs | Valid pairs | Complete | Geometric mean vs DuckDB | Suite time vs DuckDB |
|---|---:|---:|---|---:|---:|
| Raw Parquet | 88 | 66/66 | Yes | 2.4355× | 2.6348× |
| Native | 81 | 60/66 | No | — | — |
| Iceberg | 84 | 63/66 | No | — | — |
| Lance | 88 | 66/66 | Yes | 2.2194× | 3.3108× |

Raw has zero per-query wins; Lance has one. Native Q1 warmup and Q6 measured1
time out. DuckDB's Iceberg Q9 calibration1 crashes with exit -11 (SIGSEGV), so
engine Q9 is not run. These are failures, not evidence of engine correctness for
missing outputs. The combined release/screen scope peaks at 25,843,490,816 bytes
under 48 GiB, with zero OOM/max events and swap disabled.

The [SF10 archive](benchmarks/2026-09-11-ordered-scanner-sf10/manifest.json) contains
1,275 files, including the parent comparison, and verifies source, binary and
harness hashes. Lance Q9 now completes in the persistent benchmark worker, but
performance regresses on the 21 queries shared with the prior incomplete screen:
engine geometric mean 1.1459×, reference 1.0035×; Q12 is 4.4344× and Q19 1.9888×.
This historical comparison is unpaired and excludes failed parent Q9. It flags
regressions for diagnosis, not a certified causal effect or overall speedup.
Retain this candidate provisionally as a scheduling/resource change; performance
work remains necessary before promotion.

Two-sequence Lance diagnostic 37426 completed all 176 requests with the default
allocator and the same query/process caps in one worker. Audit 35908 independently
validates all 176 outputs. The 16 GiB scope peaked at 7,943,675,904 bytes, with zero
OOM/max events and swap disabled. Observed I/O threads peaked at 46 (95 total).
End-of-sequence VmData changed from 10,697,532 KiB to 10,697,596 KiB: only 64 KiB
between the first and second sequence. These are request-boundary observations,
not instantaneous process peaks or unlimited-duration acceptance.

The [190-file endurance archive](benchmarks/2026-09-11-ordered-scanner-endurance/manifest.json)
preserves all outputs, process snapshots and worker termination. Its first archive
verification caught a worker/archive manifest filename collision; the worker
manifest was renamed, the archive rebuilt and every hash reverified. No engine
rerun or output alteration was needed. The diagnostic omits reference interleaving
and uses a 180-second timeout, so it cannot certify performance.
Fresh decoded IPC, GPU residency and concurrency acceptance are outside this
provider-specific cycle and remain open. No DuckDB leadership is certified.


## Next cycle

1. Pair the parent and candidate with reversed block order across several scan
   shapes, using the existing opt-in Lance scan timing and independent typed
   outputs. Confirm whether the Q12/Q19 observations repeat and attribute time to
   provider scans versus downstream operators before choosing a change.
2. Inspect the generic filtered/multi-fragment read plan and its concurrency
   limits. Preserve the reduction in in-flight work; do not restore unbounded
   scanner fanout or relax allocator/process caps to recover throughput.
3. If extra concurrency is needed, define its shared ownership and admission
   boundary before implementing it. A fixed scanner count alone is not proof of
   a query-wide memory bound. Keep projection, subset, deletion, error and
   cancellation behavior covered by independent fixtures.
4. Re-run the resource sequence and canonical SF10 after the next implementation,
   archive every failure and push another intermediate checkpoint. Native
   timeouts, the Iceberg reference crash, raw CPU throughput and broader
   provider/residency/concurrency acceptance remain open work.
