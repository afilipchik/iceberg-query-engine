# Remaining CPU bottlenecks after declared-schema repair

Diagnostic15989 is terminal0 on frozen703b8564,518verified source inputs.
Six fresh-process outputs independently match typed canonical SF10 oracles.
Default disjoint ownership,16threads CPU0–15,4GiBquery/12GiBprocess, GPU disabled,
48GiB enclosing scope and repository TMPDIR. Instrumented single samples with a
180second diagnostic watchdog are attribution only: they do not replace the
matched DuckDB10times ceiling or clear any provider failure.

| Provider | Query | Instrumented ms |
|---|---|---:|
| raw_parquet | q01 | 618.677 |
| raw_parquet | q13 | 2284.444 |
| raw_parquet | q17 | 1826.607 |
| native | q01 | 11484.916 |
| native | q13 | 2029.649 |
| native | q17 | 1360.724 |

Native Q1's Project declares8partitions but the frontier reports1slot and no
admitted buffers or copied-output bound. It consumes59,142,609rows in916batches.
The aggregate reports1509.410msevaluation,9348.389msingestion,2489.423msrouting and
6842.878msprocessing wall time, four disjoint workers, no spill, and four result
rows. These phase counters can overlap and must not be summed as independent
query costs. This reproduces substantial CPU work even after accounting for the
missing parallel input capability; scan admission alone is not a proven remedy.
Earlier ownership experiments already demonstrated that low-NDV routing can
concentrate work and that partial ownership can regress other thread counts.
Do not change the default based on this single query.

Raw and native Q13 each show an inner aggregate with16admitted input slots and a
single-partition final projection/aggregate. That final aggregate's one partition
is not itself proof of an accidental serialization bug. Raw Q17 uses16admitted
slots; native Q17 declares8partitions but runs with1slot. Profile shared operators
before inventing query-specific rewrites or assuming all remaining cost is input
serialization. Planning materialization remains separately visible in the
uninstrumented provider samples.

## Implementation sequence

1. Reproduce native whole-segment survivor retention with a multi-batch segment,
   deletions spanning batches, NULLs/dictionaries, and an independent value oracle.
   Prove which batches are decoded and copied before the first output. Current
   read_segment_batches/filter_deleted_rows collect every survivor first.
2. Introduce incremental native IPC batch iteration preserving the immutable
   segment snapshot, checked footer/block extents, Arrow validation, deletion
   offsets and terminal errors. Keep the collecting API as an explicit consumer
   of the same iterator where materialization is required. Bounded batching is
   necessary but does not establish query-wide admission.
3. Add native admitted preparation only with reserved metadata, selection/output
   buffers and owners that survive detached arrays. Check all partitions, early
   cancellation, preparation refusal, and post-selection errors without replay.
   Test ordinary and admitted routes against independent typed results.
4. Reprofile Q1 and Q17 before implementing general aggregation ownership costing.
   Use worker count, admitted headroom and measured/estimated skew only for costing;
   never infer semantic uniqueness. Require matched4/16thread low/high-NDV controls
   and retain the known4thread Q18 regression as a protected gate.
5. Freeze each candidate, repeat matched comparisons and all provider/resource
   gates. Keep IPC/GPU residency separate and require request-scoped device proof.

No source or dependency changed during these measurements. Scope peak5681647616
bytes, swap0, zeroOOM/max events. Binary/source/dataset/driver/harness after-guards
verify. [Provider screen](build-schema-admission-provider-screen-2026-09-09.md),
[archive](benchmarks/2026-09-10-build-schema-bottleneck-diagnostics/manifest.json).
