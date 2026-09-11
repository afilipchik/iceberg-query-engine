# Frozen spill-I/O SF10 provider screen — 2026-09-09

Candidate322c8042 contains admitted spill buffering;507 source inputs are frozen.
The [shared Q18 gain](admitted-spill-io-2026-09-09.md) and
[protected comparisons](spill-io-paired-2026-09-09.md) remain distinct evidence.

Provider36745 is terminal1; independent completed-output audit66363 is terminal0.
All322 completed outputs are typed-correct:240 measured and82 warmups. The screen
has240/264 valid measured pairs. There are four engine warmup timeouts, two late
but correct warmups and two distinct DuckDB reference refusals. No measured
completion is silently relabelled as a timing pass.

| Track | Valid measured pairs | Completed warmups | Incomplete queries |
| --- | ---: | ---: | --- |
| Raw Parquet | 63/66 | 22 | Q9 late warmup |
| Native | 57/66 | 20 | Q6 late warmup; Q1/Q18 timeout |
| Iceberg | 63/66 | 21 | Q9 reference oracle refusal |
| Lance | 57/66 | 19 | Q1/Q18 timeout; Q9 reference warmup refusal |

Every mode uses canonical SF10 data/SQL,16 threads on CPUs0–15,4GiB query and12GiB
process limits, one session and three measured pairs per query. Fresh typed
DuckDB calibration sets the10× ceiling. The48GiB scope has zero max/OOM/kill events
and a cumulative23,988,498,432-byte peak. Source/binary/provider/data provenance is
preserved, with507 source hashes and the frozen binary checked after execution.

Native Q6 returns a correct701.343ms warmup above its634.831ms ceiling; this is a
late result, not a timeout or a wrong result. Its physical path is a global
aggregate over Filter/NativeStreamingScanExec. No matched study yet attributes
that late result to the buffering change. Native Q18's4,683.531ms ceiling remains
far below the separate23.7s successful diagnostic; the76% component improvement
is insufficient for this gate.

For Iceberg Q9, DuckDB refuses its oracle while allocating a268,435,456-byte block.
For Lance Q9, DuckDB refuses the warmup while allocating134,217,728 bytes. Both
report OutOfMemoryException. Dependent calibration and engine requests remain
not-run; no timing ceiling is invented from an incomplete reference. These are
reference failures, not engine result failures or scope OOM kills.

The [immutable archive](benchmarks/2026-09-09-spill-io-providers/manifest.json)
contains1,398 verified files. It records32 omitted temporary spill payloads
totalling2,032,439,760 bytes; completed outputs are retained. The archive preserves the harness, commands, all outputs/plans, samples,
reference failures and supplemental typed checks. Full residency/resource/
concurrency acceptance and DuckDB leadership remain unproven. Residency20505 is
running on the same frozen binary with separate decoded-IPC/CPU/GPU labels.
