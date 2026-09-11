# TRY_CAST construction and checked SQL metadata

Status: 782 selected tests pass, with two existing library ignores. Optimized
semantic validation, component timing, five-provider SF10 screens, protected
repeats, supported GPU residency and aggregate cap gates have completed. The previous frozen628 candidate is rejected
for36–46x TRY_CAST component overhead despite passing scoped correctness/cap gates.

The shared converter now returns Option for value conversion. TRY_CAST writes NULL
without formatting an error; strict failure constructs a descriptive error once.
Allocation/admission and invalid metadata errors remain outside that conversion
path and cannot be swallowed by TRY_CAST. A regression asserts that TRY_CAST never
calls the strict error factory. Non-null strict casts also select a values-only
loop outside row iteration. The expanded Arrow comparison exercises984 combinations
of type pair, metadata, mode and empty/non-null/nullable shape.

SQL Decimal/Numeric precision and signed scale are validated before narrowing to
Arrow's u8/i8 fields. Invalid wrapped positive/negative parameters fail even for
TRY_CAST or empty input; valid negative scales remain supported. Unsigned SQL
spellings now bind to the existing UInt8/16/32/64 domains, including exact UInt64
maximum. Timestamp precision/timezone support is unchanged and remains open.

The first compile caught a signed AST-scale mismatch in the new guard; the failed
log remains in evidence. Corrected combined run: full library685 passes plus97
selected integration passes,2 existing ignores. It includes actual spill, scalar,
cast, grouping, materialization, buffer ownership and query-budget gates. Log:
`.scratch/coercion-memory-repair/try-cast-metadata-contracts-02.log`.

Frozen628 archive: [provenance](benchmarks/2026-09-06-coercion-memory/provenance.json),
861 members, every source/evidence hash verified. Archive SHA256:
`57c04ec3237567622175a12c2ddeffd4c92b2c3bcff11886da9e38986cda0950`.
Those timings and cap results do not apply to this newer source. The current629 results below supersede pending checks for this scoped increment;
broader resource and workload gates remain required. No DuckDB
leadership or global query-memory certification is claimed.


## Frozen629 optimized results

Source SHA256 `48e4af7b017a6187650a2d58637519061897194d6c68d6ae17073660915bac34`;
release binary SHA256 `69fdcfe095276535c467eb15c5088a58b5b905385e32bf6d2d48cd852ef9bbcc`.
Release uses lance+gpu. Validation: 89 float/date, 10 dense-float and 32 new
coercion queries match DuckDB. All 58 primitive cases match the preserved engine
control; eight retain the documented integer-division difference from DuckDB.
Twenty literal cases validate: 19 canonical matches and one separately bounded
all-NULL value check retaining the Arrow schema difference. Decimal metadata
wraparound is fixed; timestamp precision/timezone mismatches remain reproduced.
At 64KiB, borrowed input succeeds while expanded literals and integer-to-double/
decimal allocations refuse by memory-budget name.

All 352 component requests pass typed validation and timing gates. Against628,
overflow-heavy TRY_CAST drops from about16.25ms to0.432ms (ratio0.02658).
Against older612 its ratio is0.9953. Integer-to-decimal improves to0.7237 of612,
but integer-to-double remains1.1312 of612. These are one-session component
measurements, not a DuckDB leadership result. Input/output are bounded and exact
row-multiset validation preserves duplicate rows. Commands, samples and hashes
are in `.scratch/try-cast-memory-repair/` pending the final evidence archive.

## Full SF10 development screen

All five tracks completed: 22 queries, three measured pairs plus one warmup pair
per query, 176 engine requests per track and 880 total. Every request passed
typed validation and the calibrated time gate. Controls use source612; this is
a development regression screen, not the three-session DuckDB leadership gate.

| Track | Suite629/612 | Geomean629/612 | Queries requiring protected repeats |
|---|---:|---:|---|
| decoded_ipc | 0.9672 | 0.9795 | q16 (1.113) |
| raw_parquet | 0.9883 | 0.9959 | None |
| native | 0.9776 | 0.9931 | None |
| iceberg | 0.9855 | 0.9844 | None |
| lance | 0.9402 | 0.9558 | q08 (1.266), q20 (1.131) |

Protected repeats completed in two fresh sessions with ten pairs each: all132
requests passed. Ratios629/612: IPC Q16 1.02474/0.99650; Lance Q8
0.97733/1.01433; Lance Q20 1.10808/0.98822. Q20 remains variable; one
session exceeds10%, the second does not. This is not evidence of zero regression.
Corrected GPU residency and both current-binary aggregate cap checks now pass;
see the scoped results below.


## Next implementation boundaries

1. Investigate integer-to-double conversion using generated loop/code profiles
   and batch-size scaling. Compare admitted construction against Arrow on the
   same values, null density and target type. Keep pre-allocation reservations;
   do not recover speed by removing the memory contract.
2. Repair temporal metadata as a shared type contract. The binder currently maps
   every Timestamp precision/timezone spelling to microseconds without timezone,
   and ScalarValue::Timestamp stores only microsecond ticks. Audit literal casts,
   extraction, constant folding, subqueries, aggregate finalize and output schema
   together; add exact pre-epoch, nanosecond, offset and empty-input cases. Until
   a domain is implemented, reject it explicitly instead of silently narrowing.
3. Extend reservations to the next measured untracked boundary, with a small
   budget refusal reproducer and escaped-buffer lifetime tests first. Remaining
   domains include typed NULL casts, string-to-numeric and decimal coercions,
   dictionary decoding, evaluator entry scopes, provider and final-result buffers.
4. Preserve the epic's public-workload, SF100, memory/concurrency and layout gates.
   Development SF10 A/B ratios cannot close those parent tasks or establish
   DuckDB leadership. GPU results must state actual device execution and residency.


## GPU validation

Corrected run02 completed40 measured CPU controls and40 required-device samples
on the custom600k-row float fixture. Per-request residency evidence confirms40
completed device runs and zero failures. Every observed cache target is268435456
bytes (256MiB); this is a cache target, not a hard VRAM allocation cap. The run
uses a16GiB host cgroup, affinity0–3 and the same frozen629 binary.
NVRTC is supplied by repository `.venv/lib/python3.12/site-packages/nvidia/cuda_nvrtc/lib`.

Run01 is preserved: all80 samples were correct, but a final assertion rejected
its default24GiB cache target. Run02 explicitly sets QE_GPU_CACHE_MB=256. Neither
run is canonicalSF10 GPU coverage or proof of query-wide GPU memory safety.
The general trace-derived gpu_execution field labels resident execution as CPU;
required-residency evidence independently confirms actual device runs. Consumers
must use that evidence rather than infer device use from plans or that field.


## Current-binary aggregate cap checks

Both aggregate scenarios complete with1,000,003 groups, exact_counts=true and
spilled=true (3,855,541,894 accounted spill bytes). The1GiB cgroup run peaks at
407MiB RSS; the2048MiB RLIMIT_DATA run, inside the8GiB outer scope, peaks at
410MiB. Both exit0. Frozen cap-binary SHA256:
`2e1124e10cbe12138d75dcdc7f33f7238746ca56044ca9808c865fc8165dbda4`.
These are aggregate spill checks, not comprehensive memory-boundary certification.


## Archived evidence

[Verified provenance](benchmarks/2026-09-06-try-cast-memory/provenance.json)
records4,511 members; every source and evidence member was checked by SHA256.
Archive SHA256 `f79762dde4df8e5247111de1c5b3004a0716b48114b73f919d901ee9e97c8d92`.
The archive preserves the failed default-cache GPU assertion and corrected run,
all five full provider screens, protected repeats, component and semantic probes,
cap logs, test logs, drivers, frozen source and both release binaries. No production
source changed during these measurements. Formatting had passed for the frozen
source; final documentation whitespace check also passed. No commit was made.
