# Generic quarter-budget spill reproduction — 2026-09-09

Frozen322c8042 can complete the same generic grouping without spilling while
tracking less than16MiB of reservations, yet spills when configured at16MiB.
This supports the [premature-spill policy hypothesis](admitted-spill-io-2026-09-09.md).
It is not a source repair or proof that canonical SF10 fits4GiB.

## Successful fixture

The fourth diagnostic driver uses two identical50,000-row IPC stream batches,
50,000 groups including a NULL key, nullable Decimal128(30,2) values and duplicate
contributions. It runs on preloaded CPU MemoryTable input with GPU disabled,
four runtime/Rayon threads and affinity0–3. The process cap is2GiB inside a4GiB
scope. Only the query limit changes between16MiB and32MiB.

`SELECT k, SUM(v) AS s FROM t GROUP BY k HAVING SUM(v) >= 100000`

Each non-NULL value is exactly `k+1` at scale2; values at indices divisible by7
are NULL. The independent expected output is exactly one row,
`(49999, Decimal128(100000.00))`, with Int64/Decimal128(38,2) output types. Both
queries satisfy that oracle. This exercises complete sums across both input
batches; filtering individual partials would lose the qualifying row.

| Query limit | Tracked reserved peak | Logical spilled bytes | Instrumented execution | Input batches |
| --- | ---: | ---: | ---: | ---: |
| 16MiB | 9,379,085 | 5,421,786 | 72.290ms | 56 |
| 32MiB | 12,655,885 | 0 | 13.771ms | 20 |

The32MiB run's tracked peak is below16MiB. Its larger input buffer domain changes
batching, so this is not a controlled timing claim; it is evidence motivating a
same-budget policy regression. Source shows the count ceiling uses one quarter
of root memory divided by an estimated per-group size, independently of actual
reservations. All507 source hashes, binary and generated input hash verify after
successful execution. No source changed for this diagnostic.

## Preserved setup failures and remaining fallback issue

- Attempt1 used a512MiB process cap and default Tokio startup workers. The engine
  failed before setup with a thread-creation panic; this does not test aggregation.
- Attempt2 uses2GiB process capacity and four Tokio workers but writes an IPC file
  instead of the benchmark adapter's required IPC stream. Preload refuses with
  `truncated metadata`. This is a fixture format error, not evidence of a broken
  Arrow decoder.
- Attempt3 fixes the stream format, but its decimal-fraction HAVING literal routes
  through unsupported decimal/float predicate binding and the collecting fallback.
  It reproduces the already-open transient-source-domain refusal at1,482,692 used
  bytes under a1,482,960-byte limit. The refusal is preserved; it is not repaired
  by this study. Attempt4 uses an integer threshold supported by the admitted
  decimal comparator to isolate the intended streaming aggregate/count policy.

## Next implementation gate

After frozen residency finishes, add a generic16MiB integration regression using
this supported shape, requiring the same independently correct output with no
unnecessary spill and no budget increase. Retain the existing actual-pressure
spill regressions, variable states, NULLs and retry/ownership checks. Evaluate the
static quarter-budget ceiling against actual admission and prepared spill scratch;
do not replace reservations with estimates or discard output headroom. Then
freeze a candidate and run native/Lance Q18 at the original4GiB limit. The known
collecting-fallback refusal remains a separate systemic issue.

The41-file [archive](benchmarks/2026-09-09-quarter-budget-reproduction/manifest.json)
preserves all four attempts, generated inputs, outputs, setup and drivers.
