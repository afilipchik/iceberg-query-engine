# Native SF10 parallel reduction diagnostic — September 9, 2026

## Result

Frozen `bd689bf6` reduces Q18 time relative to the preceding serial-reduction
candidate `cd8098d5` by an observed 27.46% at four threads and 34.61% at sixteen.
Both binaries use `QE_AGG_OWNERSHIP=partial`. Q1 is close to unchanged. All sixteen
completed outputs match the independent typed oracle; all algorithm traces pass.
These two-block observations have no confidence interval and are not DuckDB
acceptance or proof that partial ownership should become the default.

| Threads | Query | Serial reduction, block ms | Parallel reduction, block ms | Geometric mean paired ratio |
|---:|---|---|---|---:|
| 4 | Q1 | 8261.364 / 8318.471 | 8141.378 / 8259.857 | 0.989208 |
| 4 | Q18 | 8024.788 / 7997.888 | 5797.457 / 5826.182 | 0.725448 |
| 16 | Q1 | 6023.520 / 6150.118 | 5971.846 / 6155.779 | 0.996159 |
| 16 | Q18 | 7343.781 / 7412.969 | 5141.876 / 4526.491 | 0.653861 |

Second-block sixteen-thread Q18 gives direct phase attribution. The dominant
aggregate's ingestion is essentially unchanged: 1807.575 versus 1833.271 ms.
Finish falls from 5038.470 to 2072.318 ms, while output takes 1247.711 versus
1296.941 ms. Thus non-output finish falls from 3790.759 to 775.377 ms.
The candidate routes 15,087,865 partial rows through 1,855 windows, with 1,840
parallel windows: routing 155.034 ms and scoped merging 518.838 ms. These are
partial rows, not a count of distinct final groups. All query spill counters are
zero. The final HAVING output has 624 rows; reducer output batch boundaries differ
and typed comparison remains correct.

## Conditions and provenance

- Canonical SF10 native provider, original Q1/Q18 SQL, existing independent typed
  oracles; one process at a time, reaped before the next process.
- Four/sixteen query threads, fixed CPU affinity 0–15, 4 GiB query and 12 GiB
  process budget, 48 GiB scope with zero swap. This CPU mask includes its host SMT
  topology; it is not a claim of sixteen dedicated physical cores.
- Two fresh-process blocks; second block reverses thread, query and binary order.
  GPU explicitly disabled. The 180-second watchdog is a diagnostic allowance,
  not the ordinary 10× DuckDB query-time ceiling.
- Candidate reduction traces must report the requested reducer count without a
  serial fallback; Q18 must report nonzero parallel windows.
- Job92740 exits0. Source, binaries, data, provider, driver and harness hashes
  verify after execution. Scope peak 7,803,449,344 bytes; all OOM/max events zero.
- Release97747 exits0 in8m53s, features `lance,gpu`, locked/offline, one build job,
  510 frozen source inputs. Candidate SHA256
  `bd689bf675ea1977e8573d6a63587cf5a8fb882164161b26439c8793b202ab23`.
  Control SHA256
  `cd8098d5b9d82a1a80b84056dce4e65cb665129dd36938d496279a465d75fff7`.

[Implementation and correctness evidence](parallel-aggregate-reduction-2026-09-09.md).
[Immutable measurement archive](benchmarks/2026-09-09-parallel-reduction-native/manifest.json).

## Next decision

This fixes the measured serial-reduction bottleneck in the experimental path.
It does not establish superiority over the default disjoint path at every thread
count. Run a same-binary disjoint/partial comparison next, then protected query,
provider, residency, cap and concurrency gates before changing default policy.
Q1 preparation/evaluation and Q18 pre-HAVING output construction remain separate
measured costs. Do not combine their changes with this frozen comparison.
