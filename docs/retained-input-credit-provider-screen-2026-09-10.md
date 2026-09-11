# Retained input credit: canonical SF10 provider screen

Frozen38966ae6 clears LanceQ7 in the full benchmark harness. All333completed
outputs pass independent typed validation. The full screen remains incomplete:
native and Lance miss query deadlines, while Iceberg and Lance lose Q9 reference
calibration. Raw remains about3times DuckDB. No leadership is certified.

## Frozen conditions and outcome

Provider session4926 is terminal1; independent audit49224 is terminal0. Canonical
SF10, sequential raw Parquet/native/Iceberg/Lance,16threads on CPU0–15,
4GiBquery/12GiBprocess, default disjoint ownership, GPUoff, one session,
one warmup and3measured samples per query. Matched DuckDB1.4.4 calibration sets a
10×query-time ceiling; startup allowance is separate. All engine and comparator
work uses the required capped wrapper and repository TMPDIR.

| Track | Valid measured pairs | Completed warmups validated | Geomean engine/DuckDB | Suite ratio |
|---|---:|---:|---:|---:|
| Raw Parquet | 66/66 | 22 | 3.001650 | 3.373702 |
| Native | 60/66 | 21 | incomplete | incomplete |
| Iceberg | 63/66 | 21 | incomplete | incomplete |
| Lance | 60/66 | 20 | incomplete | incomplete |

Total249valid measured pairs and84completed warmups. The15missing measured pairs
stay missing. Raw has0/22query wins and a worst median ratio8.993217. One session
does not establish between-session confidence or protected regression acceptance.
The engine/provider scope peak is20,537,110,528bytes, swap0 and zero max/OOM events.
This does not erase the separately recorded reference crash or allocation refusal.

## Failure identities and resource repair

- NativeQ1 warmup timeout at3137.694msceiling. Q17 warmup completes in1371.990ms
  above its1304.624msceiling; its output passes the independent typed audit. Q6
  passes this screen. The preceding fde screen missedQ6instead ofQ17, so equal
  valid-pair counts must not be reported as identical query failures.
- IcebergQ9 DuckDB warmup crashes with exit−11 in query execution. Its calibration
  and dependent engine requests are not run. Do not invent a complete Iceberg
  suite ratio from the other21queries or call this an engine wrong-result failure.
- LanceQ1 warmup timeout at7384.096msceiling. Q9's oracle succeeds, but DuckDB
  warmup refuses a134217728-byte block allocation with OutOfMemoryException.
  Calibration/dependent samples are not run. This is a graceful reference
  allocation refusal, distinct from Iceberg's crash and cgroup OOM events.
- LanceQ7 completes all3measured samples:671.297,545.021,573.221ms, each typed
  correct. Its prior fixed-child memory refusal is cleared at unchanged budget,
  consistent with the [two-block diagnostic](retained-input-credit-q07-2026-09-10.md).

All519source inputs, frozen binary, driver and versioned harness verify after
measurement. Source and harness were unchanged. The independent audit includes
every completed warmup, including the late nativeQ17result. Requests, raw responses,
plans, reference failures, output checks, samples and provenance are retained in the
[immutable archive](benchmarks/2026-09-10-retained-input-credit-providers/manifest.json).
Temporary spill payload omissions have an explicit inventory.

## Next work

The [shared-path comparison](retained-input-credit-measurement-2026-09-10.md)
retains small rawQ1/nativeQ17/Q18/resident16Q18 slowdowns and inconsistent apparent
nativeQ1/Q9 gains. The resource fix is not a CPU leadership result. Partial-native
startup allocation is now [traced](partial-native-input-attribution-2026-09-10.md);
implement a regression-backed unused-worker repair without source replay or budget
changes. Raw logical-work and typed-input dispatch candidates remain separate.

Full provider completion, memory/concurrency acceptance, multi-session precision,
16GiBresident preload and canonical GPU device execution remain open. Reference
stability needs diagnosis under preserved conditions; never retry a failed
calibration until it passes or silently change the matched boundary.
