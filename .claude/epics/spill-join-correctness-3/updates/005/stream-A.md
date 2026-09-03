---
issue: 005
stream: sf100-certification
started: 2026-09-03T18:56:29Z
status: completed
---
## Scope
SF=100 parquet sweep at two budgets, SF=100 native sweep, oom-cap harness
at SF=100-class inputs, SF=10 no-regression legs, four suite combos,
M1/M2 gates, docs, epic close-out. Executed by the coordinator session
(HEAD 7607f34; engine binary `.scratch/sjc3-004/bin/query_engine_fixed`
built from 67f7cea — the only later commits touch docs/scripts).

## Progress
- 2026-09-03T18:56:29Z: started. Order: SF=10 perf legs on the idle machine first, then
  SF=100 parquet (64G premise, then a tight budget), SF=100 native
  (100G), harness at SF=100-class, then the three remaining suite
  combos + M1/M2, then docs.
- SF=10 native sweep launched (native_bench_compare.py --no-duckdb, 40G,
  iterations 2, MemoryMax=48G).
- 2026-09-03T18:57:45Z: **SF=10 native sweep: 22/22 OK, TOTAL 5523.89ms** (Q12 158.58ms;
  band 5288-5667ms) — `.scratch/sjc3-005/sf10_native_sweep.log`.
- Parquet cache-off leg (`QE_IPC_CACHE=0 safe_benchmark.sh --iterations 3`)
  launched. Drivers written: `oracle22.py` (fresh DuckDB SF=100 oracle,
  Q11 SF-adjusted), `sweep22.py` (serve --data|--tables, per-query
  timing + spill-trace attribution + cell-exact check). Note:
  `benchmark-parquet` hardcodes memory_limit=min(4G*SF, 64G) and has no
  override, so BOTH parquet premises run via `serve --data
  data/tpch-100gb --memory-limit <B>` (same engine, same scan path).
- 2026-09-03T18:58:12Z: **SF=10 parquet cache-off: 22/22 PASS, engine total 7.29s**
  (DuckDB 3.32s, 2.1x; band 7.03-7.40s) — `sf10_parquet_cacheoff.log`.
  DuckDB SF=100 oracle (oracle22.py, 40GB, 16 threads) and the SF=10
  harness (4 scenarios x 2 levers) launched.
- 2026-09-03T18:59:11Z: **INSERT RSS leg** (`native_append_memory_check` rebuilt at HEAD,
  `QE_MEM_CHECK_MODE=sql`, `/usr/bin/time -v`, MemoryMax=8G): 60,000,000
  rows appended in 23.1s (wall-clock inflated by the concurrent SF=100
  oracle + harness legs), **peak RSS 1,644,440 KB (~1.57GB)** — inside
  the ~1.6-1.7GB bounded-merge band. `insert_rss_sql.log`.
- DuckDB SF=100 oracle: 22/22 computed, total 46.63s (`oracle22.log`,
  `oracle/qNN.json`). SF=100 parquet sweep at the 64G premise launched
  (serve --data data/tpch-100gb --memory-limit 64G, MemoryMax=80G).
- 2026-09-03T19:00:20Z: **SF=100 parquet sweep, generous premise (64G): 22/22 CELL-EXACT**
  vs the fresh oracle, engine total **58.91s** (DuckDB 46.63s), zero spill
  traces on any query (Q09 12.4s, Q01 7.7s, Q18 5.2s the heaviest) —
  `sweep_parquet64G.log`, per-query CSVs under `parquet64G/`. Wall-clock
  shared the machine with the SF=10 harness leg. Tight premise (8G)
  launched under MemoryMax=80G (the probe side of a spilling join is
  still fully materialized — see the task-004 boundary — so the cap must
  cover it even though the engine budget is 8G).
- 2026-09-03T19:01:27Z: **SF=100 parquet at 8G: 22/22 CELL-EXACT, 47.89s, still ZERO spill
  traces** — build sides at SF=100 are far below 6.4GB after join-order
  stats / runtime filters / semi-join pushdown, so 8G is not a spilling
  premise. Stepping down: 1G then 256M (same 80G cap, 1800s/query).
  `sweep_parquet8G.log`.
- 2026-09-03T19:03:15Z: **SF=10 oom-cap harness at HEAD: 8/8 PASS** (agg 389/394MB,
  sort 775/775MB, native-scan 121/157MB completed under 1G/1G/2G on both
  levers; insert 2x clean named admission refusal under 512M, 27MB peak)
  — zero 137/134. `harness_sf10_driver.log`, `harness_sf10/`.
- 1G parquet sweep in flight: Q03 is the first query to take a spill path
  at SF=100 (18.6s, cell-exact).
- 2026-09-03T19:04:03Z: **M1 GATE: PASS, M2 GATE: PASS** (`cluster_local.sh start 3 →
  verify → stop → start 3 → verify-m2 → stop` under MemoryMax=16G; Q1/Q3/
  Q6/Q10/Q12 identical on all 3 nodes and to the single-process binary,
  FLIGHT GATE PASS x3, M2 shards identical to DuckDB) — `m1m2.log`.
- 1G parquet sweep: Q09 is running a real SF=100 spilling join
  (in-memory 1 partition / 2.34M rows, spilled 63 / 22.2M rows, probe
  333,333,330 rows collected).

## 2026-09-03T20:06:01Z — SF=100 parquet: tight premises (serve --data data/tpch-100gb, MemoryMax=80G, QE_SPILL_DEBUG=1)

| budget | cell-exact | engine total | queries with spill activity | join-spill-path (execute_spill_path) | hash-check |
|---|---|---|---|---|---|
| 64G (historical premise) | **22/22** | 58.9s | none | none | — |
| 8G | **22/22** | 47.9s | none | none | — |
| **1G (the spilling premise)** | **22/22** | 1616s | Q03, Q09, Q13, Q16, Q18, Q20 | Q09 (x2, 246 ok), Q16 (x1, 120 ok) | **366 ok / 0 mismatch** |
| 256M (harsher) | **20/22** + 2 clean named refusals | 2207s | Q03, Q05, Q07, Q09, Q12, Q13, Q14, Q16, Q18, Q20, Q21 | Q03, Q05, Q07, Q09, Q12, Q16, Q18, Q21 (1,566 ok) | **0 mismatch** |

- 256M refusals are the two DOCUMENTED boundaries, not bugs: **Q20** —
  "LEFT join build side exceeds the memory budget ... (LEFT/RIGHT/FULL
  outer joins are not spillable)"; **Q21** — "join build side exceeds the
  memory budget, but the join spill path cannot evaluate an ON-clause
  filter". Both HTTP 400 by name; zero kernel kills / rlimit aborts in
  any leg. Every completed query at every budget is cell-exact vs the
  fresh oracle. Logs `sweep_parquet{64G,8G,1G,256M}.log`, per-query
  CSVs + serve logs under `parquet<B>/`.
- Perf observation for the follow-up backlog: Q09 through the join spill
  path takes **~1,400s at both 1G and 256M** (in-memory 1 partition,
  63 spilled, 333M-row probe materialized then processed
  partition-by-partition on one thread); Q18 355s at 256M. Correct, slow
  — the PRD's "slow but correct" boundary, but the probe-side
  materialization + single-threaded spilled-partition processing are the
  obvious next join-spill work items.
- Native SF=100 sweep at 100G (Q4/Q13's previously-failing settings)
  launched.
- 2026-09-03T20:07:55Z: **SF=100 native sweep at 100G (serve --tables
  data/tpch-100gb-native, MemoryMax=110G): 22/22 CELL-EXACT, 91.79s**,
  zero spill traces — **Q4 1.45s and Q13 10.2s at their previously-failing
  settings**, both cell-exact (`sweep_native100G.log`, `native100G/`).
  Together with task 004's Q4@64M/16M spilling runs, Q4 is covered at
  both the historical premise and a genuinely spilling one.
- Launched: harness at SF=100-class inputs (agg/sort 600M synthetic rows
  @1G, native-scan on the 100GB lineitem native table @2G, insert from
  the 100GB lineitem parquet @512M; then semi-join/anti-join with a 600M-
  row build side under a 12G cap — the cap is sized for the still-
  materialized 300M-row probe side, the documented boundary, while the
  9.6GB build side is ~40x the 256MB budget and must spill) and the
  lance/gpu/pulsar suites (sequential).
- 2026-09-03T20:41:42Z: suites — **gpu 1335 / 0 / 2**, **pulsar 1329 / 0 / 2** (exit 0;
  = 1326/1320 baselines + task 003's 1 + task 004's 8, ignored +1 for
  the deliberately-ignored findings test). **lance: first attempt killed
  (exit 143) during compilation** by the safe-build scope's
  ManagedOOMMemoryPressure=kill while the 600M-row harness legs ran
  concurrently — no test binary had started; re-launched alone with
  SAFE_BUILD_MEM=64G. Harness at SF=100-class so far: **core 8/8 PASS**
  (agg 600M rows 478/491MB, sort 600M rows globally ordered 860/861MB
  under 1G; native-scan over the 100GB lineitem native table 119/133MB
  under 2G; insert from the 100GB lineitem parquet 2x clean admission
  refusal under 512M) and **join 3/3 so far** (semi-join 600M-row build
  cgroup+rlimit 5,285/5,289MB, anti-join cgroup 8,069MB under 12G —
  build ~9.6GB spilled against a 256MB budget; the peak is the
  materialized 300M-row probe side, the documented boundary).
- 2026-09-03T20:51:03Z: **harness at SF=100-class inputs: 12/12 PASS** — core 8/8 (above)
  + join 4/4: semi-join 5,285/5,289MB, anti-join 8,069/7,957MB under the
  12G cap on cgroup + rlimit, closed-form counts exact (SEMI 150,000,000
  build rows; ANTI 450,000,000), zero 137/134 anywhere.
  `harness_sf100/{core,join}_driver.log`. Next: native 1G spilling
  sweep once the lance suite build releases its 64G scope.
- 2026-09-03T21:03:38Z: **lance suite (re-run alone, 64G cap): 1391 / 0 / 3, exit 0**
  (= 1382 baseline + 9; ignored 2 + 1). All four combos green: default
  1326/0/2, lance 1391/0/3, gpu 1335/0/2, pulsar 1329/0/2; fmt clean.
  Native 1G spilling sweep launched (last measurement leg).

- 2026-09-03T21:31:08Z: **SF=100 native at 1G: 17/22 cell-exact + 5 clean native-scan admission refusals at the documented native-scan boundary (Q02/Q10/Q11/Q15/Q20: over-budget native scans feeding joins), 5 spilling queries incl. join spill on Q09/Q16, 366 hash-check-ok / 0 mismatch, zero OOM** (`sweep_native1G.log`, `native1G/`).

## Stream result (2026-09-03T21:31:08Z)

Every gate in 005.md's table is filled with measured numbers; the
consolidated verdicts are in `005.md` "Outcome" and `epic.md`
"Close-out" (G1-G5 all MET, with three named clean-refusal boundaries
stated). Status: completed.
