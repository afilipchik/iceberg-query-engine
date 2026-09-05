---
issue: 004
stream: certification
started: 2026-09-05T05:16:36Z
status: completed
---
## Progress
- 2026-09-05T05:16:36Z: coordinator runs 004 at HEAD 3b0631e (engine binary from 24a3138); default suite + M1/M2 launched
- 2026-09-05T05:17:13Z: M1 GATE PASS, M2 GATE PASS (FLIGHT GATE PASS x3) — .scratch/jss/m1m2.log. Parquet 1G sweep running on query_engine_003e (== target/release at HEAD, cmp-identical) under MemoryMax=32G, concurrent with the default suite build (timings shared the machine).
- 2026-09-05T05:27:58Z: **SF=100 parquet @1G: 22/22 CELL-EXACT, engine total 637.07s
  (2026-09-03: 1,616s)**, same spilling set (Q03, Q09, Q13, Q16, Q18,
  Q20), join spill path on Q09 (x2, 246 ok) + Q16 (120 ok), **366
  hash-check-ok / 0 mismatch**, under a 32G cap (was 80G). Q09 318.4s
  here with the default-suite build sharing the machine; task 003
  measured 222.3s on a quiet machine (peak 10.7GB under 16G). Q13 68.9s,
  Q18 65.0s, Q03 37.4s. `sweep_parquet1G.log`. 256M premise launched.
- 2026-09-05T05:34:47Z: **default suite 1340 / 0 / 1** (= 1337/0/1 after epic 1 + task
  002/003's 3 new tests), exit 0 — `suite_default.log`. **Harness core at
  SF=100-class 8/8 PASS** (agg 473/494MB, sort 839/797MB @1G; native-scan
  120/122MB @2G; insert 2x clean refusal @512M) — `harness/sf100core_
  driver.log`; join scenarios at SF=100-class under 1G were measured in
  task 003 on the same binary (8/8, 464-881MB). lance/gpu/pulsar suites
  launched (sequential, 56G cap).
- 2026-09-05T05:38:02Z: SF=10 harness 8/8 PASS on oom_cap_harness_003e (agg 393/404MB, sort 775/852MB, native-scan 119/142MB, insert 2x clean refusal 28/27MB) — harness/sf10_driver.log.
- 2026-09-05T05:44:07Z: **SF=100 parquet @256M: 20/22 CELL-EXACT + the same 2 named
  refusals (Q20 LEFT-join spill, Q21 ON-filter spill), engine total
  932.83s (2026-09-03: 2,207s)**, 11 spilling queries, join spill path on
  8 (Q03, Q05, Q07, Q09, Q12, Q16, Q18 + Q21's refusal), 0 HASH-MISMATCH;
  Q09 392s, Q18 151s, Q03 93s (all with the suite builds sharing the
  machine). Observation, not a defect: Q03 and Q18 now print half as many
  `hash-check-ok` lines as on 2026-09-03 (118 vs 236, 110 vs 220) while
  Q05/Q09/Q12/Q16 are unchanged — the streamed read-back emits one check
  per partition per side once, where a repeat execution used to re-check;
  results cell-exact either way. `sweep_parquet256M.log`. Native 1G
  sweep launched (32G cap, suites still building).
- 2026-09-05T05:58:47Z: **SF=100 native @1G: 17/22 CELL-EXACT + the same 5 native-scan
  admission refusals (Q02 part, Q10 customer, Q11/Q20 partsupp, Q15
  lineitem), engine total 855.77s (2026-09-03: 1,602s)**; spilling
  Q03, Q09 (join x2, 246 ok), Q13, Q16 (join, 120 ok), Q18; 0 mismatch.
  Q09 292s (was 1,255s); Q18 327s (was 230s — its spill is the
  aggregate's, not the join's, and the lance suite build shared the
  machine; re-check on the quiet native-100G leg's neighbour if time
  allows). `sweep_native1G.log`.
- 2026-09-05T06:33:52Z: suites — **lance 1405 / 0 / 2**, **gpu 1349 / 0 / 1** (= epic-1
  counts + task 002/003's 3 new tests). **pulsar: first run 1 failure**,
  `ddl_registered_rollup_survives_an_insert_triggered_refresh_via_
  ordinary_sql` (native_rollup_qa_closeout_tests.rs:244): one `sum_qty`
  cell differs in its LAST float digit (25929.19821324246 vs
  25929.198213242456) — the pre-existing rollup last-ULP aggregate-merge-
  order flake recorded by oom-safety-hardening 003 (`.scratch/oom003/
  pre003_rollup_flake_repro.log`), explicitly out of scope; the test
  passes **3/3 in isolation** (`pulsar_rollup_rerun.log`). cargo had
  stopped after that binary (18/31), so the pulsar suite is re-running
  with `--no-fail-fast` for full counts. Native 100G sweep running.
- 2026-09-05T06:36:35Z: **native 100G: 22/22 CELL-EXACT, 79.03s** (Q4 1.39s, Q13 9.9s).
  **pulsar --no-fail-fast: 1342 / 1 / 1** — the failure is
  `three_real_processes_serve_and_survive_a_sigterm` ("127.0.0.1:40393
  never became ready", 60s readiness window while the native-100G sweep
  loaded 65GB concurrently); **2/2 in isolation** (0.22s, 0.49s,
  `pulsar_sigterm_rerun.log`). **Parquet cache-off 7.39s, 22/22 PASS**
  (band 7.03-7.40). **INSERT RSS 1,665,924 KB (~1.59GB)**, 60M rows in
  9.2s (band ~1.6-1.7GB). Closed.
