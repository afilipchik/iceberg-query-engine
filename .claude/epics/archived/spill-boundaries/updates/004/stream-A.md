---
issue: 004
stream: certification
started: 2026-09-05T07:13:27Z
status: completed
---
## Progress
- 2026-09-05T07:13:27Z: coordinator runs 004 at HEAD 8f38273; default suite + M1/M2 launched; harness scenarios being added
- 2026-09-05T07:14:35Z: M1 GATE PASS, M2 GATE PASS (FLIGHT GATE PASS x3) — .scratch/sb/m1m2.log. Harness scenarios left-join + filtered-join added (build after the suite releases the cargo lock).
- 2026-09-05T07:28:35Z: **default suite 1353 / 0 / 1** at 8f38273 (= 1340/0/1 + task 001-003's 13 new tests), exit 0 — .scratch/sb/suite_default.log. HEAD binaries built (query_engine_head, oom_cap_harness_head); parquet 256M + native 1G sweeps launched (32G caps each).
- 2026-09-05T07:36:20Z: harness `left-join`/`filtered-join` (new; same fixture as
  semi/anti, probe columns pid/pval, filter `val = pval` or `<>`):
  **pre-fix (05b0213 + scenario code): 8/8 clean named refusals** (LEFT:
  "LEFT join build side exceeds the memory budget ... not spillable";
  filtered: "cannot evaluate an ON-clause filter"). **HEAD (8f38273): all
  runs COMPLETE with the closed-form counts on the rlimit lever**
  (left-join br1 20,000,000 = B/2; br0 40,000,000 = B; filtered eq
  10,000,000 = B/4; ne 0) **but at the default 1G cgroup cap
  three runs were killed**: left-join br0 (137, 1,024MB), filtered-join
  eq br1 + br0 (143). Peaks: left-join br1 1,006/1,041MB, filtered eq
  1,098/1,102MB (rlimit), ne 609/593MB — vs SEMI/ANTI 464-881MB on the
  same fixture. Sent back to the implementation agent as a 003 follow-up
  (root-cause + bound the emission memory), not papered over with a
  bigger cap. Logs `.scratch/sb/harness/`.
- 2026-09-05T07:45:11Z: **SF=100 native @1G on 8f38273: 22/22 CELL-EXACT** (2026-09-04:
  17/22 + 5 refusals) — Q02 0.30s, Q10 9.2s, Q11 0.68s, Q15 10.1s, Q20
  55.6s all complete; spilling Q03, Q09 (join x2, 246 ok), Q13, Q16
  (join, 120 ok), Q18, Q20; **0 HASH-MISMATCH**; total 975.5s with the
  lance suite build + the 256M parquet sweep + the agent's builds sharing
  the machine (Q18 429s vs 327s yesterday under similar load — aggregate
  spill, not the join). `sweep_native1G.log`. Will be re-run on the
  final binary after the 003 follow-up lands.
- 2026-09-05T07:48:36Z: **SF=100 parquet @256M on 8f38273: 22/22 CELL-EXACT** (2026-09-04:
  20/22 + Q20/Q21 refusals) — **Q20 65.2s via the LEFT-join spill path
  (1 execute_spill_path, 118 ok), Q21 109.5s via the filtered spill path
  (3 execute_spill_path, 334 ok)**; 0 HASH-MISMATCH on all 22; total
  1,181.9s under the same shared-machine load (Q09 496s). `sweep_
  parquet256M.log`. Both target sweeps (parquet 256M, native 1G) are
  22/22 on this binary; to be re-run on the final binary after the 003
  follow-up.
- 2026-09-05T14:21:54Z: suites at 8f38273 (--no-fail-fast): **lance 1418 / 0 / 2, gpu 1362 / 0 / 1, pulsar 1356 / 0 / 1** — each = the join-spill-streaming counts + 13 new tests; no environmental failures this time. Suites to be re-run at the final HEAD after the 003 follow-up.
- 2026-09-05T14:22:53Z: **003 follow-up landed (ec1839f)**: root cause measured with a
  1s RSS sampler + timestamped phase traces (one leg at a time, 8G scope)
  — phase A identical across legs (270-310MB); the LEFT/filtered phase-B
  plateau (992-1,180MB vs SEMI/ANTI 653-670MB) came from `gather_column`
  building each emitted batch as one 1-row `take` per output row +
  `concat` (millions of ~200-byte arrays per chunk, freed cross-thread
  and retained by the allocator); the `ne` control (no pairs → no gather,
  609MB) was the one-variable confirmation. Fix: one `interleave` per
  column (or one `take` when single-batch) and every gathered emission
  sliced to 8,192 rows so the 8-batch channel bounds bytes. A/B on the
  fixed binary: legacy gather + slicing 1,112MB; new gather alone 798MB;
  both 776MB. **Driver at the default 1G caps: 18/18 PASS** (left/
  filtered/semi/anti × orientations × levers; left 795-803MB, filtered eq
  785-842MB, semi 591-674MB, anti 618-717MB), re-verified 8/8 on a
  harness rebuilt from the final tree; chaos 220/220 (18,258 ok / 0
  mismatch); unit suites green; fmt clean. Final binaries building;
  default suite at ec1839f launched.
- 2026-09-05T14:26:38Z: at ec1839f — M1 GATE PASS, M2 GATE PASS (FLIGHT GATE PASS x3, m1m2_final.log); chaos batch A 200/200 (179 genuine-disk, 0 missed injection) on spill_chaos_harness_final.
- 2026-09-05T14:26:54Z: chaos batch B 100/100 (88 genuine-disk) — **300/300 on the final binary, 24,250 hash-check-ok / 0 HASH-MISMATCH** (.scratch/sb/chaos/).
- 2026-09-05T14:32:52Z: SF=10 harness on oom_cap_harness_final: **8/8 PASS** (agg 402/404MB, sort 826/859MB, native-scan 117/146MB, insert 2x clean refusal 29/27MB) — harness/sf10_final_driver.log. Join scenarios at the default 1G cap: 18/18 in the 003 follow-up on the same tree.
- 2026-09-05T14:41:31Z: **SF=100 native @1G on the FINAL binary (ec1839f): 22/22 CELL-EXACT, 918.1s, 0 HASH-MISMATCH** (Q02 1.1s, Q10 10.3s, Q11 0.73s, Q15 10.2s, Q20 42.9s; Q09 211s; Q18 387s under load) — sweep_native1G_final.log.
- 2026-09-05T14:44:29Z: **default suite at ec1839f: 1353 / 0 / 1**, exit 0 (suite_default_final.log). Feature suites at ec1839f launched.
- 2026-09-05T14:44:46Z: **SF=100 native @100G on the final binary: 22/22 cell-exact, 178.35s** (sweep_native100G_final.log).
- 2026-09-05T14:45:01Z: **SF=100 parquet @256M on the final binary: 22/22 CELL-EXACT, 1003.82s** (Q20 54878ms via LEFT spill, Q21 117590ms via filtered spill; 0 HASH-MISMATCH on all 22) — sweep_parquet256M_final.log. Parquet 1G on the final binary running.
- 2026-09-05T14:52:11Z: **SF=100 parquet @1G on the final binary: 22/22 cell-exact, 565.91s, 0 HASH-MISMATCH** (spilling: Q03 Q09 Q13 Q16 Q18 Q20 ) — sweep_parquet1G_final.log.
- 2026-09-05T14:59:47Z: SF=100-class harness core on the final binary: **8/8 PASS** (agg 483/489MB, sort 852/816MB @1G at 600M rows; native-scan 116/119MB @2G; insert 2x clean refusal 31/29MB) — harness/sf100core_final_driver.log. INSERT RSS leg launched.
- 2026-09-05T15:00:27Z: INSERT RSS leg (native_append_memory_check sql mode, rebuilt at ec1839f): 60M rows in 19.0s (under suite-build load), **peak RSS 1,645,708 KB (~1.57GB)** — band ~1.6-1.7GB. insert_rss_sql.log. SF=10 native band + cache-off legs wait for a quiet machine.
- 2026-09-05T15:10:37Z: suites at ec1839f (--no-fail-fast): **lance 1418 / 0 / 2, gpu 1362 / 0 / 1, pulsar 1356 / 0 / 1**, exit 0 each. SF=10 native band sweep + parquet cache-off launched on the quiet machine.
- 2026-09-05T15:11:43Z: timing legs on the final binary, first pass: SF=10 native **5,891ms** (22/22 OK; band 5288-5667; the agent measured 5,113ms at 8f38273 on a quieter machine), parquet cache-off **7.52s** (22/22 PASS; band 7.03-7.40). Load average 11.7/10.5/13.0 with none of my legs running (kube-apiserver x2, kubelet, gnome-shell, a peer session's debug serve) — both legs re-running once to separate noise from regression; the follow-up commit touches only spill-path emission code these non-spilling queries never reach.
- 2026-09-05T15:14:05Z: reruns — cache-off **7.24s** (in band); native **6,610ms** with
  load 13.7. A/B under identical load (alternating, twice): **head
  (8f38273) 6,163 / 6,113ms vs final (ec1839f) 6,226 / 5,916ms** — within
  2%, final not slower → the band miss is machine load (interactive
  desktop use: gnome-shell ~117%, firefox, a peer session's serve; load
  14-21), not the follow-up. Recorded as such; the 5,113ms quiet-machine
  number at 8f38273 stands as the epic's in-band measurement.
  `ab_native_summary.log`, `ab_native_*_[12].log`. Task 004 closed.
