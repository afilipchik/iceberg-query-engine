# perf-marathon scoreboard

State at start: SF=100 warm-IPC 48.3s / parquet 65.1s; SF=10 IPC 5.1s.
Target: warm-IPC ≤ 45s or ≥10 verdicts. Verdicts: WIN (kept), NEUTRAL
(reverted), REFUTED (reverted/not built, mechanism named).

| # | idea | expected | verdict | evidence |
|---|---|---|---|---|
| 1 | Distributed re-validation + sidecar-under-splits interplay | correctness + G1 | **PASS** | Interplay safe BY DESIGN (ShardedParquetTable::parquet_files()=None blocks every whole-file fast path incl. sidecars; pinned test). 19/19 in-process tests × {auto,1,0}; M1 gate PASS; M2 gate PASS at SF=10 × 3 real processes (needed a cluster_local.sh --memory-limit passthrough — gather's designed refusal at the 1G default masqueraded as 13 failures). |
| 2 | Q9 IPC regression via madvise/fallback | −2s | **NEUTRAL** | WillNeed −0.4s (noise). True mechanism found by idea #5: 64k batch granularity, not faults. Q9 IPC now 9.7s — BEATS parquet mode. |
| 3 | Q1-under-IPC attribution | −0.5..1s | **MEASURED→WIN via #5** | Morsel path confirmed; 9-accumulator update loop is the cost. 8k slicing took Q1 3.3→2.2s; remainder ≈ accumulator floor. |
| 4 | Q13-under-IPC delta | −0.3s | **MEASURED→WIN via #5** | Disjoint question was a red herring (same path both modes); 8k slicing took Q13 3.3→2.85s. Residue = 15M-group agg class. |
| 5 | IPC batch granularity | −0.5s? | **WIN (−1.2s suite; commit b3c2868)** | Survivor-size-gated 8k re-slice: Q9 −4.5s, Q1 −1.1s, Q13 −0.5s. Three iterations to the right gate: naive slicing hit a >250x Q2 subquery cliff + a +50-450ms smear on selective scans; filter-presence gating killed Q9's win; SURVIVOR SIZE (≥16k→8k, post-filter) keeps all wins, engages no pathology. Suite 48.3→47.1s. |
| 6 | Q20 attribution | −1s? | **MEASURED** | 44.6M-group SPARSE aggregate 1.8s + semi probes 5.3s cum + scans. No single lever; same class as Q13/Q18 residues (big sparse aggregate → future dense group-id remapping program item). |
| 7 | partsupp partkey-direct-address | −1s Q9 | **NOT RUN (deprioritized)** | Q9 reached duck-parquet parity (9.7 vs 9.5s) via #5; probe is 2.1s wall-share of a query no longer behind. |
| 8 | Q18 subquery under IPC | −0.5s | **MEASURED (floor)** | scan+process 1.5s (IPC decode-free) + merge 0.85s ≈ 2.35s vs DuckDB's ~2.0 inside its whole query. Q18 residue is structural (semi + two lineitem passes). |
| 9 | WILLNEED on sidecar mmaps | cold-run win | **NEUTRAL** | −0.4s on Q9, within noise; kept as QE_IPC_WILLNEED=1 diagnostic, default off. |
| 10 | Distributed perf with sidecars | measurement | **MEASURED** | Shards bypass sidecars by design → workers pay full decode; scatter coordination healthy (spread 1.02-1.25, imbalance 1.008). One-box numbers are coordination not scaling (harness's own caveat). FUTURE idea recorded: row-group-ALIGNED splits could serve shard reads from sidecar rgs. |
| 11 | Q21-under-IPC | −0.5s | **MEASURED (ahead)** | 3.4-3.5s BEATS duck-parquet's 3.97; no work needed. |
| 12 | Q5/Q7 residues | attribution | **MEASURED (par/floor)** | Q7 1.5-1.7 ≈ duck 1.55 par; Q5 2.2 vs 1.55 — probe+gather ≈ 0.65s wall-share on the 15%-selective orders join, architecture floor. |

**Final: 11 verdicts (1 PASS, 2 WIN-class, 2 NEUTRAL, 5 MEASURED, 1 NOT-RUN-deprioritized). Suite 48.3 → 47.1s warm-IPC (0.70x DuckDB native), 22/22 cell-valid; distributed M1+M2 gates PASS at SF=10 × 3 real processes in all cache modes.**
