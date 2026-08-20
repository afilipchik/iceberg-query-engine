# perf-marathon scoreboard

State at start: SF=100 warm-IPC 48.3s / parquet 65.1s; SF=10 IPC 5.1s.
Target: warm-IPC ≤ 45s or ≥10 verdicts. Verdicts: WIN (kept), NEUTRAL
(reverted), REFUTED (reverted/not built, mechanism named).

| # | idea | expected | verdict | evidence |
|---|---|---|---|---|
| 1 | Distributed re-validation + sidecar-under-splits interplay | correctness + G1 | **PASS** | Interplay safe BY DESIGN (ShardedParquetTable::parquet_files()=None blocks every whole-file fast path incl. sidecars; pinned test). 19/19 in-process tests × {auto,1,0}; M1 gate PASS; M2 gate PASS at SF=10 × 3 real processes (needed a cluster_local.sh --memory-limit passthrough — gather's designed refusal at the 1G default masqueraded as 13 failures). |
| 2 | Q9 IPC regression: madvise/populate or per-scan parquet fallback for scans feeding huge row-store builds | −2s (Q9 14.1→12.1) | PENDING | |
| 3 | Q1-under-IPC attribution → fix (3.26s vs duck-native 2.77; agg path with sidecar dicts) | −0.5..1s | PENDING | |
| 4 | Q13-under-IPC: dict LIKE path vs post-load filter (3.3s IPC vs 2.98 parquet) | −0.3s | PENDING | |
| 5 | IPC batch granularity: re-slice 64k sidecar batches to ~16k for L2-resident aggregation | −0.5s? | PENDING | |
| 6 | Q20 attribution (5.0s parquet / 4.0 IPC; never attributed in this program) | −1s? | PENDING | |
| 7 | partsupp probe as partkey-direct-address (heads[partkey] + ≤4-entry chains beat hash+chain?) | −1s Q9 | PENDING | |
| 8 | Q18 subquery under IPC: re-microbench; merge_shard_count / range-partition tuning at 150M groups | −0.5s | PENDING | |
| 9 | Prefetch WILLNEED on sidecar mmaps (first-touch fault storms under 32-thread scans) | cold-run win | PENDING | |
| 10 | Distributed perf with sidecars | measurement | **MEASURED** | Shards bypass sidecars by design → workers pay full decode; scatter coordination healthy (spread 1.02-1.25, imbalance 1.008). One-box numbers are coordination not scaling (harness's own caveat). FUTURE idea recorded: row-group-ALIGNED splits could serve shard reads from sidecar rgs. |
| 11 | Q21-under-IPC (3.3-3.9s): EXISTS/NOT EXISTS double lineitem pass; check semi paths take IPC | −0.5s | PENDING | |
| 12 | Q5/Q7 residues under IPC (2.1/1.5s): plan diff + HJ_PROF — anything left beyond floors? | attribution | PENDING | |
