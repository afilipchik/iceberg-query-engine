# Task 001 — Baseline pin (2026-08-18, HEAD ad3881a, warm, idle box)

Engine parquet: `scripts/sf100_full_benchmark.sh`, second (warm) sweep,
all 22 MATCH the DuckDB oracle. Engine lance: `benchmark-lance`, second
(warm) sweep, 22/22 successful (row counts match; cell validation vs
oracle CSVs rides with 008). DuckDB: `scripts/duckdb_files_bench_sf100.py`
(duckdb 1.4.4, best of 2, same files).

## SF=100 per-query (ms), warm

| Q | engine-pq | duck-pq | pq ratio | pq gap(s) | engine-lance | duck-lance | lance ratio | lance gap(s) |
|---|---|---|---|---|---|---|---|---|
| Q1 | 3422 | 1220 | 2.8x | +2.2 | 3866 | 7758 | 0.5x | −3.9 |
| Q2 | 578 | 408 | 1.4x | +0.2 | 457 | 432 | 1.1x | +0.0 |
| Q3 | 5097 | 1774 | 2.9x | +3.3 | 3387 | 2757 | 1.2x | +0.6 |
| Q4 | 3504 | 918 | 3.8x | +2.6 | 3611 | 1625 | 2.2x | +2.0 |
| Q5 | 7984 | 1550 | 5.2x | +6.4 | 7380 | 2059 | 3.6x | +5.3 |
| Q6 | 952 | 635 | 1.5x | +0.3 | 1897 | 4811 | 0.4x | −2.9 |
| Q7 | 3025 | 1551 | 2.0x | +1.5 | 3248 | 2461 | 1.3x | +0.8 |
| Q8 | 4602 | 2029 | 2.3x | +2.6 | 2970 | 2689 | 1.1x | +0.3 |
| Q9 | 21948 | 9458 | 2.3x | +12.5 | 23653 | 9401 | 2.5x | +14.3 |
| Q10 | 4116 | 1327 | 3.1x | +2.8 | 5119 | 2351 | 2.2x | +2.8 |
| Q11 | 293 | 221 | 1.3x | +0.1 | 1250 | 326 | 3.8x | +0.9 |
| Q12 | 1293 | 908 | 1.4x | +0.4 | 2346 | 2193 | 1.1x | +0.2 |
| Q13 | 2982 | 1477 | 2.0x | +1.5 | 3551 | 2073 | 1.7x | +1.5 |
| Q14 | 1031 | 851 | 1.2x | +0.2 | 1396 | 1321 | 1.1x | +0.1 |
| Q15 | 858 | 823 | 1.0x | +0.0 | 1497 | 3454 | 0.4x | −2.0 |
| Q16 | 1959 | 698 | 2.8x | +1.3 | 2638 | 656 | 4.0x | +2.0 |
| Q17 | 2347 | 1666 | 1.4x | +0.7 | 2746 | 2085 | 1.3x | +0.7 |
| Q18 | 9301 | 4175 | 2.2x | +5.1 | 11565 | 8837 | 1.3x | +2.7 |
| Q19 | 1302 | 1188 | 1.1x | +0.1 | 5272 | 2293 | 2.3x | +3.0 |
| Q20 | 5432 | 2576 | 2.1x | +2.9 | 6365 | 2994 | 2.1x | +3.4 |
| Q21 | 6565 | 3974 | 1.7x | +2.6 | 5786 | 5891 | 1.0x | −0.1 |
| Q22 | 717 | 679 | 1.1x | +0.0 | 2400 | 602 | 4.0x | +1.8 |
| **Total** | **89307** | **40103** | **2.23x** | +49.2 | **102401** | **69068** | **1.48x** | +33.3 |

(Cold-ish first passes: parquet 91.9s, lance 111.9s. Lance load 8.8s not
counted, consistent with prior methodology.)

## Worst absolute gaps (parquet | lance)

1. **Q9 +12.5s | +14.3s** — probe gather (prior epic's attribution; 2b-lite, task 006)
2. **Q5 +6.4s | +5.3s** — worst parquet RATIO (5.2x); NOT previously flagged; attribution needed
3. **Q18 +5.1s | +2.7s** — join-probe drain (2a, tasks 002/003)
4. **Q3 +3.3s | +0.6s**, **Q20 +2.9s | +3.4s**, **Q10 +2.8s | +2.8s**
5. **Q4 +2.6s | +2.0s** — task 004
6. **Q19 +0.1s | +3.0s** — lance-only, pushdown whitelist (task 005)
7. **Q22 +0.0s | +1.8s**, **Q16 +1.3s | +2.0s**, **Q11 +0.1s | +0.9s** — lance ratios 4.0x/3.8x on small absolutes

## Reading

- The engine's lance leg WINS on scan-bound queries (Q1 0.5x, Q6 0.4x,
  Q15 0.4x vs duck-lance) and loses on the same join-bound queries as
  parquet — shared mechanisms (2a/2b) carry both formats.
- Q5 (5-way join, no large aggregate output) is now the worst parquet
  ratio at 5.2x and was absent from every prior plan. Attribution added
  to the 004 window.
- duck-lance Q21 (5.9s) is slower than engine-lance (5.8s) — parity there.
