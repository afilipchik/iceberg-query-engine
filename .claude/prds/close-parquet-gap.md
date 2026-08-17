# PRD: Close the parquet like-for-like gap vs DuckDB (SF=100)

**Problem.** On IDENTICAL parquet files, warm, same machine: engine 87.1s,
DuckDB 1.4.4 `read_parquet` 39.4s → 2.21x. The previously used "vs native"
baseline (65.8s) flattered the engine because DuckDB-native Q9 is
pathological. The real opponent is DuckDB's parquet reader.

**Goal.** Engine total ≤ 55s at SF=100 this epic (≈1.4x); every change
individually measured, cell-exact 22/22 preserved, 960+ tests green.
Non-goal: selection-vector execution (separate epic; too large).

**Top absolute gaps (ms, engine − duckdb-parquet, warm best):**
Q9 +14 300 · Q18 +4 900 · Q3 +3 400 · Q21 +2 800 · Q8 +2 800 · Q10 +2 600 ·
Q4 +2 600 · Q20 +2 600 · Q5 +2 100 · Q1 +1 900 · Q13 +1 500. Sum ≈ 47s.

**Constraints.** Memory-safe always; no wrong answers ever (Q11 lesson:
row counts are not answers — validation is full-value); measurements are
serialized on this machine (no parallel benchmark runs).
