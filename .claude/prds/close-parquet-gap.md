---
name: close-parquet-gap
description: Close the like-for-like parquet gap to DuckDB at SF=100 (87.1s vs 39.4s, 2.21x)
status: completed
created: 2026-08-17T00:00:00Z
updated: 2026-08-25T00:00:00Z
---

> **Bookkeeping note (2026-08-25).** This PRD predates the frontmatter
> convention adopted by later epics — the fields above were added
> retroactively from the epic's own recorded close-out date (2026-08-17)
> and status, not re-derived. `status: completed` reflects the epic
> running its course and closing, not that the ≤55s goal below was
> met — see the superseded-note and epic close-out immediately below for
> the honest, already-recorded outcome.

> **Superseded (2026-08-23).** This PRD's own stated goal (≤55s at SF=100)
> was not met by its epic, which closed with an honest partial result and
> two documented negative findings rather than a numbered task
> decomposition — see `.claude/epics/close-parquet-gap/epic.md`'s own
> close-out. Its one concrete unresolved item, Q4 attribution, was picked
> up and fully resolved by the `duckdb-parity` epic
> (`.claude/epics/duckdb-parity/epic.md`: "Q4 3.50→1.74s"). No further
> action needed against this PRD.

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
