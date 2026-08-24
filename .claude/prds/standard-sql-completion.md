---
name: standard-sql-completion
description: Close the standard-SQL gaps found by probe - the full window function suite, grouping sets, and the missing expression forms
status: completed
created: 2026-08-21T15:16:42Z
---

# PRD: standard-sql-completion

## Executive Summary

An empirical probe (`.scratch/sql_gap_probe.py`, 59 standard-SQL probes
against the release binary) found 34 failures. 21 of them are one missing
architecture: window functions — every ranking function (ROW_NUMBER, RANK,
DENSE_RANK, PERCENT_RANK, CUME_DIST, NTILE), every navigation function (LAG,
LEAD, FIRST_VALUE, LAST_VALUE, NTH_VALUE), every aggregate-OVER form, frames,
and the named WINDOW clause. The rest are grouping extensions
(GROUPING SETS / ROLLUP / CUBE), missing expression forms (IS [NOT] DISTINCT
FROM, quantified ANY/ALL, OVERLAY, DATE + INTERVAL arithmetic), and two real
bugs the probe exposed (`GROUP BY <ordinal>` binds wrongly; LATERAL loses
outer columns). This epic implements the SQL-standard window function suite in
full and closes the expression/grouping gaps, validated cell-by-cell against
DuckDB.

## Problem Statement

The engine advertises broad SQL support (100+ scalar/aggregate functions, all
22 TPC-H queries), but the moment an analyst writes `LAG(price) OVER (...)` —
bread-and-butter analytical SQL since SQL:2003 — the query fails. Window
functions are the single largest visible hole in the SQL surface, and the
scattered smaller gaps (ANY/ALL, IS DISTINCT FROM, ROLLUP) each turn a
standard query into a rewrite exercise.

## User Stories

1. **Analyst**: "Ranking, running totals, and lag/lead deltas work as they do
   everywhere else: `ROW_NUMBER() OVER (PARTITION BY g ORDER BY t)`,
   `SUM(x) OVER (ORDER BY t ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)`,
   `LAG(x, 2, 0)`."
   - *Acceptance*: all 11 standard window functions plus aggregate-OVER for
     COUNT/SUM/AVG/MIN/MAX, with PARTITION BY, ORDER BY, ROWS and RANGE
     frames, and the named WINDOW clause — results cell-identical to DuckDB.
2. **Report builder**: "`GROUP BY ROLLUP (region, nation)` gives me subtotals
   and a grand total; `GROUP BY GROUPING SETS` and `CUBE` work too."
   - *Acceptance*: the three grouping extensions return DuckDB-identical
     rows (order-insensitive).
3. **Migrating user**: "Standard predicates just work: `x IS DISTINCT FROM y`,
   `price > ANY (SELECT ...)`, `OVERLAY(s PLACING r FROM p FOR l)`,
   `DATE '2020-05-01' + INTERVAL '3' DAY`."
   - *Acceptance*: each returns the standard result, validated vs DuckDB.
4. **Anyone using ordinals**: "`GROUP BY 1` groups by the first SELECT item."
   - *Acceptance*: probe bug fixed, with regression tests.

## Functional Requirements

- Window functions: ROW_NUMBER, RANK, DENSE_RANK, PERCENT_RANK, CUME_DIST,
  NTILE(n), LAG(x[, offset[, default]]), LEAD(x[, offset[, default]]),
  FIRST_VALUE, LAST_VALUE, NTH_VALUE(x, n); aggregates COUNT/SUM/AVG/MIN/MAX
  over windows. OVER () / PARTITION BY / ORDER BY (multi-key, ASC/DESC,
  NULLS FIRST/LAST); ROWS and RANGE frames (UNBOUNDED/N PRECEDING,
  CURRENT ROW, N/UNBOUNDED FOLLOWING); default frame semantics per the
  standard (RANGE UNBOUNDED PRECEDING..CURRENT ROW when ORDER BY present,
  whole partition otherwise); named `WINDOW w AS (...)` clause.
- Multiple window expressions per SELECT, over different windows, mixed with
  ordinary expressions; window functions in expressions (e.g.
  `price - LAG(price) OVER (...)`).
- GROUPING SETS, ROLLUP, CUBE (grouped columns NULL-padded per the standard;
  the `GROUPING()` marker function included so ambiguous NULLs are
  distinguishable).
- IS DISTINCT FROM / IS NOT DISTINCT FROM; `<op> ANY/SOME/ALL (subquery)`;
  OVERLAY; DATE/TIMESTAMP ± INTERVAL literals.
- Bug fixes: GROUP BY ordinal; LATERAL outer-column loss IF the fix is
  contained (else documented + deferred with a failing-test marker).
- Distributed: window queries execute correctly through `serve` — the gather
  path runs the ORIGINAL statement on the initiator, so correctness must hold
  there; no scatter support for windows is required.

## Non-Functional Requirements

- **Memory safety rule holds**: window execution must go through the memory
  pool (sorting reuses the existing external sort; per-partition buffering is
  tracked). OOM is never acceptable; being slow is.
- **No regression** to existing suites: all current tests stay green, TPC-H
  plans unchanged (window code adds nodes only when OVER appears).
- Correctness bar: every new feature validated against DuckDB cell-by-cell,
  same tolerance conventions as `tests/duckdb_validated.rs` (exact for
  int/string/NULL, 1e-6 relative for floats).

## Success Criteria

1. The probe battery (promoted to a committed script) passes every window,
   grouping, and expression probe listed above.
2. New DuckDB-validated window/grouping test suite green (>= 60 cases:
   every function x partitioned/unpartitioned x framed/default, ties,
   NULLs, empty partitions, NTILE remainders, LAG defaults).
3. All 22 TPC-H queries still cell-exact; full suite green in default and
   lance builds.
4. A window query returns correct answers through Flight and HTTP on a
   3-node cluster (gather path).
5. CLAUDE.md / README / trino-function-implementation.md updated.

## Constraints & Assumptions

- sqlparser 0.52 already parses OVER/WINDOW/frames/GROUPING SETS (verified:
  failures are `Not implemented` in the binder, not parse errors) — no parser
  bump needed.
- DuckDB in `.venv` is the oracle, as established.
- Single-threaded window execution is acceptable for v1; parallel
  partition-wise execution is an optimization left for a perf pass.

## Out of Scope

- RECURSIVE CTEs (separate architecture: iterative execution).
- NATURAL JOIN (rare; explicit USING works).
- BETWEEN SYMMETRIC (sqlparser 0.52 cannot parse it; needs a parser bump).
- Window aggregates beyond COUNT/SUM/AVG/MIN/MAX (STDDEV etc. OVER) — nice to
  have, not gating.
- IGNORE NULLS / RESPECT NULLS clauses (sqlparser 0.52 support unverified);
  EXCLUDE frame clauses; GROUPS frames.
- Scatter-mode distribution of window queries.
- Parallel/spilling window operator beyond reusing the existing sort
  infrastructure (documented limitation if a single partition's frame state
  exceeds memory).

## Dependencies

- Existing: binder/logical-plan/physical-operator infrastructure, external
  sort, memory pool, DuckDB oracle scripts, `tests/duckdb_validated.rs`
  conventions, gather-mode distributed execution.
- `.claude/plans/trino-function-implementation.md` Priority 5 sketches the
  intended shape (WindowExpr / WindowNode / WindowExec) — this epic realizes
  it.
