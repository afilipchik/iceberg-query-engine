# Provider CPU excess triage — historical630 evidence, 2026-09-07

This analysis was prepared during the frozen638 build. It uses the last completed
630 canonical SF10 provider screen, not638 measurements. Source630 was rejected
for a protected cast component regression, although its full provider requests
passed correctness/time gates. This is query-level triage, not operator attribution,
not a new benchmark run and not leadership evidence.

The reusable script is `.scratch/temporal-domain-repair/rank-provider-excess.py`.
It checks completed queries, uses matched DuckDB calibration medians and steady
candidate medians, and records SHA256 for every input result file. Raw output is
`historical-630-query-excess.json` in the same directory. After638's screen, rerun
with prefix `temporal-domain-full` and a fresh output path.

## Equal weighting and sensitivity

Primary score: for each required CPU track, divide positive per-query excess
(engine minus DuckDB median) by that track's DuckDB suite total, then weight each
track one quarter. Required tracks are raw Parquet, native, Iceberg and Lance.
IPC and GPU are separate modes and do not enter this score. Negative per-query
excess is zero for triage; complete suite ratios still retain all wins and losses.

| Query | Primary relative-excess score |
|---|---:|
| Q18 | 0.18728 |
| Q13 | 0.12523 |
| Q1 | 0.10881 |
| Q9 | 0.08920 |
| Q10 | 0.08325 |
| Q16 | 0.07555 |
| Q12 | 0.05516 |

A secondary normalization gives each track one quarter of its positive-excess
mass. That ranks Q16 first (0.29062), because it disproportionately emphasizes a
rare regression in the otherwise winning Iceberg track. Preserve both views and
inspect per-track values; do not silently choose whichever scoring rule favors
a proposed implementation.

## What the evidence supports

Historical native Q18:1083.9ms versus174.8ms DuckDB; Q13:938.8 versus241.9;
Q1:735.8 versus108.6. Q18 and Q13 have nested spillable join/aggregate plans;
Q1 is filter/project/aggregate. The plan labels do not prove actual spilling,
operator-exclusive CPU time or the cardinality of intermediate states.

There is also time outside the reported phase intervals. Computing each native
sample's outer response.ms minus its own parse/plan/optimize/execute durations,
then taking the median, gives42.68ms forQ18,22.43ms forQ13 and83.52ms forQ1.
These are same-sample residuals, not subtraction of independently computed medians.
The context source times binding and physical planning separately into plan_time,
then captures execute_time before result dictionary decoding and function exit.
Rollup substitution, plan display/capping, output normalization and local object
destruction are among the uncovered work. The residual is not proof that all of
that time is teardown, nor is it evidence the end-to-end benchmark omitted it:
the embedded runner's timer includes context.sql through return.

## Next bounded attribution

First rerun the ranking on638, preserving its fresh DuckDB calibrations and all
failures. For surviving dominant shapes, use a separate diagnostic build/session
to split provider preparation, hash build/probe, aggregate update/merge/finalize,
result normalization and plan/state destruction. Record actual input/output rows,
spill counters, reservations and CPU/wall time. Assign stable operator paths; do
not add nested wall durations as exclusive CPU time. Retain the ordinary binary
as the performance control.

Choose a shared operator change only after this attribution. The current evidence
does not demonstrate casts account for30% of remaining excess and does not justify
an engine rewrite or more query-specific tuning. Continue the semantic/resource
work while retaining the unresolved cast component regression explicitly.
