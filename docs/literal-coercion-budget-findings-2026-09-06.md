# Remaining literal and coercion admission gaps

Historical reproducer below uses frozen620. Current source623 addresses scoped
flat literal expansion and passes770 selected tests; its release build is pending.
See [literal admission](literal-memory-admission-2026-09-06.md). Coercion remains
open; no new optimized result is inferred from these tests.

A contained optimized probe of frozen release 620 confirms that its new primitive
output path does not establish a query-wide budget. With a 64 KiB query limit,
16,384 resident Int64 input rows and one execution thread:

| Query | Output logical bytes | Reserved peak | Independent values |
|---|---:|---:|---|
| `SELECT i FROM t` | 131072 | 0 | Match; borrowed input control |
| `SELECT 1 AS v FROM t` | 131072 | 0 | Match; newly expanded literal |
| `SELECT CAST(i AS DOUBLE) AS v FROM t` | 131072 | 0 | Match; new coercion output |
| `SELECT CAST(i AS DECIMAL(20, 0)) AS v FROM t` | 262144 | 0 | Match; new decimal output |

All queries complete instead of refusing. The borrowed input alone is not a
working-allocation violation; the other three allocate new outputs exceeding
the query budget with no reservations. This probe ran in a 10 GiB scope with
swap disabled and an 8 GiB process cap. It was not a performance run and did not
compete with latency measurements.

Binary SHA-256: `82dc15c4c595cc9fe3342ba42cdf4493277ad7440c117bf84a71dadcf35a0205`.
Raw setup/input/output, metrics and source are preserved under
`.scratch/admitted-construction-repair/remaining-budget-control/` and
`remaining-budget-probe.py`. The upcoming 621 candidate has not changed these
entry points; its behavior must still be observed, not inferred as fixed.

The shared filter `scalar_to_array` is currently infallible and expands literal
vectors with ordinary Arrow/Vec allocation. Its callers include projection,
comparison, constant folding and subquery handling. Migration must make admission
failure explicit at these boundaries, preserve constant-folding error deferral,
and cover primitive/decimal/date/time/boolean/string values. List literal behavior
also needs an explicit typed contract rather than silent JSON/null conversion.

Coercion in `planner/numeric.rs` continues to call ordinary Arrow casts for types
not handled by reserved integer-string conversion. Operator output admission does
not cover these live input temporaries. They require pre-admitted construction
and ownership just like final outputs; estimates or post-allocation observation
cannot close the gap. Track direct API and worker entry points as well as scoped
projection evaluation. Preserve all remaining resource gates until these are
integrated and tested.
