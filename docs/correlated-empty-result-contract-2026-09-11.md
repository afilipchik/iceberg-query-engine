# Correlated scalar aggregate: empty-result restoration

Pre-repair source had a reproduced wrong-result bug independent of the repaired
membership multiplicity. Regression16461 returns no rows instead of keys NULL
and2 for COUNT(v), COUNT(v)+1 and COALESCE(SUM(v),7). Strengthened96126 also
reproduces NULLIF(COUNT(v),1). Every optimized plan is required to decorrelate;
this is not a non-activation control. Both attempts are terminal101.

Fixture: outer keys1,1,2,NULL with flag1; inner key1/value5. Compare each scalar
subquery to its proper empty result:0,1,7,0 respectively. Key1 must not match:
COUNT=1, COUNT+1=2, SUM=5 and NULLIF(1,1)=NULL. Key2 and the NULL outer key have
no qualifying inner rows and must match their scalar empty results. The current
LEFT join exposes NULL for the missing grouped result, dropping both rows.

Required repair:

1. Derive the scalar subquery's empty-input result from its original logical
   aggregate and projection expressions before adding correlation group keys.
   COUNT contributes typed0; nullable aggregate families contribute a typed NULL.
   Preserve expression composition, result types and aliases. Do not detect
   aggregate identity by searching for COUNT/SUM in display strings.
2. Distinguish a missing grouped row from an existing row whose scalar result is
   NULL. A fresh presence marker projected inside the right subtree can survive
   the LEFT join; select the empty-result expression only when that marker is
   NULL. COALESCE(result,empty_default) is incorrect for NULLIF(COUNT(v),1).
3. Rebind only proven output identities. Both marker and scalar aliases must be
   fresh across both schemas. Do not evaluate volatile/fallible empty expressions
   during planning. The current evaluate_case implementation evaluates selected
   rows only; preserve that property for matched rows and empty outer input.
4. Preserve grouped-scalar, HAVING, LIMIT/OFFSET and unsupported-expression
   semantics. An original grouped aggregate may have no row on empty input;
   HAVING can suppress the scalar row. Derive their existence correctly or use a
   documented pre-execution fallback for unsupported shapes. A universal zero
   or default replacement is not a valid solution.
5. Test matched NULL, missing key, NULL correlation key, duplicate outer rows,
   empty left/right, COUNT DISTINCT, expression-derived defaults, aliases,
   multiple correlations, errors/short-circuiting and optimizer convergence.
   Retain the independent SUM/COUNT multiplicity regressions. Measure the new
   physical plan and resource behavior before making performance claims.

The intentionally failing source/test snapshot remains archived. The candidate
implementation and current validation below repair these cases. The previous membership repair's both-mode45945 validation is terminal:
each1,092 library/11ignored,116 contracts pass, historical native/spill failures
unchanged. Its25-file archive verifies520 inputs and predates this new test.
The red attempts ended before implementation began. The same-binary morsel diagnostic
is prepared but has not run; correctness takes priority.

## Implemented candidate

`subquery_decorrelation/empty_result.rs` symbolically derives typed empty outputs
for global COUNT/COUNT DISTINCT/SUM/AVG/MIN/MAX through direct projections,
column references, literals, arithmetic, unary expressions, casts, CASE,
COALESCE and NULLIF. It never executes expressions in planning. Original grouped
aggregates, HAVING/LIMIT wrappers, other aggregate/function domains and unknown
operators retain scalar execution before any source selection. Their fallback
behavior is exercised, not treated as rewritten coverage.

The correlated right projection carries a fresh boolean presence field. The
post-join comparison uses lazy CASE(presence IS NULL, empty_expression, actual).
Matched NULL results remain NULL. Both internal aliases avoid either schema;
scalar output selection now requires one exact output name rather than matching
aggregate-name substrings. This does not replace the planned broader output-ID
migration. No dependency, memory-budget or ownership-default change.

Initial4729 did not compile because ScalarFunction is Clone, not Copy; corrected
21478 passes the four-case regression. Expanded34129 passes35 related integrations.
The suite now covers COUNT DISTINCT, empty filtered input, COUNT(NULL), explicit
grouping, HAVING, LIMIT0, lazy strict-cast errors and incoming internal-name
collisions. Grouped/HAVING/LIMIT cases explicitly retain correct scalar execution.
Broad25322 terminal0 passes1,030 library tests/3ignored and48 integrations
with default features through the48GiB/one-job wrapper.
No optimized performance or complete resource certification belongs to this source.
The earlier red test/source archive remains immutable.


[Repair archive](benchmarks/2026-09-11-empty-scalar-repair/manifest.json) verifies
nine files and521 source inputs. Both-mode feature validation16808 completed: each1092 library/11ignored and117 contracts pass; historical native/spill failure names persist;
source remains frozen during validation. Further performance/resource acceptance
is pending; no optimized release has been built for this repair.
