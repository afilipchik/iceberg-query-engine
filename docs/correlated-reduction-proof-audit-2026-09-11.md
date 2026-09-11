# Correlated aggregate reduction: duplicate proof audit

Frozen0c867c5c has a reproduced wrong-result bug:
`add_semi_join_reduction` in `src/optimizer/rules/subquery_decorrelation.rs`
constructs an INNER join under the correlated aggregate. Candidate source
selection prefers filtered dimension-like scans and matches correlation fields,
but does not prove source-key uniqueness before selecting INNER. The comment
justifies this with a physical build-side concern. Current physical planning can
choose a right-side build for LEFT/SEMI/ANTI without swapping their SQL semantics.
That historical rationale must be checked against current contracts.

A membership reduction may exclude irrelevant input rows; it must not multiply
relevant rows when its source has duplicate correlation keys. An inner join can
multiply those rows. This is particularly visible with SUM/COUNT; uniform
multiplication inside AVG can conceal the issue in ordinary TPC-H-shaped data.

Prepared probe: outer rows have keys1,1,2,NULL,3 and flags1,1,1,1,0. Right keys
are1,1,1,2,NULL,3 with values2,3,NULL,7,99,1. For filtered outer rows, compare a
correlated SUM against thresholds7,7,6,0,0, and COUNT(value) against2,2,0,0,0.
Both independent expected outputs contain only key2. Duplicating the right rows
through the outer key1 source could incorrectly admit both key1 rows. Use the
frozen0c867c5c runner and preserve full optimized/physical plans and typed arrays.

The first aliased probe completes correctly but retains `(scalar subquery)` in
its plan; it does not exercise decorrelation. The second, unaliased form activates
that rewrite and returns `[2,1,1]` for both SUM and COUNT instead of `[2]`.
Both use the same frozen binary and fixtures. Preserve the first probe as a
non-activation control. The [34-file probe archive](benchmarks/2026-09-11-correlated-reduction-probes/manifest.json)
verifies the frozen520-input source against its archived tar, including both
exact drivers, fixtures, requests, responses and typed arrays.

Permanent regression95874 fails with duplicate key1 outputs. Current source
replaces the reduction's INNER join with SEMI: original aggregate input on the
preserved left, reduction source on the right, original aggregate input schema.
No source uniqueness is assumed; physical planning chooses build orientation
separately. Focused95743 passes34 integrations. The expanded12-case matrix covers
SUM/COUNT, aliased/unaliased SQL and populated/empty-left/empty-right inputs;
broad96185 terminal0 passes1,030 library tests/3ignored and47 integrations
with default features, locked/offline,48GiB/one build job and repository TMPDIR. No performance measurement belongs to this repair yet.

Current physical planning can build from the right for SEMI while preserving left
outputs. Its runtime-filter wiring currently excludes that orientation together
with LEFT/ANTI; unlike those joins, SEMI may safely discard nonmatching probe
rows. Any follow-up must bind filters to the actual probe subtree by proven
column lineage and preserve build/probe initialization/ownership. Do not change
SEMI back to INNER to recover performance. Source matching by names/suffixes and
multiple-correlation lineage need further semantic coverage.

The prior provider20601 and independent audit89129 are terminal and archived
before these source edits. Canonical correctness did not detect this duplicate
case. The planned same-binary morsel route control remains prepared but has not
run; correctness repair takes priority.


[Repair archive](benchmarks/2026-09-11-correlated-reduction-repair/manifest.json)
preserves the520-input source snapshot and regression logs. Source changes from
frozen0c867c5c are confined to `subquery_decorrelation.rs` and
`semantic_proof_tests.rs`; no dependency, budget or ownership default changes.
Formatting and whitespace checks pass. Feature/provider/resource acceptance for
this newer source remains open.

Both-mode lance/gpu feature validation45945 is active; source is frozen for it.
