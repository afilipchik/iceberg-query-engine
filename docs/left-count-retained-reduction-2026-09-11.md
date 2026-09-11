# Duplicate-preserving LEFT COUNT preaggregation

The candidate extends the existing eager LEFT COUNT rule without assuming a
unique left key. It preaggregates right rows by their equality key, joins the
counts to the original left rows, and retains grouped SUM(COALESCE(count,0)).
Each duplicate left row contributes separately. NULL keys do not match; unmatched
and all-null-count groups emit zero. Empty grouped input remains empty.

Scope: one direct left group key equal to the single join key, one direct
right-column non-distinct COUNT, LEFT equijoin with no residual ON predicate.
COUNT(*), scalar aggregation, multiple keys/counts, and other group expressions
keep their existing plans. Right-subtree filters stay in place. The separate
unique-left shortcut still requires structural uniqueness to remove the final
aggregate. Numeric accumulation uses the existing Int64 SUM/count contracts;
this is nonnegative count accumulation, not reassociation of signed values.

The new route requires a right row/NDV costing estimate with NDV at most70%of
rows and positive estimated rows. This is a heuristic, not a semantic proof or
resource-admission guarantee. Identity lineage follows Scan, Filter,
SubqueryAlias and direct-column Project nodes by bound ordinal. Filters can make
the estimate inaccurate. Unknown lineage/statistics decline. No query name,
fixed scale factor, or schema-specific routing is used. Existing spillable
physical planning and query-wide limits remain in force.

The prior qualifier/name repair is retained. Internal count names now avoid
both input schemas and the original aggregate output schema. A retained final
SUM cannot match the COUNT pattern on the next optimizer iteration.

Regression94380 fails the missing-rewrite assertion before implementation.
3887 passes24 related integrations after implementation.47168 passes24 after
expanding identity-lineage cases. The two executed matrices cover ordinary and
memory-managed planning, unique/duplicate left keys, populated/empty inputs,
nullable keys and count arguments, empty and repeated input batches, qualified
references, filtered/reordered aliases, independent exact Int64 values, logical
output schemas and convergence. New costing assertions cover missing, zero,
small, unique and overstated NDV without erasing final duplicate reduction.

Broad39449 terminal0:1,030 library passes/3 ignored and46 integrations pass,
with default features, locked/offline,48GiB/one build job, repository TMPDIR. No source changes during validation. This candidate has no
performance evidence yet. Canonical Q13 motivates reducing join fanout, but is
not an activation condition. Full scalar/multiple-key generalization and
resource/provider/concurrency acceptance remain open.

Release14244 terminal0 in8m51s freezes0c867c5c with520 verified source inputs
and lance/gpu features. Diagnostic10779 terminal0 validates three typed-correct outputs and active
retained-count plans in raw/native/resident modes;193-file archive verifies520
inputs, zero OOM/max events. Paired97041 terminal0:80 correct outputs, Q13 improves across all four modes;
raw/resident4Q9 regression signals remain. Independent resource test completes
at1/4/16MiB with actual spill at1MiB and exact10,002-group oracle. Both-mode
feature validation20503 is active; source remains frozen. See [measurement](left-count-measurement-2026-09-11.md).
