# Outer join ON predicate investigation

The current canonical Q13 optimized plan retains a right-only NOT LIKE predicate
inside its LEFT JOIN. `predicate_pushdown.rs:262` decomposes the join's own
residual filter only for Inner/Cross. The Left branch sends incoming WHERE
predicates to the preserved left input and keeps the ON filter unchanged.
These are distinct semantic rules; extending the existing WHERE movement to the
right side would be wrong.

The local DuckDB source at commit
`1c27c54f27bea397d18d86f7c0d6a9f1d4aa85f8`,
`src/planner/binder/tableref/plan_joinref.cpp:25–60,170–199`, explicitly allows
right-only ON filters to move into the right child for LEFT joins and left-only
ON filters into the left child for RIGHT joins. FULL joins preserve both sides.
The source comparison establishes a missing general capability, not its measured
performance value in this engine.

Prepared `.scratch/live-schema-boundary/q13-prefilter-requests.jsonl`: eight
balanced original/prefilter diagnostic requests, with the right-only ON predicate
manually expressed as a right-input subquery filter. SQL and separate output
paths are preserved. This is a semantic-rewrite experiment only; canonical
benchmark SQL and all acceptance gates remain unchanged. Execute after the
current detailed-profile release build (session23648) is terminal. Compare all
eight outputs to the preserved typed Q13 oracle and preserve physical plans and
both timing orders. No optimizer change has been applied.

If measured benefit warrants implementation, move eligible ON conjuncts using
separate lists from WHERE conjuncts. Require qualified column identity, supported
types and an explicit expression-safety policy. Cover both LEFT/RIGHT, duplicates,
NULL keys/values, unmatched preserved rows, empty inputs, multiple batches,
cross-side residuals, preserved-side predicates, WHERE predicates and volatile
or error-producing expressions. Do not infer pushability from NDV or uniqueness
estimates. FULL joins and unsupported expressions must retain their semantics.

Previous goal turn: progress through release validation and instrumentation.
Current build has been polled on its original handle and remains active; no
duplicate heavy job was started. Detailed sampling and the balanced rewrite
experiment remain unmeasured.
