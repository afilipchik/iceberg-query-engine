# Outer join ON predicate pushdown

The actual implementation now has a balanced, unchanged-SQL release comparison:
Q13 median falls from591.156ms to490.414ms, **17.0% lower**. Each binary has12
steady samples and4 warmups across four alternating-order blocks. All32 results
pass the independent typed ordered oracle. Per-block candidate/control median
ratios are0.8242,0.8540,0.8343,0.8279. The optimized plan moves the ON predicate
to the orders scan and removes the comment column before the join. The two
frozen binaries share the detailed diagnostic code, with all timers disabled.
This is a measured production-rule benefit for one diagnostic workload, not
suite leadership or completion of the broader CPU task.

The subsequent unchanged canonical SF1 suite passes63 measured typed pairs
across21 queries. Q13 still times out; the watchdog termination causes two
subsequent worker-unavailable samples. No suite score is valid. A fresh canonical
SF10 raw-Parquet run is now active, using16 threads, CPUs0–15,4GiB query memory,
12GiB process cap,3 samples and one session under a32GiB cgroup. Its results
remain pending in `.scratch/public-bench/outer-on-sf10-raw-01/`.

A balanced diagnostic of equivalent SQL reduced canonical SF1 Q13's median
from593.880ms to501.508ms (15.6%). Eight requests ran in original/prefilter,
prefilter/original order twice, with unchanged16-thread/4GiB query settings.
All eight outputs match the typed ordered DuckDB oracle. This is evidence for
a general rewrite, **not a measured speedup of the new optimizer implementation**.
Both forms still miss the approximately350ms current query ceiling.

The missing capability is separating ON predicates from WHERE predicates at
outer joins. A right-only ON condition may filter the right input of a LEFT JOIN:
unmatched left rows are still emitted. A right-only WHERE condition has different
semantics and cannot be moved there by the same rule. The symmetric ON rule
applies to RIGHT JOIN. DuckDB's local `plan_joinref.cpp:25–60,170–199` implements
that distinction at commit`1c27c54f27bea397d18d86f7c0d6a9f1d4aa85f8`.

## Implementation

`PredicatePushdown::outer_on_predicates` now splits eligible ON conjuncts into
their own input-filter list, using qualified column ownership and retaining a
residual join predicate. Existing incoming WHERE lists remain separate.
Only the non-preserved input of LEFT/RIGHT joins receives these predicates.
FULL joins are unchanged.

An explicit total-expression policy admits same-type scalar comparisons,
column IS NULL/IS NOT NULL, and Utf8 LIKE/NOT LIKE with a literal pattern without
escapes. Functions, casts, arithmetic, subqueries, ambiguous coercions, cross-side
references and potentially invalid escaped patterns stay at the join. Moving
evaluation must not introduce volatile re-evaluation or errors on unmatched
input rows. This is a capability boundary, not a statistical semantic proof.

The new integration test first reproduced the retained right-only residual.
After implementation, its initial two tests pass for LEFT/RIGHT multiplicity,
NULL/unmatched rows, multiple/empty batches and retained unsafe predicates.
Final gates pass:888 library tests (10 explicit ignores) and28 selected
integration tests, including all three new tests with WHERE, NULL-predicate and
empty-input cases. No failures or integration skips occurred.
Two earlier test-harness import/API compile errors are preserved separately from
the actual before-fix plan assertion failure. Formatting and whitespace checks pass.

## Ingestion diagnostics

Four detailed runs also passed typed oracles. Sampled inner phases put key
encoding and hash lookup/new-group setup ahead of state preparation individually,
but clock overhead and periodic sampling prevent exact CPU attribution. The
outer aggregate reports roughly23ms with per-batch logging versus12ms in the
previous diagnostic: logging2,344 small batches materially perturbs it. Do not
use those intervals as acceptance latency or extrapolate sample totals into
exact exclusive CPU. This evidence supports investigating typed batch key/lookup
work, but does not justify another state representation rewrite by itself.

[Archived commands, requests, responses, comparison results, source and test logs](benchmarks/2026-09-07-outer-on-predicate/manifest.json)
preserve the experiment. The measured release binary predates the optimizer
change. Rebuild it, rerun unchanged canonical SQL, then broader workload/provider
and protected gates before claiming an accepted improvement. Q13 completion,
fresh canonical SF10 and the full milestone remain open.
