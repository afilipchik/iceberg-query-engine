# LEFT JOIN count preaggregation: bound identity

The executed regression `left_count_rewrite_executes_with_qualified_internal_name_collision`
reproduces `ColumnNotFound("r.fk")` after the eager LEFT COUNT rewrite. The
ordinary logical plan executes against an independent expected multiset first;
the test separately requires rewrite activation and equal logical output schemas.
The fixture includes duplicate left and right keys, nullable keys/count arguments,
two input batches, and a left column named `__ea_cnt`. DISTINCT on the exact left
subtree supplies the structural uniqueness proof. No NDV estimate supplies proof.

The rewrite previously constructed its right grouped key with `relation: None`
but retained the original qualified join predicate. The repair derives that field from the bound grouping expression.
That first repair exposed `ColumnNotFound("__ea_cnt")` in session98760: the
unqualified generated count collided with the left user column. The final repair
chooses a count name absent from both input schemas, checking names regardless
of relation. Original output schema and structural eligibility remain unchanged.

Evidence: `.scratch/parallel-aggregate-input/left-count-qualified-red-runtime.log`
(session33945, terminal101, one runtime failure). The initial22363 attempt did
not compile because the test called `apply` instead of `optimize`; preserve that
as test-construction evidence, not a reproduced engine failure. Tests run locked,
offline, through the 48 GiB/one-build-job wrapper, with repository TMPDIR.

The a4103dfa benchmark remains frozen and archived independently. This optimizer
candidate has no performance measurement and does not expand rewrite eligibility.


Validation:39952 passes23 related integrations; expanded11996 again passes23.
The new test executes12 combinations of ordinary/memory-managed planning,
structurally unique/duplicate left keys, and populated/empty-left/empty-right
inputs. Both original and rewritten plans must match the independent Int64
multiset, including NULL keys/count arguments; all declared partitions are driven.
It asserts rewrite activation only with proof, logical schema preservation, and
optimizer idempotence. The memory-managed path uses64MiB; this small fixture
does not prove actual disk spill. No resource policy changed.

Library76443 terminal0:1,030 passes/3 ignored with default features. These counts
are not the earlier lance/gpu1,092/11 feature gate. All commands use
`TMPDIR="$PWD/.scratch" SAFE_BUILD_MEM=48G SAFE_BUILD_JOBS=1 scripts/claude-safe-build.sh`:

```
cargo test --locked --offline --test semantic_proof_tests left_count_rewrite_executes -- --nocapture
cargo test --locked --offline --test semantic_proof_tests --test qualified_column_identity --test optimizer_convergence_contract
cargo test --locked --offline --lib
```

Current source differs from frozen a4103dfa in only the eager-aggregation rule
and semantic-proof test. No dependency or physical module routing changed.
[Evidence archive](benchmarks/2026-09-11-left-count-binding/manifest.json) includes
all five regression attempts, library output and a verified520-input source snapshot.
This fixes execution errors, not a demonstrated silent wrong result or throughput
regression. General duplicate-preserving preaggregation remains a separate task.
