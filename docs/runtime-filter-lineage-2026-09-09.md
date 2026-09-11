# Runtime-filter lineage wrong answer — 2026-09-09

## Reproduced failure

A correctness-only run of frozen `7b5b7e93` loses a valid join row:

```sql
SELECT b.k AS bk, x.k AS pk
FROM b
JOIN (SELECT k + 1 AS k FROM p) x ON b.k = x.k;
```

Build values are `[1, 2, NULL, 2]`; probe values are `[0, 1, NULL]` followed by
10,000 integers100..10099. Independent equality after computing `k+1` requires
`[(1,1),(2,2),(2,2)]`. With default runtime filtering, actual output is only
`[(2,2),(2,2)]`. Renaming the computed field to `shifted` restores all three
rows. `RT_DISABLE=1` also restores both query forms. This switch was used only
as a diagnostic control, not as a proposed engine fix.

Both completed controls exit0, use typed Int64 Parquet with NULLs, two threads,
64MiB query limit and4GiB process cap inside a4GiB wrapper scope. This was a
small correctness test alongside a release build; its timings are not performance
evidence. An initial1GiB process-cap attempt failed to spawn runtime threads; a
second attempt completed the SQL but the Python reader incorrectly expected IPC
file framing. Both attempts are preserved. The corrected reader uses IPC streams.
Optional GPU startup reported unavailable NVRTC and CPU execution continued;
this is not GPU coverage.

[Evidence archive](benchmarks/2026-09-09-runtime-filter-lineage/manifest.json):
30 verified files including inputs, outputs, stdout/stderr, optimized/physical
plans and all three drivers. Binary hash:
`7b5b7e93f893269df4df151c767c0982406252b8c59caf2cdd352eccc60fc7a3`.

## Source cause

`physical/planner.rs` follows children while their name is `Project`, then links
an output join-column name to the underlying provider field by name. It does not
check whether that Project computes a new value. Here the build filter contains
1 and2, but it is applied to original probe `k`, discarding0 before the projection
could turn it into matching1. A linked descendant join also republishes the same
scan/schema pair; output-column provenance is not represented there either.

The existing full-signed-domain tests validate filter membership and overflow,
not this planner linkage. Extending the existing name-based registry to native,
resident, Iceberg or Lance would extend the semantic risk. Those providers do
not currently expose the same runtime-filter hook, consistent with the larger
native/resident Q12 probe row count. That performance opportunity must follow the
semantic repair.

## Required shared repair

1. Replace name-based Project traversal with an explicit output-ordinal to source
   runtime-filter target mapping. Resolve qualified columns against the actual
   output schema and reject ambiguity. A source target includes its configuration
   and original column ordinal, not just a same-named field.
2. Direct column projections and aliases may forward the resolved child target.
   Computed expressions, casts without a proven representation-preserving mapping,
   and unknown operators decline before linking. This preserves a correct ordinary
   join and does not disable all runtime filters.
3. Join propagation must map retained output ordinals to the actual probe child,
   preserving build/probe orientation and outer/semi/anti semantics. Do not forward
   a build-side column merely because its name appears in the probe schema.
4. Regressions need the reproduced computed-name collision, direct aliases that
   still link, renamed/reordered and shadowed columns, duplicate/NULL keys, nested
   joins and ordinary/spill routes. Compare exact typed results with an independent
   oracle and inspect whether the intended filter linked.
5. Separately admit filter key staging, bitmap/set payload and configuration
   ownership before extending providers. Current construction uses unreserved
   `Vec`/set allocation; failure of this optional optimization must decline before
   source consumption. A memory estimate cannot justify dropping rows.

The permanent SQL regression33715 reproduced the lost row (one test failed,
one direct-alias test passed). Candidate source now replaces the planner's scan
pointer/name registry with `PhysicalOperator::runtime_filter_target(output_ordinal)`.
The default declines. Streaming Parquet maps its projected ordinal to the original
file ordinal; Project forwards only an actual column/alias with the same type.
Inner joins and build-left LEFT joins restore retained ordinals and forward only
their actual probe output. Other join shapes and unknown wrappers decline.

This preserves independent AND-combined scan slots without name-based traversal.
The shared qualified-column resolver rejects ambiguity. Filter payload allocation
is unchanged and remains a separate admission gap; providers are not widened.
No new source module or dependency is introduced.

First candidate compilation47367 failed on a missing DataType qualification;
95772 exits0 after correction: both SQL lineage tests and both signed-domain tests
pass. A capability regression additionally checks direct alias linking, computed
output refusal, retained ordinal mapping, both build orientations and protected
join types. The initial broad builds exposed two test-compilation migrations: Arrow schema
construction required a Vec (3600), and an existing planner test referenced the
removed registry (66623). The latter now asserts the stronger behavior: a reordered
Int64 alias resolves to original file column zero; Date32 declines.

Final broad33167 exits0: **1,020 library passes,11 ignored and72 integrations**.
Spill20703 exits101: six pass and the same seven historical names fail. Formatting
and whitespace pass. All jobs are terminal. No new release/performance certification
exists. [Repair evidence](benchmarks/2026-09-09-runtime-filter-lineage-repair/manifest.json)
contains nine verified files and501 source inputs; six source files plus the new
integration test differ from frozen dbca414a.
Frozen gather/quantum binarydbca414a retains the old lineage bug; its24 checked
outputs do not clear this broader semantic failure.

## Next ownership gate

Before extending runtime filters, admit staging keys, bitmap/set capacity and
payload lifetime in the query hierarchy. Decline optional preparation cleanly on
admission refusal; do not consume or replay probe input. Also review build-key
expression reuse: the current publisher evaluates `build_keys` before subsequent
hash-table construction. For computed/volatile expressions, repeated evaluation
cannot prove the filter represents the actual evaluated build-key domain. Use the
same evaluated arrays, or conservatively require a structural column proof before
publishing. This is a source-level follow-up, not another reproduced wrong answer.

The remaining resident copying cost and full paired/provider/residency/resource/
concurrency gates stay open. Do not extend name-based filters or claim the24-query
diagnostic clears the full semantic/resource contract.
