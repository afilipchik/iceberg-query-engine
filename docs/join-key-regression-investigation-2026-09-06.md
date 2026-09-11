# Q9 packed-key proof change: source/plan audit

Read-only analysis of two completed artifacts; no engine, build, profile or test
execution was performed. The final latency run is separate and is not analyzed here.

## Observed differences

| Artifact | Q9 pairs | Median engine ms | Median DuckDB ms | Reported engine/DuckDB ratio |
|---|---:|---:|---:|---:|
| `tpch-sf10-decimal-candidate-01` | 30 | 1204.662620 | 505.437769 | 2.383404 |
| `tpch-sf10-latest-contracts-01` | 3 | 1469.448773 | 507.625491 | 2.894750 |

The second engine median is about 22% higher. These are different runs, not a
controlled single-change experiment. Both pin CPUs 0–15, but the candidate binary
has default features while the later binary has Lance/GPU compiled in. Their binary
hashes differ:

- candidate: `5f07f14398a2c6b0b0f4bfcfe5da4238b715aa3641fe2e5da6037802c3d3ac89`
- later: `29f66805d43b6b9ba20b9331c6ab642078c487f052d044e3aa803529ee45e4c6`

Within each run all preserved Q9 optimized plans are identical. Across runs the
partsupp-to-lineitem join changes from one packed Int64 key:

```
CAST(ps_suppkey AS Int64) * 2097152 + CAST(ps_partkey AS Int64)
  = CAST(l_suppkey AS Int64) * 2097152 + CAST(l_partkey AS Int64)
```

to the original exact two-key join:

```
ps_suppkey = l_suppkey AND ps_partkey = l_partkey
```

Join order, surrounding projections and aggregate shape are otherwise unchanged in
the preserved textual optimized plans. The physical operator-name trees are
identical: SpillableHashJoin nodes under a SpillableHashAggregate and ExternalSort.
Those names do not expose the executed hash-table specialization or spill admission.

The manifests' engine-source hash maps differ in exactly three files:
`optimizer/rules/packed_join_keys.rs`, `optimizer/rules/packed_group_keys.rs` and
`physical/morsel_agg.rs`. Aggregation changes therefore remain another confounder.
Source comparison uses `.scratch/decimal-candidate-source.tar.gz` and
`.scratch/contracts-latest-source.tar.gz`, not an assumption that current HEAD is
identical to either measured build.

## Why the packing rewrite no longer applies

The earlier PackedJoinKeys gathered footer-style min/max by unqualified column name
from the table-statistics registry and treated them as semantic proof of collision
freedom. The replacement ignores this registry for correctness and asks for
structural/type/predicate domains on the actual join subtrees.

The canonical dataset manifest declares all four part/supplier key columns Int64.
Their type domains alone include negative values and cannot prove a nonnegative,
i64-safe arithmetic packing. In addition, the current `domain_at` deliberately does
not propagate domains through joins; the lineitem-side input is a multi-join subtree.
Thus two distinct proof gaps prevent the old rewrite. Adding join lineage alone
would not establish the missing numeric bounds. Re-enabling unqualified-name
statistics as proof would reintroduce the original correctness risk.

## Specific execution capability potentially lost

In-memory SpillableHashJoin delegates to HashJoinExec. Replacing a single packed
Int64 expression with two Int64 expressions removes the single-key shape and adds
a second key hash/comparison component. This is a plausible cost increase.

Do **not** call this a proven fallback to scalar/generic row-object hashing:
`VectorizedHashTable` accepts composite Int64 keys. Its `i64_key_bufs` optimization
is enabled when every key array is Int64, including two-key arrays, and its probe
loops compare all key columns directly. Those conditions fit both recorded plans.
Likewise, a specialized auxiliary i64 HashMap is only built when the vectorized
path does not serve the join. The plan names do not establish that condition.
Direct-address mode requires one key plus a suitable actual range; the packed
supplier/part domain is sparse, so availability cannot be assumed from one-key
shape alone. Historical comments claiming a 10× generic-path gap are not evidence
for this current implementation.

Current source starting points: `PackedJoinKeys::try_pack` and `integer_domain`;
`HashJoinExec` vectorized-table build, `i64_key_bufs`, `probe_batch` and
`probe_batch_stream`; and `SpillableHashJoinExec::compute_build_decision`.

## Safe next experiment

First use opt-in diagnostics outside primary latency measurements to verify actual
in-memory/spill admission, vectorized-table mode, key count, build/probe time and
hash-chain comparisons. Run matched source/features and preserve plans and traces.
Do not attribute the 22% difference to packing until a one-change experiment measures it.

Prefer a physical two-Int64 key specialization that keeps the logical two-column
predicate and exact tuple equality. Use fixed two-column buffers and direct typed
comparisons rather than per-candidate generic loops. A fused hash may combine the
two integer components with wrapping arithmetic, but that value must remain a hash
only: every candidate must compare both original keys. Hash collisions are allowed;
false join matches are not. Keep the selected hash function identical on build and
probe and preserve the existing NULL-nonmatch rule. This avoids requiring invented
bounds or converting a full 128-bit pair domain into a supposedly unique i64 value.

Required tests: both Int64 extrema and negative values, NULL in either component,
duplicates on each side, deliberately colliding fused hashes with unequal pairs,
multiple batches/partitions, residual predicates and forced spill with an independent
typed oracle. Charge any new retained buffers to existing admission. Validate the
whole public-development workload and protected suite before adopting a measured
improvement; no query ID, table name or benchmark-specific constant is permitted.
