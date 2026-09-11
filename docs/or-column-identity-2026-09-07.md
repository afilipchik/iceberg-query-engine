# Structural identity in OR-predicate derivation

The OR optimizer could add a false implication by treating printed column names
as semantic identity. A structural key fix is applied and all five focused
regressions pass. Broader SQL/feature validation passes; neither complete quoted-name
SQL support nor performance acceptance is established.

## Reproduced failure

Column(relation="a", name="b.c") and Column(relation="a.b", name="c") both
print as `a.b.c`. For `first=1 OR second=2`, the old map merges those columns and
derives `first IN (1,2)`. With first=NULL and second=2, the original filter is
true but the augmented one is not. The direct-rule test reproduces the lost match
independently of SQL binding or physical column lookup.

Contained red session4729 runs the five `structural_or_identity_contract` library
tests with lance+gpu and exits101: four real assertion failures, one positive
control passes. The failures cover lost filter matches, the expanded implication
matrix, an existing displayed IN hiding another column's derivation, and unstable
HashMap-derived output order. The passing control confirms that one structural
column constrained in every branch still derives. Source before adding tests was
verified against frozen642; only derive_or_predicates.rs changed among production
inputs. Dependencies are unchanged.

## Fix and validation

Per-disjunct maps now use Column Eq/Hash, preserving the relation/name boundary.
Existing and derived conjuncts compare as Expr, not display strings. An explicit
first-occurrence vector determines output order; HashMap iteration does not.
Equality, reversed equality and literal IN branches retain the previous scope
and value-count limit. No workload, table or query-name special case was added.

Focused green session42347 passes all five tests. The expanded matrix checks432
row/expression combinations with an independent three-valued evaluator: NULL and
three integers, equality/reversed equality/IN, reversed OR branches, and AND
branches. Tests also check repeated augmentation, positive derivation and stable
order across64 rewrites. Formatting and whitespace checks pass.

Broader session8146 runs the library and ten integration suites with lance+gpu,
48 GiB/jobs1, serial tests and the existing repo NVRTC path. It covers convergence,
semantic proofs, numeric/Boolean/comparison and timestamp contracts, partitions,
materialization, reservations and Lance. Session8146 exits0:759 library and105
integration tests pass (864 total), with10 explicitly ignored library tests. Eight
are CUDA tests requiring isolated processes; session60312 passes all eight, each
in its own contained process with actual CUDA available. Total selected passes
are872; the focused five are included in that count.
The other two are the preexisting dependent-join and dedicated IPC-sidecar tests. Commands/logs/source
hashes are in `.scratch/or-column-identity-repair/`. No new optimized binary or
performance result is yet attributed to this fix.

## Remaining schema and physical identity gap

This establishes the rule's implication contract, not full SQL column resolution.
Separate source findings remain:

- PlanSchema::qualified_index joins relation/name with an unescaped dot and can
  overwrite a structurally different field.
- SchemaField::to_arrow_field writes that joined string as the Arrow field name.
- PlanSchema::from_qualified_arrow splits on the first dot; arbitrary quoted
  relation/column names cannot roundtrip through this representation.
- Physical lookup against Arrow names cannot recover previously lost identity.

These findings are not yet separately executed SQL reproducers. A tuple-key
logical map alone would leave the physical gap. Follow with logical lookup,
Arrow roundtrip and independently checked SQL tests before selecting a lossless
bound-column identity/position contract across projections, joins, providers and
subqueries. Do not relabel a binding error as an OR-rule failure or certify all
quoted identifiers from direct-rule tests.

## Frozen optimized-build checkpoint

Source647 archive SHA256
`7b3115679b80300de11edf89ffb35f51bfdb018edb0f2cc0baa1d633de608243`
preserves and verifies every source member. Release session45755 builds the
lance+gpu embedded runner and cap harness in64 GiB/jobs1. No measurements from
older binaries are attributed to this source. A separate six-query Parquet/SQL
probe against DuckDB is prepared for the remaining schema identity gap; it has
not run and must not be counted as coverage.
