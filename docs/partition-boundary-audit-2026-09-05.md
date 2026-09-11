# Materialization boundary audit — 2026-09-05

This source audit led to two defensive API changes and six deterministic
physical-API regression tests in `tests/materialization_partition_audit_tests.rs`.
The tests have **not been run** at this checkpoint. Production changes were made
after the baseline source was archived, without overlapping the running benchmark
with a build. Do not describe these cases as measured benchmark failures.

## DelimJoin: omitted partitions in a reachable physical API

Before this change, `DelimJoinExec::execute` declared one output partition, then
requested only partition zero from both its outer and inner children. Neither its public constructors nor
`PhysicalPlanner`'s explicit `LogicalPlan::DelimJoin` route requires single-partition
children. Fixed-partition reproducers isolated each side: inputs represented IDs 1 and
2; a semi join should have returned both, whereas source inspection predicted
only ID 1. These unexecuted reproducers were replaced with refusal regressions
when the broader semantic defects made a partition-only repair insufficient.

The normal SQL optimizer currently does **not** emit this operator:
`FlattenDependentJoin` remains registered in `Optimizer::new`, but its `optimize`
method returns `plan.clone()` and never calls `flatten_plan`. This bug is therefore
an exposed physical/explicit-logical API defect, not established attribution for
canonical TPC-H latency or result failures.

Implemented behavior: DelimJoinExec now returns `QueryError::NotImplemented`
identifying its missing exact dependent-join contract, before reading either child
or publishing any DelimState. This applies even to single-partition inputs and all
join kinds. Constructors, type interfaces and explicit physical planning remain
available for a future implementation; executing such a plan now refuses cleanly.
The range check still runs first.

Partition repair alone cannot certify DelimJoin. Its separate hash-only equality
implementation uses hashes as proof of equality, hashes unsupported values by row
index, and does not implement ordinary SQL NULL join semantics. Its scalar join
also selects the first match and drops unmatched rows instead of validating scalar
cardinality and producing NULL. These are independently visible source risks.
Keep the SQL rewrite disabled until typed equality, NULL/cardinality behavior and
query-wide memory ownership are repaired or the operator is replaced with shared,
validated execution components.

## VectorSearch: planner-safe topology, unguarded constructor

The optimizer recognizes a global Sort+Limit shape. `PhysicalPlanner` reconstructs
that exact Sort+Limit as the fallback. Both operators produce one partition and
consume their full inputs; calling fallback partition zero is valid for this
current planner route.

However, the public `VectorSearchExec::new` accepts any physical fallback and the
executor unconditionally calls partition zero. The new direct-API regression
supplies a two-partition fallback and expects an explicit error. Silently returning
one partition violates the contract. Merely concatenating partitions would not
prove global top-k ordering either.

Implemented guard: before attempting either the index provider or exact fallback,
require exactly one fallback output partition and return a named `Execution`
contract error otherwise. Both zero and multiple partitions are refused without
consuming the fallback or calling the provider. The planner-generated global
fallback remains valid. Approximation still requires explicit Indexed mode.

## Spillable: reviewed partition-zero calls are guarded

- `collect_input_partitions_concurrently` uses partition zero only when the child
  declares one partition; otherwise it launches every declared partition.
- `stream_merge_input_partitions` has the same one-partition guard and merges all
  child partitions in its multi-partition branch.
- The in-memory aggregate delegation calls `HashAggregateExec::execute(0)` after
  collecting the input; HashAggregateExec declares one output and drains all of
  its MemoryTableExec partitions.
- The in-memory sort delegation calls `SortExec::execute(0)`; SortExec has one
  output and drains all input partitions through the reviewed collection helper.

No omitted-partition defect was found at these spillable call sites. This does
not certify their memory accounting or cancellation lifecycle: collection and
bounded producer channels are separate resource-ownership concerns.

## Validation pending

Run `materialization_partition_audit_tests` through
`scripts/claude-safe-build.sh`, with TMPDIR set to repository `.scratch`, after the
current benchmark finishes. Six tests cover:

- DelimJoin refusal with multipart outer and multipart inner children, asserting
  zero child executions and no published DelimState.
- DelimJoin refusal for single-partition Semi, Anti, Mark and Single joins.
- Invalid zero/multipart vector fallback in Exact and Indexed modes, asserting
  zero provider and fallback calls.
- Valid one-partition vector fallback in Exact and provider-unavailable Indexed
  mode, asserting its exact IDs and one fallback execution.

Also rerun `partition_contract` and existing vector-search tests. Record the
command, features, revision/source hash and actual failures. No build or test
command was launched by this audit task.
