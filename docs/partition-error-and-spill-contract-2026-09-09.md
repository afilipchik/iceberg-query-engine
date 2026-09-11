# Typed partition causes and retained spill contract — 2026-09-09

The query boundary now preserves typed errors while attaching the partition and
failure phase. Previously `ExecutionContext::sql` formatted execution/collection
errors into a new `Execution(String)`, so a clean named memory refusal lost its
`MemoryLimit` identity. The same execution wrapper affected native create/replace,
CTAS and INSERT setup. They now share `execute_partition`; SELECT collection uses
`collect_partition` and adds collection context without replaying input.

`QueryError::Partition` owns the original error and a typed `PartitionPhase`.
`root()` iteratively unwraps shared and partition context. `kind()` retains the
historical public `Execution` query-log category at partition boundaries, including
through Shared; Display keeps the preceding message format. Error source chains
remain inspectable. Root-aware consumers, including existing Flight status mapping,
can now see the underlying cause. The partition wrapper grants no retry capability.
Native writer-specific Storage string wrappers remain separate open boundaries;
this does not claim every error wrapper in the engine is repaired.

The cap harness also still parsed the old diagnostic string despite the existing
MemoryLimit variant. Its own tests rejected actual denials, while a new test
proved matching Execution text could be accepted. It now accepts typed admission
pressure through shared/partition context, plus its unchanged exact legacy join
guard. Unrelated IO/allocator failures and matching display text fail closed.
This is refusal classification, not successful-query certification.

## Regression evidence

Red50825 runs three real boundary tests: execution failure, collection failure
following one yielded batch, and the actual fixture-backed256KiB aggregate SQL.
All three fail specifically because the typed cause was lost. The test operator
uses a declared nonzero partition, one-shot execution counting and an admitted
stream owner; failure must release that owner and publish no partial result.
Green64859 passes all3 with preserved pointer identity for original errors,
stable display/category, IO causes and rejection of lookalike text. Native
execution setup subsequently adopts the same tested helper.

Cap classifier red24697 runs4 tests:1pass/3fail, including the existing actual/
shared-denial tests and new matching-text regression. This is reproduced harness
failure, not an inference from documentation. Broad97446 passes all9 cap-example tests.

## Corrected retained-result test

The independent [fixture audit](spill-test-output-floor-2026-09-09.md) proves the
old `agg_spill_matches_in_memory` success expectation cannot fit256KiB under the
current retained Arrow result API. The test is replaced by two explicit contracts:

- `retained_aggregate_over_budget_is_typed_refusal`: enumerate actual input
  keys/dates independently, assert the unencoded key/date/COUNT floor exceeds
  the budget, require an actual typed memory refusal, and verify reservations
  return to baseline. Unrelated query failures cannot satisfy this assertion.
- `retained_aggregate_actual_spill_matches_independent_typed_oracle`: retain a
  complete result under16MiB while a separate2% spill policy forces real disk
  work. An independent Parquet reader joins every actual key/date, preserving
  duplicates and NULL semantics. Compare exact keys, group multiplicity, COUNT,
  NULLs and floating SUM within an explicit finite summation error bound.
  The bound accounts for summation order; values are not rounded to strings.

Initial fixture test69678 failed because its oracle assumed integral quantities;
the actual custom fixture has fractional Float64 quantities. Corrected95510
passes both tests. The positive query returns14,785 groups, spills2,741,595 bytes,
peaks at3,196,658 tracked reserved bytes and retains3,057,480 bytes until the result
is dropped. Thus measured retained admission is much larger than the295,700-byte
minimum floor. These are accounting values, not exact RSS. The output remains
charged while retained and returns to baseline after drop.

This changes the assertion to match a proven resource contract; it does not make
the old256KiB query complete. The archived old failure remains preserved in the
[optional-output gate](benchmarks/2026-09-09-optional-output-validity/manifest.json).
Other legacy join/page/metadata refusals remain open.

## Reproduction commands and scope

All invocations use `TMPDIR="$PWD/.scratch" SAFE_BUILD_MEM=48G SAFE_BUILD_JOBS=1
scripts/claude-safe-build.sh cargo test --locked --offline --features lance,gpu`.
Focused arguments: `--lib partition_error_contract_tests`;
`--example oom_cap_harness classifier`;
`--test spill_tests retained_aggregate -- --nocapture`.
Broad97446 terminates0:1,042 library passes/11ignored,61 integration passes and
9 cap-example passes. Its arguments are the optional-validity broad command plus
`--example oom_cap_harness --test typed_memory_pressure --test partition_contract`.
Full spill76280 terminates101:8pass/6fail. The six remaining failures are join,
outer join, COUNT DISTINCT, semi, anti and filtered semi/anti legacy-budget cases.
The old aggregate success assertion was replaced by two explicit tests; this is
not a claim that the old query now completes at256KiB. Formatting and whitespace
checks pass. The [verified archive](benchmarks/2026-09-09-partition-error-spill-contract/manifest.json)
preserves all508 source inputs, red/green logs, initial fixture-oracle failure,
full gates and original files before this change.

Four source inputs differ from the preceding optional-validity archive:
`src/error.rs`, `src/execution/context.rs`, `examples/oom_cap_harness.rs`, and
`tests/spill_tests.rs`. No dependency, operator algorithm, memory budget default,
query scheduling policy or benchmark ceiling changes. There is no new release or
performance claim for this source. Frozen0f30c946 measurements remain distinct.
