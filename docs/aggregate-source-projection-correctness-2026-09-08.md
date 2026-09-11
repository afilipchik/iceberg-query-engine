# Aggregate source projection correctness — September 8, 2026

A direct logical-to-physical planner regression reproduces a wrong answer in the
source extraction used by the specialized Parquet aggregate. Input values
`[1, 1, NULL, 2]`, projected as `x + 10 AS x`, must sum to 34. With morsel
execution disabled the result is 34; with it enabled the result was 4 because
`try_extract_parquet_source` discarded Project unconditionally. This is a
reproduced physical-planner contract failure, not yet a demonstrated SQL frontend
reproducer: the current optimizer preserves values in the tested SQL forms.

The native dense source extractor had the same unconditional Project recursion.
Both extractors now share `source_preserving_project`. It allows only column
values whose exact output field equals the resolved input field, with unambiguous
output identity and matching expression/schema width. Subsets and reordered
fields remain eligible because aggregate expressions bind by column identity.
Renames, computed values and ambiguous duplicate output identities decline before
execution and retain their ordinary physical Project. Exact composition through
renames remains future work; this guard must not be mistaken for that performance
implementation or for closing direct-file aggregation's resource-accounting gaps.

`tests/aggregate_binding_transparency.rs` covers five ordinary SQL forms over
three Parquet row groups, duplicate/NULL/all-NULL groups and empty input; computed
filtered input, DISTINCT and ordered LIMIT have independent expected values.
Display aliases are explicit so the tests do not impose an unrelated naming
convention. The first test attempt assumed an unaliased qualified field's label
was unqualified; that assertion was corrected, not reported as an engine bug.
The physical-planner regression compares both routes with the independent sum.
A unit regression covers transparent subsets/reordering and rejection of renames
and ambiguous output identities.

Evidence under `.scratch/parallel-aggregate-input/`:

- `aggregate-binding-transparency-01.log`: initial display-label assertion failure.
- `aggregate-binding-transparency-02.log`: test setup compilation errors, corrected.
- `aggregate-binding-transparency-03.log`: actual red regression, 4 versus 34;
  two SQL tests pass.
- `aggregate-binding-transparency-04.log`: all three integration tests pass after
  the shared guard, zero skips.
- `aggregate-source-guard-library-01.log`: job88947 completed; 988 passed,
  zero failed, 10 existing ignores, 15.35s.

Commands use repository TMPDIR, locked/offline lance,gpu, Rayon4 and one build job
inside the required 48GiB safe-build scope. No benchmark result from frozen
9c868dc0 certifies this newer source. Continue the exact-binding/capability task
and freeze/revalidate subsequent performance candidates. The earlier strict
wrapped-query timeouts remain failures.

## Broader validation and existing native failures

Selected integration jobs41609/11369 complete: 48 passed, four failed, zero
skips across seven files. The four failures are all in native_streaming_scan_tests:
aggregate_over_oversized_table, filtered_aggregate_over_oversized_table,
deletion_vector, and join_over_oversized_table completion checks (full names in
logs). Three refuse query-pool allocations at a 262144-byte budget; the plain
join refuses a materializing native scan estimated at 979452 bytes against a
209715-byte scan budget. These are completion failures, not observed OOMs or
wrong output. Do not turn them into passing tests by increasing their budget.

Control94285 compiled and ran the same native test file from an isolated checkout
of frozen9c868dc0 source. All 488 extracted source hashes match the immutable
release manifest. It reproduces exactly the same four failures, including the
allocation requested/used/limit values, with five passes. This demonstrates these
failures predate the projection guard. Current native_table_validation,
qualified_column_identity and systemic_numeric_tests each pass all12 tests.
Formatting and whitespace checks pass. No heavy job remains active.

The control uses the same locked/offline features, thread count and safe-build
scope; its command and verified hash count are preserved in
aggregate-source-guard-native-control-provenance.json. It shares the target cache
but has a separate source directory; the main worktree was not reverted.

Immutable evidence: `docs/benchmarks/2026-09-08-aggregate-source-guard/`, with
current source snapshot, red/green logs, native control provenance, and hashes.
The only changed frozen input from9c868dc0 is src/physical/planner.rs; the new
integration test is additional. This checkpoint makes no new performance claim.
