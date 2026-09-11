# Legacy spill refusal stacks — 2026-09-11

The owned-debugger audit completed all six existing tests with the expected memory-refusal failures. These are reproduced failures, not passing spill/resource coverage. No query budget, assertion or engine source changed. Source/binary/fixture hashes verify after execution; the exact debug test binary is frozen in repository scratch.

| Existing test | Captured denial stacks |
|---|---:|
| join_spill_matches_in_memory | 3 |
| outer_join_spill_matches_in_memory | 3 |
| count_distinct_spill_matches_in_memory | 3 |
| semi_join_exists_spill_matches_in_memory | 1 |
| anti_join_not_exists_spill_matches_in_memory | 1 |
| filtered_semi_anti_join_spill_matches_in_memory | 1 |

All stacks and terminal refusal messages are retained in stack-summary.json and the original logs. A captured denial can be followed by successful fallback; its position alone does not prove it caused the final error. Review caller frames before attributing the terminal refusal. Outer/filtered tests stop at their first failing case; later branches remain uncovered.

Cgroup maximum17179869184bytes, peak2434600960bytes, swap maximum0. Events: `low 0; high 0; max 0; oom 0; oom_kill 0; oom_group_kill 0`.

[Protocol](legacy-spill-refusal-attribution-2026-09-11.md). [Verified archive](benchmarks/2026-09-11-legacy-refusal-traces/manifest.json). The engine source snapshot is linked to the batch-view archive; fixture and frozen-test-binary paths and SHA256 values are retained. This artifact supplies allocation-site evidence for review, not a completed resource repair.

## Source-grounded attribution review

Each8KiB semi/anti test records exactly one allocation-denial stack: `streaming_parquet_scan::admitted::Reader::open` line59 while `SpillableHashJoinExec::compute_build_decision` polls its build input. The schema-construction reservation starts at4096bytes per full source field plus512, before metadata-map charges;37,376bytes matches the base charge for nine fields. This identifies a schema admission envelope, not proof of that much allocated metadata or permission to remove its reservation. Audit projected/shared metadata ownership before replacing it.

Grouped inner/outer tests first hit two fixed Int64 output reservations, then `admitted_page_body::decode_page_body` line103. The fixed decoder already halves a refused output quantum; the later reservation is for the decoded page body. COUNT(DISTINCT) instead ends its captured denials at `AdmittedPageHeader::read` → `read_page_range`: the reader reserves `min(chunk_remaining,max_header_bytes)` before parsing, which can include substantial page-body bytes. Incremental bounded header parsing is a separate concrete investigation. These caller sequences identify distinct allocation paths; they do not establish that reducing one reservation will make the whole query complete.

All six failures remain failures. Nineteen archive files verify523source inputs and eight fixture hashes; the debug binary is frozen as `legacy_spill_refusal_tests_a38f93a08c67`. No OOM/max events occurred. Investigate schema reservation derivation, bounded header reads and whole-page progress independently, retaining typed refusal/cleanup and positive independent-oracle spill gates.

## Minimum-progress review after the header repair

The incremental-header candidate removes the speculative-window barrier in focused tests, but all six legacy spill tests still fail. COUNT(DISTINCT) now reports9512additional bytes with254089used of262144; the new caller site has not yet been traced. Do not infer it from the requested size alone.

A separate source review shows why cutting Reader::open's schema allowance alone is insufficient. `admitted_batch::AdmittedBatchReader::next_batch` reserves4096bytes per selected column for handoff metadata before pulling columns, plus a ReservedVec of ArrayRefs. `admitted_batch::finish` repeats the same per-column allowance for its separate handoff. For a nonempty two-column handoff, metadata alone consumes8192bytes, before any vector owner, reader, payload or upstream state. This is a lower bound on the current reservation model, not measured allocator consumption; it proves that row-quantum halving cannot make such a handoff fit an8KiB total budget. The exact selected-column count of each failing legacy query still needs its actual boundary evidence.

Reader::open also sizes its initial schema allowance from all source fields before determining the union of output, predicate and active runtime-filter columns. A future ownership repair must examine both schema construction and handoff metadata, accounting for their simultaneous lifetimes. Merely lowering the4096constants, increasing fixture budgets, dropping reservations, or calling a clean refusal a successful spill would not close the contract.

Next evidence should identify the new COUNT(DISTINCT) refusal site and measure a feasible minimum working set for the active scan/join pipeline, including borrowed schemas, retained batches and dictionaries. Positive spill coverage must use an independent typed oracle and prove an actual spill at a feasible budget; below-minimum cases should separately prove named refusal, cleanup and no replay. Existing legacy failures remain preserved until a deliberate resource-contract change has that evidence.
