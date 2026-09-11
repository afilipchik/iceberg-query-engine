# COUNT(DISTINCT) refusal after incremental headers — 2026-09-11

The owned-debugger audit completed the existing COUNT(DISTINCT) test with its expected memory-refusal failure. These are reproduced failures, not passing spill/resource coverage. No query budget, assertion or engine source changed. Source/binary/fixture hashes verify after execution; the exact debug test binary is frozen in repository scratch.

| Existing test | Captured denial stacks |
|---|---:|
| count_distinct_spill_matches_in_memory | 3 |

All stacks and terminal refusal messages are retained in stack-summary.json and the original logs. A captured denial can be followed by successful fallback; its position alone does not prove it caused the final error. Review caller frames before attributing the terminal refusal. This focused diagnostic does not rerun the other five legacy failures.

Cgroup maximum17179869184bytes, peak2963824640bytes, swap maximum0. Events: `low 0; high 0; max 0; oom 0; oom_kill 0; oom_group_kill 0`.

[Protocol](legacy-spill-refusal-attribution-2026-09-11.md). [Verified archive](benchmarks/2026-09-11-incremental-header-refusal-traces/manifest.json). The engine source snapshot is linked to the incremental-header archive; fixture and frozen-test-binary paths and SHA256 values are retained. This artifact supplies allocation-site evidence for review, not a completed resource repair.

## Caller attribution

The first two captured denials are fixed Int64 output reservations handled by the existing output-quantum reduction. The third is `AdmittedPageHeader::read_body` → `read_page_range`, requiring the full encoded page body while SpillableHashJoin::compute_build_decision polls its build input. It requests9512bytes including the buffer owner with254089used of262144. It is no longer the speculative header window. All13archive files verify524source inputs and eight fixtures; the exact debug binary is frozen as incremental_header_refusal_tests_9b0b7ebbd05b.

The join computes its spill threshold from accumulated estimated payload plus index size after a child batch arrives. Its source can refuse the memory needed to produce that next batch before the threshold decision executes. This identifies the admission/retention boundary that requires investigation; it does not establish the complete live allocation composition. Preserve terminal stream semantics: spilling retained input cannot justify replaying an already-failed source. Minimum working-memory requirements and retained ownership must be addressed together.


A later source526 [ownership ledger](first-batch-refusal-ledger-2026-09-11.md) now
shows zero retained join build batches at the same encoded-body refusal boundary.
For that reproduction, investigate first-batch reader working space; join retention
is ruled out. The ledger remains partial and does not identify every live allocation.
