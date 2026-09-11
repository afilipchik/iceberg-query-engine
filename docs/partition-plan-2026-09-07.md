# Global multi-run split progress and reader reuse

`PartitionPlan` in `physical/morsel_agg/repartition.rs` scans every run belonging
to a partition with a shared representative key and global child counts. It can
find a split even when each individual run contains only one key, provided those
keys differ across runs. The shared scan also powers the existing single-run
plan, with full frame/count/EOF checks and query-owned temporary memory.

A prepared partition borrows all source runs, holds two output writers, one frame
buffer and one reusable reader slot. It applies one canonical-key split across
all sources and returns exactly two completed child runs, verifying nonzero
counts against the global plan. Failure drops partial children and preserves
all source ownership. The source-run slice must itself come from an admitted
collection in the eventual scheduler; this primitive does not allocate or own
an unbounded source list.

`RunReader::reset` reuses its existing query reservation to open the next source
only after the current run has completed. It checks layout identity and trailing
EOF, opens a separate handle and verifies the new expected file length before
replacing the previous owner/cursor. Skipping unread frames is refused. IO or
integrity failure poisons the reader; a successful transition requires no new
query-pool reservation. This avoids allocating a new reader slot while the
prepared repartition's working budget is full.

Final gates pass **90 morsel plus16 ownership tests**, zero failures/ignores;
two new tests cover an empty run followed by independently unsplittable runs
containing three copies of key0 and five copies of key1. The global scan proves
3/5 progress, prepared repartition succeeds under a full pool, and merging its
two children yields exact COUNT3 and COUNT5 results. Late corruption in the last
source after planning removes partial children without removing sources. Reader
reuse rejects an unread run and appended trailing data, but succeeds across
completed runs with no free pool memory.

The first formatting attempt found a missing closing brace, corrected before
compilation/testing. Final formatting passes. The earlier90-test run is not
additional final coverage. Commands use48G containment, one build job, four Rayon
threads and lance/gpu features. [Evidence](benchmarks/2026-09-07-partition-plan/)
contains nine SHA256-verified members and653 current source-input hashes, source,
isolated deltas, commands and logs. Manifest SHA256:
`eff611c6636967787e77a3f40bd0a3931f369496611fdb2476e96f4fe6c8807a`.

No dependencies or live query routing changed. No full-library/integration,
end-to-end spill, dedicated cap/GPU or performance gate ran. Both consuming-source
replay failures and the unchanged256KiB query-completion case remain unresolved
and were not rerun. The complete scheduler still needs admitted pending-task
storage, pre-reserved working space, merge-versus-split decisions, oversized-group
handling and bounded result emission. Global strict split progress is implemented;
that does not yet prove the scheduler or live query completes. Continue the
existing [implementation contract](../.claude/epics/realistic-benchmarks-duckdb-leadership/updates/2026-09-07-fused-spill-implementation.md).
