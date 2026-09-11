# Admitted partition scheduler for completed spill runs

`physical/morsel_agg/partition_scheduler.rs` now composes complete run collections,
resumable partial merging, global split planning and transactional child publication.
It visits complete merged partitions through a borrowed callback. This is an
execution path over already-written partial runs; live query ingestion and Arrow
result emission are not connected yet.

Each pending task owns a root run collection or one child run inside ReservedVec.
The scheduler tries to merge all task runs. Typed query-memory denial or an explicit
group working limit discards that attempt's temporary state, retains the original
spill runs and plans a globally proven split. It admits two task slots before
writing children, then stores the larger child first so the smaller child is
processed next. Strict child row-count reduction plus smaller-first traversal
limits pending siblings; actual task storage is still admitted under the pool.
`ReservedVec::pop` moves a task out while retaining its backing reservation.

RunMerge now exposes a group-limit outcome only for a new key. Existing keys
continue merging at the limit, and EOF is still checked. The group count is an
optional working-set control, not a byte-accounting proof: query reservations
remain active independently. Non-memory errors propagate. If pressure remains
and the partition has no possible key split, the scheduler refuses by name rather
than repartitioning indefinitely. Oversized-row/frame admission can also refuse.

Only complete leaf partitions reach the output callback. The callback must admit
any retained Arrow/scalar copies and treat a later error as query failure; it is
not a result collector or SQL-output ownership certificate. Input-ingestion queues,
run accumulation/compaction during ingestion and live worker policy remain open.
The source spill files can be reread after an abandoned merge attempt; original
query input is not reexecuted by this storage scheduler.

Final gates pass **93 morsel plus16 ownership tests**, zero failures/ignores;
three new tests include:

- Seventeen groups across three runs, forced recursive splitting at two resident
  groups, NULL keys/all-NULL inputs, exact large decimal SUM and weighted AVG.
  Every group is emitted exactly once with an independent typed oracle.
- Duplicate updates at a one-group limit, zero unnecessary splits and cleanup
  when the output callback returns an error.
- **Actual256KiB query-pool pressure**,2048 COUNT groups in32 runs, with the explicit
  group limit set to usize::MAX. Admission-induced splitting occurs and every
  exact COUNT result is emitted once. All files and pool reservations clean up.

That last test is distinct from the unresolved live256KiB exact-decimal query
regression. It does not close that gate or establish parallel-worker coverage.
Formatting passes. The earlier92-test run is not additional final coverage.
Commands use48G containment, one build job, four Rayon threads and lance/gpu
features. [Evidence](benchmarks/2026-09-07-partition-scheduler/) contains12 verified
archive members and654 current source-input hashes, snapshots, isolated deltas,
commands and logs. Manifest SHA256:
`9038eaaa77cadaf0af4f660657af83784b537ccd2ad26bceb50829860de36b31`.

No dependencies or live planner/worker routing changed. No full-library/integration,
original end-to-end spill, dedicated cap/GPU or optimized performance gate ran.
Both consuming-source replay failures and the original256KiB query-completion
case remain unresolved and were not rerun. Next connect evaluated input and
applied-row cursors to prepared flushing, bound ingestion run accumulation, and
implement admitted result emission before enabling the route. Then run the
unchanged live correctness/resource gates and matched benchmarks. Continue the
existing [implementation contract](../.claude/epics/realistic-benchmarks-duckdb-leadership/updates/2026-09-07-fused-spill-implementation.md).
