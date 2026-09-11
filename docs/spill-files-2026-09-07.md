# Owned spill files and completed-run publication

`physical/morsel_agg/spill_files.rs` adds private query-local run writers,
completed-run capabilities and readers over the checked row frames. An incomplete
writer cannot produce a reader. `finish` checks successful writes and expected
file length before transferring ownership of a completed run. This is API-level
publication of ephemeral query spill, not a durable catalog, atomic rename or
crash-recovery protocol. No fsync is required or claimed.

The owner admits its fixed metadata and path allocation from the bound query
pool before copying a path or creating a file. Names use stack-formatted UUIDs;
create_new never adopts an existing file. Only a successfully created file is
removed on drop. Failed/incomplete writers close their handle before dropping
the owner. Completed readers retain the shared file owner, and the last reader
or run capability removes the file. Cleanup failures are logged; the outer query
must keep its spill directory alive and remains responsible for directory-level
cleanup. The owner does not delete directories or foreign files.

Completed runs retain their expected frame count, byte length and query-local
run/layout identity. Reader creation admits reader metadata and checks file length.
Each reader opens a separate file handle, avoiding Unix File::try_clone shared
seek offsets. Reads check every frame and reject early EOF or extra bytes. The
last payload is not returned until the declared-count EOF check succeeds. Empty
runs are valid. A budget-denied frame read retains its count and rewinds for retry;
other IO/validation failures poison the reader.

The writer does not clear input groups. A future flush controller must retain
source state until finish succeeds, then account for the published run in an
admitted collection before releasing state. File handles, paths and reader metadata
are owned here; run collections, parent-directory ownership and bounded merge
scheduling remain separate integration requirements.

Final gates: **76 morsel plus16 ownership tests pass**, zero failures/ignores;
four new tests use real files in the repository TMPDIR under the required cgroup.
They verify independent interleaved readers, last-reader file lifetime, removal
of incomplete runs, a real read-only-descriptor write failure, refusal to finish
a failed writer, preservation of an unrelated sentinel file, pre-create admission
denial, complete-suffix loss, extra bytes both before and after reader creation,
empty runs, read admission retry and reader-construction admission. All test
owners release their pool reservations, and expected files are removed.

Formatting passes. Commands use48G containment, one build job, four Rayon threads
and lance/gpu features. No dependencies changed. [Evidence](benchmarks/2026-09-07-spill-files/)
contains nine SHA256-verified members,651 current source-input hashes, source,
isolated deltas, commands and logs. Manifest SHA256:
`a33e1ac5490630b531e3634eab16d49d3ddb6d25d24cf83ee9afe1729752397f`.

No live query routing changed. No full-library/integration, end-to-end spill,
dedicated cap/GPU or performance gate ran. Both consuming-source replay failures
and the unchanged256KiB query-completion case remain unresolved and were not
rerun. Next compose bounded flush/run ownership, partition merge/repartition and
retained-input worker/cursor integration. The row/file components do not yet
eliminate live source replay. Continue the existing
[implementation contract](../.claude/epics/realistic-benchmarks-duckdb-leadership/updates/2026-09-07-fused-spill-implementation.md).
