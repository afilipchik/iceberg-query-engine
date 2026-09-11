# Spill-directory lifetime ownership

`RunDirectory` in `physical/morsel_agg/spill_files.rs` now owns the private parent
of active run files. Its path/owner metadata is admitted from the bound query
pool before allocation and directory creation. It creates exactly one UUID-named
directory under a configured existing scratch root, never adopting an existing
one. File and directory paths share the checked, admitted path-construction helper.

Production `RunWriter::create` and `RunCollection::prepare_flush` now require an
owned directory reference instead of a borrowed Path. File owners retain that
reference, and reader ownership transitively retains it. Directory/layout identity
must match before a writer creates a file. The last file owner removes its file
before releasing the directory; the last directory reference removes the empty
directory. Dropping a controller handle cannot delete a directory with active
readers. The configured scratch root is still external and must remain available.

Cleanup uses remove_dir, never recursive deletion of unknown contents. A nonempty
or otherwise unremovable directory produces a warning and is left intact. Failed
directory creation neither adopts nor deletes an existing/foreign path. Small
fixed Arc allocations follow the existing pre-admitted owner contract. No new
dependency is introduced.

Final gates: **83 morsel plus16 ownership tests pass**, zero failures/ignores;
two new real-filesystem tests verify parent lifetime after controller/run handles
drop, successful reads through that lifetime, final file/directory cleanup,
pre-create admission denial, creation failure under a foreign file, and layout
mismatch without file creation or additional retained charges. Existing file,
flush and merge tests now create owned directories through test helpers and all
continue to pass. Fixtures leave their query-pool reservations released.

Formatting passes. Commands use48G containment, one build job, four Rayon threads
and lance/gpu features. [Evidence](benchmarks/2026-09-07-spill-directory/) contains
eight SHA256-verified members and652 current source-input hashes, source snapshots,
isolated deltas, commands and logs. Manifest SHA256:
`824469289821e7689a2939b03a8e2508c78889c91cfe1dc1229af6d2672b9517`.

No live query routing changed. No full-library/integration, end-to-end spill,
dedicated cap/GPU or performance gate ran. Both consuming-source replay failures
and the unchanged256KiB query-completion case remain unresolved and were not
rerun. Next implement bounded partition scheduling/repartition around prepared
flush and resumable merge, then connect retained-input workers. The live query
path still does not use these components. Continue the existing
[implementation contract](../.claude/epics/realistic-benchmarks-duckdb-leadership/updates/2026-09-07-fused-spill-implementation.md).
