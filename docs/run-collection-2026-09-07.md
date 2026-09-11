# Admitted run collection and prepared flush publication

`RunCollection` and `PreparedFlush` in `physical/morsel_agg/spill_files.rs` now
connect resident grouped rows to completed spill files. The collection uses
`ReservedVec<SpillRun>` so its own metadata growth is admitted, in addition to
file/path/reader ownership. The controller must prepare a flush before filling
resident state to the query working budget.

Preparation admits the next collection slot and creates its private run writer.
An exclusive collection borrow prevents intervening insertion from consuming
that slot. Flushing writes every resident group, finishes and verifies the file,
and inserts the completed run into the admitted slot. Only afterward does it
clear the source groups. Clearing releases selected payloads and logical rows;
key/state/index capacities remain admitted for reuse. This does not claim that
all resident buffer memory is returned to the pool.

Dropped preparations remove their unused/incomplete file. Write, finish or
publication failure preserves source groups, while the failed run owner removes
its file. Earlier published runs are unaffected. An empty flush creates no
collection entry. Source layout identity is checked before writing. Consuming
the run collection uses a reservation-owning iterator, preserving its metadata
charge while its allocation remains alive. Readers can retain files beyond the
collection/iterator lifetime through the existing shared owner.

Final gates: **78 morsel plus16 ownership tests pass**, zero failures/ignores;
two new tests exercise real files. A pre-admitted flush publishes two groups
with the query pool completely occupied, then restores both exact COUNT results.
The test checks file lifetime through collection consumption and a surviving
reader. Failure coverage includes pre-preparation admission denial, dropped
preparation, post-write file truncation before finish, and a real read-only
writer failure after an earlier run was published. Source groups and prior runs
remain intact; failed files and reservations clean up.

Formatting passes. Commands used the required48G cgroup, one build job, four
Rayon threads and lance/gpu features. [Evidence](benchmarks/2026-09-07-run-collection/)
contains six SHA256-verified members and651 current source-input hashes, source,
isolated delta, commands and logs. Manifest SHA256:
`5ba45f512cd7b85ac7cf0e10b2bb2720f32fa48bbbecc950b2e771a9e7ccd273`.

No dependency or live query routing changed. No full-library/integration,
end-to-end spill, dedicated cap/GPU or performance gate ran. Both consuming-source
replay failures and the unchanged256KiB query-completion case remain unresolved
and were not rerun. Next implement bounded partition processing/merge/repartition,
parent-directory ownership and retained-input worker/cursor integration. The
controller must provision flush metadata/IO working space before memory pressure,
handle oversized rows/partitions, and avoid unbounded run accumulation. This
collection owns memory but is not itself that bounded merge scheduler. Continue
the existing [implementation contract](../.claude/epics/realistic-benchmarks-duckdb-leadership/updates/2026-09-07-fused-spill-implementation.md).
