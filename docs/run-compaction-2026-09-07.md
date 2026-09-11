# Bounded partial-run accumulation and transactional compaction

RunCollection now compacts completed partial runs through PartitionScheduler into
one completed run. Original file owners remain retained until the output finishes;
admission, merge or write failure cleans temporary state and leaves originals
available. Sharing the originals admits a separate metadata collection and clones
file-owner references, not payloads. Publication reuses the existing first slot,
then releases replaced owners. This is query-local publication, not durable commit.

`prepare_flush_bounded` compacts before preparing another flush when the run count
reaches its configured maximum (at least two). Call it between batches, before
filling resident state. The subsequent successful flush stays within that count.
All scheduler, frame and collection allocations still require query admission;
the count limit does not guarantee that an arbitrary working set fits. The
lower-level prepare method remains available. Live ingestion does not call this
new method yet.

Two new regressions pass. Twelve rounds repeatedly compact seven groups including
a NULL key, with different partial weights each round. Independent expected values
are COUNT 78, AVG 6500/78 and exact decimal coefficient 650*((1<<80)+17).
A separate failure test checks pool denial and a truncated output before finish:
original paths remain readable, temporary files disappear, and retry succeeds.
Both tests verify final file and reservation cleanup.

Final validation: **95 morsel plus 16 ownership tests passed**, zero failures or
ignores. Formatting passes. Commands use the required 48G systemd containment,
one build job, four Rayon threads and lance/gpu features. The initial morsel run
is preserved but is not additional coverage.
[Evidence](benchmarks/2026-09-07-run-compaction/) contains seven verified members,
654 source-input hashes, the source snapshot, isolated delta, commands and logs.
Manifest SHA256: `42844f6fee6de6365f6173639cb0b0c6d7a606a5d477ec3eefe7d2003d1e9d26`.

No live routing or dependencies changed. No performance measurement or full
integration/cap gate ran. Both consuming-source replay failures and the original
256KiB live decimal completion failure remain open and were not rerun. Next connect
retained evaluated inputs and applied-row cursors to prepared flushing, provide
admitted result emission, and enforce worker working sets before enabling the
route. The upstream rationale is recorded in the
[local DuckDB/ClickHouse comparison](local-engine-source-comparison-2026-09-07.md).
