# Spill controller over retained evaluated batches

`physical/morsel_agg/ingestion_controller.rs` now owns resident grouped rows,
key/state scratch, a prepared spill writer and a bounded completed-run collection.
It admits the writer and run slot before filling resident state and keeps that
writer across input batches. No file is opened for every batch. On typed admission
pressure it flushes committed partial rows, releases all old resident capacities,
compacts runs when needed, prepares the next writer and resumes the same borrowed
batch at its exact first unapplied row. It does not evaluate expressions or access
an upstream provider.

The controller also accepts a positive group working limit. Ingestion stops before
a new group exceeds it; duplicates still update at the limit. This is a working-set
control, not byte-budget proof. All actual growth remains subject to the query
pool. A row that fails against empty state returns its typed MemoryLimit instead
of flushing nothing and looping. Other failures are terminal; a poisoned controller
cannot accept more input or finalize a partial result.

PreparedFlush can transfer its admitted writer into the controller; the collection
retains slot capacity and publication checks it. Finishing a flush preserves the
existing complete-file-before-state-clear contract. Admission for the next working
set happens after old key, state and selected-value scratch drops. This matters:
clearing logical rows alone would retain the capacities that caused pressure.

With no spill, finish visits resident groups directly and deletes the unused empty
spill file. Otherwise it flushes the final resident suffix, releases ingestion
scratch and runs the partition scheduler. Output is still a borrowed callback:
retained result copies need separate admission, and any later failure invalidates
all partial query output. Global empty-aggregate output belongs to the enclosing
operator. Statistics count ingestion flushes, rows and completed ingestion-run
bytes; they do not include physical IO from intermediate compaction/repartition.
The split count covers the final scheduler, not prior compactions.

Three new tests pass:

- Four retained batches, each with1000 groups including NULL, COUNT, full-i128
  decimal SUM and AVG. Under a256KiB pool, both group-limit32 and usize::MAX runs
  complete through repeated flush/compaction. The latter has no finite group limit,
  so its intermediate flushes are caused by real pool denial. The caller admits
  each retained evaluated batch. Independent outputs are COUNT4, SUM10*((1<<80)+17)
  and AVG25 for every group, emitted once. Files and pool owners clean up.
- An oversized selected string causes one flush, then a named empty-state memory
  refusal; further ingestion/finalization is rejected. This isolates controller
  state pressure, not the caller's oversized input construction.
- Duplicate rows at a one-group limit finish without spilling; output callback
  failure cleans up owners and files.

Final gate: **104 aggregate component tests passed**, zero failures or ignores;
formatting passes. The initial test-fixture borrow compilation error is preserved.
[Evidence](benchmarks/2026-09-07-ingestion-controller/) contains 11 verified members and
656 current source-input hashes. Manifest SHA256:
`1e3c2fe2038fa59df447e4225318dc3f78a61e06bfa7fb1d0b7e696c0915bf1e`.

This is now an ingestion-through-spill-and-merge controller, but live planner and
parallel worker routing remain unchanged. Admitted Arrow output, worker working-set
policy and binding into the production operator remain next. The two original
consuming-source replay regressions and the original256KiB live decimal query
were not rerun and remain open. This component's256KiB test does not close them.
No full integration/cap/GPU or performance gate ran; no speedup is claimed.
