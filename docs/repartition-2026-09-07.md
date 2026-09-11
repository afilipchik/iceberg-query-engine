# Exact-key two-way spill repartition

`physical/morsel_agg/repartition.rs` adds a prepared two-way copy of a completed
spill run. It holds one admitted frame buffer, one reader and two prepared output
writers. It copies verified row payloads without decoding selected strings/lists,
finalizing aggregates or retaining all rows. The source is borrowed until finish,
so failure cannot consume its ownership. This is a repartition primitive, not the
complete bounded partition scheduler or a live query route.

Routing uses a discriminating bit of the canonical key bytes, with a presence
bit before every byte. This distinguishes a key from its longer prefix extension.
`KeySplit::first_difference` returns a separating bit for two unequal byte keys,
independent of hash collisions. Equal canonical keys always take the same side.
No multiplication by key length is needed to represent the bit position. A future
scheduler must choose representatives from its current partition and verify split
progress; this method alone does not select balanced splits or guarantee scheduler
termination. Empty child runs are representable and must be discarded or skipped
by that scheduler.

Frame copying checks source/destination layout identity and gives each child a
new run identity, ordinal sequence and checksums. The row payload stays unchanged.
The reader checks the complete source count/EOF, and repartition independently
counts copied rows. Both children must finish before they are returned. On any
error the child owners remove their files while the borrowed source remains.
A retry may reread the spill file; it never reexecutes original query input.
Partial-state semantic validation still belongs to row restoration; copying does
not reinterpret or certify arbitrary encoded aggregate values.

Final gates pass **86 morsel plus16 ownership tests**, zero failures/ignores;
three new tests cover:

- Eight partial rows split under a completely occupied query pool after frame
  and writer preparation. Before/after payload multisets match byte-for-byte,
  each child obeys its key side, and merging both children gives four groups
  with COUNT2 and the independently expected AVG values.
- Read admission denial and late source checksum corruption after partial child
  output. Child files clean up, the source remains, and retry after restoring
  the corrupted test byte preserves all eight rows.
- Empty/prefix byte keys, high-bit differences, symmetric split selection and
  equal-key refusal, without a hash-based uniqueness assumption.

Formatting passes. Commands use48G containment, one build job, four Rayon threads
and lance/gpu features. The earlier86-test run is not additional coverage.
[Evidence](benchmarks/2026-09-07-repartition/) contains12 SHA256-verified members
and653 current source-input hashes, snapshots, isolated deltas, commands and logs.
Manifest SHA256:
`b175bb5c4d3d6cca4ca6af46f3855989a3ed0b0e2a769c0b8b5befe8b06b5dea`.

No dependencies or live query routing changed. No full-library/integration,
end-to-end spill, dedicated cap/GPU or performance gate ran. Both consuming-source
replay failures and the unchanged256KiB query-completion case remain unresolved
and were not rerun. Next build the bounded partition scheduler with explicit split
progress, oversized-row/group handling, admitted run/task metadata and pre-reserved
flush/merge working space; then connect retained-input workers. Continue the
existing [implementation contract](../.claude/epics/realistic-benchmarks-duckdb-leadership/updates/2026-09-07-fused-spill-implementation.md).
