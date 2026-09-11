# Correlated membership repair: feature gates

Validation45945 terminal1 completes both disjoint and experimental partial modes.
Each passes1,092 library tests/11ignored and116 contract integrations. Native/IPC
remains58pass/2fail default and57pass/3fail partial; spill/numeric/decimal totals
28pass/6fail each, including all13 systemic numeric and7 focused decimal/input
checks. Legacy spill remains8pass/6fail. No failure names were added or removed
versus the frozen0c867c5c validation. Unchanged names do not prove identical causes.

Commands use locked/offline lance/gpu features, no-fail-fast, Rayon16, repository
TMPDIR and48GiB/one-build-job containment. All520 source inputs match the archived
repair snapshot before and after validation. No optimized binary or performance
measurement belongs to this newer source yet. The existing native/spill failures
and full provider/residency/resource/concurrency acceptance remain open.

The repair uses a true semi-join for correlated aggregate membership reduction.
[The executed SUM/COUNT duplicate oracle](correlated-reduction-proof-audit-2026-09-11.md)
is green. Source inspection also identifies empty scalar COUNT restoration as a
separate untested edge case: an unmatched grouped result becomes NULL after a
LEFT join. A targeted zero-versus-NULL probe must precede performance work.

[Validation archive](benchmarks/2026-09-11-correlated-reduction-validation/manifest.json).
