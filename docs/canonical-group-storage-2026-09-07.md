# Canonical keys and atomic grouped storage

`physical/morsel_agg/key_rows.rs` provides canonical key encoding and flat admitted
storage. `group_rows.rs` couples it to the partial aggregate rows so a failed
state allocation cannot publish an orphan key. Both remain **outside the live
query path**. Hash indexing, complete state serialization, bounded spill/merge
and worker integration remain required to eliminate input replay.

## Key contract

The bound key layout owns its type metadata and query budget. Keys encode each
field's validity, fixed-width little-endian bits, and explicit string/list
lengths. NULL differs from an empty string or list, and composite field
boundaries are preserved. Floating signed zeros share one representation; all
NaN payloads/signs share another. This normalization applies recursively in
lists. Decimal coefficients retain all128 bits at their bound scale, and
timestamp ticks retain their bound unit/timezone domain.

Key equality requires both complete byte equality and the same bound layout.
Hashes only select candidate groups; they never prove equality. Hashing is
deterministic across workers in this executable/query, not a cross-version
durable-file promise. Layout Arc identity is an in-process guard, not serialized
file identity. A future reader must verify file/layout identity and validate
encoded keys; this change does not add a decoder.

`KeyWorkspace` reuses admitted encoding scratch. Any failed encoding invalidates
its key view, preventing accidental reuse of the previous row's key. `KeyRows`
stores payload bytes and end offsets in two flat admitted vectors rather than
allocating a vector per key. Capacity growth may be retained on denial, but
logical row count and existing key bytes remain unchanged. Truncation supports
rollback or reuse after a completed flush.

Dictionary/large/view encodings are not accepted directly by this binding.
The caller must normalize logical values before binding while retaining the
required logical domain. Unsupported types decline binding before consumption;
no new fallback routing is installed by this component.

## Key/state coupling

`GroupLayout` binds keys and aggregate slots to the same query budget.
`GroupRows::append_empty` rolls back a successfully appended key if state
admission then fails. A hash-table index may be published only after this method
succeeds; the future index must separately admit its metadata. The store itself
does not perform hash lookup or deduplication.

Merge compares the complete canonical keys before preparing aggregate changes.
This adds a guard against combining different groups even if an index or caller
selects the wrong candidate. Clearing the store drops selected payload leases
while retaining admitted flat capacities for reuse. It is only valid after all
needed state has been flushed or consumed; it is not a spill implementation.

## Verification

Final gates pass **55 morsel tests and 11 ownership tests**, with zero failures
or ignores. Seven tests are new; earlier selected runs are not additional
coverage. Formatting passes. The new tests cover:

- Composite string boundaries, NULL/empty distinctions, floating zero/NaN
  equivalence, explicit encoded bytes, and equality despite an injected hash collision.
- Exact large decimal coefficients, unsigned/signed integer extremes,
  timestamp ticks, nested Float32 normalization and bound-layout separation.
- Partial payload/offset growth, failed-workspace invalidation, rollback/retry,
  zero-column global keys and unsupported dictionary binding.
- State admission failing after key insertion, index alignment on retry,
  different-key merge refusal and equal-key merge with no available pool capacity.
- Selected-payload release on clear and reuse of existing capacity with a full pool.

The first fixture build failed because an array repetition required Copy for
ScalarValue; inline const initialization corrected it. That log is retained.
[The archive](benchmarks/2026-09-07-canonical-group-storage/) preserves code,
isolated patches, commands, logs and647 source-input hashes.

No full library/integration, end-to-end spill, dedicated cap/GPU or optimized
benchmark gate was rerun. The two consuming-source replay regressions and the
separate256KiB query-completion case remain open and were not rerun here.

Next add admitted exact-key hash indexing and complete state-row serialization,
including selected values and read-time validation. Then implement bounded file
flush/merge, oversized-partition handling and worker/cursor integration. Run the
unchanged end-to-end resource/correctness gates before measuring the new path.
Continue the existing
[implementation contract](../.claude/epics/realistic-benchmarks-duckdb-leadership/updates/2026-09-07-fused-spill-implementation.md).
