# Admitted exact-key group index and atomic publication

The grouped partial-state component now includes a query-budgeted hash index.
It publishes a new group only after the complete aggregate row commits. A failed
or discarded preparation rolls back its key and state row, leaving the index
unchanged. This is a prerequisite for preserving state during a spill transition;
it remains **outside live query ingestion and spilling**.

## Contract

`physical/morsel_agg/group_rows.rs` stores `(hash, row index)` pairs in a pinned
hashbrown table. Lookup compares the complete canonical key and bound layout;
a collision never establishes equality. Index growth reserves old storage plus
the full new allocation bound before allocation. The bound is shared with the
existing raw-key index for the same pair/control layout. Actual capacity is
checked afterward. Allocator refusal and layout violations are terminal execution
errors; only typed query-budget denial is eligible for a later spill transition.

`PreparedGroup` holds exclusive access to the index, key rows and prepared state.
For a new key, index capacity is admitted first. Key/state storage is appended,
then every aggregate update is prepared in reusable scratch. Committing installs
the row and publishes the index entry without further admission. Any earlier
error or token drop removes the unpublished logical row and key. Retained buffer
capacity may grow on denial; existing groups and their aggregate values do not
change. Direct empty-row insertion and direct update by row number are private.

`StateRows` now supports rollback of a newly appended row even when preparation
fails arity, layout, type or payload checks. Existing-row preparation retains its
previous rollback semantics. The new indexed partial-merge API verifies the
complete source key and combines internal aggregate states: COUNT adds counts,
AVG preserves sums and weights, and decimal SUM retains its full coefficient.
Selected payloads retain their admitted ownership when the source is dropped.

Clearing removes index entries and logical rows, releases selected payloads and
retains reusable admitted capacities. A poisoned index is destroyed with its
lease. A future worker must invalidate any cached group IDs at each flush;
this component introduces no dictionary cache or generation tracking.

## Verification

Final focused gates pass **59 morsel tests plus 11 ownership tests**, with zero
failures or ignores. Four new tests are included in that total:

- 300 distinct keys forced into one hash collision chain remain distinct and
  accept repeated updates while the query pool is completely occupied.
- New-group payload denial, wrong arity and discarded preparation publish no
  index entry. Existing values survive denial, and clear/retry reuses capacity.
- 20,000 groups exercise table growth, verifying each actual allocation against
  its admitted bound and checking every stable row lookup.
- Indexed partial merges preserve COUNT 4, weighted AVG 25 and an exact decimal
  coefficient above 64 bits. A selected string survives source destruction;
  mismatched source keys and discarded fresh merges do not alter the target.

Commands run through `scripts/claude-safe-build.sh` with a 48G cgroup, one build
job, four Rayon threads and `lance,gpu` features. Formatting passes. Earlier
55- and 58-test runs are preserved but are not additional coverage.
[The evidence archive](benchmarks/2026-09-07-indexed-group-storage/) contains
14 SHA256-verified members, isolated source deltas, full changed files, logs,
commands, dirty revision provenance and 647 current source-input hashes.
Manifest SHA256: `792801811244e0275b3e16d0f65646b3ef2b2e22839f55e1d67d7882ae841390`.

No full-library/integration, end-to-end spill, dedicated cap/GPU or optimized
performance gate ran for this change. The two consuming-source replay failures
and the separate unchanged 256KiB query-completion case remain open and were not
rerun. No dependency or live execution routing changed.

## Next implementation boundary

Complete key/selected/fixed row serialization, validated decoding and versioned
file/layout identity. In-process Arc identity and query-local hashes are not a
durable format. The decoder must restore partial states rather than reapply
aggregates to finalized values. Then implement bounded flush, partition merge
and oversized-partition handling with pre-admitted IO scratch, and connect the
worker to the retained input and first-unapplied-row cursor. Arrow-to-state input
adapters must also avoid unadmitted string/list clones.

The unchanged one-shot input, actual-spill and low-budget gates must pass before
measuring this path. Follow the existing
[implementation contract](../.claude/epics/realistic-benchmarks-duckdb-leadership/updates/2026-09-07-fused-spill-implementation.md).
