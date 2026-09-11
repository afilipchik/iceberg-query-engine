# Comparator scaling: implemented exact streaming and remaining oracle reuse

The first increment is implemented in `scripts/benchmark/compare.py`: strict-schema, zero-tolerance bag comparisons use complete native Arrow equality across aligned slices, then restart the existing exact comparator on any inequality or unsupported native encoding. Buffered readers retain current batches; equality checks cover every row and column and require both streams to end. No sampling or hash-only acceptance is used.

The final harness gate passed **75 tests with zero skips**, including eight new regressions, under a 4 GB cap. Synthetic scaling through **60 million rows / 7.293 GB combined IPC input** completed its comparison in **3.161 seconds**, with approximately **267 MiB process peak RSS** and **14.1 MB Arrow peak allocation** (both include fixture generation/imports). This is aligned synthetic validation, not a measured speedup or provider latency claim. Source snapshots, scripts, exact logs/results, regeneration instructions and checksums are in [the comparator evidence archive](benchmarks/2026-09-06-comparator-aligned/README.md).

A subsequent strict-schema exact reordered-bag backend is now implemented in
`scripts/benchmark/exact_bag.py`. It stages a narrow, checked Arrow domain to
Parquet, then compares full typed keys and signed multiplicities with pinned
DuckDB, one thread, 512 MiB buffer limit and a 64 GiB spill ceiling. Invalid
input and resource failures remain explicit; unsupported domains retain SQLite.
The integrated harness passes **83 tests with zero skips** (both optional Lance
and exact-bag spill tests enabled). An independent full lineitem check passes
59,986,052 rows per side in 118.600 seconds including staging, with actual disk
spilling and about 1.62 GiB process RSS inside a 4 GiB cap. The direct Arrow-reader
prototypes were rejected after shutdown crashes following OOM. See
[exact reordered-bag evidence](benchmarks/2026-09-06-comparator-exact-bag/README.md).
This changes untimed validation, not query execution or benchmark tolerances.

**Remaining:** reusable oracle indexes, selected LIMIT/tie-group indexing, and
faster fallback for domains outside the new narrow exact backend are proposals only. The current fast path does not accelerate tolerant, ordered or LIMIT comparisons. Existing fallback cannot decode non-microsecond-aligned timestamp(ns) into Python datetimes without pandas; that conversion now returns an explicit validation error rather than escaping. Native equal timestamp(ns) streams remain exact. No truncation was introduced.

The original read-only audit and next-step design follow. Its complexity analysis applies to the fallback paths that remain. No engine source changed in this increment.

## Remaining fallback costs

`compare.rows` slices batches into 4,096 rows, converts every column to Python objects, and visits every row. `exact` turns every integer/decimal into an exact rational with decimal strings; `encoded` constructs JSON for each row. `compare_files` then performs one SQLite indexed insert/upsert per row, committing every 4,096 rows. The 8 MiB SQLite cache and disk temp mode bound SQLite's cache but do not make this CPU/I/O work cheap.

For exact bags of N actual and M expected rows with D distinct rows, cost is O((N+M) * row-encoding cost + (N+M) log D) indexed work and O(D * serialized-row-width) disk. JSON/rational/hex expansion can greatly exceed Arrow payload size. Each comparison rebuilds expected counts. Provider conversion compares both the engine and reference against the same source in separate databases, so ~60M lineitem rows mean ~240M Python row encodings/indexed updates per provider (two complete comparisons, each reading actual + expected), in addition to conversion/readback. This is an operation count estimate, not a measurement.

`run.run` builds one oracle file per query/session but reparses/reindexes it for three calibration validations and both sides of every measured pair: 3 + 2*S comparisons. With S=3 that is nine oracle preparations. `compare_slice_files` indexes **every unlimited oracle row**, including groups whose required rank interval is zero. An actual LIMIT of ten therefore does not bound preprocessing. Legacy `limit_ties` additionally scans the oracle once to find/validate its cutoff, then rereads it for comparison. All of this is outside recorded engine query time, but it extends runs and perturbs cache/disk state before subsequent samples.

For floating buckets, `_match`/`matching` retain bounded augmenting-path matching, with potentially cubic work in bucket size and extra Python scans. `max_float_bucket=2000` is a correctness-preserving refusal threshold, not permission to approximate large ambiguous groups.

## Increment 1: implemented exact typed aligned-stream fast path

Implemented and validated as described above. The following requirements explain its proof boundary; the resource-fault and broader reordered-bag cases below remain future work.

- Restrict initially to `policy='bag'`, `rel_tol=abs_tol=0`, and **identical complete Arrow schemas**. Provider conversion already checks identical column names/types before comparison; keep that gate. Do not apply the fast acceptance rule to permissive schema-family matching or to LIMIT/hidden-key semantics.
- Read two IPC streams together, align their batches by slicing to the shorter remaining segment, and compare corresponding Arrow arrays with native `Array.equals`/`RecordBatch.equals` after the exact-schema gate. Never call `to_pylist`, build JSON rows, or put an equal stream in SQLite. Check both streams end together and preserve exact row counts, including zero-column and empty batches.
- If every aligned segment is equal, the ordered sequences are exactly equal, which proves exact bag equality including duplicate multiplicity. This is a complete proof, not an assumption that providers preserve order.
- Any segment inequality, unsupported native equality domain, dictionary-codebook difference, or incompatible batch/type representation **declines the fast path** and restarts the existing exact comparator on the original immutable files. It must not report wrong results just because providers reorder rows. NaNs that native equality treats as unequal also decline; this cannot cause a false positive. Never use Arrow's approximate equality option.
- Preserve current logical float semantics (all NaNs equal, signed zeros equal) in fallback. Raw buffer identity/checksums are insufficient: bitmap padding, offsets, string views, dictionary codebooks and unused capacity need not match.
- Working memory is a bounded number of Arrow batch views; the original buffers remain pinned until each segment is consumed. Ensure IPC compression/decode size is included in the external process budget; a row-count chunk is not a universal byte bound for arbitrary strings/dictionaries. Fail by name if allocation/disk limits are unavailable.
- Report comparator strategy, rows/bytes processed, fast-path decline reason and fallback use. This likely eliminates Python/SQLite costs for unchanged-order provider roundtrips, but measure that hypothesis rather than promise it.

No schema coercion, provider data re-encoding, replacement of the independent source, or dropping of preserved actual/reference Arrow artifacts is needed.

## Increment 2: proposed query/session oracle reuse

Introduce an explicit owned prepared-oracle object, not a global filename-keyed cache:

`prepare_oracle(expected_path, policy_contract, scratch_budget) -> PreparedOracle`

`compare_to_oracle(actual_path, PreparedOracle) -> validation_record`

- `run.run` constructs it once after a successful oracle execution and uses it for calibration and all paired samples in that query/session. `providers.convert` can reuse one prepared exact source for its engine and reference comparison when the aligned-stream path declines.
- Store a completed immutable SQLite oracle with canonical full typed keys and multiplicity, schema, row count, order-group/rank metadata and projected visible-column mapping. Build privately; publish only after the complete input and order validation succeed. An interrupted or failed preparation is never reusable.
- Key persistent identity by full expected-file digest, Arrow schema, comparator/canonical-encoding version, policy, exact order specifications, output-column mapping, LIMIT/OFFSET contract, and any floating normalization/matching settings. Retain independent-reference provenance (SQL/data hashes, extension version/settings). Float tolerances must not be silently inherited from another query.
- For actual comparisons, use a fresh small actual-count database/overlay attached to the immutable expected database. Verify exact count equality, or slice membership with the required group cardinality. Never decrement expected counts in a shared oracle: concurrent comparisons and a failed first sample must not contaminate later samples.
- For in-run reuse, own the completed oracle artifact in a private run directory and validate it at preparation; include hashes in the run's final integrity gate. Do not recompute its full hash at every sample just to avoid Python decoding—that still adds a full I/O pass. Cross-run reuse requires validating the file digest before reuse; path/mtime alone is not evidence of identity. Cache hashes establish artifact identity only; full canonical values remain the answer proof.
- Disk admission must cover the immutable oracle plus concurrent actual overlays, input IPC files, SQLite journal/temp files, and interrupted-build cleanup. Keep concurrency explicit and bounded. Current 8 MiB cache settings are useful but do not impose a disk quota or bound Python matching work.

## LIMIT/ties: avoid indexing irrelevant rows

Within oracle preparation, validate order and count over the complete unlimited oracle once. Build rank-group boundaries. Index visible row multiplicities only for groups intersecting `[offset, offset+limit)`; include **all** rows in any intersecting boundary tie, not merely the first arbitrary LIMIT result. If the current group might intersect an OFFSET boundary, its prefix cannot be discarded: either spool the current group until its rank extent is known or use two passes (first ranks, then selected groups). A two-pass sequential scan once is still much cheaper than indexed insertion of all oracle rows on every comparison.

For hidden ORDER BY keys, preserve the existing group-ID assignment of actual positions and explicit visible-column mapping. Duplicates can exist within and across groups. Interior groups require their full multiplicity; boundary groups require the exact permitted number of members, each with available multiplicity. An unordered LIMIT is one full membership domain, so all expected row counts remain necessary. If an enormous tie group is actually eligible, exact checking still has to process it; no top-K shortcut proves membership.

For floating comparisons, keep full capacitated matching in eligible exact-key/order-group buckets. Do not greedily cancel apparently exact float pairs before tolerant matching: tolerance edges are non-transitive, so that can destroy a valid full assignment. Reuse encoded expected values and group counts, not assignments from a previous actual result.

## Later exact fallback improvement, only if needed

For reordered SF10 tables, an immutable expected index still leaves expensive Python JSON encoding/index updates. A versioned compiled Arrow-to-canonical-key stream plus external sort/merge of **full keys and run counts** can replace random B-tree insertions. Full-key comparison must resolve every collision; hashes may partition or locate keys but cannot be the final comparison. Canonicalization must preserve signed/unsigned full domains, i128 coefficients/scales, NULLs, string/binary lengths, decoded dictionaries, temporal precision/timezone, NaN/signed-zero policy, and exact numeric equivalence where the current contract allows it. Avoid relying solely on the query engine under test for this comparator. This is a separate implementation/proof task, not a prerequisite for increments 1 and 2.

## Required tests and capped measurements

Use the existing comparator tests as the compatibility gate, then add:

1. Equal streams with different batch boundaries, empty batches, duplicate rows, NULLs, zero-column rows and empty results. Assert fast acceptance consumes every row and opens no SQLite index.
2. One changed value or duplicate count in the final chunk; reordered rows that are equal as a bag; different dictionary mappings/null dictionary values; different slice offsets/backing capacity. Fast decline must invoke exact fallback and preserve correct outcomes.
3. Exact i64/u64 boundaries, decimal precision 38 and differing scale/schema, timestamps with adjacent nanoseconds/timezones, binary vs text, Unicode, signed zero, infinities and multiple NaN payloads. No new scalar conversion through Python datetime may truncate ns precision.
4. Reuse one oracle for correct, incorrect, then correct actual results and for concurrent comparisons. Mutate oracle bytes/schema/policy/mapping/tolerance and require invalidation; interrupt index creation and require refusal/rebuild, never reuse.
5. LIMIT 0, OFFSET past end, OFFSET/LIMIT inside the same huge tie, both boundaries in distinct ties, hidden order keys, duplicates spanning groups, NULL/NaN order keys, unordered LIMIT and existing augmenting-path counterexamples. Compare with the current exact implementation on bounded fixtures.
6. Resource tests with a small validation budget, long string/dictionary buffers and a disk quota/fault-injected full disk. Keep comparator failure distinct from engine wrong-answer/refusal and preserve evidence.

After tests pass, run contained validation-only scaling at increasing deterministic row counts and then the unchanged full SF10 table. Record wall time, CPU time, rows, decoded bytes, peak cgroup memory, SQLite/temp peak disk, strategy/fallback, oracle build time versus reuse time, and exact outcome. Do not run validation benchmarks concurrently with engine latency measurements. Expected target: O(N) native comparison for equal provider streams; one oracle preprocessing cost per query/session and O(actual-size) membership checks for small LIMIT outputs. No unmeasured speedup claim or weakening of the exact gate.
