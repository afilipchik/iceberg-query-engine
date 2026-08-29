# oom-safety-hardening task 002 — stream F

Streaming two-phase reservation for `SpillableHashAggregateExec`.
Branch: `epic/spill-size-estimate-fix`. Sole source file:
`src/physical/operators/spillable.rs` (+ `tests/spill_tests.rs`).

## Design (commit a0b760d)

Mirrors `SpillableHashJoinExec`'s shape (spill-join-correctness-2 task
002 + oom-safety-hardening task 007), no new pattern invented:

1. **Phase 1 — reserve, possibly spill** (`execute()`): the
   collect-fully-then-decide `collect_input_partitions_concurrently`
   call is replaced by `stream_merge_input_partitions` (the join's own
   bounded-channel merged stream, reused) with a running
   `estimate_batch_size` total checked per batch against
   `memory_limit * spill_threshold`. Never crossed → the unchanged
   in-memory `HashAggregateExec` delegate, reached without ever risking
   an unbounded collection. Crossed → prefix + crossing batch + rest of
   the stream chain into `aggregate_with_spilling`, entered mid-stream
   (the exact `finish_via_spill` shape).
2. **Ingestion** (`aggregate_with_spilling`): 007/spill-join task 002's
   open-writer plumbing reused byte-for-byte — one `ArrowWriter` per
   spilled partition kept open for the whole ingestion phase
   (`append_batch_streaming` + `close_spill_writers`), replacing the old
   `write_batches_to_parquet` + `merge_parquet_files`
   read-rewrite-rename-per-eviction (the same O(n²)-in-bytes pattern the
   join already had fixed; `merge_parquet_files` is now dead and
   deleted). Largest-first eviction via `evict_agg_partition_to_disk`
   (mirror of `evict_build_partition_to_disk`; differs only in leaving
   the partition resident-and-empty, since the aggregate re-reads
   spilled + resident rows together). New `SpilledAggPartition` tracks
   per-partition spilled rows + estimated raw bytes for the finalize
   accounting. Ingestion charges batch bytes only — deliberately NOT a
   predicted-state charge like the join's streaming loop, because
   during aggregate ingestion no hash/accumulator state exists yet and
   the eventual state covers spilled rows too; the state is priced
   where it materializes (next point). Divergence documented in-code.
3. **Accounted finalize** (007's discipline, applied at the point the
   state exists): per partition, BEFORE materializing anything,
   `raw_bytes + predicted_agg_state_bytes(rows, group_cols, n_aggs,
   n_distinct)` is priced against the same threshold.
   `predicted_agg_state_bytes` is the aggregate analogue of
   `predicted_hash_table_bytes` — worst-case all-unique groups
   (documented constants: 56B amortized map bucket shared with the join
   constant, 32B/GroupValue, 384B/AccumulatorState approximation,
   48B/row per DISTINCT set entry), so duplicate-heavy keys only ever
   err toward the chunked path (safe direction, bounded extra I/O).
4. **Chunked read-back** (`aggregate_partition_chunked`, the analogue of
   007's chunked `process_spilled_partition`): a partition priced over
   the threshold is NOT `read_parquet`-materialized; its spill file is
   streamed back in 8,192-row batches and re-routed with the SAME
   group-key hash at modulus `NUM_PARTITIONS * fan`
   (`fan = predicted/threshold`, clamped to [2, 64]) into per-sub open
   writers; residues of top-level partition `idx` are exactly
   `{idx + 64j}` so sub `j = i/64` holds a group-disjoint subset — each
   sub-aggregation still sees every row of each of its groups (a
   diverged residue fails loudly, never silently splits a group).
   Chunking a join's build rows arbitrarily is legal; an aggregate's is
   not — hence BY GROUP, one level only (documented residual: one sub's
   real state, targeted at ~threshold by `fan`).
5. **Dictionary-aware routing** (`partition_batch_by_group_hash`): the
   agg now routes through its own copy of the fixed-seed hash loop that
   decodes Dictionary group keys to their values first —
   `extract_join_key` has no Dictionary arm (falls to `JoinValue::Null`),
   which would put EVERY row of a Dictionary-keyed GROUP BY in one
   partition and make sub-partitioning structurally unable to split it.
   Deliberately a separate function: `partition_batch_by_hash` is join
   partition-routing, hard-bounded from this task (open ~0.34%
   duplicate-counting bug lives in that logic; its routing stays
   byte-identical). `batch_with_actual_types` reconciliation is
   preserved by construction on the new write paths: every spill write
   uses the batches' OWN actual schema (Parquet round-trips Dictionary
   via the embedded Arrow schema) and the only declared-schema batch
   construction (`aggregate_batches_external`'s output) builds from
   scalar group values, never re-wraps input arrays.

Documented residual constants on top of the threshold at finalize:
not-yet-processed partitions' resident batches (~threshold total, held
by ingestion design), one partition's/sub's read-back + real aggregation
state (~threshold via the gate), the accumulated output batches
(irreducible — the operator returns a materialized result), a global
(no GROUP BY) DISTINCT's value set (unsplittable without a sorted spill
of the values; documented in-code).

## Harness scenario 1 (agg) — before

Pre-fix evidence (task 001 `harness_final` + re-confirmed post-007
`harness_postfix007`, 250M rows / ~4GB into a 256MB `memory_limit`,
1G cap):

| lever | before |
|---|---|
| cgroup 1G | FAIL exit 137 oom-sigkill, peak 1025MB (killed at cap) |
| rlimit (QE_MEM_CAP=2048M) | FAIL exit 134 abort-at-rlimit, peak ~1975MB |

## Harness scenario 1 (agg) — after

`.scratch/oom002/harness_postfix002/` (QE_SPILL_DEBUG=1, same caps,
`scripts/oom_cap_harness.sh` wrapped in a `systemd-run` scope):

| lever | after |
|---|---|
| cgroup 1G | **PASS exit 0 COMPLETED, groups=1000003 (correct), peak RSS 403MB, wall 64.2s** |
| rlimit (QE_MEM_CAP=2048M, 8G containment) | **PASS exit 0 COMPLETED, groups=1000003, peak RSS 405MB, wall 58.9s** |

Observed spill activity (QE_SPILL_DEBUG traces in both logs):

- `[spill-agg] threshold crossed mid-stream at 101 buffered batches
  (212992000 bytes, threshold 214748364)` — phase 1 handed off at the
  crossing point, never buffering past the threshold (the old code
  would have collected all ~4GB first; pre-fix peaks 1025MB+/1975MB
  were exactly that collection dying).
- `[spill-agg] in-memory rows 12835803, spilled files 64 (spilled rows
  237164197)` — 237M of 250M rows routed to disk during ingestion.
- 64x `[spill-agg] partition N finalize predicted ~2.2GB bytes >
  threshold 214748364 — chunked read-back with fan 11` — the finalize
  gate priced every partition's read-back + worst-case state over the
  threshold and took the sub-partitioned path; peak RSS 403MB ≈
  resident batches (~205MB) + one sub's read-back + real state +
  runtime baseline, i.e. within the documented residual constants,
  ~0.4x of the 1G cap instead of blowing through it.

## Tests

- 5 new unit tests in `spillable.rs`
  (`agg_spill_dictionary_group_by_cell_exact_vs_in_memory` — Dictionary
  GROUP BY + COUNT(DISTINCT), spilled vs unlimited byte-identical;
  `agg_spill_chunked_finalize_high_cardinality_cell_exact` — 997 groups
  x 204,800 unique ids through the chunked finalize, byte-identical +
  self-consistency sum; `group_hash_routing_consistent_across_fanout` —
  the residue invariant the chunked path's correctness rests on;
  `group_hash_routing_splits_dictionary_group_keys`;
  `predicted_agg_state_bytes_is_conservative_and_monotonic`).
  Spillable module: 24/24 (19 pre-existing + 5 new).
- 1 new SQL-level test in `tests/spill_tests.rs`
  (`agg_spill_chunked_finalize_matches_in_memory`, 128KB limit COUNT
  DISTINCT GROUP BY l_partkey over a join input — forces the chunked
  finalize through the full SQL path).
