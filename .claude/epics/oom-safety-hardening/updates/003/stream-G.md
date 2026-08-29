# oom-safety-hardening task 003 — stream G

Streaming two-phase reservation for `ExternalSortExec`.

## Before-state (evidence, harness sort scenario, from 002's post-fix full-matrix re-run at effectively-current sort code)

`.scratch/oom002/harness_postfix002/`:
- `sort_cgroup.log`: OOM-killed by the kernel under the 1G cgroup cap
  (journal: "A process of this unit has been killed by the OOM killer").
- `sort_rlimit.log`: `memory allocation of 1048576 bytes failed`,
  terminated by signal 6 (abort), peak RSS 1,919,756 kB (~1875MB) at the
  ~1975MB effective rlimit.

Root cause (confirmed by reading `ExternalSortExec::execute`): it calls
`collect_input_partitions_concurrently`, which fully materializes the
ENTIRE input (~4GB for the 250M-row scenario) into one `Vec<RecordBatch>`
BEFORE the `total_size > memory_limit * spill_threshold` comparison ever
runs — the identical collect-fully-then-decide hole tasks 002 (agg) and
the join's `compute_build_decision` already closed for their operators.

## Design

Two coupled changes — both required for the harness flip, verified by
arithmetic before writing code:

1. **Ingestion (the task's named target)** — mirror 002's phase-1 shape
   exactly: `stream_merge_input_partitions` + running
   `estimate_batch_size` total per batch. Under-threshold completion →
   the unchanged in-memory `MemoryTableExec` + `SortExec[::with_fetch]`
   delegate. Crossing → prefix + crossing batch + rest of stream chained
   into the EXISTING `generate_runs` (already batch-by-batch: buffers to
   ~threshold, cuts a sorted run via `flush_run`, repeats). Nothing
   collected in phase 1 is re-pulled.

2. **Bounded merge delivery (forced by the scenario's own numbers)** —
   ingestion alone canNOT flip the scenario: with a 256MB limit the
   204.8MB threshold cuts ~20 runs from 4GB, so (a) `multi_pass_merge`
   materializes each 8-run chunk's merged output (~1.6GB) in a Vec
   before writing it, and (b) the final `streaming_k_way_merge`
   accumulates the ENTIRE 4GB sorted output in `result_batches` — both
   die under the 1G cap regardless of ingestion. Fix WITHOUT changing
   the merge algorithms: refactor the k-way merge core into a
   sink-callback form (`streaming_k_way_merge_into`); multi-pass chunk
   merges sink straight into an open `ArrowWriter`
   (`append_batch_streaming`, 007's plumbing); the final merge runs on a
   `spawn_blocking` thread sinking into a bounded (cap 4) channel that
   IS the operator's output stream. Merge invariants preserved verbatim:
   buffer-transition flush, carried-run bookkeeping (`still_needed`),
   `batch_with_actual_types` on every constructed merge batch,
   `build_merged_batch_final` defensive fallback.

`fetch` (`ORDER BY ... LIMIT` under spill): applied on the output stream
via a `scan` adapter that calls the SAME `truncate_batches_to_limit`
per batch (semantics unchanged, one implementation); hitting the limit
ends the stream → receiver drops → the merge thread's next sink send
fails → it aborts early and cleans the spill dir.

Merge perf (needed to fit the 900s harness timeout at 250M rows):
- hoist sort-key column evaluation to once per LOADED buffer (was: 2
  `evaluate_expr` calls per row-comparison — billions of calls at 250M
  rows × fanin 8);
- `build_merged_batch` gains an `arrow::compute::interleave` fast path
  (all referenced buffers present + same column types), falling back to
  the existing per-row take/concat path (kept verbatim) otherwise —
  Dictionary columns etc. still reconcile through
  `batch_with_actual_types`.

Memory bound of the new spill branch (worst case): phase-1 prefix
(~threshold) → handed to `generate_runs` (same bytes, re-buffered to
~threshold) → `flush_run` concat+sort transient (~2x run size) → merge:
fanin × 8192-row read buffers + one 8192-row output batch + ≤4 channel
batches. Peak ≈ 3× threshold ≈ 615MB at the scenario's 256MB limit —
inside the 1G cap with the ~30-60MB runtime baseline.

## Log

- design + before-state recorded; implementation followed in 6422fef.
- Unit suite after the rewrite: spillable 24/24 (including
  `k_way_merge_survives_a_run_needing_more_than_one_buffer_load`,
  `external_sort_multi_pass_merge_survives_a_leftover_singleton_chunk`,
  `external_sort_spill_path_handles_dictionary_encoded_columns`,
  `external_sort_spill_path_enforces_limit_under_forced_spill`).
  spill_tests 9/9 (prior 8 + new SQL-level
  `sort_spill_with_limit_matches_in_memory`: ORDER BY ... LIMIT 50 with a
  unique sort key, ordered cell-exact vs unlimited, spill asserted).
- Small-scale smoke (25M rows, 25MB limit, QE_SPILL_DEBUG, 512M scope):
  crossing at 10 buffered batches / 19.2MB vs 20.97MB threshold; 22
  sorted runs (multi-pass reduction exercised, 22 > fanin 8); COMPLETED,
  globally ordered, peak RSS 186MB, 5.6s.

### Harness sort scenario: before -> after (same caps)

| lever | before (002's post-fix matrix, same sort code) | after (6422fef) |
|---|---|---|
| cgroup 1G | FAIL oom-sigkill (kernel killed the unit) | **PASS exit 0, rows=250000000 globally ordered, peak 776MB, 53.9s** |
| rlimit ~1975MB | FAIL abort ("memory allocation of 1048576 bytes failed", peak ~1875MB) | **PASS exit 0, rows=250000000 globally ordered, peak 831MB, 43.7s** |

Logs: `.scratch/oom003/harness_sort_postfix/`. Peak ~776-831MB matches
the predicted ~3x-threshold residual (flush_run's concat + sort copies of
a ~205MB run buffer) — the documented, bounded worst case of this design,
inside the 1G cap.
