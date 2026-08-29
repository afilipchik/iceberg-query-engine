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

- (this commit) design + before-state recorded; implementation follows.
