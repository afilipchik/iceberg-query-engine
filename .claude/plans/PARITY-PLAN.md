# DuckDB Parity Plan — the three remaining rewrites

State when written (2026-08-09, commit `1409369`): **8.7–8.9s vs DuckDB native
2.94s = 2.9–3.0x; ~2.1x like-for-like** (DuckDB reading the identical parquet:
4.09–4.45s). 22/22 benchmarks pass, all 22 cell-exact at SF=10, 793 tests.
The goal-loop took the engine 339x → 2.9x; engine-level tuning has asymptoted
(rounds 21–30 in the memory log: six neutral experiments at the ±2% noise
floor, all reverted). Every remaining query's cost decomposes into
**parquet decode + join-output gather + one structurally hard join**.

Per-query like-for-like ratios and phase breakdowns are in the memory file
(`2026-08-08-cbo-join-breakthrough.md`, rounds 22–30). Worst absolute:
Q09 1.47s (1.1x native — at DuckDB's own speed), Q20 0.94s, Q21 0.83s,
Q18 0.57s, then a dozen 200–450ms queries at 1.3–3x like-for-like.

Execute each phase from a FRESH session. Never regress the invariants:
memory-safe always, `cargo fmt` before commit, full `cargo test` + cell-exact
validation (`.scratch/validate22.py` pattern) + `safe_benchmark.sh` after
every change, commit-or-revert per lever.

---

## Rewrite 1 — Dictionary-aware string processing (est. −0.6 to −1.0s total)

**Evidence**: Q01 (434ms, 2.9x l4l) spends ~250ms decoding 7 columns of which
2 are dictionary-encoded 1-char strings, plus ~100ms of per-row string
group-id resolution. Q16/Q10/Q02 gather wide strings through joins. DuckDB
processes dictionary columns without materializing strings.

**Design**:
1. `ParallelParquetSource` and `StreamingParquetScanExec` request
   `Dictionary(Int32, Utf8)` for low-cardinality string columns (heuristic:
   parquet column chunk fully dictionary-encoded AND dict size ≤ 4096 —
   readable from the column chunk metadata's encodings + dict page).
   Arrow's `ParquetRecordBatchReaderBuilder::with_schema` coerces.
2. `AggregationState`: group-key accessor for `DictionaryArray` uses the
   **key index** as the raw key (dict values registered once per batch for
   output/collision handling — machinery already exists from the perfect-hash
   collision fix, round Q21).
3. Joins/filters on dictionary columns: evaluate on indices where possible
   (equality to literal = index lookup in dict values once per batch).
4. Downstream operators must accept dictionary arrays or fall back via
   `arrow::compute::cast` at the operator boundary (add a normalizing shim in
   FilterExec/ProjectExec first, then remove it operator by operator).

**Gates**: start with scans feeding MorselAggregateExec only (Q01/Q16);
expand to join probe sides last. Cell-exact validation after each expansion.

## Rewrite 2 — Late materialization / fused join→aggregate (est. −0.8 to −1.2s)

**Evidence**: Q21's orders⋈lineitem join materializes ~30M gathered rows that
feed straight into an aggregate; Q09 gathers 6.6M rows through three
consecutive joins; Q20's LEFT join gathers 1.1M×N columns to evaluate one
comparison. `create_joined_batch`'s take-based gather is the measured cost
(HJ_TIMING probe lines; gather ≈ 60–70% of probe wall time on wide outputs).

**Design** (two independent halves):
- **2a. Fused probe→aggregate**: when a HashJoin's ONLY consumer is an
  aggregation (planner can see this), the probe path pushes (group-key
  columns, agg-input columns) directly into `AggregationState::process_batch`
  per probe batch instead of building joined RecordBatches. The build-side
  columns needed are gathered per batch as today, but ONLY the ≤3 columns the
  aggregate actually uses — skip constructing the full joined schema.
  Implement as a `JoinAggregateExec` that owns both (planner fuses
  `Aggregate(Join)` when group_by+aggs reference ≤4 columns total).
- **2b. Deferred wide-column gather**: for joins whose parents only filter/
  re-join on keys, emit (build_row_id, probe_row_id) pairs plus key columns,
  gathering the remaining columns at the pipeline sink (mirror of the
  deferred-decoration rule that already works at the Limit level — see
  GroupKeyReduction::try_defer_decorations for the pattern and its pitfalls:
  unique aliases for id columns, referential integrity, JoinReorder
  interference).

**Gates**: 2a first (contained, biggest for Q21/Q09/Q20). Row-id columns in
2b interact with every operator — only attempt after 2a is green.

## Rewrite 3 — Decode path (est. −0.5 to −0.8s, hardest)

**Evidence**: single-column SUM over lineitem = 51ms; DuckDB-parquet does
whole Q01 (7 cols + 9 aggregates) in 141ms. arrow-rs per-column decode is
~1.5–2x DuckDB's parquet reader on this data; overlapped decode helps but
the per-page decompress+decode is the floor.

**Options in order of sanity**:
1. Wire dictionary reads (Rewrite 1) — cheapest decode win, do first.
2. Page-level pruning: column index (page statistics) based skipping for
   the band filters (arrow-rs supports page index; the RowFilter path
   currently decodes full pages).
3. Only if 1+2 insufficient: bespoke decoders for the hot physical types
   (PLAIN/DICT f64 and i64 with Snappy) writing straight into aligned
   buffers, behind the existing `ParquetFileInfo` metadata layer.

## Sequencing and exit

1. Rewrite 1 scans (Q01/Q16) → full gauntlet → commit.
2. Rewrite 2a JoinAggregateExec (Q21, Q09, Q20, Q18) → gauntlet → commit.
3. Rewrite 1 join-side dictionaries (Q10/Q02/Q22 gathers).
4. Rewrite 3.2 page pruning (Q06/Q12/Q14/Q19/Q20 bands).
5. Re-measure; if >1.3x like-for-like remains, evaluate 2b and 3.3.

**Exit criterion**: like-for-like total (DuckDB on the same parquet, warm,
same machine: `.scratch/duck_parquet_bench.py`) within 1.0x. The native-table
comparison (2.94s) additionally includes DuckDB's storage-format advantage;
parity there requires an owned storage format (out of scope — closest
equivalent would be caching decoded/dictionary-compressed columns, i.e. a
buffer pool, which is a fourth project).
