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

## Rewrite 2 — Late materialization / fused join→aggregate (REVISED: est. −0.2 to −0.4s for 2a; 2b carries the rest)

**Estimate correction (2026-08-09, after rounds 35–36)**: the original
−0.8/−1.2s figure conflated the gather with must-do work. The fused
streaming aggregate ALREADY consumes join batches without an intermediate
materialization barrier; expressions must evaluate over joined data
regardless; and the join output schemas are already projection-minimal.
Fusing the aggregate into the probe (2a) saves only the joined-batch
allocation + one channel hop — real but small. The larger remaining
mechanism is DuckDB-style selection-vector execution (operators pass
(batch, sel) pairs and never compact between filter/join stages), which
is 2b generalized and a substantially bigger rewrite. Round 35 also
proved the OUTPUT-repartitioning variant is a net loss — fuse INTO the
probe or don't bother.


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

## Rewrite 4 — Owned storage layer: Arrow IPC sidecar cache (est. −2 to −3s; the direct path to NATIVE parity)

**Reframing (2026-08-09)**: the exit criterion below treats native-table
parity as out of scope because DuckDB's 2.94s prices in its owned storage
format. But an owned format v0 is concrete: maintain an **Arrow IPC
sidecar** per parquet file (`.qecache/<table>.arrow`, built once on first
registration), and load it with **mmap zero-copy** (`arrow::ipc::reader`
over a memory-mapped buffer — batches reference the mapping directly, no
decode). Parquet decode — the single largest residual cost bucket — drops
to ~0 for warm files, exactly as DuckDB's native reads do.

**Integration points** (the catch is that hot paths bypass the provider):
1. `ParquetTable::scan` (eager/prescan paths) — trivial swap-in.
2. `ParallelParquetSource` — add an IPC mode: the IPC file footer gives
   per-batch offsets; work units become batch ranges instead of row
   groups; morsel/dense-aggregation loops consume the mmap'd batches
   unchanged (they already take `RecordBatch`).
3. `StreamingParquetScanExec` — same, with the caveat that runtime-filter
   RowFilter pushdown doesn't apply to IPC (filtering happens post-load;
   cheap because load is free). Static filters evaluate vectorized on
   mmap'd batches.
4. Row-group pruning maps to batch-range pruning: store per-batch min/max
   for filter columns in a tiny sidecar footer (or reuse parquet footers,
   since batch boundaries can mirror row groups 1:1 — simplest v0).

**Memory safety**: mmap pages are file-backed (evictable, never OOM);
the spillable operators are unaffected.

**Honesty note**: this changes the benchmark's storage premise — document
clearly that engine times are then measured against the engine's own
cache format, the same premise as DuckDB-native times. The like-for-like
parquet comparison remains reported alongside.

## Sequencing and exit

1. ~~Rewrite 1 scans (Q01/Q16)~~ DONE (commits 96f7d43 + 01e03ed).
2. **Rewrite 4 IPC sidecar cache** — biggest single lever remaining and
   the only one that closes NATIVE-storage distance; start with
   ParallelParquetSource IPC mode (lineitem dominates).
3. Selection-vector execution (2b generalized) — the main engine-side
   mechanism left; large.
4. Rewrite 2a right-sized (−0.2/−0.4s) and join-side dictionaries —
   only if 2+3 leave a gap. Page pruning (3.2) is DOA on this data
   (shipdates are unclustered within row groups; verified).

**Exit criterion**: like-for-like total (DuckDB on the same parquet, warm,
same machine: `.scratch/duck_parquet_bench.py`) within 1.0x. The native-table
comparison (2.94s) additionally includes DuckDB's storage-format advantage;
parity there requires an owned storage format (out of scope — closest
equivalent would be caching decoded/dictionary-compressed columns, i.e. a
buffer pool, which is a fourth project).

---

## 2026-08-16 BMAD round — measured state, PRD, stories

**Analyst (all re-measured today, same machine, warm page cache):**

* Engine on parquet: **7.66s** (22/22 pass, per-query log
  `logs/safe_benchmark_*_20260816*`). Worst ratios: Q02 6.9x, Q05 6.0x,
  Q22 5.9x, Q03 5.7x. Worst absolute: Q09 1.47s (1.1x native — at parity),
  Q21 582ms, Q18 525ms, Q13 521ms.
* DuckDB native, re-baselined via `duckdb_rebaseline.py`: **3.32s**
  (Q09 rose 1277→1543ms). True native ratio today: **2.31x**.
* IPC sidecar v0 (`QE_IPC_CACHE=1`), warm: **9.42s — a net REGRESSION**
  (Q06 97→558ms, Q15 106→546ms, Q18 525→989ms). Root cause: the read path
  is `File`+`BufReader` — it copies and validates every batch of an
  uncompressed sidecar (lineitem: 7.2GB vs 2.8GB parquet), so it pays MORE
  memory traffic than parquet decode saved. The plan said mmap zero-copy;
  v0 never implemented it. This is why the flag was left default-off.

**PRD (targets are measured, warm, this machine):**

* G1: warm IPC total ≤ **4.5s** this round, correctness unchanged
  (22/22 cell-exact vs DuckDB, full suite green, memory-safety intact).
* G2 (exit, unchanged from above): like-for-like parquet parity ≤1.0x;
  native-premise comparison reported alongside with the storage caveat.
* Non-goals this round: shuffle/M3, selection-vector execution (2b).

**Stories:**

1. **S1 — mmap zero-copy IPC reads**: `FileDecoder` over a `Buffer` built
   with `Buffer::from_custom_allocation` on a `memmap2::Mmap` (arrow-ipc
   53.4.1 has the full API; alignment preserved because FileWriter pads and
   mmap is page-aligned, so `build_aligned` stays zero-copy). Gate:
   Q06 warm-IPC within 1.3x of its parquet-path time; no test changes.
2. **S2 — cover the remaining scan paths** (streaming/filtered, eager) if
   S1's numbers justify it; static filters evaluate vectorized post-load.
3. **S3 — re-measure everything**, update this file with the verdict:
   default-on, keep opt-in, or delete the cache (a cache that loses is
   worse than no cache; deletion is a legitimate outcome).

**S3 verdict (2026-08-17, all gates run):**

* S1 (mmap zero-copy via `FileDecoder` + `Buffer::from_custom_allocation`)
  and S2 (streaming + eager scan paths wired; dict-coercion scans and
  string-filter eager scans keep parquet) shipped. Warm totals, same
  machine: **6.37s IPC vs 7.45s parquet-only vs 9.42s broken v0**; DuckDB
  native re-measured 3.32s → **1.92x native** (was 2.31x), G1 (≤4.5s)
  exceeded. Q09 1.1x, Q12/Q18 1.6x, Q19 1.8x, Q17/Q21 ~2x.
* Verdict: **keep `QE_IPC_CACHE=1` opt-in** — it costs ~2.6x the parquet
  footprint in sidecar disk and a one-time build; the benchmark reports
  both premises. Not default until sidecar lifecycle (eviction, rebuild on
  schema change) is designed.
* The hunt also fixed three latent engine bugs the gauntlet exposed, all
  reachable WITHOUT the cache: a spilled hash join's build batches were
  consumed by the FIRST execution (re-execution silently joined an empty
  build side — zero rows as an answer); an empty row group ENDED the
  streaming scan's unfold instead of advancing (silently dropping the
  partition's remaining row groups); and `estimate_batch_size` counted
  buffer CAPACITY, over-counting sliced arrays. Plus: the spillable
  aggregate re-chunks oversized batches so the memory budget stays
  enforceable against any producer's batch size.
* Remaining per-query residue at 1.9x: Q13 4.3x (o_comment LIKE decode),
  Q22 5.4x, Q16 5.1x, Q14 4.7x — string/dictionary work (Rewrite 1's join
  side) and selection-vector execution (2b) are the next levers.
