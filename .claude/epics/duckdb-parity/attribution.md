# Attribution round 1 (2026-08-18, SF=100 parquet, HJ_TIMING/AGG_TIMING)

## Q18 (9.3s vs duck-pq 4.2s)

- The IN-subquery aggregate ALONE (`SELECT l_orderkey FROM lineitem GROUP
  BY l_orderkey HAVING SUM(l_quantity)>300`) = **4.0s engine vs 2.0s
  DuckDB** (604M rows → 150M groups → 6.5M qualifying orders on this
  generator's data). ~2.0s of the 5.1s gap is this one aggregate.
- Final join: build 6.5M orders (1 batch), probes 604M lineitem rows at
  0.3–1.0s/partition wall; final agg merge 0.56s, worker busy only 6.2s.
- Runtime filter for the final join: build keys 6.5M over o_orderkey
  domain ~600M → **skipped by the 64M-bit bitmap cap** (see Q4). With the
  cap widened, lineitem probe should prune 604M → ~26M.

## Q9 (21.9s vs duck-pq 9.5s)

- **Aggregation worker busy sum: 243.8s CPU** for 175 output groups —
  36% of the query's total CPU. Cause: group keys are a join-gathered
  PLAIN `n_name` StringArray (25 distinct, hashed per row) +
  `EXTRACT(YEAR o_orderdate)` per row, over ~604M joined rows. The
  dictionary fast path in AggregationState needs DictionaryArray input;
  `create_joined_batch` materializes plain strings.
- partsupp join (80M-row build, packed key): probes 9–15s per ~10M-row
  partition — VHT random access + row-store gather; the 2b territory.
- Both mechanisms are independent; the agg one is cheaper to fix:
  **emit DictionaryArray for small build-side string columns in
  create_joined_batch** (take indices ARE the dict keys — near zero
  cost) + ensure AggregationState uses dict indices as raw keys.

## Q5 (8.0s vs duck-pq 1.55s, worst ratio 5.2x)

- Final agg trivial (160 groups); all probe walls small (~280ms).
- Runtime filters already prune lineitem to ~17M probe rows.
- The 8.4s drain is upstream scan/decode + build drains — NOT visible in
  HJ probe timing. Needs perf-level attribution (next round). Suspects:
  lineitem decode + RowFilter bitmap evaluation, orders scan, the
  17.1M-row build drain (> 16M rows, so ITS runtime filter is skipped by
  the rows cap).
- Group key here is also n_name via joins → same dictionary-gather win
  applies to whatever agg cost exists.

## Q4 (3.5s vs duck-pq 0.92s) — ROOT CAUSE FOUND

- Semi join: build = 5.4M date-filtered orders, probe = **452.5M
  lineitem rows in ONE partition** (Semi forces output_partitions=1),
  probe 1.68s, agg trivial.
- `RT_DEBUG`: filter linked BUT `publish: skip=true` —
  `(max-min) >= 64_000_000 && keys > 4_000_000`. The 64M-bit bitmap cap
  is an SF=10 artifact (o_orderkey domain 60M fits at SF=10, 600M at
  SF=100 does not). 600M bits = 75MB — nothing on this box.
- **Fix implemented**: `BITMAP_MAX_BITS = 2^31` (256MB cap) in
  hash_join.rs publish site. Expect Q4 probe 452M → ~22M rows and decode
  pruning via the deferred-probe path that already exists.

## Revised lever ranking (evidence-based, replaces the PRD's guess)

1. **Runtime-filter cap widening** (done, in build) — Q4, Q18, possibly
   Q12/Q3/Q21 shapes. Cheap, correctness-safe (superset pruning).
2. **Dictionary-preserving join gather** — Q9 (−5s?+), Q5, Q7, Q8, Q10,
   Q3 (any query grouping on join-decorated low-NDV strings). Replaces
   the generic "2a fusion" as task 002's core.
3. **High-cardinality int-key aggregate** (Q18 subquery, 4.0s vs 2.0s):
   radix/disjoint partitioning beyond the 64M-range gate — task 003
   companion.
4. **2b-lite deferred gather / probe cost** — Q9's partsupp probe
   (9–15s/partition) — task 006 unchanged.
5. **Q5 deep attribution with perf** — before any Q5-specific work.
