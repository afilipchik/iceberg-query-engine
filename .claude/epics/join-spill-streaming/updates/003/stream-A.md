---
issue: 003
stream: spill-streaming
started: 2026-09-05T04:10:00Z
status: in_progress
---
## Scope
Phase B K-way parallel under the SHARED budget (chunk budget = threshold
/ K), streaming read-back of the spill files, rayon-parallel probing
within a partition, K + per-partition elapsed traces.

## 2026-09-05T04:10:00Z — code done, unit/integration gates green

- `plan_phase_b`: K = min(available_parallelism, 8, spilled) further
  clamped by `threshold / predicted_table(largest spilled partition)` (a
  partition that out-sizes the budget is read back in K× more chunks at
  budget/K and each chunk re-probes the whole probe partition — K jobs do
  K× the lookups in the same wall time, so the budget is not split there;
  the CPU goes to the intra-partition parallel probe instead). Overrides:
  `SpillableHashJoinExec::with_spill_join_parallelism(Some(k))` (tests /
  harnesses), then `QE_SPILL_JOIN_PARALLELISM`; clamped to the spilled
  count. Trace: `phase B: spilled_partitions=.. parallelism K=.. (hw=..
  budget=.. override=..) chunk_table_budget=.. threshold=..`.
- `process_spilled_partitions`: `JoinSet::spawn_blocking` jobs, ≤ K in
  flight, every job's batches sent as produced; first error surfaces
  after the in-flight jobs drain; `tx.is_closed()` per spawn.
- `process_spilled_partition` STREAMS both files (`open_spill_reader`,
  8,192-row batches = the writer's row groups): build chunk = whole
  batches while predicted table + batch bytes ≤ chunk budget; per chunk
  the probe file is re-opened and probed in groups of 16 batches with
  rayon `par_iter` (INNER: per-batch `probe_partition`, outputs sent;
  probe-side SEMI/ANTI: per-batch bitmaps by file position, OR across
  chunks, final emission pass; build-side SEMI/ANTI: per-chunk
  `AtomicBool` bitmap via `mark_build_matches_atomic`, per-chunk
  emission). Footprint per job = one chunk + one probe group + bitmaps +
  the output batch in flight — never a partition file. Hash-check lines
  unchanged (build checksum over the single streamed read, probe checksum
  on its first pass). Per-partition trace: `process_spilled_partition
  idx=.. done: build_rows=.. build_batches=.. chunks=.. probe_rows=..
  chunk_table_budget=.. elapsed=..`.
- Test `spilled_partitions_processed_in_parallel_are_cell_exact`: K
  forced to 3 and 8 over INNER (both orientations + sparse), SEMI/ANTI
  both orientations × dense/sparse — rows identical to the in-memory
  delegate and equal to the naive ground truth (22 runs).
- Gates: `spillable::tests` 34/34; `spill_tests` 12/12;
  `native_dictionary_semi_anti` 4/4; fmt clean. Release build launched.

## 2026-09-05T04:50:00Z — first 003 measurements; phase A found to be the pace-setter

- Chaos (003 binary): **300/300** (200 @seed 20260905 tpch-10mb: 179
  genuine-disk; 100 @seed 777 tpch-100mb: 92 genuine-disk), 0 disk-
  expected-but-missing, **26,054 hash-check-ok / 0 HASH-MISMATCH**.
- Harness @1G on 003 (600M build, 256MB budget): semi-join build_right=0
  cgroup **PASS 838MB** (002: memcg-killed), build_right=1 cgroup/rlimit
  **470/456MB**; remaining legs running.
- Q9 SF=100 parquet @1G under MemoryMax=16G, 003 binary: **607.3s,
  CELL-EXACT**, 2 spill-path joins, **246 hash-check-ok / 0 mismatch**,
  serve peak RSS 9.8GB (whole engine pipeline: 32 parquet scan
  partitions, the probe merge channel, two joins, the aggregate). Second
  join (63 spilled partitions of ~2.34M build rows / ~5.2M probe rows,
  K=2 by the budget clamp — its largest partition's predicted table is
  337MB of an 859MB threshold): 428s (002: 665s); first join (60
  spilled, K=5): 587s. Target ≤300s NOT met by K alone.
- Root cause, measured not assumed: `top` during the run showed the
  serve process at **~173% CPU of 3,200%** — phase B was starved by its
  consumer. With the output streamed (002), the second join's phase B
  feeds the first join's phase A, which hashed 333M probe rows, probed
  the residents and fed 60 spill writers on ONE thread. Fix (this
  commit): phase A gathers probe batches into ~262k-row groups, each
  processed on the blocking pool — re-sliced to 8,192-row pieces,
  rayon-parallel `partition_batch_by_hash` + resident probing
  (build-side SEMI/ANTI bitmaps now `AtomicBool`), then the spill writes
  rayon-parallel ACROSS partitions (one writer each) — outputs sent per
  group. Also fixed: `SpilledPartition::build_rows` froze at the
  eviction-time count (appends never added to it) — diagnostic-only
  before, but `plan_phase_b` sizes K from it now.
- Gates on the rewrite: `spillable::tests` 34/34, `spill_tests` 12/12,
  `native_dictionary_semi_anti` 4/4, fmt clean. Release build launched
  for the Q9 re-measurement.
