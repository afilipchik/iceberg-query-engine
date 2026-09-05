---
issue: 003
stream: spill-streaming
started: 2026-09-05T04:10:00Z
status: completed
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

## 2026-09-05T05:20:00Z — Q9 target met on the phase-A-parallel binary (003b = f728df0)

- Q9 SF=100 parquet @1G under MemoryMax=16G / QE_MEM_CAP=16G, sweep22.py
  Q9-only: **257.95s, CELL-EXACT (175 rows), 2 spill-path joins, 246
  hash-check-ok / 0 HASH-MISMATCH**, serve peak RSS 10.2GB (`/usr/bin/time
  -v`). Second join 90.8s (was 428s on 003, 665s on 002); first join
  237.4s (was 587s). Serve process ran at ~815% CPU during phase B (was
  ~173% before the phase-A rewrite). Other legs (two 1G harness legs,
  the 002 Q9 record) were still running on the machine — the number is
  if anything pessimistic.
- Chaos on 003b: **300/300** (179 + 92 genuine-disk), 0 mismatch.
- Harness @1G on 003b (both orientations, both levers) launched; SF=10
  native sweep queued for a quiet machine after them.

## 2026-09-05T06:00:00Z — phase-A memory regression found by measurement, redesigned

- Harness @1G on 003b (f728df0): semi-join build_right=0 **910 / 937MB**,
  build_right=1 **858 / 867MB**, anti-join build_right=1 **921 / 858MB** —
  all PASS but ~+400MB over 003 (838/839, 470/456, 477/467) — and
  anti-join build_right=0 cgroup **memcg-KILLED**. A regression of the
  first phase-A rewrite, not acceptable.
- Localized with `.scratch/jss/rss_trace.py` (QE_SPILL_DEBUG lines
  timestamped + VmRSS sampled every 1s, semi-join build_right=1 under
  2G, 0 resident partitions): phase A peak **573MB on 003b vs 207MB on
  003**, and phase B then sits ~+380MB higher on 003b (812 vs 432MB) —
  memory retained from phase A. Shape of the first rewrite: per
  8,192-row piece, `partition_batch_by_hash` produced 64 ~128-row `take`
  outputs on a rayon thread, later freed on the blocking thread —
  ~2,300 sub-KB cross-thread-freed allocations per group × 1,145 groups,
  the pattern mimalloc's per-thread heaps retain. (Phase A wall: 40s →
  7.3s, so the parallelism itself worked.)
- Redesign (this commit): partition-MAJOR. Concatenate the group once;
  rayon over 8,192-row ranges computes per-row partition ids via a new
  shared `partition_row_ids` (the single routing definition —
  `partition_batch_by_hash` is now a wrapper over it, so routing is
  byte-identical by construction); then rayon over PARTITIONS: one
  ~4k-row `take` per partition, probed / spilled / ANTI-emitted on the
  same thread that allocated it. 32× fewer allocations per group, and
  resident probing in 4k-row batches instead of 128-row ones.
- Gates on the redesign: `spillable::tests` 34/34, `spill_tests` 12/12,
  `native_dictionary_semi_anti` 4/4, fmt clean. Release build (003c)
  launched; the whole 003 battery (RSS trace, harness @1G ×8, Q9, chaos
  300, SF=10 sweep) is re-run on it — 003b's numbers are superseded.

## 2026-09-05T06:40:00Z — the +300MB is allocator retention across rayon threads; bounded pool

- 003c (partition-major phase A, 7a8f287) did NOT move the phase-A peak
  (583MB vs 573MB) — the allocation-shape hypothesis was wrong. Q9 on
  003c, quiet machine: **243.6s CELL-EXACT, 246/0, peak 10.9GB**; chaos
  300/300.
- Controlled experiments, same leg (semi-join build_right=1, 600M build,
  2G scope, `rss_trace.py`): global 32-thread rayon pool **583MB** phase-A
  peak; `RAYON_NUM_THREADS=8` **279MB**; `MIMALLOC_PURGE_DELAY=0` **305MB**;
  single-threaded phase A (003) 207MB. Verdict: freed memory retained
  per rayon thread by mimalloc, proportional to the number of threads
  that touched a group's short-lived allocations — not live data, and
  not fixable by reshaping the work.
- Fix (this commit): phase A runs its two `par_iter`s on its OWN rayon
  pool of min(available_parallelism, 8) threads (`spill_join_phase_a_pool`,
  `OnceLock`), phase B stays on the global pool (its 003 peak of 432MB on
  this leg was the accepted baseline). Gates green (34/34, 12/12, 4/4,
  fmt). Release 003d building; RSS trace + full battery re-run on it.

## 2026-09-05T07:05:00Z — 003d (8cd02c1) battery: Q9 224s, phase A 284MB, chaos 300/300

- RSS trace, semi-join build_right=1, 600M build, 2G scope: phase-A peak
  **284MB** (003c 583MB; single-threaded 003 207MB); whole-run peak 562MB
  at 228s (003: 432MB, 003c: 814MB).
- Q9 SF=100 parquet @1G under 16G, quiet machine, 003d: **224.3s,
  CELL-EXACT, 246 hash-check-ok / 0 mismatch**, serve peak RSS 11.0GB;
  second join 77.4s (K=2), first join 207.9s (K=5).
- Chaos 003d: **300/300** (179 + 92 genuine-disk), 0 mismatch.
- Launched: harness @1G ×8 on 003d, Q4 SF=100 native @64M under 8G on
  003d. SF=10 native sweep queued behind the harness (quiet machine).

## 2026-09-05T07:40:00Z — 003d harness margins too thin; pool sized by the budget, per call

- Harness @1G on 003d: semi-join build_right=0 **952 / 962MB** (003:
  838/839), build_right=1 **625 / 589MB** (003: 470/456), anti-join
  build_right=1 cgroup 624MB — all PASS, but ~+120-150MB over 003 and
  the build_right=0 anti leg (003: 872MB) still to come. Not a margin to
  certify a 1G cap against.
- Change (this commit): phase A's pool is sized by the OPERATOR'S budget,
  not the machine — one worker per 64MB of `memory_threshold`, clamped
  to [1, 8] (Q9 @1G: 8; the harness's 256MB budget: 3; Q4 @64M: 1) — and
  created per call, dropped before phase B so its threads exit and
  release what they retained. "probe collected" trace gains
  `phase_a_threads=`. Gates green (34/34, 12/12, 4/4, fmt). Release 003e
  building; the whole battery re-runs on it.

## 2026-09-05T08:05:00Z — 003e (24a3138) battery: phase A back at the single-thread footprint

- 003d record closed: anti-join build_right=0 **991MB cgroup / 978MB
  rlimit** — PASS by 30-40MB; exactly the near-miss the budget-sized
  pool was made for.
- 003e RSS trace (semi-join build_right=1, 256MB budget → `phase_a_threads=3`):
  phase-A peak **213MB** (single-threaded 003: 207MB; 003d 284MB; 003c
  583MB); whole-run peak 474MB (003: 432MB).
- Q9 SF=100 parquet @1G under 16G on 003e (`phase_a_threads=8`):
  **222.3s, CELL-EXACT, 246 hash-check-ok / 0 mismatch**, peak 10.7GB;
  second join 79.3s (K=2), first join 206.3s (K=5).
- Chaos 003e: **300/300** (179 + 92 genuine-disk), 0 mismatch.
- Launched: harness @1G ×8 on 003e, Q4 @64M on 003e; SF=10 native sweep
  next, alone.

## 2026-09-05T08:45:00Z — closed

- Harness @1G on 003e: **8/8 COMPLETED** — build_right=0 semi 829/870MB,
  anti 849/881MB; build_right=1 semi 466/496MB, anti 475/464MB (cgroup /
  rlimit), counts exact, 3:20-6:07 wall.
- Q4 @64M under 8G on 003e: 40.1s CELL-EXACT, peak 2,889MB (`phase_a_threads=1`).
- SF=10 native sweep, quiet machine: **22/22 OK, 5,578.12ms** (band
  5288-5667). No engine/harness processes left running.
- 001/002/003 closed with Outcome sections; stream files completed.
