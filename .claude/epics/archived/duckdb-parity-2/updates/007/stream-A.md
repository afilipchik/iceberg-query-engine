---
issue: 007
stream: main
started: 2026-08-23T02:20:00Z
status: completed
completed: 2026-08-23T10:30:00Z
---
## Scope
See .claude/epics/duckdb-parity-2/007.md — final QA close-out task for the
duckdb-parity-2 epic. Validation/docs only (fix small regressions if QA
finds any). Full suite x4 feature combos, cell-exact SF=10+SF=100, fresh
benchmark sweep (both cache premises), confirmatory SF=100 sweep, NEW:
Iceberg-table comparison + CPU/GPU split, M1/M2 gates, CLAUDE.md update,
epic close-out write-up, archive to .claude/epics/archived/.

## Pre-flight context gathered (reading phase, complete)
Read 007.md in full, epic.md (Success Criteria G1-G5, phase-1 checkpoint),
001-006.md outcome sections in full, updates/003 + updates/004 stream-A.md
in full (the two with the most measurement nuance). Key facts carried
forward for the close-out write-up:

- **001**: doc/premise fix. SF=10 re-baseline: cache-off 7.40s (2.23x
  native/1.77x like-for-like), cache-on 5.88s (1.77x/1.41x). SF=100 skipped
  (optional, time-boxed).
- **002**: Q13 agg-side. disjoint_group_hint floor 2M->1M. Q13 SF=10
  413.1ms -> 246.9ms avg (-40.2%). SF=100 unaffected by design (range=15M
  already qualified); surfaced separate finalize_disjoint_states residue
  (fixed by 006).
- **003**: Q13 join-side pruning to Left/Right/Full+filter. Shipped
  correctly, all gates identical, tests added. MEASURED NEGLIGIBLE WALL
  IMPACT: controlled A/B 276.98ms vs 277.2ms — because ProjectionPushdown
  (pre-existing, unrelated rule) had ALREADY cut Q13's join inputs to 4
  cols before this task's gate ever ran; this task drops exactly 1 more
  redundant column. Real remaining cost: permanent double-gather of
  o_comment (u32_path stays filter.is_none()-gated), out of scope.
- **004**: Q16 anti-join parallel probe. Real, large, controlled-A/B-
  verified win: anti-probe 41.5ms->6.2ms (6.7x), Q16 total 151.7ms->120.0ms
  (~21% faster, the epic's best single-query win). Secondary beneficiary:
  Q22 anti-probe 33.1->6.3ms (5.3x), Q22 total 198.2->162.4ms (~18%
  faster). Q21 unaffected (already on the filtered probe_semi_anti_parallel
  path). Oversight not correctness choice, confirmed 3 independent ways.
- **005**: Q16 distinct_set hasher std::HashSet -> hashbrown::HashSet.
  Landed, zero-risk, not isolated from 004 (shared checkout constraints)
  but expected small per its own pre-measurement (not Q16's dominant cost).
- **006**: Stage 0 kill-switch CLEARS (24.5-44.5% win, 10/10 points,
  1-aggregate shape; degrades at 2 aggs +10.8%, regresses at 3 aggs -9.9%).
  Stage 1 correctly DID NOT PROCEED: neither Q10 nor Q20 reaches the
  raw_groups boxed tier — both already on the leaner raw_sums tier via
  GroupKeyReduction/EagerAggregation. Shipped smaller in-scope fix instead:
  finalize_disjoint_states single-state fast path, SF=100 Q13 merge step
  ~205ms/iter -> ~168ms/iter avg.
- Phase-1 checkpoint numbers (already in epic.md before 003/004/006 landed):
  cache-off 8.86s->7.32s, cache-on 5.99s->5.79s (partial, before 003/004/006).

## Plan for this task (checkpointed so it survives interruption)
1. [x] Read all prerequisite docs (001-006 + epic.md + perf-marathon epic.md
   reference style + dependency-modernization archive commit pattern).
2. [x] `cargo fmt --all -- --check` — CLEAN (confirmed before any other work).
3. [x] Full suite default (`cargo test --release`) — **995 passed, 0
   failed, 1 ignored.** Binary snapshotted to
   `.scratch/bins/query_engine-default` (sha256 6960879a...) before the
   next feature build could overwrite `target/release/query_engine`.
4. [x] Full suite --features lance (needed a cold rebuild — lance pulls
   ~490 crates, took ~15 min wall clock) — **1059 passed, 0 failed, 2
   ignored (1 lib + 1 doctest), across all 16 test result groups, zero
   failures anywhere.** (1059 exceeds task 004's documented "lance 1052"
   baseline by 7 — expected, accounts for 2 new duckdb_validated fixtures
   + hash_join.rs/spillable.rs new unit tests task 003 added, which lance
   also compiles/runs since it's a superset build.) Binary snapshotted to
   `.scratch/bins/query_engine-lance` (sha256 971302041d...).
5. [x] Full suite --features gpu — **995 passed, 0 failed, 1 ignored**
   (identical count to default — gpu adds no new gated test files, unlike
   lance). Built with `LD_LIBRARY_PATH=$PWD/.venv/lib/python3.12/
   site-packages/nvidia/cuda_nvrtc/lib`. Took ~7.5 min wall (much faster
   than lance's ~15 min, as expected — cudarc alone vs lance's ~490-crate
   tree). Binary snapshotted to `.scratch/bins/query_engine-gpu` (sha256
   a59449e3...).
6. [x] Full suite --features pulsar — **998 passed, 0 failed, 1 ignored**.
   Binary snapshotted to `.scratch/bins/query_engine-pulsar` (sha256
   f8b8be4d...).

**ALL 4 FEATURE COMBOS GREEN, ZERO FAILURES ANYWHERE:**
| build | passed | failed | ignored |
|---|---|---|---|
| default | 995 | 0 | 1 |
| lance | 1059 | 0 | 2 |
| gpu | 995 | 0 | 1 |
| pulsar | 998 | 0 | 1 |

`target/release/query_engine` restored to the default-feature binary
(sha256 6960879a..., matches `.scratch/bins/query_engine-default`) as the
canonical binary going forward. All 4 binaries preserved in
`.scratch/bins/` for the remaining GPU-split phase (needs `-gpu`
specifically) and for reproducibility during this task.
7. [x] Cell-exact SF=10 (22/22) — **ALL 22 CELL-EXACT** confirmed via
   `.scratch/validate22.py` against a fresh `.scratch/bins/
   query_engine-default benchmark-parquet --path data/tpch-10gb --save-csv
   .scratch/engine_csv` run (QE_IPC_CACHE=1). Run opportunistically
   concurrent with the gpu build (correctness-only check, timing from this
   run is CONTAMINATED by build CPU contention and NOT used as an official
   number — 5.81s total seen here, will re-measure cleanly later once all
   4 builds are done). Row counts: Q01=6,Q02=100,Q03=10,Q04=5,Q05=5,Q06=1,
   Q07=4,Q08=2,Q09=175,Q10=20,Q11=100,Q12=2,Q13=24,Q14=1,Q15=1,Q16=320,
   Q17=1,Q18=100,Q19=1,Q20=3953,Q21=100,Q22=7 — all match known-correct
   values from prior epochs (e.g. Q13=24, Q20=3953 cited in task 006).
8. [ ] Cell-exact SF=100 (22/22) — pending, will run after builds finish
   (SF=100 run itself is long enough that I want a clean, uncontended
   measurement for its timing to double as the confirmatory benchmark
   sweep too).
9. [x] Fresh SF=10 benchmark sweep, both cache premises — clean, uncontended
    (all builds done first). `scripts/safe_benchmark.sh` full-suite (3
    iterations, reports iteration-1 per query per the script's own
    parsing) via `.scratch/bins/query_engine-default`:
    - cache-off (`QE_IPC_CACHE=0`): **7.03s total, 2.1x native (3.32s)**.
    - cache-on (`QE_IPC_CACHE=1`): **5.75s total, 1.7x native**.
    - Like-for-like DuckDB (read_parquet VIEWS over the same files, NOT
      native tables; ad hoc `.scratch/duckdb_likeforlike_sf10.py` reusing
      `duckdb_rebaseline.py`'s `tpch_queries()` helper, 16 threads, 64GB,
      best-of-3): **4.22s** (close to the previously-documented 4.18s,
      confirms methodology). => like-for-like ratios: cache-off **1.67x**,
      cache-on **1.36x**. Both improved from task 001's own re-baseline
      (7.40s/2.23x/1.77x cache-off, 5.88s/1.77x/1.41x cache-on) — the
      002+003+004+005+006 net effect on the full suite.
    - Tight, low-noise single-query numbers for Q13/Q16 specifically
      (direct `benchmark-parquet --query N --iterations 8`, no
      `safe_benchmark.sh` per-query systemd-run/timeout wrapper overhead —
      matches how tasks 002/003/004 measured their own deltas):
      - **Q13**: cache-off avg 259.9ms (min 239.3ms), cache-on avg
        223.0ms (min 206.3ms). PRD baseline band was "415-500ms depending
        on premise" => **~37-48% improvement**.
      - **Q16**: cache-off avg 131.4ms (min 113.0ms), cache-on avg 114.9ms
        (min 98.2ms). PRD baseline band was "153-224ms depending on
        premise" => **~23-49% improvement**, consistent with task 004's
        own tighter controlled-A/B ~21% figure (its number was likely
        under a slightly different premise/moment; same direction and
        magnitude).
10. [x] Confirmatory SF=100 sweep — single run (AUTO cache premise, sidecars
    fresh so resolves to cache-on-equivalent; `--sf 100 --iterations 1
    --save-csv .scratch/engine_csv_sf100`, `.scratch/bins/
    query_engine-default`): **50.66s total, 22/22 successful**. Cell-exact
    via `.scratch/validate22_sf100.py`: **ALL 22 CELL-EXACT** (incl. Q13's
    surprising-looking-but-CORRECT 2 rows at SF=100 vs 24 at SF=10 —
    verified against the DuckDB oracle, not just row-count-eyeballed).
    Q9/Q18 spot-check (the two queries named in the task file):
    **Q9 = 11.21s, Q18 = 4.86s** — both comfortably inside/better than
    the historical documented ranges (Q9 12.1-18.7s, Q18 7.6s depending on
    cache premise/epoch) — **unregressed**, consistent with tasks
    003/006's own SF=100 spot-checks this epic already ran.
11. [x] Iceberg-table benchmark — `scripts/iceberg_bench_compare.py` (NEW,
    committed script): `serve --tables data/tpch-10gb-iceberg` (Iceberg
    auto-detected) vs DuckDB `INSTALL/LOAD iceberg; iceberg_scan(<highest
    metadata.json>)`, 2 iterations best-of-N, `.scratch/bins/
    query_engine-default`. **Engine 8.325s vs DuckDB-iceberg 6.745s =
    1.23x.** Row counts match on all 22 queries (correctness sanity
    check; full cell-exact wasn't re-run here since the plain-parquet leg
    already proves the reader path is correct — Iceberg resolves to an
    ordinary ParquetTable per CLAUDE.md). **Notable finding**: Iceberg's
    manifest/snapshot indirection costs the ENGINE only ~+18.5% over its
    own plain-parquet cache-off baseline (7.03s -> 8.325s) but costs
    DuckDB's `iceberg_scan` ~+60% over its own like-for-like plain-parquet
    baseline (4.22s -> 6.745s) — so the competitive ratio actually
    NARROWS under Iceberg (1.23x) vs plain parquet (1.67x cache-off/1.36x
    cache-on). Both premises reported per the task's own "report
    alongside, not instead of" instruction.
12. [x] CPU vs GPU split benchmark — `.scratch/bins/query_engine-gpu`,
    `LD_LIBRARY_PATH` set, cache-off premise (QE_IPC_CACHE=0) throughout
    for a clean comparison basis:
    - Full-suite, `QE_GPU=0` (CPU-only path in a gpu build): **7.17s**
      (vs default-binary-no-gpu-feature 7.03s — within ~2% noise, confirms
      gpu-build harness overhead is negligible when GPU routing is off).
    - Full-suite, GPU enabled (default), SINGLE COLD PASS (all 22 queries
      once, `safe_benchmark.sh`'s own methodology): **7.87s — WORSE**,
      expected and consistent with CLAUDE.md's own documented mechanism
      (first touch of any column is ALWAYS CPU + triggers an async
      background upload; a single un-repeated pass never amortizes that
      cost). Q01 706ms (vs 399ms CPU), Q06 351.8ms (vs 102ms CPU), Q15
      415ms (vs 168ms CPU) — all cold-upload artifacts on this pass.
    - **Targeted warm, repeated single-query measurement** (Q1/Q6/Q14/Q15,
      6 iterations each, iteration 1 = cold/upload, 2-6 = warm).
      GPU engagement CONFIRMED via `nvidia-smi` (VRAM 1066 -> 1572 MiB
      during a run, not just assumed):
      - **Q1**: CPU steady ~288-322ms (avg 302ms) vs GPU warm ~315-430ms
        — NO improvement (statistically flat to slightly worse).
      - **Q6**: CPU steady ~89-101ms (avg 93ms) vs GPU warm ~94-100ms —
        ESSENTIALLY IDENTICAL, no measurable win despite confirmed VRAM
        engagement.
      - **Q14**: CPU ~125-134ms vs GPU warm ~130-139ms — flat (also
        structurally ineligible for offload: Q14 JOINs `lineitem` with
        `part`, and GPU offload doesn't cover joins, so this is the
        expected "mechanism doesn't engage" case, not a kernel-level one).
      - **Q15**: CPU ~123-140ms vs GPU warm mostly ~129-132ms with one
        305ms outlier — inconclusive/flat.
      - **IMPORTANT CORRECTING FINDING**: CLAUDE.md's existing "Q6 shape
        39.5x/58.7x, full Q1 17.0x/8.9x" numbers are from
        `examples/gpu_price_bench.rs` — an ISOLATED KERNEL microbenchmark
        over SYNTHETIC, already-VRAM-resident columns with NO scan/decode/
        plan overhead. That is a real, correctly-measured result at the
        kernel level. But at the FULL TPC-H QUERY level (via
        `benchmark-parquet` over real SF=10 parquet), scan+decode+filter
        — not the final SUM/aggregate reduction — dominates Q1/Q6's total
        wall time, so even a 58x-faster reduction kernel doesn't move
        total wall time measurably. This is a genuine, well-evidenced
        "no effect visible at this measurement level" finding, distinct
        from (but complementary to) the already-known "joins aren't
        covered" caveat — reporting both honestly in CLAUDE.md/epic.md
        rather than letting the kernel-level number stand unqualified for
        full-query expectations.
    - Q13/Q16/Q20 (the epic's own target queries) were NOT separately
      GPU-measured: join/distinct-heavy, structurally ineligible by
      construction (no joins, no DISTINCT, per CLAUDE.md's own documented
      GPU scope) — measuring would force a comparison where the mechanism
      structurally cannot engage, which the task explicitly says not to do.
13. [x] M1/M2 distributed gates — DONE early (ran opportunistically while
    the lance build compiled in the background, since M1/M2 only need the
    default-feature binary, already snapshotted). Results:
    - `cluster_local.sh verify` (M1, `.scratch/bins/query_engine-default`,
      3 nodes, `data/tpch-1mb`): **M1 GATE: PASS** — all 5 checks (cluster
      membership agreement, Q1/Q3/Q6/Q10/Q12 byte-identical across 3 nodes
      + single-process, healthz/readyz 200 on all nodes, Flight==HTTP on
      all 3 nodes via flight_validate.py --quick, SIGTERM node-2 handled
      cleanly with survivors marking it down and still answering).
    - `cluster_local.sh verify-m2` (M2, fresh 3-node cluster): **M2 GATE:
      PASS** — all 4 checks (work division imbalance 1.0000/1.0001 at
      3/8 nodes, 13 scatter-path cell-exact-vs-DuckDB cases, 13 gather-path
      cases incl. joins/subqueries/DISTINCT/ORDER BY/STDDEV/CTE all
      identical to DuckDB, 2 correctly-refused-by-name cases, per-node
      timing sane).
    - `tests/distributed_cluster.rs` (19 tests): already covered — part of
      the earlier full default `cargo test --release` run (995/0/1),
      confirmed via the per-binary test count breakdown matching task
      006's own documented mapping (distributed_cluster=19).
    - Nothing this epic touched (planner.rs, hash_join.rs, spillable.rs,
      hash_agg.rs, morsel_agg.rs) is on the distributed scatter/gather
      hot path in a way that changed behavior — consistent with
      perf-marathon's own standing finding "shards bypass sidecars by
      design." This was a confirmation check, not a new validation, as
      the task file anticipated.
14. [x] CLAUDE.md update (SF=10 section final numbers, Q13/Q16 residue
    status, plus new Iceberg-benchmark and GPU-split subsections)
15. [x] Epic close-out section in epic.md (headline table, G1-G5
    accounting all MET, named residues as one class, commit hashes)
16. [x] Moved epic dir to .claude/epics/archived/duckdb-parity-2/ via
    `git mv` (mirrored ee4414e's dependency-modernization pattern exactly
    -- verified: 001-006.md and updates/001-006 renamed 100% similarity,
    007.md/epic.md renamed-with-modification since I'd already edited
    them, new updates/007/stream-A.md added at the archived path).
17. [x] Final commits: `2aeb9d5` (benchmark/CLAUDE.md work) and `ce564f4`
    (close-out + archive + PRD status). Working tree clean after both.
    NOT merged to main (confirmed: main's HEAD is at an unrelated prior
    commit) -- left to the user/orchestrating session per instructions.

## TASK COMPLETE (2026-08-23T10:30:00Z)

All acceptance criteria met. Summary of final state:
- 4/4 feature-combo suites green: default 995/0/1, lance 1059/0/2,
  gpu 995/0/1, pulsar 998/0/1 (passed/failed/ignored).
- 22/22 cell-exact SF=10 and SF=100 (fresh runs this session, not
  carried over).
- `cargo fmt --all -- --check` clean (verified twice: before touching
  anything, and again since -- this task made zero Rust source changes).
- M1 + M2 distributed gates PASS (`cluster_local.sh verify` /
  `verify-m2`), plus `tests/distributed_cluster.rs` (19 tests) green as
  part of the default suite.
- SF=10 headline: cache-off 8.86s(epic-start)->7.03s, cache-on
  5.99s->5.75s. Q13 415-500ms->223-260ms avg, Q16 153-224ms->115-131ms
  avg.
- New Iceberg-table benchmark (`scripts/iceberg_bench_compare.py`):
  engine 8.325s vs DuckDB-iceberg 6.745s = 1.23x, row counts match all 22.
- New CPU/GPU split: GPU engagement confirmed via VRAM, but no measurable
  full-query win at SF=10 even for structurally-eligible queries (Q1/Q6)
  -- scan/decode dominates, not the aggregate kernel. Documented as a
  correcting finding against the GPU epic's own kernel-level numbers.
- Epic G1-G5: all MET (see epic.md's close-out for full accounting).
- No code regression found; no fix was needed (this task stayed
  validation/docs-only as scoped).
- Epic archived to `.claude/epics/archived/duckdb-parity-2/`, status
  `completed`, progress `100%`. PRD status `completed`. Not merged to
  main.

Background agent (accbc8cd8e2675eb9) REPORTED BACK — findings folded in:

- **Cell-exact SF=10/SF=100**: `.scratch/validate22.py` (SF=10, no args,
  regexes queries straight out of `src/tpch/queries.rs`, reads
  `.scratch/engine_csv/qNN.csv`, compares vs fresh DuckDB-over-parquet
  views, 2-decimal + 0.02-abs tolerance) and `.scratch/validate22_sf100.py`
  (same pattern, `data/tpch-100gb`, relative-tolerance for SF=100
  magnitude) BOTH EXIST ON DISK right now (uncommitted scratch, per project
  convention) — reuse as-is, don't recreate. Must run
  `benchmark-parquet --path <dir> --save-csv .scratch/engine_csv[_sf100]`
  first (stale copies already present from earlier tasks, need refresh).
  No committed script does the same full loop; closest committed ones
  (`sf100_engine_validate.py`) use looser 1%/0.01 tolerance.
- **Iceberg**: `benchmark-parquet` does NOT auto-detect Iceberg (confirmed
  in code: BenchmarkParquet handler hardcodes `<table>.parquet` lookups,
  main.rs 615-743). `serve --tables <dir>` DOES (is_iceberg_dir checked
  BEFORE lance/parquet, main.rs:1238-1253, `build_serve_context`).
  Plan: write `scripts/iceberg_bench_compare.py` (new, committed — matches
  project convention of committed duckdb_*_bench.py scripts): spins up
  `serve --tables data/tpch-10gb-iceberg`, POSTs the 22 queries to /sql,
  vs DuckDB `INSTALL/LOAD iceberg; iceberg_scan(<highest metadata.json>)`
  per iceberg_gen.py's own `read_back_duckdb()` pattern. All 8 tables'
  highest metadata.json confirmed present (2 snapshots each, `00001-*`
  highest) — data/tpch-10gb-iceberg is valid, no regeneration needed.
- **M1/M2**: `./scripts/cluster_local.sh start 3` / `verify` / `verify-m2`
  (delegates to `distributed_validate.py`) / `stop`. Plus
  `scripts/claude-safe-build.sh cargo test --release --test
  distributed_cluster` for the in-process M1 test gate. No documented
  aggregate wall-clock; per-step timeouts are 20-60s each.
- **GPU**: `LD_LIBRARY_PATH=$PWD/.venv/lib/python3.12/site-packages/nvidia/cuda_nvrtc/lib`
  required for build+run; confirmed present on disk. `QE_GPU=0` disables
  routing at plan time. `examples/gpu_price_bench.rs` is a fixed synthetic
  microbenchmark (no TPC-H data, no CLI args) — useful as a pattern
  reference only, not directly reusable for a TPC-H CPU/GPU split; will
  instead run `benchmark-parquet` SF=10 twice (once `QE_GPU=0`, once
  default) on the SAME gpu-featured binary, per queries, and separately
  call out Q1/Q6/Q14 as the mechanism-eligible queries per CLAUDE.md's own
  "GPU Aggregate Offload" scope (fused SUM/MIN/MAX/COUNT/AVG single-col
  predicates over device-resident PARQUET columns only — no joins/Iceberg/
  Lance/distributed).
- **benchmark-parquet flags**: `-p/--path`, `-i/--iterations`,
  `-q/--query <1-22>`, `-s/--sf`, `--save-csv <dir>`. Output line format:
  `Q{:02}: {:>8} rows in {:>8.3}ms` (regexable).

## Binary snapshot discipline (IMPORTANT — cargo overwrites target/release/query_engine per feature build)
Since all 4 feature combos share ONE output binary path
(`target/release/query_engine`), each successive `--features X` build
OVERWRITES the previous binary. Snapshotting immediately after each
build+test completes, BEFORE starting the next build:
- [x] `.scratch/bins/query_engine-default` saved (sha256
  6960879ac305c6e78e5b5f8f4db0b18d865efec2d67cf7e3af0aed0a991c5d2d)
  immediately after the default suite's 995/0/1 pass, before the lance
  build (already in flight) could overwrite it.
- [ ] `.scratch/bins/query_engine-lance` — save once lance build/test done.
- [ ] `.scratch/bins/query_engine-gpu` — save once gpu build/test done.
- [ ] `.scratch/bins/query_engine-pulsar` — save once pulsar build/test done.
All later benchmark/validation phases (cell-exact, SF=10/100 sweeps, M1/M2,
Iceberg) use `.scratch/bins/query_engine-default` explicitly, not
`target/release/query_engine` (which will be whatever was built last) —
except the GPU split phase, which explicitly needs `query_engine-gpu`.

## Notes / risks being tracked
- Shared (non-worktree) checkout — per task instructions, work directly
  here, no worktree. Other agents from this session may still have
  background processes; checked `ps`/`uptime` at start (load 0.52, low) —
  clear to proceed. Will re-check before any heavy concurrent step.
- Per CLAUDE.md's OOM incident: never run bare cargo; never run two heavy
  release builds concurrently against the shared 125G box beyond what the
  80G-capped cgroup scopes can jointly tolerate — sequencing builds, not
  parallelizing them.
- default `cargo test --release` came back near-instantly on first check
  (test output within 2s of launch) — build was already warm from task
  006's own work on this same branch/checkout; good sign for wall-clock
  budget.
