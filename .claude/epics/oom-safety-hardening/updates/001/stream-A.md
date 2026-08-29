# oom-safety-hardening task 001 — stream A progress

Agent: stream A (harness + root-cause + incident). Branch: epic/spill-size-estimate-fix.

## 2026-08-29 — session start

Read: task 001 (incl. PRIORITY LEAD), spill-size-estimate-fix 001 Outcome,
PRD, the three repros, alloc_profile.rs, lib.rs allocator wiring,
claude-safe-build.sh, enforce_safe_build.sh.

Build state check: `target/release/examples/{_control_int32_repro,
spill_dictionary_oversized_build_repro}` were built 2026-08-28 21:41, but
`src/execution/alloc_profile.rs` was modified 2026-08-29 14:20 → rebuild
required before profiling. Kicked off
`scripts/claude-safe-build.sh cargo build --release --example _control_int32_repro
--example spill_dictionary_oversized_build_repro --example spill_join_oom_repro`
(background).

Note: `enforce_process_memory_cap()` is only called from `src/main.rs` —
the example binaries do NOT self-apply `QE_MEM_CAP`. For the rlimit lever
on examples, replicate it externally with `ulimit -d <KB>` (same
`RLIMIT_DATA`, same abort-at-cap semantics) inside the systemd-run scope,
or via `prlimit --data`.

## 2026-08-28 12:35 INCIDENT — ROOT CAUSE IDENTIFIED (with evidence)

### Journal evidence

`journalctl --user` 2026-08-28 12:22–12:36 (local, = 19:22–19:36Z):

- 12:22:14 `qe-stress-605874.scope` started: `QE_SPILL_DEBUG=1
  ./target/release/query_engine serve --bind 127.0.0.1:7805 --tables
  data/tpch-10gb-native --memory-limit 200M` (capped scope, 900M).
- 12:27:29 `sjdict-611390.scope` OOM-kill (kernel, memcg 900M):
  `Killed process 611392 (spill_dictionar) total-vm:3240280kB anon-rss:917572kB`.
- 12:28:32 `sjdict-sanity-612397.scope` OOM-kill (memcg 3G):
  `Killed process 612404 (spill_dictionar) total-vm:6386328kB anon-rss:3137324kB`.
- 12:30:28→12:31:44 safe-build rebuild of the dict repro; 12:32:07
  `sjdict-616478.scope` run completes (14.4s CPU, no OOM — the 738MB-peak run).
- 12:33:23→12:34:38 safe-build of `_control_int32_repro`.
- **12:35:18** systemd-oomd: `Killed
  /user.slice/.../vte-spawn-d6d26f79-....scope due to memory pressure for
  /user.slice/user-1000.slice/user@1000.service being 82.92% > 50.00% for
  > 20s`; scope record: `systemd-oomd killed 11 process(es)`,
  `Consumed 17h 51min CPU time, 107.2G memory peak, 1.1G memory swap peak`.
- systemd-oomd's own candidate dump at 12:35:18 shows the vte-spawn scope's
  **Current Memory Usage: 58.4G**, Pressure Avg10=89.04 — i.e. the 107.2G
  figure is the scope's LIFETIME peak (17h51min-CPU-old scope), NOT the
  memory at kill time. The kill-time working set was ~58.4G and climbing.

### What was running (session transcript evidence)

`~/.claude/projects/-media-afilipchik-nvme6tb-src/bee61271-.../subagents/
agent-a045bd48d2571dce6.jsonl` (the spill-size-estimate-fix task-001
subagent) shows, at **2026-08-28T19:34:40Z = 12:34:40 local** — 38 seconds
before the kill:

```
/usr/bin/time -v ./target/release/examples/_control_int32_repro
```

run BARE — no systemd-run scope, no cap, directly inside the terminal's
vte-spawn cgroup. (The same subagent had also run the dictionary repro bare
twice at 19:28:40Z/19:31:46Z — those survived because that repro plateaus
~738MB; the control repro does not plateau.) The PreToolUse enforce hook
did not exist yet — it was created later that day in direct response.

### Verdict (G4)

The 2026-08-28 12:35 incident = `examples/_control_int32_repro` (plain
Int32 60M-row build side, 500MB configured `memory_limit`,
`SpillableHashJoinExec` spill path) executed UNWRAPPED in the terminal's
cgroup. The engine-side mechanism is the SAME memory-accounting hole in
`build_with_partitioning`/`append_batch_streaming` this task's step 1
targets; uncapped, it grew the scope to ≥58.4G (≈80x the 720MB logical
build side) in ≤38s before systemd-oomd killed the terminal. NOT a fourth
undocumented gap — it is the known accounting hole plus the (now
hook-closed) bypass. The "107.2G peak" in the incident record is the
scope's lifetime peak, not this event's own allocation.

Corroboration: later kernel records (19:33:43, 19:48:57 local) show
`_control_int32_` killed at 3G memcg caps with anon-rss ~3.1G,
total-vm ~6.4G — growth is unbounded, cap-limited only.

## PROFILER: found dead-on-arrival, fixed (2 bugs in alloc_profile.rs)

The diagnostic allocator (`QE_ALLOC_PROFILE=1`) had NEVER successfully run
enabled — any binary run with the var set hung at startup (1 thread, futex
wait, ~9MB RSS, 0% CPU; confirmed live on the freshly-rebuilt dict repro).
Root cause: `enabled()` used `OnceLock::get_or_init` with a closure calling
`std::env::var` — `var_os` heap-allocates the RETURNED VALUE whenever the
variable is PRESENT, every heap allocation re-enters `enabled()` (we are
the global allocator), and a reentrant `get_or_init` deadlocks. With the
var UNSET, `var_os` returns `None` without allocating — which is why the
bug hid: disabled runs (all runs to date) never allocated inside init.
Fixed with a tri-state `AtomicU8` that reads "disabled" DURING its own env
read. Second latent deadlock fixed in the same pass: `record_dealloc`
locked `live()` with no reentrancy guard — `record_alloc`'s
`live().lock().insert()` resizing the map's own >=256KB backing table
frees the old table mid-insert → same-thread relock → deadlock (would have
hit any long profiled run once ~10K large allocations were live). Both
fixes validated: profiled dict repro now runs to completion.

## ROOT CAUSE OF THE ACCOUNTING HOLE — profiler evidence (dict repro)

Run: `QE_ALLOC_PROFILE=1 RUST_BACKTRACE=1` dict repro under
`systemd-run -p MemoryMax=2G` → completes, `RESULT: PASS`,
`peak_rss_mb=784` (unprofiled control run same day: 737MB), allocator-level
`global_peak=640.4MB` against a 30MB configured budget.
Log: `.scratch/oom001/dictprof2.log`.

**Dominant call site at EVERY recorded peak (the only site with live
>=256KB allocations, all snapshots):**

```
hashbrown::raw::RawTableInner::fallible_with_capacity
hashbrown::raw::RawTable<T,A>::reserve_rehash
query_engine::physical::operators::spillable::build_hash_table
query_engine::physical::operators::spillable::SpillableHashJoinExec::execute_spill_path
```

Timeline (allocator totals): phase-1 flat collection peaks ~54MB (the
budget, correctly bounded); then `execute_spill_path`'s in-memory-partition
`build_hash_table` loop (lines 868-875 of spillable.rs) climbs 163 → 452MB
BEFORE the `execute_spill_path START` trace prints; spilled-partition
processing (`process_spilled_partition` → `read_parquet` + second
`build_hash_table` call site, line 1240) pushes to the 640MB final peak.
FINAL snapshot: 343MB across 4 live hashbrown tables; the remaining
~300MB of the peak is untracked-small allocations — exactly what the
mechanism predicts (per-key `Vec<HashEntry>` heap buffers + per-key
`JoinKey.values` Vec heap buffers, ~32B each, millions of them).

**Mechanism (quantified):** the spill path's hash tables are
`HashMap<JoinKey, Vec<HashEntry>>` where `JoinKey { values: Vec<JoinValue> }`
and `HashEntry { batch_idx: usize, row_idx: usize }` (16B). For a
unique-Int64-key build side each row costs:
- ~56B amortized in the hashbrown table entry ((24B Vec header for key
  values + 24B Vec header for entries) × 8/7 load factor + control), plus
- one ~32B heap allocation for the key's 1-element `Vec<JoinValue>`, plus
- one ~32B heap allocation for the 1-element `Vec<HashEntry>`,
= **~120-150 bytes/row across 3 allocations, vs the ~12 bytes/row of raw
batch data that `estimate_batch_size`/`total_memory` accounts for — a
~10-20x amplification that is NEVER checked against the memory budget.**

Concretely for the dict repro (30MB limit → 24MB threshold): the 2
in-memory partitions correctly hold ~22.5MB of BATCHES (1,873,454 rows —
the accounting works for batches), but `execute_spill_path` then builds
unbudgeted hash tables over those same rows: 1.87M keys × ~150B ≈ ~280MB,
held live for the entire probe + spilled-partition phase; each spilled
partition read-back (938K rows) adds a transient table + read-back batches
≈ ~150-200MB more. Sum matches the observed 640MB allocator peak / 737MB
RSS ≈ 24x budget.

For the control repro (500MB limit → 400MB threshold): in-memory
partitions hold ~400MB of batches = ~33M rows → the in-memory table build
alone needs 33M × ~130-200B ≈ **~5-7GB**, unbounded by any budget check.

**Confirmed by two further runs:**
- Profiled control repro under 3G memcg cap
  (`.scratch/oom001/ctrlprof.log`): killed (exit 137, kernel memcg record)
  at allocator total 2830.8MB with **1347.5MB tracked across 15 live
  hashbrown tables, all at the same
  `build_hash_table ← execute_spill_path` site** — died mid-way through
  the in-memory-partition table-building loop (the `execute_spill_path
  START` trace, which prints only AFTER that loop, never appeared).
  Untracked remainder ≈ per-key small Vec heap buffers, as predicted.
- Unprofiled control repro under a generous 24G scope cap
  (`.scratch/oom001/ctrlbig.log`): survived table build, reached spilled-
  partition processing (idx=35 of 62), then systemd-oomd killed the scope
  on PRESSURE (67.95% > 60% for 20s) at **7.3G current usage** — i.e. the
  mechanism PLATEAUS around ~7-8GB for this repro (≈15x its 500MB
  budget), matching 33M keys × ~130-200B + ~400MB batches + transients.
  (The kill was pressure-based because the box was heavily loaded — 10G
  free — not because 24G was exceeded.)

Amplification is consistent across both repros: dict 30MB budget → 737MB
peak (~24x); control 500MB budget → ~7.3GB+ (~15x).

**Named root cause: `SpillableHashJoinExec::execute_spill_path` (and
`process_spilled_partition`) build per-partition
`HashMap<JoinKey, Vec<HashEntry>>` hash tables whose real footprint is
~10-20x the batch bytes the spill decision budgeted, with zero memory
accounting — the budget bounds batches, not the join tables built over
them.** The same shape exists in the ALREADY-RULED-IN in-memory fast path
(`HashJoinExec`) but there the whole build side fit the budget by
definition; in the spill path the in-memory partitions are deliberately
kept AT the budget, so the table amplification lands entirely on top.

Also noted: `append_batch_streaming`'s doc comment (spillable.rs
~line 3226) claims the row-count-flush fix made the control case "now
complete cleanly" — contradicted by spill-size-estimate-fix 001's own
Outcome ("still genuinely OOM-killed at the same ~3G cap, unchanged") and
by this session's reruns. The comment is stale/wrong; the Outcome is
correct.

## Next

- [in progress] profiled control repro under 3G cap (confirm site at death)
- [in progress] build examples/oom_cap_harness.rs (+ scripts/oom_cap_harness.sh written)
- [ ] run harness: 4 scenarios × 2 levers, record pre-fix evidence
