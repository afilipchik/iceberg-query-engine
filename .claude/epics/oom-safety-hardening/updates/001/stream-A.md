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

## Next

- [in progress] rebuild repros with current alloc_profile.rs
- [ ] profile dict repro (QE_ALLOC_PROFILE=1, completes at ~738MB under 900M cap)
- [ ] profile control repro under caps, iterate to catch peak
- [ ] name dominant call site(s)
- [ ] build oom_cap_harness (4 scenarios × 2 levers)
