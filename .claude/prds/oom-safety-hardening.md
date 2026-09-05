---
name: oom-safety-hardening
description: Close every remaining path where the engine can be killed by the OS OOM killer instead of spilling to disk or refusing cleanly, and prove it under real memory caps
status: backlog
created: 2026-08-29T02:18:54Z
---

# PRD: oom-safety-hardening

## Executive Summary

CLAUDE.md already states the engine's own Memory Safety Rule: "THE ENGINE
MUST BE MEMORY-SAFE BY DEFAULT. OOM IS NEVER ACCEPTABLE." In practice this
is only partially true today. A real OOM kill on 2026-08-28 (systemd-oomd,
107.2G peak, the whole terminal scope killed) happened while investigating
native-table query behavior, and a code audit done the same day confirms
the guarantee has known, documented holes:

1. `SpillableHashAggregateExec::execute` (`src/physical/operators/
   spillable.rs:1850`) and `ExternalSortExec::execute`
   (`spillable.rs:2184`) both still call `collect_input_partitions_
   concurrently` — the exact "materialize everything, then decide whether
   to spill" pattern that was already found and fixed for
   `SpillableHashJoinExec` (`spill-join-correctness-2` epic, task 002, a
   Photon-style streaming two-phase reservation). Aggregation and sort
   never got the equivalent fix — confirmed by direct code inspection,
   not assumed.
2. `NativeTable::scan()` (`src/storage/native_table.rs:667`) is not
   incremental — it materializes every active segment into one
   `Vec<RecordBatch>`. It is protected only by a pre-flight admission
   check (`check_scan_budget`, line 204) that REFUSES cleanly before
   touching data if the active segment set's on-disk size exceeds
   `memory_limit * spill_threshold` — this avoids an OOM, but it does not
   let a query that needs more memory than the budget actually COMPLETE
   the way a spilling operator would. A native-table scan larger than its
   configured budget cannot run at any speed; it can only refuse.
3. `create_table_as_select`/`insert_into_native_table`
   (`src/execution/context.rs`) merge per-partition streams via
   `bounded_partition_merge` with a concurrency cap
   (`QE_INSERT_MERGE_CONCURRENCY`, default 8) — this cut peak RSS 70%
   (SF=10: 5.38GB→1.63GB; SF=100: 5.86GB→1.67GB) but has no FORMAL
   admission check the way the read path does. Already-measured: a 1-2GB
   cgroup cap now completes; a 512MB cap still gets `SIGKILL`ed
   (documented in CLAUDE.md's own "Mutation: memory safety" section).
4. `SpillableHashJoinExec`'s own build-side size ESTIMATE
   (`estimate_batch_size`) is separately known to be wrong for
   Dictionary-typed columns by up to ~4,000x (a ~42MB build side reported
   as ~167.7GB) — the `spill-size-estimate-fix` epic exists for
   this specific bug but is at 0% as of 2026-08-29 (both tasks open,
   nobody on it); this epic sequences that work FIRST (the estimate
   feeds every spill decision the fixes below make) rather than waiting
   on a separate timeline.
5. There is a SEPARATE, already-open, unconfirmed-root-cause correctness
   bug in `SpillableHashJoinExec`'s spill/partition/probe logic (a
   ~0.34%-rate duplicate-row-counting bug, `spill-join-correctness`/
   `spill-join-correctness-2` epics) — this is a WRONG ANSWER bug, not an
   OOM bug, and stays explicitly out of this PRD's scope; it is tracked
   separately and any future root-causing work belongs there.

This PRD closes gaps 1-3 (the OOM-shaped gaps), builds on gap 4's fix
once it lands, and explicitly excludes gap 5 (a correctness bug, wrong
class of problem for this PRD).

**Update 2026-08-29 (after this PRD was written): a new in-binary
containment layer now exists and this epic builds on it.**
`enforce_process_memory_cap()` (`src/execution/memory.rs`, called first
thing in `main()`) hard-caps the process via `setrlimit(RLIMIT_DATA)` —
64G default, sized with `QE_MEM_CAP` (no unlimited spelling). Verified
live: a 1G-capped `generate --sf 1` aborts the ENGINE (exit 134) with
the terminal untouched. Two consequences for this epic: (a) the blast
radius of any remaining OOM gap is now the engine process, never the
terminal — but an engine ABORT at the cap is still a failure of the
Memory Safety Rule ("complete by spilling or refuse cleanly"), so every
gap below remains real work; (b) `QE_MEM_CAP` is a second, cheaper
adversarial-test lever alongside `systemd-run -p MemoryMax=` — the G6
harness should exercise BOTH (rlimit trips on mapped anonymous bytes,
the cgroup cap on RSS; they catch overshoot at different points).

## Problem Statement

The engine's advertised guarantee — "being slow on a larger-than-memory
dataset is acceptable, crashing is not" — does not hold everywhere yet.
Three concrete operator/path classes can still either be killed by the
OS OOM killer (aggregation, sort) or can only refuse rather than
complete (native-table reads; native-table writes have no formal
admission check at all, only an empirically-tuned concurrency cap).
Nothing currently PROVES, end-to-end and under a real memory cap, that
every spillable code path in the engine actually stays within its
configured `--memory-limit`. The 2026-08-28 incident is exactly the
failure mode this PRD exists to close: an in-budget-looking operation
that in fact grew past its cap because some path in the chain wasn't
covered by either a streaming/spilling execution strategy or a
pre-flight admission check.

## User Stories

**As someone running a query engine process with a configured
`--memory-limit`,** I want the process to never be killed by the OS OOM
killer regardless of how large my data or how memory-hungry my query is
— it should either complete (more slowly, by spilling) or refuse
cleanly with a named error, and nothing else.
- Acceptance: for every operator class this PRD covers, a real,
  hardware-backed test (a genuine `systemd-run --scope -p
  MemoryMax=<tight>` cap, matching this codebase's own established
  verification pattern from `spill-join-correctness-2`'s
  `examples/spill_join_oom_repro.rs`) shows the process completes or
  refuses cleanly — never `SIGKILL`/exit-code-137'd by the kernel.

**As someone querying a native table larger than the configured memory
budget,** I want the engine to spill/stream through the excess rather
than refuse the query outright, whenever the query shape allows it (a
scan feeding an aggregate or a join, not just a raw dump).
- Acceptance: a native-table query whose source segments exceed the
  current `check_scan_budget` threshold completes successfully (slower,
  with real spill activity observable via `QE_SPILL_DEBUG=1`) instead of
  being refused, for at least the aggregate and join consumer shapes.

**As someone running `INSERT`/`CREATE TABLE ... AS SELECT` against a
native table from a large source,** I want a formal, named admission
check on this path (matching the read path's `check_scan_budget`), not
just an empirically-tuned concurrency knob that happens to reduce peak
RSS.
- Acceptance: a write whose genuine working set cannot fit under
  `memory_limit * spill_threshold` is refused cleanly and by name before
  doing expensive work, exactly like `check_scan_budget` does for reads;
  a write that DOES fit completes with no regression to the already-
  measured 70% RSS reduction.

## Functional Requirements

1. Give `SpillableHashAggregateExec` and `ExternalSortExec` the same
   streaming, two-phase-reservation execution strategy
   `SpillableHashJoinExec::execute_spill_path` already has — read input
   batch by batch with a running size total, decide to spill DURING
   ingestion rather than after collecting everything, so a build side
   that would OOM before the spill decision runs today instead spills
   cleanly. Mirror the shape of the join's own fix; do not invent a new
   pattern.
2. Make `NativeTable::scan()` (or a new physical operator wrapping it,
   analogous to `StreamingParquetScanExec` vs. `ParquetTable::scan()`)
   capable of STREAMING a segment set larger than `check_scan_budget`'s
   threshold into a consumer that can itself spill (aggregate, join),
   rather than refusing outright. A raw, unaggregated `SELECT *` over an
   oversized table may still need to refuse (there is no way to spill a
   full materialization the caller explicitly asked for) — document that
   boundary explicitly rather than silently narrowing scope.
3. Add a formal, named pre-flight admission check to the `INSERT`/CTAS
   write path (`create_table_as_select`/`insert_into_native_table` in
   `src/execution/context.rs`), matching `check_scan_budget`'s shape and
   error style, that refuses cleanly when a source's estimated working
   set cannot fit under the budget — closing the gap left by task 005 of
   `native-tables-mutation` (bounded but not admission-controlled).
4. Fold in `spill-size-estimate-fix`'s Dictionary-column size-estimate
   fix once that epic completes (no new code — a dependency, not a
   reimplementation).
5. Build ONE reusable adversarial verification harness — a real
   `systemd-run --scope -p MemoryMax=<N>` wrapper plus a battery of
   queries chosen to stress each covered operator (a genuinely large
   aggregate GROUP BY, a genuinely large `ORDER BY`, a native-table scan
   over budget feeding an aggregate, a large `INSERT`/CTAS) — and run it
   against every operator this PRD touches. Reuse
   `examples/spill_join_oom_repro.rs`'s pattern rather than inventing a
   new harness shape per operator.
6. Root-cause, as a diagnostic side task (not necessarily a full fix if
   it turns out to be environmental), what specifically drove the
   2026-08-28 107.2G spike — confirm which operator/path was actually
   responsible rather than assuming it was one of the three gaps named
   above. If it turns out to be a fourth, previously-undocumented gap,
   name it and fold a fix into this epic's scope.

## Non-Functional Requirements

- **Cell-exact correctness preserved.** Every fix here changes WHEN/
  HOW a path spills or streams, never the rows it produces. Validate
  against the existing DuckDB-oracle and byte-for-byte reference
  patterns already established in this codebase (`spill_tests.rs`,
  `native_delete_tests.rs`, etc.) — no new tolerance, no new adaptation.
- **No performance regression to the already-measured, non-adversarial
  case.** The 2026-08-23 native-tables benchmark numbers
  (`5.324s`/`1.23x` SF=10 total, excluding Q12) and the 70% RSS
  reduction from `native-tables-mutation` task 005 must not regress —
  re-run and re-confirm both as part of this epic's close-out.
- **Every build/test/bench/repro command in this epic runs through
  `scripts/claude-safe-build.sh`, and every ad-hoc heavy command (repro
  scripts, `serve` invocations against real data) runs through
  `systemd-run --user --scope -p MemoryMax=...` or the equivalent — this
  is now enforced by a `PreToolUse` hook
  (`scripts/claude_hooks/enforce_safe_build.sh`), not just documented.**
  Every adversarial "does this actually spill instead of OOM" test in
  this epic must use a REAL, tight memory cap and confirm via `ps`/
  `journalctl -k`/exit code that no OOM kill occurred — never assume
  from code reading alone.
- Explicitly does NOT change the spill file format, the 80%-of-
  memory-limit spill threshold's own percentage, or
  `SpillableHashJoinExec`'s spill decision logic (already fixed
  separately) beyond what `spill-size-estimate-fix` lands.

## Success Criteria

- G1: `SpillableHashAggregateExec` and `ExternalSortExec` no longer
  fully materialize their input before deciding to spill — verified by
  a real, hardware-backed adversarial test (tight `MemoryMax` cap) that
  fails on the CURRENT code and passes after the fix, mirroring
  `examples/spill_join_oom_repro.rs`'s own pre-fix/post-fix pattern.
- G2: at least the aggregate and join consumer shapes can complete a
  native-table query whose source exceeds `check_scan_budget`'s current
  threshold, via real spilling, instead of being refused. The
  raw-materialization boundary (if any remains) is explicitly documented,
  not silently narrowed.
- G3: the `INSERT`/CTAS write path has a formal, named pre-flight
  admission check; an oversized source is refused cleanly and by name,
  not `SIGKILL`ed; the existing 70% RSS reduction is unregressed.
- G4: the 2026-08-28 incident's actual root cause is identified and
  named (not assumed) — either it is one of G1-G3's own fixes, or a
  distinct fourth gap that gets its own fix inside this epic.
- G5: cell-exact correctness preserved everywhere touched; full suite
  green in all four feature combinations (default/lance/gpu/pulsar); no
  regression to the recorded native-table/non-adversarial performance
  numbers named above.
- G6: one reusable, documented adversarial harness exists and is run
  against every operator this PRD covers, with real before/after
  evidence (not just code review) for each.

## Constraints & Assumptions

- Builds on `spill-join-correctness-2`'s already-shipped streaming
  two-phase reservation for `SpillableHashJoinExec` as the reference
  pattern for G1 — do not re-derive the approach.
- Builds on `spill-size-estimate-fix` (in progress) for the Dictionary-
  column estimate bug; this PRD's own task 1 depends on that epic being
  merged, and folds its result in rather than duplicating it.
- The already-open ~0.34%-rate duplicate-row-counting bug in
  `SpillableHashJoinExec` (correctness, not OOM) is explicitly OUT OF
  SCOPE — do not attempt to root-cause or fix it here even if work in
  this epic touches nearby code; if a fix here happens to shed new
  light on it, document the observation and hand it back to that bug's
  own tracking, don't silently absorb it into this epic's scope.
- This machine's own environment-level guardrail
  (`scripts/claude_hooks/enforce_safe_build.sh`) prevents a BYPASSED
  command from taking down the terminal, but does not make the ENGINE
  itself memory-safe — that guardrail and this PRD solve two different
  layers of the same incident class and both are needed.

## Out of Scope

- The `SpillableHashJoinExec` duplicate-row-counting correctness bug
  (tracked in `spill-join-correctness`/`spill-join-correctness-2`).
- Full compaction / deletion-vector-density fixes for native tables
  (a separate, already-named future epic).
- Changing the spill file format or the 80% spill-threshold percentage.
- GPU-path memory safety (GPU offload already never engages in
  distributed/serve contexts by design, and VRAM budget/eviction was
  already solved in `native-tables-tiering`).
- Distributed/scatter-path memory admission control (each node already
  inherits `--memory-limit` locally; a cluster-wide admission story is a
  separate, larger effort not attempted here).

## Dependencies

- `src/physical/operators/spillable.rs` (`SpillableHashAggregateExec`,
  `ExternalSortExec`, and the already-fixed `SpillableHashJoinExec` as
  reference).
- `src/storage/native_table.rs` (`check_scan_budget`, `scan`).
- `src/execution/context.rs` (`create_table_as_select`,
  `insert_into_native_table`, `bounded_partition_merge`).
- `spill-size-estimate-fix` epic (in progress) — task 1 of this epic
  depends on it merging first.
- `examples/spill_join_oom_repro.rs` — the adversarial-cap pattern to
  reuse for G6's harness.
- `scripts/claude-safe-build.sh` / `scripts/claude_hooks/
  enforce_safe_build.sh` for every build and every ad-hoc heavy command
  in this epic.
