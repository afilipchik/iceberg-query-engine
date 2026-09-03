#!/usr/bin/env bash
# oom_cap_harness.sh — oom-safety-hardening epic, task 001 (PRD G6): the ONE
# reusable adversarial memory-cap harness. Runs each scenario of
# examples/oom_cap_harness.rs under BOTH cap levers and emits a
# machine-readable RESULT line per (scenario, lever).
#
#   lever=cgroup : systemd-run --user --scope -p MemoryMax=<cap>
#                  (kernel memcg kill → exit 137 on failure)
#   lever=rlimit : QE_MEM_CAP=<cap> (the binary's own RLIMIT_DATA cap via
#                  enforce_process_memory_cap(); allocation failure → engine
#                  abort, exit 134) — run inside a GENEROUS containment scope
#                  so even a cap bug can never touch the terminal.
#
# Verdict rules (PRD: "a scenario passes only if the engine completes or
# refuses cleanly under BOTH levers"):
#   exit 0 (COMPLETED) or exit 2 (clean named REFUSED) → PASS
#   exit 137 (SIGKILL/memcg), 134 (abort at rlimit), 124 (timeout),
#   anything else → FAIL
#
# EVERY child runs inside its own systemd scope — nothing here can ever put
# the invoking terminal's cgroup at risk. Invoke the script itself through a
# wrapper to satisfy the PreToolUse hook, e.g.:
#   systemd-run --user --scope -p MemoryMax=8G -- scripts/oom_cap_harness.sh
# (the outer scope only contains this script's own negligible footprint; each
# scenario child gets its OWN scope + cap, created below).
#
# Env knobs:
#   OOM_HARNESS_SCENARIOS  default "agg sort native-scan insert"
#                          (also: semi-join anti-join — spill-join-correctness-3
#                          task 004; cap OOM_HARNESS_CAP_JOIN, default 1G)
#   OOM_HARNESS_LEVERS     default "cgroup rlimit"
#   OOM_HARNESS_TIMEOUT    per-run timeout seconds (default 900)
#   OOM_HARNESS_LOGDIR     default .scratch/oom001/harness_<timestamp>
#   OOM_HARNESS_CAP_AGG    cap for agg/sort (default 1G)
#   OOM_HARNESS_CAP_SCAN   cap for native-scan (default 2G)
#   OOM_HARNESS_CAP_INSERT cap for insert (default 512M)
#   OOM_HARNESS_BIN        harness binary (default target/release/examples/oom_cap_harness)
# plus every QE_HARNESS_* knob examples/oom_cap_harness.rs documents.
set -uo pipefail

cd "$(dirname "$0")/.."
# OOM_HARNESS_BIN overrides the harness binary (spill-join-correctness-3 task
# 004: run a pinned pre-fix build of the example through the same driver for
# an honest before/after verdict).
BIN="${OOM_HARNESS_BIN:-target/release/examples/oom_cap_harness}"
if [[ ! -x "$BIN" ]]; then
  echo "ERROR: $BIN not built. Run: scripts/claude-safe-build.sh cargo build --release --example oom_cap_harness" >&2
  exit 2
fi

SCENARIOS="${OOM_HARNESS_SCENARIOS:-agg sort native-scan insert}"
LEVERS="${OOM_HARNESS_LEVERS:-cgroup rlimit}"
RUN_TIMEOUT="${OOM_HARNESS_TIMEOUT:-900}"
LOGDIR="${OOM_HARNESS_LOGDIR:-.scratch/oom001/harness_$(date +%Y%m%d_%H%M%S)}"
mkdir -p "$LOGDIR"

cap_for() {
  case "$1" in
    agg | sort) echo "${OOM_HARNESS_CAP_AGG:-1G}" ;;
    native-scan) echo "${OOM_HARNESS_CAP_SCAN:-2G}" ;;
    insert) echo "${OOM_HARNESS_CAP_INSERT:-512M}" ;;
    semi-join | anti-join) echo "${OOM_HARNESS_CAP_JOIN:-1G}" ;;
    *) echo "1G" ;;
  esac
}

overall=0
for scenario in $SCENARIOS; do
  cap="$(cap_for "$scenario")"
  for lever in $LEVERS; do
    unit="oomharness-${scenario}-${lever}-$$-$RANDOM"
    log="$LOGDIR/${scenario}_${lever}.log"
    start_ts="$(date '+%Y-%m-%d %H:%M:%S')"
    if [[ "$lever" == "cgroup" ]]; then
      timeout -s KILL "$RUN_TIMEOUT" systemd-run --user --scope --quiet --collect \
        --unit="$unit" \
        -p MemoryMax="$cap" -p MemorySwapMax=0 \
        -p ManagedOOMMemoryPressure=kill \
        -- /usr/bin/time -v "$BIN" "$scenario" >"$log" 2>&1
      code=$?
    else
      # rlimit lever: QE_MEM_CAP applies RLIMIT_DATA inside the binary
      # itself; the outer scope is pure containment in case the rlimit
      # lever itself has a hole — the terminal must never be the victim.
      #
      # RLIMIT_DATA counts VIRTUAL private-anonymous mappings, not RSS:
      # mimalloc reserves a ~1GiB arena up front and 32 tokio worker
      # stacks reserve ~0.3G more, none of it resident. Measured on this
      # box: the example fails thread-spawn (EAGAIN, exit 101) at
      # QE_MEM_CAP=1G before the scenario even starts, runs at 1536M,
      # and completed a 1.66GB-RSS CTAS at 2048M (allocations INSIDE the
      # reserved arena don't recount, so usable-beyond-startup is closer
      # to the cap than a naive "cap minus 1.3G" model suggests). So the
      # rlimit lever's cap = scenario cap + fixed 1024MB virtual headroom
      # — enough to get past startup reservations, tight enough that the
      # scenarios' real multi-GB pre-fix appetites still hit it.
      cap_mb="$(numfmt --from=iec "${cap^^}" 2>/dev/null || echo $((1024 * 1024 * 1024)))"
      cap_mb=$((cap_mb / 1024 / 1024))
      rlimit_cap="$((cap_mb + 1024))M"
      timeout -s KILL "$RUN_TIMEOUT" systemd-run --user --scope --quiet --collect \
        --unit="$unit" \
        -p MemoryMax=8G -p MemorySwapMax=0 \
        -p ManagedOOMMemoryPressure=kill \
        --setenv=QE_MEM_CAP="$rlimit_cap" \
        -- /usr/bin/time -v "$BIN" "$scenario" >"$log" 2>&1
      code=$?
    fi

    peak_kb="$(grep -oP 'Maximum resident set size \(kbytes\): \K[0-9]+' "$log" | tail -1)"
    peak_mb=$(( ${peak_kb:-0} / 1024 ))
    # /usr/bin/time exits 128+signal when the child is signalled; the child
    # itself exits 134 on abort. Normalize both spellings.
    detail="$(grep -oE 'HARNESS RESULT: (COMPLETED|REFUSED)[^|]*' "$log" | head -1)"
    case "$code" in
      0) verdict=PASS reason=completed ;;
      2) verdict=PASS reason=clean-refusal ;;
      134) verdict=FAIL reason=abort-at-rlimit ;;
      137) verdict=FAIL reason=oom-sigkill ;;
      124) verdict=FAIL reason=timeout ;;
      *) verdict=FAIL reason="exit-$code" ;;
    esac
    # Journal evidence for kernel/memcg kills of this exact unit. NOTE:
    # the unit name itself contains "oomharness", so the kill filter must
    # match the KILL PHRASES, never just the substring "oom".
    kill_line="$(journalctl --user -q --since "$start_ts" --no-pager 2>/dev/null \
      | grep -F "$unit" \
      | grep -E 'killed by the OOM killer|oom-kill|systemd-oomd killed' | head -1)"
    [[ -n "$kill_line" ]] && echo "JOURNAL: $kill_line" >>"$log"
    # A memcg kill can surface as other codes (a fast kill takes
    # /usr/bin/time down with the scope before it can report → 143 or
    # empty output) — reclassify from direct evidence. The abort match is
    # the EXACT Rust OOM-abort message; never bare "abort" (the mem-cap
    # startup banner contains the word "aborts").
    if [[ "$verdict" == FAIL ]]; then
      if grep -qiE 'terminated by signal 9' "$log" || [[ -n "$kill_line" ]]; then
        reason=oom-sigkill
      elif grep -qE 'memory allocation of [0-9]+ bytes failed' "$log"; then
        reason=abort-at-rlimit
      fi
    fi

    echo "RESULT scenario=$scenario lever=$lever cap=$cap exit=$code peak_rss_mb=$peak_mb verdict=$verdict reason=$reason detail=${detail:-n/a} log=$log"
    [[ "$verdict" == "FAIL" ]] && overall=1
  done
done
exit $overall
