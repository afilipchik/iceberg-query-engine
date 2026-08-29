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
#   OOM_HARNESS_LEVERS     default "cgroup rlimit"
#   OOM_HARNESS_TIMEOUT    per-run timeout seconds (default 900)
#   OOM_HARNESS_LOGDIR     default .scratch/oom001/harness_<timestamp>
#   OOM_HARNESS_CAP_AGG    cap for agg/sort (default 1G)
#   OOM_HARNESS_CAP_SCAN   cap for native-scan (default 2G)
#   OOM_HARNESS_CAP_INSERT cap for insert (default 512M)
# plus every QE_HARNESS_* knob examples/oom_cap_harness.rs documents.
set -uo pipefail

cd "$(dirname "$0")/.."
BIN=target/release/examples/oom_cap_harness
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
      # itself; the outer scope (4x the cap, min 4G) is pure containment
      # in case the rlimit lever itself has a hole — the terminal must
      # never be the victim.
      timeout -s KILL "$RUN_TIMEOUT" systemd-run --user --scope --quiet --collect \
        --unit="$unit" \
        -p MemoryMax=8G -p MemorySwapMax=0 \
        -p ManagedOOMMemoryPressure=kill \
        --setenv=QE_MEM_CAP="$cap" \
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
    # Journal evidence for kernel/memcg kills of this exact unit.
    kill_line="$(journalctl --user -q --since "$start_ts" --no-pager 2>/dev/null \
      | grep -F "$unit" | grep -i 'oom' | head -1)"
    [[ -n "$kill_line" ]] && echo "JOURNAL: $kill_line" >>"$log"

    echo "RESULT scenario=$scenario lever=$lever cap=$cap exit=$code peak_rss_mb=$peak_mb verdict=$verdict reason=$reason detail=${detail:-n/a} log=$log"
    [[ "$verdict" == "FAIL" ]] && overall=1
  done
done
exit $overall
