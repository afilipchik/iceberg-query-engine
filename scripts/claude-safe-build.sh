#!/usr/bin/env bash
# claude-safe-build.sh — run a build/test command in an isolated cgroup scope
# so it can NEVER take down the terminal or the Claude session running it.
#
# Background (2026-08-22 incident): `cargo test --release --features lance`
# ran inside the terminal's own cgroup scope, ballooned it to a 105G peak,
# and systemd-oomd killed the whole scope — terminal, Claude session, and
# remote-control bridge included. Isolating the build in its own transient
# scope with a hard MemoryMax makes the build the designated OOM victim:
# if it exceeds the cap, the kernel kills processes inside the build scope
# only, cargo reports a failed/killed job, and the session survives.
#
# Usage:
#   scripts/claude-safe-build.sh cargo test --release --features lance
#   SAFE_BUILD_MEM=48G SAFE_BUILD_JOBS=8 scripts/claude-safe-build.sh cargo bench
#
# Tunables (env):
#   SAFE_BUILD_MEM       hard cap, kernel-OOM inside scope past this (default 80G)
#   SAFE_BUILD_MEM_HIGH  soft throttle cap (default = hard cap, i.e. disabled:
#                        with swap off, a lower soft cap makes overruns crawl
#                        under reclaim throttling instead of failing fast)
#   SAFE_BUILD_JOBS      parallel rustc jobs                          (default 8)
set -euo pipefail

if [[ $# -eq 0 ]]; then
  echo "usage: $0 <command> [args...]" >&2
  exit 2
fi

MEM_MAX="${SAFE_BUILD_MEM:-80G}"
MEM_HIGH="${SAFE_BUILD_MEM_HIGH:-$MEM_MAX}"
JOBS="${SAFE_BUILD_JOBS:-8}"

# --scope: run in-place (stdout/stderr stay attached) but in a fresh
# transient cgroup under app.slice, outside the terminal's vte-spawn scope.
# MemorySwapMax=0: no swap thrash bleeding pressure into the rest of the
# user slice. ManagedOOMMemoryPressure=kill: if systemd-oomd acts on
# sustained pressure, this scope volunteers itself as the victim.
exec systemd-run --user --scope --quiet --collect \
  --unit="safe-build-$$-$RANDOM" \
  -p MemoryMax="$MEM_MAX" \
  -p MemoryHigh="$MEM_HIGH" \
  -p MemorySwapMax=0 \
  -p ManagedOOMMemoryPressure=kill \
  -p ManagedOOMMemoryPressureLimit=80% \
  --setenv=CARGO_BUILD_JOBS="$JOBS" \
  -- "$@"
