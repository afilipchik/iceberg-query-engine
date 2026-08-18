#!/bin/bash
# Run a heavy command (benchmark, cargo build/test) in its own transient
# systemd user scope.
#
# Why: 2026-08-17 20:40, systemd-oomd killed the ENTIRE terminal scope —
# 47 processes including the interactive Claude Code session — because
# user-slice memory pressure sat above 50% for >20s during an SF=100
# benchmark + release-LTO test-link window. oomd kills the scope with the
# reclaim activity; when the heavy job runs in its OWN scope, that scope
# is the kill target and the terminal survives. A MemoryHigh cap keeps
# the job's reclaim pressure inside its own cgroup in the first place.
#
# Usage: scripts/oomsafe.sh <command...>
#   OOMSAFE_MEMHIGH=70G scripts/oomsafe.sh ./target/release/query_engine ...
MEM_HIGH="${OOMSAFE_MEMHIGH:-96G}"
exec systemd-run --user --scope --quiet --collect \
  -p MemoryHigh="$MEM_HIGH" \
  -- "$@"
