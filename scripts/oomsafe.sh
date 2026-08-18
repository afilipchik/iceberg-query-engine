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
#   OOMSAFE_MEMHIGH=48G scripts/oomsafe.sh cargo test ...   # cap builds/tests
#   scripts/oomsafe.sh ./scripts/sf100_full_benchmark.sh     # benchmarks: NO cap
#
# MemoryHigh counts the scope's PAGE CACHE, not just anon memory — a
# capped SF=100 sweep reads ~32GB of parquet into its own cgroup and gets
# reclaim-throttled (+2.2s measured on the suite). Benchmarks therefore
# run scope-only (still the oomd kill target); set OOMSAFE_MEMHIGH only
# for builds/tests where throttling is acceptable.
if [ -n "$OOMSAFE_MEMHIGH" ]; then
  exec systemd-run --user --scope --quiet --collect \
    -p MemoryHigh="$OOMSAFE_MEMHIGH" \
    -- "$@"
fi
exec systemd-run --user --scope --quiet --collect -- "$@"
