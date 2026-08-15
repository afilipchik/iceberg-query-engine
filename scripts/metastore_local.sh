#!/usr/bin/env bash
# Local Apache Gravitino metastore harness.
#
# Runs a single Gravitino 1.3.0 server on 127.0.0.1:8090 from a user-space
# tarball (no docker, no sudo, no system Java). The JDK and the Gravitino
# distribution live under .scratch/metastore/ and the embedded H2 entity
# store writes to .scratch/metastore/data — nothing outside the repo is
# touched. The Rust engine integrates against the REST API at
# http://127.0.0.1:8090/api (see /api/version, /api/metalakes, ...).
#
# Usage:
#   scripts/metastore_local.sh start     Start the server (idempotent)
#   scripts/metastore_local.sh stop      Stop the server (idempotent)
#   scripts/metastore_local.sh status    Show process + REST liveness
#   scripts/metastore_local.sh wipe      Stop and erase all metastore state
#
# One-time setup performed elsewhere (already done in this checkout):
#   - Temurin JDK 17 extracted under .scratch/metastore/jdk/jdk-*/
#   - gravitino-1.3.0-bin extracted under .scratch/metastore/
#   - conf/gravitino.conf patched: webserver.host=127.0.0.1, H2 storagePath
#     under .scratch/metastore/data, aux services disabled.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." >/dev/null && pwd)"

MS_DIR="$REPO_ROOT/.scratch/metastore"
GRAVITINO_HOME_DIR="$MS_DIR/gravitino-1.3.0-bin"
STATE_DIR="$MS_DIR"
PID_FILE="$STATE_DIR/gravitino.pid"
LOG_DIR="$MS_DIR/logs"
HOST="127.0.0.1"
PORT="${METASTORE_PORT:-8090}"
URL="http://$HOST:$PORT"

RED='\033[0;31m'; GREEN='\033[0;32m'; CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'

die()  { echo -e "${RED}error:${NC} $*" >&2; exit 1; }
info() { echo -e "${CYAN}==>${NC} $*"; }
ok()   { echo -e "${GREEN}PASS${NC} $*"; }

command -v curl >/dev/null 2>&1 || die "curl is required"

find_jdk() {
    local jdk
    jdk="$(ls -d "$MS_DIR"/jdk/jdk-*/ 2>/dev/null | head -1 || true)"
    [[ -n "$jdk" && -x "${jdk}bin/java" ]] \
        || die "no JDK under $MS_DIR/jdk/ (expected an extracted Temurin jdk-17 tarball)"
    echo "${jdk%/}"
}

# Pid of the running GravitinoServer belonging to THIS checkout, if any.
# The launcher double-forks (nohup), so the pid file alone is advisory; the
# authoritative check greps for the server class pinned to this GRAVITINO_HOME.
server_pid() {
    # `|| true`: under pipefail an unmatched grep would fail the pipeline and,
    # with set -e, silently kill the whole script when the server is down.
    ps xww \
        | { grep -F 'org.apache.gravitino.server.GravitinoServer' || true; } \
        | { grep -F "$GRAVITINO_HOME_DIR" || true; } \
        | grep -v grep | awk '{print $1}' | head -1 || true
}

wait_for_api() {
    local timeout="${1:-90}"
    local deadline=$((SECONDS + timeout))
    while [[ $SECONDS -lt $deadline ]]; do
        if curl -s --max-time 2 "$URL/api/version" 2>/dev/null | grep -q '"version"'; then
            return 0
        fi
        sleep 0.3
    done
    return 1
}

export_env() {
    JAVA_HOME="$(find_jdk)"
    export JAVA_HOME
    export GRAVITINO_HOME="$GRAVITINO_HOME_DIR"
    export GRAVITINO_LOG_DIR="$LOG_DIR"
}

# ── start ────────────────────────────────────────────────────────────────────

cmd_start() {
    [[ -d "$GRAVITINO_HOME_DIR" ]] \
        || die "Gravitino not found at $GRAVITINO_HOME_DIR (extract gravitino-1.3.0-bin.tar.gz there)"
    local pid; pid="$(server_pid)"
    if [[ -n "$pid" ]]; then
        info "Gravitino already running (pid $pid)"
        echo -e "${GREEN}metastore up${NC}: $URL  (REST base: $URL/api)"
        return 0
    fi

    export_env
    mkdir -p "$LOG_DIR" "$MS_DIR/data"
    local t0=$SECONDS
    info "starting Gravitino 1.3.0 (JAVA_HOME=$JAVA_HOME)"
    "$GRAVITINO_HOME_DIR/bin/gravitino.sh" start >/dev/null

    pid="$(server_pid)"
    [[ -n "$pid" ]] || { tail -30 "$LOG_DIR/gravitino-server.out" 2>/dev/null || true
                         die "Gravitino process did not appear (see $LOG_DIR)"; }
    echo "$pid" > "$PID_FILE"

    info "waiting for $URL/api/version"
    if ! wait_for_api 90; then
        tail -30 "$LOG_DIR/gravitino-server.out" 2>/dev/null || true
        die "server never answered /api/version (log: $LOG_DIR/gravitino-server.log)"
    fi
    ok "REST API is answering (started in $((SECONDS - t0))s, pid $pid)"
    echo -e "${GREEN}metastore up${NC}: $URL  (REST base: $URL/api)"
}

# ── stop ─────────────────────────────────────────────────────────────────────

cmd_stop() {
    local pid; pid="$(server_pid)"
    if [[ -z "$pid" ]]; then
        info "Gravitino is not running"
        rm -f "$PID_FILE"
        return 0
    fi
    info "stopping Gravitino (pid $pid)"
    kill -TERM "$pid" 2>/dev/null || true
    local deadline=$((SECONDS + 30))
    while [[ $SECONDS -lt $deadline ]] && kill -0 "$pid" 2>/dev/null; do sleep 0.3; done
    if kill -0 "$pid" 2>/dev/null; then
        info "SIGTERM ignored, sending SIGKILL"
        kill -KILL "$pid" 2>/dev/null || true
    fi
    rm -f "$PID_FILE"
    info "stopped"
}

# ── status ───────────────────────────────────────────────────────────────────

cmd_status() {
    local pid; pid="$(server_pid)"
    if [[ -z "$pid" ]]; then
        echo "Gravitino: not running"
        return 1
    fi
    echo -e "${BOLD}Gravitino: running${NC} (pid $pid) at $URL"
    local body
    body="$(curl -s --max-time 5 "$URL/api/version" || true)"
    if [[ -n "$body" ]]; then
        ok "$URL/api/version -> $body"
    else
        echo -e "${RED}FAIL${NC} process alive but $URL/api/version is not answering"
        return 1
    fi
}

# ── wipe ─────────────────────────────────────────────────────────────────────

cmd_wipe() {
    cmd_stop
    info "wiping metastore state ($MS_DIR/data, logs, distribution-local data)"
    rm -rf "$MS_DIR/data" "$LOG_DIR" "$GRAVITINO_HOME_DIR/data" "$GRAVITINO_HOME_DIR/logs"
    mkdir -p "$MS_DIR/data" "$LOG_DIR"
    info "wiped (next start begins with an empty entity store)"
}

CMD="${1:-help}"
case "$CMD" in
    start)  cmd_start ;;
    stop)   cmd_stop ;;
    status) cmd_status ;;
    wipe)   cmd_wipe ;;
    *)
        sed -n '2,21p' "$0" | sed 's/^# \?//'
        ;;
esac
