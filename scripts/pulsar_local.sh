#!/usr/bin/env bash
# Local Apache Pulsar standalone — the test broker for the engine's Pulsar
# catalog integration. Same shape as metastore_local.sh: no docker, no sudo,
# the repo's own JDK17, everything under .scratch/pulsar.
#
#   scripts/pulsar_local.sh start|stop|status|wipe
#
# Ports: broker 6650 (binary protocol), admin/web 8085 (8080 is taken on
# this box). Standalone runs with
# -nss -nfw (no stream storage, no function worker) so it starts in seconds
# and stays a single process.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PULSAR_HOME="$ROOT/.scratch/pulsar/apache-pulsar-4.0.5"
JAVA_HOME_DIR="$ROOT/.scratch/metastore/jdk/jdk-17.0.20+8"
STATE="$ROOT/.scratch/pulsar/state"
PIDFILE="$STATE/standalone.pid"
LOG="$STATE/standalone.log"
ADMIN="http://127.0.0.1:8085"

RED='\033[0;31m'; GREEN='\033[0;32m'; CYAN='\033[0;36m'; NC='\033[0m'
info() { echo -e "${CYAN}==>${NC} $*"; }
ok()   { echo -e "${GREEN}PASS${NC} $*"; }
die()  { echo -e "${RED}error:${NC} $*" >&2; exit 1; }

[[ -d "$PULSAR_HOME" ]] || die "pulsar not found at $PULSAR_HOME"
[[ -d "$JAVA_HOME_DIR" ]] || die "jdk17 not found at $JAVA_HOME_DIR (metastore_local.sh installs it)"
export JAVA_HOME="$JAVA_HOME_DIR"
export PATH="$JAVA_HOME/bin:$PATH"

is_up() { curl -sf --max-time 3 "$ADMIN/admin/v2/clusters" >/dev/null 2>&1; }

case "${1:-status}" in
start)
    if is_up; then ok "pulsar already answering on $ADMIN"; exit 0; fi
    mkdir -p "$STATE"
    info "starting pulsar standalone (broker 6650, admin 8085)"
    sed -i 's/^webServicePort=.*/webServicePort=8085/' "$PULSAR_HOME/conf/standalone.conf"
    # A test broker must not garbage-collect idle topics between produce and
    # query (standalone deletes subscription-less inactive topics in ~60s).
    sed -i 's/^brokerDeleteInactiveTopicsEnabled=.*/brokerDeleteInactiveTopicsEnabled=false/' "$PULSAR_HOME/conf/standalone.conf"
    grep -q '^brokerDeleteInactiveTopicsEnabled=false' "$PULSAR_HOME/conf/standalone.conf" \
        || echo 'brokerDeleteInactiveTopicsEnabled=false' >> "$PULSAR_HOME/conf/standalone.conf"
    PULSAR_STANDALONE_USE_ZOOKEEPER="" nohup "$PULSAR_HOME/bin/pulsar" standalone -nss -nfw \
        > "$LOG" 2>&1 &
    echo $! > "$PIDFILE"
    deadline=$((SECONDS + 180))
    while [[ $SECONDS -lt $deadline ]]; do
        if is_up; then
            ok "pulsar up in ${SECONDS}s (pid $(cat "$PIDFILE"))"
            echo -e "${GREEN}pulsar up${NC}: admin $ADMIN  broker pulsar://127.0.0.1:6650"
            exit 0
        fi
        sleep 1
    done
    tail -20 "$LOG"
    die "pulsar never became ready (log: $LOG)"
    ;;
stop)
    if [[ -f "$PIDFILE" ]]; then
        kill "$(cat "$PIDFILE")" 2>/dev/null || true
        # standalone forks java; sweep by home path.
        pkill -f "apache-pulsar-4.0.5" 2>/dev/null || true
        rm -f "$PIDFILE"
        ok "stopped"
    else
        pkill -f "apache-pulsar-4.0.5" 2>/dev/null && ok "stopped (by pattern)" || echo "not running"
    fi
    ;;
status)
    if is_up; then ok "pulsar answering on $ADMIN"; else echo "pulsar: not running"; fi
    ;;
wipe)
    "$0" stop || true
    rm -rf "$PULSAR_HOME/data" "$STATE"
    ok "wiped standalone data"
    ;;
*)
    die "usage: $0 start|stop|status|wipe"
    ;;
esac
