#!/usr/bin/env bash
# Local multi-process cluster harness.
#
# Runs N *separate OS processes* of `query_engine serve` on one host, wired
# together with a static peer list, and drives them over real TCP. This is the
# M1 testbed: kind/Docker cannot run on the development machine (no docker
# daemon, no passwordless sudo, and unprivileged user namespaces are blocked by
# kernel.apparmor_restrict_unprivileged_userns=1), so N processes over loopback
# is the highest fidelity available here. It exercises the same things pods do —
# separate address spaces, real sockets, real serialization, real partial
# failure — and it is what the acceptance gate is actually run against.
#
# Usage:
#   scripts/cluster_local.sh start [N]        Start N nodes (default 3)
#   scripts/cluster_local.sh status           Print each node's /cluster view
#   scripts/cluster_local.sh query "<SQL>"    Run SQL on every node, diff results
#   scripts/cluster_local.sh verify           Run the full M1 acceptance gate
#   scripts/cluster_local.sh kill <i>         SIGTERM node i
#   scripts/cluster_local.sh stop             Stop everything
#
# Options (env or flags): --data DIR (default ./data/tpch-1mb), --base-port P
# (default 17700), --binary PATH.

set -euo pipefail

NODES=3
DATA_DIR="${QE_DATA:-./data/tpch-1mb}"
BASE_PORT="${QE_BASE_PORT:-17700}"
BINARY="${QE_BINARY:-./target/release/query_engine}"
STATE_DIR=".scratch/cluster-local"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'

die()  { echo -e "${RED}error:${NC} $*" >&2; exit 1; }
info() { echo -e "${CYAN}==>${NC} $*"; }
ok()   { echo -e "${GREEN}PASS${NC} $*"; }
bad()  { echo -e "${RED}FAIL${NC} $*"; }

command -v curl >/dev/null 2>&1 || die "curl is required"

CMD="${1:-help}"; shift || true

# Positional N for `start`, then flags.
if [[ "${1:-}" =~ ^[0-9]+$ ]]; then NODES="$1"; shift; fi
while [[ $# -gt 0 ]]; do
    case "$1" in
        --data)      DATA_DIR="$2"; shift 2 ;;
        --base-port) BASE_PORT="$2"; shift 2 ;;
        --binary)    BINARY="$2"; shift 2 ;;
        --nodes)     NODES="$2"; shift 2 ;;
        *)           break ;;
    esac
done

port_of() { echo $((BASE_PORT + $1)); }
addr_of() { echo "127.0.0.1:$(port_of "$1")"; }

peer_list() {
    local n="$1" out=""
    for ((i = 0; i < n; i++)); do
        out+="127.0.0.1:$(port_of "$i")"
        [[ $i -lt $((n - 1)) ]] && out+=","
    done
    echo "$out"
}

running_nodes() {
    [[ -f "$STATE_DIR/nodes" ]] || return 0
    cat "$STATE_DIR/nodes"
}

# The membership view of node $1, reduced to the fields every node must agree
# on. Only the `members` array: the `node` block above it describes the
# responder and legitimately differs, and timestamps cannot agree.
member_view() {
    curl -s --max-time 5 "http://$(addr_of "$1")/cluster" \
        | sed -n '/"members"/,$p' \
        | grep -E '"(address|node_id|status)"' | tr -d ' '
}

# Wait until every one of $1 nodes reports $1 members, all `up`. "Cluster up"
# means converged, not merely "processes exist" — checking earlier is how a
# gate becomes flaky and then gets ignored.
wait_converged() {
    local n="$1" timeout="${2:-60}"
    local deadline=$((SECONDS + timeout))
    while [[ $SECONDS -lt $deadline ]]; do
        local all_ok=1
        for ((i = 0; i < n; i++)); do
            local body; body="$(curl -s --max-time 5 "http://$(addr_of "$i")/cluster" || true)"
            local count ups
            count="$(echo "$body" | grep -o '"member_count": *[0-9]*' | grep -o '[0-9]*$' || echo 0)"
            ups="$(echo "$body" | sed -n '/"members"/,$p' | grep -c '"status": *"up"' || true)"
            [[ "$count" == "$n" && "$ups" == "$n" ]] || all_ok=0
        done
        [[ $all_ok -eq 1 ]] && return 0
        sleep 0.3
    done
    return 1
}

# ── start ────────────────────────────────────────────────────────────────────

cmd_start() {
    [[ -x "$BINARY" ]] || die "binary not found at $BINARY (cargo build --release)"
    [[ -d "$DATA_DIR" ]] || die "data directory not found: $DATA_DIR"
    for t in nation region part supplier partsupp customer orders lineitem; do
        [[ -f "$DATA_DIR/$t.parquet" ]] || die "$DATA_DIR is missing $t.parquet"
    done

    if [[ -f "$STATE_DIR/nodes" ]]; then
        info "a cluster is already recorded; stopping it first"
        cmd_stop || true
    fi
    mkdir -p "$STATE_DIR"
    echo "$NODES" > "$STATE_DIR/nodes"
    echo "$BASE_PORT" > "$STATE_DIR/base_port"

    local peers; peers="$(peer_list "$NODES")"
    info "starting $NODES nodes on ports $(port_of 0)..$(port_of $((NODES - 1)))"
    info "peer list: $peers"

    for ((i = 0; i < NODES; i++)); do
        "$BINARY" serve \
            --bind "127.0.0.1:$(port_of "$i")" \
            --node-id "$i" \
            --peers "$peers" \
            --data "$DATA_DIR" \
            --discovery-interval-ms 500 \
            > "$STATE_DIR/node$i.log" 2>&1 &
        echo $! > "$STATE_DIR/node$i.pid"
        echo "    node $i  pid $(cat "$STATE_DIR/node$i.pid")  $(addr_of "$i")  log $STATE_DIR/node$i.log"
    done

    info "waiting for /readyz on every node"
    for ((i = 0; i < NODES; i++)); do
        local deadline=$((SECONDS + 120)) code=""
        while [[ $SECONDS -lt $deadline ]]; do
            code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 \
                    "http://$(addr_of "$i")/readyz" 2>/dev/null || true)"
            [[ "$code" == "200" ]] && break
            sleep 0.2
        done
        [[ "$code" == "200" ]] || {
            echo "--- node $i log ---"; tail -30 "$STATE_DIR/node$i.log"
            die "node $i never became ready (last /readyz: ${code:-no response})"
        }
        echo "    node $i ready"
    done

    info "waiting for membership convergence"
    wait_converged "$NODES" 60 \
        || die "nodes started but never converged on a $NODES-member view (see $STATE_DIR/node*.log)"
    echo -e "${GREEN}cluster up${NC}: $NODES nodes, entry point http://$(addr_of 0)"
}

# ── status ───────────────────────────────────────────────────────────────────

cmd_status() {
    local n; n="$(running_nodes)"
    [[ -n "$n" ]] || die "no cluster recorded in $STATE_DIR (run: $0 start)"
    BASE_PORT="$(cat "$STATE_DIR/base_port")"
    for ((i = 0; i < n; i++)); do
        echo -e "${BOLD}--- node $i  http://$(addr_of "$i")/cluster ---${NC}"
        curl -s --max-time 5 "http://$(addr_of "$i")/cluster" || echo "(unreachable)"
        echo
    done
}

# ── query ────────────────────────────────────────────────────────────────────

cmd_query() {
    local sql="${1:-}"
    [[ -n "$sql" ]] || die "usage: $0 query \"<SQL>\""
    local n; n="$(running_nodes)"; BASE_PORT="$(cat "$STATE_DIR/base_port")"
    mkdir -p "$STATE_DIR/results"

    for ((i = 0; i < n; i++)); do
        curl -s -X POST --data "$sql" \
             "http://$(addr_of "$i")/sql?format=csv" \
             -o "$STATE_DIR/results/node$i.csv" \
             -w "node $i: HTTP %{http_code}\n"
    done
    echo -e "${BOLD}--- node 0 result ---${NC}"
    cat "$STATE_DIR/results/node0.csv"

    local same=1
    for ((i = 1; i < n; i++)); do
        if ! diff -q "$STATE_DIR/results/node0.csv" "$STATE_DIR/results/node$i.csv" >/dev/null; then
            same=0
            bad "node $i differs from node 0"
            diff "$STATE_DIR/results/node0.csv" "$STATE_DIR/results/node$i.csv" | head -20
        fi
    done
    [[ $same -eq 1 ]] && ok "all $n nodes returned byte-identical results"
}

# ── verify: the M1 acceptance gate ───────────────────────────────────────────

cmd_verify() {
    local n; n="$(running_nodes)"
    [[ -n "$n" ]] || die "no cluster running (run: $0 start)"
    BASE_PORT="$(cat "$STATE_DIR/base_port")"
    local failures=0

    echo -e "${BOLD}=== M1 acceptance gate, $n local processes ===${NC}"

    # 1. Identical membership view on every node.
    info "1/4  /cluster agrees on every node"
    if ! wait_converged "$n" 60; then
        bad "the cluster never converged on an all-up $n-member view"
        failures=$((failures + 1))
    fi
    local ref="" agree=1
    for ((i = 0; i < n; i++)); do
        local view; view="$(member_view "$i")"
        if [[ -z "$ref" ]]; then ref="$view"; fi
        if [[ "$view" != "$ref" ]]; then
            agree=0
            bad "node $i has a different membership view"
            diff <(echo "$ref") <(echo "$view") || true
            failures=$((failures + 1))
        fi
    done
    local count
    count="$(curl -s "http://$(addr_of 0)/cluster" | grep -o '"member_count": *[0-9]*' | grep -o '[0-9]*$')"
    if [[ "$count" != "$n" ]]; then
        bad "member_count is $count, expected $n"; failures=$((failures + 1))
    elif [[ $agree -eq 1 ]]; then
        ok "all $n nodes report the same $n-member view (address/node_id/status)"
    fi

    # 2. Every node's TPC-H answers are byte-identical to the single-process
    #    binary's. The reference is produced by `benchmark-parquet --save-csv`,
    #    which never touches the server code at all — the comparison is against
    #    the engine as it has always run, not against another server.
    info "2/4  TPC-H results match the single-process binary, byte for byte"
    mkdir -p "$STATE_DIR/verify" "$STATE_DIR/ref"
    for q in 1 3 6 10 12; do
        local qq; qq="$(printf 'q%02d' "$q")"
        local sql
        sql="$("$BINARY" query --num "$q" --sf 0.001 2>/dev/null \
               | awk '/^Query:$/{f=1;next} /^Schema:/{f=0} f')"
        if [[ -z "${sql// }" ]]; then
            bad "could not extract the SQL text for Q$q"; failures=$((failures + 1)); continue
        fi

        "$BINARY" benchmark-parquet --path "$DATA_DIR" --query "$q" \
            --save-csv "$STATE_DIR/ref" > /dev/null 2>&1 \
            || { bad "single-process reference run failed for Q$q"; failures=$((failures + 1)); continue; }

        local agree=1
        for ((i = 0; i < n; i++)); do
            curl -s -X POST --data "$sql" \
                 "http://$(addr_of "$i")/sql?format=csv" \
                 -o "$STATE_DIR/verify/${qq}_node$i.csv"
            if ! diff -q "$STATE_DIR/ref/$qq.csv" "$STATE_DIR/verify/${qq}_node$i.csv" >/dev/null 2>&1; then
                agree=0
                bad "node $i disagrees with the single-process binary on Q$q"
                diff "$STATE_DIR/ref/$qq.csv" "$STATE_DIR/verify/${qq}_node$i.csv" | head -10
            fi
        done
        if [[ $agree -eq 1 ]]; then
            ok "Q$q identical on all $n nodes AND to the single-process binary ($(( $(wc -l < "$STATE_DIR/ref/$qq.csv") - 1 )) rows)"
        else
            failures=$((failures + 1))
        fi
    done

    # 3. Health/readiness semantics.
    info "3/4  /healthz and /readyz"
    for ((i = 0; i < n; i++)); do
        local h r
        h="$(curl -s -o /dev/null -w '%{http_code}' "http://$(addr_of "$i")/healthz")"
        r="$(curl -s -o /dev/null -w '%{http_code}' "http://$(addr_of "$i")/readyz")"
        if [[ "$h" == "200" && "$r" == "200" ]]; then
            ok "node $i healthz=$h readyz=$r"
        else
            bad "node $i healthz=$h readyz=$r"; failures=$((failures + 1))
        fi
    done

    # 4. SIGTERM the last node; the rest must report it down and keep serving.
    info "4/4  SIGTERM node $((n - 1)); survivors must mark it down, not crash"
    local victim=$((n - 1)) vpid
    vpid="$(cat "$STATE_DIR/node$victim.pid")"
    kill -TERM "$vpid"
    local deadline=$((SECONDS + 20))
    while [[ $SECONDS -lt $deadline ]] && kill -0 "$vpid" 2>/dev/null; do sleep 0.2; done
    if kill -0 "$vpid" 2>/dev/null; then
        bad "node $victim ignored SIGTERM"; failures=$((failures + 1))
    else
        wait "$vpid" 2>/dev/null && ok "node $victim exited cleanly on SIGTERM" \
            || ok "node $victim exited on SIGTERM"
    fi

    deadline=$((SECONDS + 30))
    local noticed=0
    while [[ $SECONDS -lt $deadline ]]; do
        noticed=1
        for ((i = 0; i < victim; i++)); do
            curl -s "http://$(addr_of "$i")/cluster" \
                | grep -A3 "\"$(addr_of "$victim")\"" | grep -q '"status": *"down"' || noticed=0
        done
        [[ $noticed -eq 1 ]] && break
        sleep 0.5
    done
    if [[ $noticed -eq 1 ]]; then
        ok "survivors report $(addr_of "$victim") as down"
    else
        bad "survivors never marked the departed node down"; failures=$((failures + 1))
    fi

    local still=1
    for ((i = 0; i < victim; i++)); do
        local code
        code="$(curl -s -o /dev/null -w '%{http_code}' -X POST \
                --data "SELECT COUNT(*) AS n FROM orders" \
                "http://$(addr_of "$i")/sql?format=csv")"
        [[ "$code" == "200" ]] || { still=0; bad "node $i stopped serving (HTTP $code)"; }
    done
    if [[ $still -eq 1 ]]; then
        ok "survivors still answer queries"
    else
        failures=$((failures + 1))
    fi

    echo
    if [[ $failures -eq 0 ]]; then
        echo -e "${GREEN}${BOLD}M1 GATE: PASS${NC}"
    else
        echo -e "${RED}${BOLD}M1 GATE: $failures FAILURE(S)${NC}"; return 1
    fi
}

# ── kill / stop ──────────────────────────────────────────────────────────────

cmd_kill() {
    local i="${1:-}"
    [[ -n "$i" ]] || die "usage: $0 kill <node-index>"
    [[ -f "$STATE_DIR/node$i.pid" ]] || die "no pid recorded for node $i"
    kill -TERM "$(cat "$STATE_DIR/node$i.pid")" && info "SIGTERM sent to node $i"
}

cmd_stop() {
    local stopped=0
    for f in "$STATE_DIR"/node*.pid; do
        [[ -e "$f" ]] || continue
        local pid; pid="$(cat "$f")"
        if kill -TERM "$pid" 2>/dev/null; then stopped=$((stopped + 1)); fi
        rm -f "$f"
    done
    sleep 1
    for f in "$STATE_DIR"/node*.pid; do
        [[ -e "$f" ]] || continue
        kill -KILL "$(cat "$f")" 2>/dev/null || true
        rm -f "$f"
    done
    rm -f "$STATE_DIR/nodes" "$STATE_DIR/base_port"
    info "stopped $stopped node(s)"
}

case "$CMD" in
    start)  cmd_start ;;
    status) cmd_status ;;
    query)  cmd_query "$@" ;;
    verify) cmd_verify ;;
    kill)   cmd_kill "$@" ;;
    stop)   cmd_stop ;;
    *)
        sed -n '2,25p' "$0" | sed 's/^# \?//'
        ;;
esac
