#!/usr/bin/env bash
# End-to-end Kubernetes test on kind: build the image, load it, apply the
# manifests, wait for readiness, run smoke queries against the Service, tear
# down.
#
# ###########################################################################
# # UNVALIDATED-ON-CLUSTER                                                  #
# #                                                                         #
# # This script has NEVER been run to completion. The machine it was written#
# # on has no docker, no podman, no kind and no kubectl, no passwordless    #
# # sudo, and kernel.apparmor_restrict_unprivileged_userns=1 blocks         #
# # unprivileged user namespaces, so rootless containers are unavailable    #
# # too. Nothing in this file has produced a real result. Every claim about #
# # the Kubernetes path in the M1 report is explicitly marked as unverified.#
# #                                                                         #
# # What HAS been verified here, as N separate OS processes over real TCP:  #
# #   scripts/cluster_local.sh verify                                       #
# #   cargo test --release --test distributed_cluster                       #
# # Those exercise the same server, the same discovery code, the same       #
# # membership diff and the same SIGTERM path. What they do not exercise is #
# # kubelet, CoreDNS, the probes, and cross-node networking — which is      #
# # exactly what this script is for.                                        #
# ###########################################################################
#
# Usage:
#   scripts/kind_test.sh              full cycle, then tear down
#   scripts/kind_test.sh --keep       leave the cluster running
#   scripts/kind_test.sh --no-build   reuse an existing query-engine:dev image
#   scripts/kind_test.sh --data DIR   dataset to mount (default ./data/tpch-1mb)

set -euo pipefail

CLUSTER_NAME="query-engine"
IMAGE="query-engine:dev"
DATA_DIR="./data/tpch-1mb"
KIND_CONFIG="kind-cluster.yaml"
KEEP=0
BUILD=1
NAMESPACE="default"
ENTRY="http://localhost:30777"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'

die()  { echo -e "${RED}error:${NC} $*" >&2; exit 1; }
info() { echo -e "${CYAN}==>${NC} $*"; }
ok()   { echo -e "${GREEN}PASS${NC} $*"; }
bad()  { echo -e "${RED}FAIL${NC} $*"; }

while [[ $# -gt 0 ]]; do
    case "$1" in
        --keep)     KEEP=1; shift ;;
        --no-build) BUILD=0; shift ;;
        --data)     DATA_DIR="$2"; shift 2 ;;
        --image)    IMAGE="$2"; shift 2 ;;
        -h|--help)  sed -n '2,30p' "$0" | sed 's/^# \?//'; exit 0 ;;
        *)          die "unknown argument: $1" ;;
    esac
done

# ── Prerequisites: fail loudly and specifically ──────────────────────────────

missing=()
for tool in docker kind kubectl; do
    command -v "$tool" >/dev/null 2>&1 || missing+=("$tool")
done

if [[ ${#missing[@]} -gt 0 ]]; then
    echo -e "${RED}${BOLD}Cannot run: missing ${missing[*]}${NC}" >&2
    cat >&2 <<'EOF'

This script needs docker, kind and kubectl. It is expected to fail on the
development machine used to write it -- that machine cannot run containers at
all:

  * no docker or podman binary, and no daemon to talk to
  * no passwordless sudo, so neither can be installed or started
  * kernel.apparmor_restrict_unprivileged_userns = 1 (Ubuntu 24.04 default),
    which blocks unprivileged user namespaces, so rootless podman is out too
    ('unshare --user --map-root-user' fails with EPERM)

Install on a Docker-capable machine:

  # docker: https://docs.docker.com/engine/install/
  go install sigs.k8s.io/kind@latest         # or: brew install kind
  curl -LO "https://dl.k8s.io/release/$(curl -Ls https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
  sudo install -m 0755 kubectl /usr/local/bin/kubectl

Then re-run this script from the repository root. Until it has been run there,
every Kubernetes artifact in this repo (Dockerfile, k8s/*.yaml,
kind-cluster.yaml) is UNVALIDATED-ON-CLUSTER.

In the meantime, the equivalent multi-process test DOES run here:

  cargo build --release
  ./scripts/cluster_local.sh start 3
  ./scripts/cluster_local.sh verify
  ./scripts/cluster_local.sh stop
  cargo test --release --test distributed_cluster
EOF
    exit 127
fi

docker info >/dev/null 2>&1 || die "docker is installed but the daemon is not reachable"
[[ -d "$DATA_DIR" ]] || die "dataset not found: $DATA_DIR (cargo run --release -- generate-parquet --sf 0.001 --output $DATA_DIR)"
[[ -f "$KIND_CONFIG" ]] || die "missing $KIND_CONFIG (run from the repository root)"

cleanup() {
    if [[ $KEEP -eq 1 ]]; then
        echo -e "${YELLOW}--keep:${NC} cluster '$CLUSTER_NAME' left running. Delete with:"
        echo "    kind delete cluster --name $CLUSTER_NAME"
        return
    fi
    info "tearing down"
    kind delete cluster --name "$CLUSTER_NAME" >/dev/null 2>&1 || true
}
trap cleanup EXIT

FAILURES=0

# ── 1. Cluster ───────────────────────────────────────────────────────────────

info "creating kind cluster '$CLUSTER_NAME' (1 control-plane + 3 workers)"
if kind get clusters 2>/dev/null | grep -qx "$CLUSTER_NAME"; then
    info "cluster already exists, reusing it"
else
    # The manifest mounts ./data/tpch-1mb; honour --data by rewriting into a
    # temporary copy rather than editing the checked-in file.
    CONFIG_TO_USE="$KIND_CONFIG"
    if [[ "$DATA_DIR" != "./data/tpch-1mb" ]]; then
        mkdir -p .scratch/kind
        CONFIG_TO_USE=".scratch/kind/kind-cluster.yaml"
        sed "s#hostPath: ./data/tpch-1mb#hostPath: $DATA_DIR#" "$KIND_CONFIG" > "$CONFIG_TO_USE"
    fi
    kind create cluster --config "$CONFIG_TO_USE" --wait 120s
fi
kubectl cluster-info --context "kind-$CLUSTER_NAME" >/dev/null

# ── 2. Image ─────────────────────────────────────────────────────────────────

if [[ $BUILD -eq 1 ]]; then
    info "building $IMAGE (release build; expect several minutes on a cold cache)"
    docker build -t "$IMAGE" .
else
    docker image inspect "$IMAGE" >/dev/null 2>&1 || die "$IMAGE not present and --no-build was given"
fi

info "loading $IMAGE into every kind node"
kind load docker-image "$IMAGE" --name "$CLUSTER_NAME"

# ── 3. Manifests ─────────────────────────────────────────────────────────────

info "applying manifests"
kubectl apply -f k8s/service-headless.yaml
kubectl apply -f k8s/service.yaml
kubectl apply -f k8s/statefulset.yaml

info "waiting for 3 ready pods (this includes the table load)"
if ! kubectl rollout status statefulset/query-engine --timeout=300s; then
    bad "the StatefulSet never became ready"
    kubectl get pods -o wide
    kubectl describe statefulset query-engine | tail -40
    for p in $(kubectl get pods -l app=query-engine -o name); do
        echo "--- logs $p ---"; kubectl logs "$p" --tail=50 || true
    done
    exit 1
fi
kubectl get pods -l app=query-engine -o wide

# Three workers exist so the three pods should be spread across them. Not fatal
# if they are not (kind's scheduler may co-locate), but worth reporting: a
# single-node placement makes the network path a loopback and quietly weakens
# every conclusion drawn from this run.
NODES_USED="$(kubectl get pods -l app=query-engine -o jsonpath='{.items[*].spec.nodeName}' | tr ' ' '\n' | sort -u | wc -l)"
info "pods are spread across $NODES_USED node(s)"

# ── 4. The M1 gate, through the Service ──────────────────────────────────────

echo -e "${BOLD}=== M1 gate on Kubernetes ===${NC}"

info "1/4  every pod reports the same 3-member view"
REF=""
for i in 0 1 2; do
    # The image has no curl by design, so read /cluster through the API
    # server's pod-proxy rather than from inside the container.
    VIEW="$(kubectl get --raw "/api/v1/namespaces/$NAMESPACE/pods/query-engine-$i:7777/proxy/cluster" \
            | sed -n '/"members"/,$p' | grep -E '"(address|node_id|status)"' | tr -d ' ')"
    [[ -z "$REF" ]] && REF="$VIEW"
    if [[ "$VIEW" != "$REF" ]]; then
        bad "query-engine-$i disagrees about membership"
        diff <(echo "$REF") <(echo "$VIEW") || true
        FAILURES=$((FAILURES + 1))
    fi
done
COUNT="$(kubectl get --raw "/api/v1/namespaces/$NAMESPACE/pods/query-engine-0:7777/proxy/cluster" \
         | grep -o '"member_count": *[0-9]*' | grep -o '[0-9]*$')"
if [[ "$COUNT" == "3" ]]; then
    ok "all three pods report the same 3-member view (DNS discovery)"
else
    bad "member_count is $COUNT, expected 3"; FAILURES=$((FAILURES + 1))
fi

info "2/4  smoke queries through the Service at $ENTRY"
QUERIES=(
  "SELECT COUNT(*) AS n FROM lineitem"
  "SELECT l_returnflag, l_linestatus, COUNT(*) AS c FROM lineitem GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus"
  "SELECT n_name, COUNT(*) AS c FROM customer JOIN nation ON c_nationkey = n_nationkey GROUP BY n_name ORDER BY n_name"
)
mkdir -p .scratch/kind/results
qi=0
for sql in "${QUERIES[@]}"; do
    qi=$((qi + 1))
    # Ten calls: the Service load-balances, so this hits several pods. Every
    # answer must be identical -- in M1 each pod holds the whole dataset, so a
    # difference means a node is serving from a different (or partial) view.
    first=""
    same=1
    for _ in $(seq 1 10); do
        body="$(curl -sf -X POST --data "$sql" "$ENTRY/sql?format=csv" || echo "__REQUEST_FAILED__")"
        [[ "$body" == "__REQUEST_FAILED__" ]] && { bad "query $qi failed through the Service"; same=0; break; }
        [[ -z "$first" ]] && first="$body"
        [[ "$body" == "$first" ]] || same=0
    done
    if [[ $same -eq 1 ]]; then
        ok "query $qi: 10 Service calls, all identical"
        echo "$first" > ".scratch/kind/results/q$qi.csv"
    else
        bad "query $qi: answers differed between pods"; FAILURES=$((FAILURES + 1))
    fi
done

info "3/4  health endpoints"
for i in 0 1 2; do
    H="$(kubectl get --raw "/api/v1/namespaces/$NAMESPACE/pods/query-engine-$i:7777/proxy/healthz" >/dev/null 2>&1 && echo ok || echo fail)"
    R="$(kubectl get --raw "/api/v1/namespaces/$NAMESPACE/pods/query-engine-$i:7777/proxy/readyz" >/dev/null 2>&1 && echo ok || echo fail)"
    if [[ "$H" == "ok" && "$R" == "ok" ]]; then
        ok "query-engine-$i healthz=$H readyz=$R"
    else
        bad "query-engine-$i healthz=$H readyz=$R"; FAILURES=$((FAILURES + 1))
    fi
done

info "4/4  delete a pod: SIGTERM, clean exit, survivors keep serving"
kubectl delete pod query-engine-2 --wait=false
# The survivors must keep answering while pod 2 is gone and coming back.
sleep 5
if curl -sf -X POST --data "SELECT COUNT(*) AS n FROM orders" "$ENTRY/sql?format=csv" >/dev/null; then
    ok "the Service still answers with one pod down"
else
    bad "the Service stopped answering when a pod was deleted"; FAILURES=$((FAILURES + 1))
fi
# A pod that ignored SIGTERM would take the full terminationGracePeriodSeconds
# (30s) to die; a clean one is gone in about drain-ms.
if kubectl wait --for=delete pod/query-engine-2 --timeout=25s >/dev/null 2>&1; then
    ok "pod terminated within the grace period (SIGTERM was honoured)"
else
    bad "pod needed SIGKILL — the process is ignoring SIGTERM"; FAILURES=$((FAILURES + 1))
fi
kubectl rollout status statefulset/query-engine --timeout=180s >/dev/null \
    && ok "the StatefulSet healed back to 3 ready pods" \
    || { bad "the StatefulSet did not recover"; FAILURES=$((FAILURES + 1)); }

echo
if [[ $FAILURES -eq 0 ]]; then
    echo -e "${GREEN}${BOLD}M1 KUBERNETES GATE: PASS${NC}"
    echo "Record this run: the artifacts are no longer UNVALIDATED-ON-CLUSTER."
else
    echo -e "${RED}${BOLD}M1 KUBERNETES GATE: $FAILURES FAILURE(S)${NC}"
    exit 1
fi
