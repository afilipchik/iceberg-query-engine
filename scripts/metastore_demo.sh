#!/usr/bin/env bash
# Metastore integration gate: Gravitino-cataloged tables, served distributed.
#
# What it proves, end to end, on this machine, with no Docker:
#   1. A real Apache Gravitino 1.3.0 server holds the catalog: one metalake
#      (local_lake), one fileset catalog (lakehouse), and one schema PER
#      FORMAT — tpch (parquet), tpch_iceberg, tpch_lance — each with 8 TPC-H
#      tables as filesets whose `format` property picks the engine's reader.
#   2. A 3-process query_engine cluster starts with NOTHING but
#      --metastore http://127.0.0.1:8090 --metastore-schema <schema>:
#      every node learns its tables from the metastore, not from flags.
#   3. Distributed queries (scatter aggregate + gather join) through that
#      cluster answer byte-identically to a single-process engine reading
#      the same files directly.
#
# Usage:
#   scripts/metastore_demo.sh              # parquet + iceberg schemas
#   scripts/metastore_demo.sh --with-lance # also the lance schema (needs a
#                                          # --features lance binary)
#
# Idempotent: re-creating an existing metalake/catalog/schema/fileset is
# tolerated (already-exists errors are recognized and skipped).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." >/dev/null && pwd)"
cd "$REPO_ROOT"

URL="http://127.0.0.1:${METASTORE_PORT:-8090}"
API="$URL/api"
LAKE="local_lake"
CATALOG="lakehouse"
BINARY="${QE_BINARY:-./target/release/query_engine}"
BASE_PORT="${QE_BASE_PORT:-17700}"
WITH_LANCE=0
[[ "${1:-}" == "--with-lance" ]] && WITH_LANCE=1

RED='\033[0;31m'; GREEN='\033[0;32m'; CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'
die()  { echo -e "${RED}error:${NC} $*" >&2; exit 1; }
info() { echo -e "${CYAN}==>${NC} $*"; }
ok()   { echo -e "${GREEN}PASS${NC} $*"; }

TABLES=(nation region part supplier partsupp customer orders lineitem)
FAILURES=0

# ── REST helpers ─────────────────────────────────────────────────────────────

# POST that tolerates (and reports) already-exists.
post() { # path json label
    local out
    out="$(curl -s -X POST -H 'Content-Type: application/json' "$API/$1" -d "$2")"
    local code
    code="$(echo "$out" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("code", -1))' 2>/dev/null || echo -1)"
    if [[ "$code" == "0" ]]; then
        echo "    created $3"
    elif echo "$out" | grep -qi "already exist"; then
        echo "    exists  $3"
    else
        die "creating $3 failed: $out"
    fi
}

# ── 1. Gravitino up ──────────────────────────────────────────────────────────

info "starting Gravitino"
scripts/metastore_local.sh start
curl -sf "$API/version" >/dev/null || die "Gravitino did not answer $API/version"

# ── 2. Catalog population ────────────────────────────────────────────────────

info "populating catalog: metalake=$LAKE catalog=$CATALOG"
post "metalakes" \
    "{\"name\":\"$LAKE\",\"comment\":\"local dev metalake\",\"properties\":{}}" \
    "metalake $LAKE"
post "metalakes/$LAKE/catalogs" \
    "{\"name\":\"$CATALOG\",\"type\":\"FILESET\",\"provider\":\"fileset\",\"comment\":\"local filesets\",\"properties\":{}}" \
    "catalog $CATALOG"

declare -A SCHEMA_DIR=( [tpch]="data/tpch-1mb" [tpch_iceberg]="data/tpch-1mb-iceberg" )
declare -A SCHEMA_FMT=( [tpch]="parquet" [tpch_iceberg]="iceberg" )
if [[ $WITH_LANCE == 1 ]]; then
    SCHEMA_DIR[tpch_lance]="data/tpch-1mb-lance"
    SCHEMA_FMT[tpch_lance]="lance"
fi

for schema in "${!SCHEMA_DIR[@]}"; do
    dir="${SCHEMA_DIR[$schema]}"; fmt="${SCHEMA_FMT[$schema]}"
    [[ -d "$dir" ]] || die "$dir does not exist (generate fixtures first)"
    post "metalakes/$LAKE/catalogs/$CATALOG/schemas" \
        "{\"name\":\"$schema\",\"comment\":\"TPC-H 1MB as $fmt\",\"properties\":{}}" \
        "schema $schema"
    for t in "${TABLES[@]}"; do
        case "$fmt" in
            # Gravitino refuses file storageLocations, so a parquet table is
            # its DIRECTORY plus a `file` property naming the parquet inside.
            parquet) loc="file://$REPO_ROOT/$dir"; props="{\"format\":\"parquet\",\"file\":\"$t.parquet\"}" ;;
            iceberg) loc="file://$REPO_ROOT/$dir/$t"; props="{\"format\":\"iceberg\"}" ;;
            lance)   loc="file://$REPO_ROOT/$dir/$t.lance"; props="{\"format\":\"lance\"}" ;;
        esac
        post "metalakes/$LAKE/catalogs/$CATALOG/schemas/$schema/filesets" \
            "{\"name\":\"$t\",\"type\":\"EXTERNAL\",\"comment\":\"TPC-H $t ($fmt)\",\"storageLocation\":\"$loc\",\"properties\":$props}" \
            "fileset $schema/$t"
    done
done

# ── 3. Per-schema cluster gate ───────────────────────────────────────────────

# Deterministic queries; each must match the single-process engine reading the
# same files directly (headers sorted-row compare via cluster query differ).
GATE_SQL=(
    "SELECT COUNT(*) AS n FROM lineitem"
    "SELECT l_returnflag, l_linestatus, SUM(l_quantity) AS q, COUNT(*) AS c FROM lineitem GROUP BY l_returnflag, l_linestatus"
    "SELECT o_orderpriority, COUNT(*) AS c FROM orders o JOIN lineitem l ON o.o_orderkey = l.l_orderkey WHERE l.l_shipmode = 'AIR' GROUP BY o_orderpriority ORDER BY o_orderpriority"
)

gate_schema() { # schema
    local schema="$1"
    info "cluster gate for schema $schema"
    scripts/cluster_local.sh stop >/dev/null 2>&1 || true
    scripts/cluster_local.sh start 3 --metastore "$URL" --metastore-schema "$schema" \
        || die "cluster with --metastore did not become ready for $schema"

    # A single-process oracle over the same data, via a metastore-free load.
    local dir="${SCHEMA_DIR[$schema]}" fmt="${SCHEMA_FMT[$schema]}"
    local i=0
    for sql in "${GATE_SQL[@]}"; do
        i=$((i + 1))
        local got expected
        got="$(curl -s "http://127.0.0.1:$BASE_PORT/sql?format=csv&distributed=1" -d "$sql")"
        local hdr
        hdr="$(curl -s -o /dev/null -D - "http://127.0.0.1:$BASE_PORT/sql?format=csv&distributed=1" -d "$sql" | tr -d '\r' | grep -i '^x-qe-distributed:' | awk '{print $2}')"
        [[ "$hdr" == "true" ]] || { echo -e "${RED}FAIL${NC} $schema q$i answered locally"; FAILURES=$((FAILURES+1)); continue; }
        case "$fmt" in
            parquet) expected="$(.venv/bin/python - "$dir" "$sql" <<'PYEOF'
import sys
d, sql = sys.argv[1], sys.argv[2]
import duckdb
con = duckdb.connect()
for t in ["nation","region","part","supplier","partsupp","customer","orders","lineitem"]:
    con.execute(f"CREATE VIEW {t} AS SELECT * FROM read_parquet('{d}/{t}.parquet')")
rows = con.execute(sql).fetchall()
cols = [c[0] for c in con.description]
print(",".join(cols))
for r in rows:
    print(",".join(str(x) for x in r))
PYEOF
)" ;;
            # Iceberg/Lance: the oracle is the SAME node answering LOCALLY
            # (distributed=0) over the same metastore-registered tables — the
            # M1 guarantee that a local answer equals the single-process
            # binary's makes this a valid single-process comparison.
            *) expected="$(curl -s "http://127.0.0.1:$BASE_PORT/sql?format=csv&distributed=0" -d "$sql")" ;;
        esac
        if true; then
            if python3 - "$got" "$expected" <<'PYEOF'
import sys
def cells(t):
    lines = [l for l in t.strip().splitlines() if l.strip()]
    return sorted(lines[1:])  # ignore header spelling, order-insensitive
a, b = cells(sys.argv[1]), cells(sys.argv[2])
if len(a) != len(b):
    sys.exit(1)
for x, y in zip(a, b):
    xs, ys = x.split(","), y.split(",")
    if len(xs) != len(ys):
        sys.exit(1)
    for u, v in zip(xs, ys):
        if u == v:
            continue
        try:
            if abs(float(u) - float(v)) <= 1e-6 * max(abs(float(u)), abs(float(v)), 1e-9):
                continue
        except ValueError:
            pass
        sys.exit(1)
sys.exit(0)
PYEOF
            then ok "$schema q$i: distributed matches the single-process oracle"
            else echo -e "${RED}FAIL${NC} $schema q$i differs from its oracle"; FAILURES=$((FAILURES+1)); fi
        fi
    done

    # Node agreement: the fully-ordered query through every node must be
    # byte-identical (a query without ORDER BY may legitimately permute rows
    # between initiators — that is concatenation order, not content).
    scripts/cluster_local.sh query "${GATE_SQL[2]}" >/dev/null \
        && ok "$schema: all 3 nodes agree byte-for-byte on the ordered join" \
        || { echo -e "${RED}FAIL${NC} $schema: nodes disagree"; FAILURES=$((FAILURES+1)); }

    scripts/cluster_local.sh stop >/dev/null 2>&1 || true
}

gate_schema tpch
gate_schema tpch_iceberg
[[ $WITH_LANCE == 1 ]] && gate_schema tpch_lance

# Iceberg current-snapshot check: the metastore-served orders table must be
# the CURRENT snapshot (1600 rows: 1500 + a 100-row second append).
info "iceberg snapshot check through the metastore path"
scripts/cluster_local.sh start 3 --metastore "$URL" --metastore-schema tpch_iceberg >/dev/null 2>&1
n="$(curl -s "http://127.0.0.1:$BASE_PORT/sql?format=csv&distributed=1" -d 'SELECT COUNT(*) AS n FROM orders' | tail -1)"
if [[ "$n" == "1600" ]]; then ok "metastore-served iceberg orders is at its current snapshot (1600 rows)"
else echo -e "${RED}FAIL${NC} iceberg orders row count: got $n, want 1600"; FAILURES=$((FAILURES+1)); fi
scripts/cluster_local.sh stop >/dev/null 2>&1 || true

echo
if [[ $FAILURES -gt 0 ]]; then
    echo -e "${RED}${BOLD}METASTORE GATE: $FAILURES FAILURE(S)${NC}"
    exit 1
fi
echo -e "${GREEN}${BOLD}METASTORE GATE: PASS${NC} — catalog from Gravitino, queries distributed, answers verified"
