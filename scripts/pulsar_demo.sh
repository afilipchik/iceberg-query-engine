#!/usr/bin/env bash
# The Pulsar acceptance gate: schemas registered, deterministic rows produced
# over the WebSocket producer, queried back through the engine's topic tables,
# values compared EXACTLY; discovery and refusal semantics checked.
#
#   scripts/pulsar_demo.sh [rows]   (default 10000; needs --features pulsar
#                                    release binary + pulsar_local.sh start)

set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
ADMIN="http://127.0.0.1:8085"
BINARY="./target/release/query_engine"
PRODUCER="./target/release/examples/pulsar_produce"
ROWS="${1:-10000}"

GREEN='\033[0;32m'; RED='\033[0;31m'; CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'
info() { echo -e "${CYAN}==>${NC} $*"; }
ok()   { echo -e "${GREEN}PASS${NC} $*"; }
die()  { echo -e "${RED}FAIL${NC} $*"; exit 1; }

bash scripts/pulsar_local.sh start >/dev/null
[[ -x "$BINARY" && -x "$PRODUCER" ]] || die "build first: cargo build --release --features pulsar --example pulsar_produce"

info "cleaning any previous gate topics"
for t in events_json events_avro no_schema_topic; do
    curl -s -X DELETE "$ADMIN/admin/v2/schemas/public/default/$t/schema" >/dev/null || true
    curl -s -X DELETE "$ADMIN/admin/v2/persistent/public/default/$t?force=true" >/dev/null || true
done

SCHEMA='{"type":"record","name":"Event","fields":[{"name":"id","type":"long"},{"name":"category","type":"string"},{"name":"value","type":"double"},{"name":"flag","type":"boolean"},{"name":"note","type":["null","string"]}]}'
info "registering schemas"
for pair in "events_json JSON" "events_avro AVRO"; do
    set -- $pair
    python3 -c "import json,sys; print(json.dumps({'type':sys.argv[2],'schema':sys.argv[1],'properties':{}}))" "$SCHEMA" "$2" \
        | curl -sf -X POST -H "Content-Type: application/json" -d @- \
          "$ADMIN/admin/v2/schemas/public/default/$1/schema" >/dev/null || die "schema $1"
done

info "producing $ROWS rows into each topic"
expect="$($PRODUCER $ADMIN $ROWS | tr '\n' ' ')"
want_sum=$(echo "$expect" | grep -oE 'sum_value=[0-9.]+' | cut -d= -f2)
want_flags=$(echo "$expect" | grep -oE 'flags=[0-9]+' | cut -d= -f2)

info "querying through the engine"
out="$(printf '.pulsar %s public/default\nSELECT COUNT(*) AS n, SUM(value) AS s, SUM(CASE WHEN flag THEN 1 ELSE 0 END) AS f FROM events_json;\nSELECT COUNT(*) AS n, SUM(value) AS s, SUM(CASE WHEN flag THEN 1 ELSE 0 END) AS f FROM events_avro;\n.quit\n' "$ADMIN" | $BINARY repl 2>&1)"
echo "$out" | grep -q "Registered 2 pulsar topic(s)" || die "discovery did not find exactly the two topics: $(echo "$out" | grep -i pulsar | head -2)"
rowlines="$(echo "$out" | grep -E '^\| [0-9]' | tr -s ' ')"
[[ "$(echo "$rowlines" | wc -l)" == "2" ]] || die "expected two result rows, got: $rowlines"
while IFS= read -r line; do
    n=$(echo "$line" | cut -d'|' -f2 | tr -d ' ')
    s=$(echo "$line" | cut -d'|' -f3 | tr -d ' ')
    f=$(echo "$line" | cut -d'|' -f4 | tr -d ' ')
    [[ "$n" == "$ROWS" ]] || die "row count $n != $ROWS"
    python3 -c "import sys; sys.exit(0 if abs(float('$s')-float('$want_sum'))<1e-6 else 1)" || die "sum $s != $want_sum"
    [[ "$f" == "$want_flags" ]] || die "flags $f != $want_flags"
done <<< "$rowlines"
ok "both topics: COUNT=$ROWS, SUM=$want_sum, flags=$want_flags — exact"

info "refusal: a schemaless topic fails registration BY NAME"
curl -sf -X PUT "$ADMIN/admin/v2/persistent/public/default/no_schema_topic" >/dev/null || true
refuse="$(printf '.pulsar %s public/default\n.quit\n' "$ADMIN" | $BINARY repl 2>&1 || true)"
echo "$refuse" | grep -q "no_schema_topic" || die "schemaless topic not refused by name: $(echo "$refuse" | grep -i error | head -1)"
ok "schemaless topic refused by name"
curl -sf -X DELETE "$ADMIN/admin/v2/persistent/public/default/no_schema_topic" >/dev/null || true

echo -e "${GREEN}${BOLD}PULSAR GATE: PASS${NC} — discovery, JSON+AVRO decode, exact values, refusals"
