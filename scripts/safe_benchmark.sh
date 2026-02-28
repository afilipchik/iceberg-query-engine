#!/usr/bin/env bash
# Safe TPC-H Benchmark Runner
#
# Runs each TPC-H query as a separate subprocess with memory limits and timeouts.
# Uses systemd-run cgroup scoping for OS-level memory isolation, with prlimit fallback.
# This prevents OOM kills from taking down the parent session.

set -euo pipefail

# ── Defaults ──────────────────────────────────────────────────────────────────

DATA_DIR="./data/tpch-10gb"
MEM_LIMIT="32G"
QUERIES=""
TIMEOUT_MULTIPLIER=10
MIN_TIMEOUT=5
USE_CGROUP=true
ITERATIONS=1
BINARY="./target/release/query_engine"

# DuckDB reference times (milliseconds) at SF=10
declare -A DUCKDB_MS=(
    [1]=89   [2]=13   [3]=84   [4]=80   [5]=49   [6]=24
    [7]=61   [8]=76   [9]=8    [10]=98  [11]=10  [12]=66
    [13]=131 [14]=35  [15]=33  [16]=40  [17]=75  [18]=283
    [19]=87  [20]=161 [21]=201 [22]=36
)

# ── Colors ────────────────────────────────────────────────────────────────────

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
DIM='\033[2m'
RESET='\033[0m'

# ── Usage ─────────────────────────────────────────────────────────────────────

usage() {
    cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Safe TPC-H Benchmark Runner — each query runs in an isolated subprocess.

Options:
  --data PATH              Parquet data directory (default: $DATA_DIR)
  --mem SIZE               Memory limit per query, e.g. 32G (default: $MEM_LIMIT)
  --queries LIST           Query list: "1,5,21" or "1-22" (default: all)
  --timeout-multiplier N   Multiplier on DuckDB times (default: $TIMEOUT_MULTIPLIER)
  --iterations N           Number of iterations per query (default: $ITERATIONS)
  --binary PATH            Path to query_engine binary (default: $BINARY)
  --no-cgroup              Use prlimit instead of systemd-run
  --help                   Show this help

Examples:
  $(basename "$0") --data ./data/tpch-10gb --mem 32G
  $(basename "$0") --data ./data/tpch-1mb --mem 1G --queries 1,5,21
  $(basename "$0") --data ./data/tpch-10gb --queries 1-5 --timeout-multiplier 20
EOF
    exit 0
}

# ── Parse args ────────────────────────────────────────────────────────────────

while [[ $# -gt 0 ]]; do
    case "$1" in
        --data)       DATA_DIR="$2"; shift 2 ;;
        --mem)        MEM_LIMIT="$2"; shift 2 ;;
        --queries)    QUERIES="$2"; shift 2 ;;
        --timeout-multiplier) TIMEOUT_MULTIPLIER="$2"; shift 2 ;;
        --iterations) ITERATIONS="$2"; shift 2 ;;
        --binary)     BINARY="$2"; shift 2 ;;
        --no-cgroup)  USE_CGROUP=false; shift ;;
        --help)       usage ;;
        *)            echo "Unknown option: $1"; usage ;;
    esac
done

# ── Expand query list ─────────────────────────────────────────────────────────

expand_queries() {
    local spec="$1"
    local result=()

    if [[ -z "$spec" ]]; then
        for i in $(seq 1 22); do result+=("$i"); done
    else
        IFS=',' read -ra parts <<< "$spec"
        for part in "${parts[@]}"; do
            if [[ "$part" == *-* ]]; then
                local lo hi
                lo="${part%%-*}"
                hi="${part##*-}"
                for i in $(seq "$lo" "$hi"); do result+=("$i"); done
            else
                result+=("$part")
            fi
        done
    fi
    echo "${result[@]}"
}

QUERY_LIST=($(expand_queries "$QUERIES"))

# ── Parse memory limit to bytes ───────────────────────────────────────────────

parse_mem() {
    local s="$1"
    local num="${s%[GgMmKk]*}"
    local suffix="${s##*[0-9]}"
    case "${suffix^^}" in
        G) echo $(( num * 1024 * 1024 * 1024 )) ;;
        M) echo $(( num * 1024 * 1024 )) ;;
        K) echo $(( num * 1024 )) ;;
        *) echo "$num" ;;
    esac
}

MEM_BYTES=$(parse_mem "$MEM_LIMIT")

# ── Compute per-query timeout (seconds) ──────────────────────────────────────

query_timeout() {
    local q="$1"
    local duckdb_ms="${DUCKDB_MS[$q]:-100}"
    local timeout_ms=$(( duckdb_ms * TIMEOUT_MULTIPLIER ))
    local timeout_s=$(( (timeout_ms + 999) / 1000 ))  # ceiling division
    if (( timeout_s < MIN_TIMEOUT )); then
        timeout_s=$MIN_TIMEOUT
    fi
    echo "$timeout_s"
}

# ── Check isolation method ────────────────────────────────────────────────────

ISOLATION_METHOD="none"

if $USE_CGROUP; then
    if command -v systemd-run &>/dev/null; then
        # Test if user scope works
        if systemd-run --user --scope --quiet -p MemoryMax=1G /bin/true 2>/dev/null; then
            ISOLATION_METHOD="cgroup"
        else
            echo -e "${YELLOW}Warning: systemd-run --user --scope not available, trying prlimit${RESET}"
        fi
    fi
fi

if [[ "$ISOLATION_METHOD" != "cgroup" ]]; then
    if command -v prlimit &>/dev/null; then
        ISOLATION_METHOD="prlimit"
    else
        echo -e "${YELLOW}Warning: Neither systemd-run nor prlimit available. Running without memory limits.${RESET}"
        ISOLATION_METHOD="none"
    fi
fi

# ── Validate prerequisites ────────────────────────────────────────────────────

if [[ ! -f "$BINARY" ]]; then
    echo -e "${RED}Error: Binary not found at $BINARY${RESET}"
    echo "Run: cargo build --release"
    exit 1
fi

if [[ ! -d "$DATA_DIR" ]]; then
    echo -e "${RED}Error: Data directory not found: $DATA_DIR${RESET}"
    exit 1
fi

# Check for at least one parquet file
if ! ls "$DATA_DIR"/*.parquet &>/dev/null; then
    echo -e "${RED}Error: No .parquet files found in $DATA_DIR${RESET}"
    exit 1
fi

# ── Set up logging ────────────────────────────────────────────────────────────

mkdir -p logs
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="logs/safe_benchmark_${TIMESTAMP}.log"

log() {
    echo "$@" | tee -a "$LOG_FILE"
}

log_no_newline() {
    printf "%s" "$@" | tee -a "$LOG_FILE"
}

# ── Print header ──────────────────────────────────────────────────────────────

echo "" | tee -a "$LOG_FILE"
echo -e "${BOLD}=== Safe TPC-H Benchmark Runner ===${RESET}" | tee -a "$LOG_FILE"
log "Memory limit: $MEM_LIMIT | Data: $DATA_DIR | Isolation: $ISOLATION_METHOD"
log "Timeout multiplier: ${TIMEOUT_MULTIPLIER}x DuckDB | Min timeout: ${MIN_TIMEOUT}s"
log "Queries: ${QUERY_LIST[*]}"
log "Iterations: $ITERATIONS"
log "Log: $LOG_FILE"
echo "" | tee -a "$LOG_FILE"

# ── Run queries ───────────────────────────────────────────────────────────────

declare -A RESULTS_MS
declare -A RESULTS_ROWS
declare -A RESULTS_STATUS

PASS_COUNT=0
FAIL_COUNT=0
OOM_COUNT=0
TIMEOUT_COUNT=0
ERROR_COUNT=0
TOTAL_ENGINE_MS=0

for q in "${QUERY_LIST[@]}"; do
    qpad=$(printf "Q%02d" "$q")
    timeout_s=$(query_timeout "$q")
    duckdb_ms="${DUCKDB_MS[$q]:-0}"

    # Build the command
    ENGINE_CMD="$BINARY benchmark-parquet --path $DATA_DIR --query $q --iterations $ITERATIONS"

    # Wrap with isolation
    case "$ISOLATION_METHOD" in
        cgroup)
            FULL_CMD="systemd-run --user --scope --quiet -p MemoryMax=$MEM_BYTES -p MemorySwapMax=0 timeout ${timeout_s}s $ENGINE_CMD"
            ;;
        prlimit)
            # Use --data (max data segment size) instead of --as (virtual address space)
            # since --as is too restrictive and blocks thread creation
            FULL_CMD="prlimit --data=$MEM_BYTES timeout ${timeout_s}s $ENGINE_CMD"
            ;;
        none)
            FULL_CMD="timeout ${timeout_s}s $ENGINE_CMD"
            ;;
    esac

    # Run the query
    log_no_newline "$(printf "%-5s" "$qpad:")"

    TMPOUT="logs/.bench_output_$$"
    set +e
    eval "$FULL_CMD" > "$TMPOUT" 2>&1
    EXIT_CODE=$?
    set -e
    OUTPUT=$(cat "$TMPOUT" 2>/dev/null || echo "")
    rm -f "$TMPOUT"

    # Parse output — look for pattern: Q01:    N rows in    X.XXXms
    QUERY_MS=""
    QUERY_ROWS=""
    if [[ "$OUTPUT" =~ Q[0-9]+:\ *([0-9]+)\ rows\ in\ *([0-9]+\.[0-9]+)ms ]]; then
        QUERY_ROWS="${BASH_REMATCH[1]}"
        QUERY_MS="${BASH_REMATCH[2]}"
    fi

    # Determine status
    STATUS=""
    STATUS_COLOR=""
    if (( EXIT_CODE == 137 )); then
        STATUS="OOM"
        STATUS_COLOR="$RED"
        OOM_COUNT=$(( OOM_COUNT + 1 ))
        FAIL_COUNT=$(( FAIL_COUNT + 1 ))
    elif (( EXIT_CODE == 124 )); then
        STATUS="TIMEOUT"
        STATUS_COLOR="$YELLOW"
        TIMEOUT_COUNT=$(( TIMEOUT_COUNT + 1 ))
        FAIL_COUNT=$(( FAIL_COUNT + 1 ))
    elif (( EXIT_CODE == 139 )); then
        STATUS="SEGFAULT"
        STATUS_COLOR="$RED"
        ERROR_COUNT=$(( ERROR_COUNT + 1 ))
        FAIL_COUNT=$(( FAIL_COUNT + 1 ))
    elif (( EXIT_CODE != 0 )); then
        STATUS="ERROR($EXIT_CODE)"
        STATUS_COLOR="$RED"
        ERROR_COUNT=$(( ERROR_COUNT + 1 ))
        FAIL_COUNT=$(( FAIL_COUNT + 1 ))
    elif [[ -n "$QUERY_MS" ]]; then
        # Calculate ratio
        if (( duckdb_ms > 0 )); then
            RATIO=$(echo "scale=1; $QUERY_MS / $duckdb_ms" | bc 2>/dev/null || echo "?")
        else
            RATIO="?"
        fi
        STATUS="PASS"
        STATUS_COLOR="$GREEN"
        PASS_COUNT=$(( PASS_COUNT + 1 ))
        # Accumulate total
        TOTAL_ENGINE_MS=$(echo "$TOTAL_ENGINE_MS + $QUERY_MS" | bc 2>/dev/null || echo "$TOTAL_ENGINE_MS")
    else
        STATUS="ERROR(no output)"
        STATUS_COLOR="$RED"
        ERROR_COUNT=$(( ERROR_COUNT + 1 ))
        FAIL_COUNT=$(( FAIL_COUNT + 1 ))
    fi

    RESULTS_MS[$q]="${QUERY_MS:-0}"
    RESULTS_ROWS[$q]="${QUERY_ROWS:-0}"
    RESULTS_STATUS[$q]="$STATUS"

    # Print result line
    if [[ "$STATUS" == "PASS" ]]; then
        printf "  %8sms  ${DIM}(DuckDB: %4sms, ratio: %5sx, rows: %s)${RESET}     ${STATUS_COLOR}[%s]${RESET}\n" \
            "$QUERY_MS" "$duckdb_ms" "$RATIO" "$QUERY_ROWS" "$STATUS" | tee -a "$LOG_FILE"
    elif [[ "$STATUS" == "OOM" ]]; then
        printf "  ${RED}OOM KILLED${RESET} after exceeding ${MEM_LIMIT} memory limit (timeout: ${timeout_s}s)     ${STATUS_COLOR}[%s]${RESET}\n" \
            "$STATUS" | tee -a "$LOG_FILE"
    elif [[ "$STATUS" == "TIMEOUT" ]]; then
        printf "  ${YELLOW}TIMEOUT${RESET} after ${timeout_s}s (${TIMEOUT_MULTIPLIER}x DuckDB ${duckdb_ms}ms)     ${STATUS_COLOR}[%s]${RESET}\n" \
            "$STATUS" | tee -a "$LOG_FILE"
    else
        printf "  ${RED}%s${RESET}     ${STATUS_COLOR}[%s]${RESET}\n" \
            "$STATUS" "$STATUS" | tee -a "$LOG_FILE"
        # Log the error output for debugging
        if [[ -n "$OUTPUT" ]]; then
            echo "  Output: $(echo "$OUTPUT" | head -5)" >> "$LOG_FILE"
        fi
    fi
done

# ── Summary ───────────────────────────────────────────────────────────────────

echo "" | tee -a "$LOG_FILE"
echo -e "${BOLD}=== SUMMARY ===${RESET}" | tee -a "$LOG_FILE"

TOTAL_QUERIES=${#QUERY_LIST[@]}
TOTAL_DUCKDB_MS=0
for q in "${QUERY_LIST[@]}"; do
    TOTAL_DUCKDB_MS=$(( TOTAL_DUCKDB_MS + ${DUCKDB_MS[$q]:-0} ))
done

# Format totals
if (( $(echo "$TOTAL_ENGINE_MS > 1000" | bc 2>/dev/null || echo 0) )); then
    ENGINE_DISPLAY=$(printf "%.2fs" "$(echo "$TOTAL_ENGINE_MS / 1000" | bc -l 2>/dev/null || echo 0)")
else
    ENGINE_DISPLAY="${TOTAL_ENGINE_MS}ms"
fi

if (( TOTAL_DUCKDB_MS > 1000 )); then
    DUCKDB_DISPLAY=$(printf "%.2fs" "$(echo "$TOTAL_DUCKDB_MS / 1000" | bc -l)")
else
    DUCKDB_DISPLAY="${TOTAL_DUCKDB_MS}ms"
fi

if (( TOTAL_DUCKDB_MS > 0 )) && [[ "$TOTAL_ENGINE_MS" != "0" ]]; then
    TOTAL_RATIO=$(echo "scale=1; $TOTAL_ENGINE_MS / $TOTAL_DUCKDB_MS" | bc 2>/dev/null || echo "?")
else
    TOTAL_RATIO="?"
fi

# Build failure detail
FAIL_DETAIL=""
if (( OOM_COUNT > 0 )); then FAIL_DETAIL+="${OOM_COUNT} OOM"; fi
if (( TIMEOUT_COUNT > 0 )); then
    [[ -n "$FAIL_DETAIL" ]] && FAIL_DETAIL+=", "
    FAIL_DETAIL+="${TIMEOUT_COUNT} timeout"
fi
if (( ERROR_COUNT > 0 )); then
    [[ -n "$FAIL_DETAIL" ]] && FAIL_DETAIL+=", "
    FAIL_DETAIL+="${ERROR_COUNT} error"
fi

if (( FAIL_COUNT == 0 )); then
    echo -e "${GREEN}${PASS_COUNT} passed${RESET}, 0 failed out of ${TOTAL_QUERIES} queries" | tee -a "$LOG_FILE"
else
    echo -e "${GREEN}${PASS_COUNT} passed${RESET}, ${RED}${FAIL_COUNT} failed${RESET} (${FAIL_DETAIL}) out of ${TOTAL_QUERIES} queries" | tee -a "$LOG_FILE"
fi

log "Total engine: $ENGINE_DISPLAY | Total DuckDB: $DUCKDB_DISPLAY | Ratio: ${TOTAL_RATIO}x"
log "Log saved to: $LOG_FILE"
echo "" | tee -a "$LOG_FILE"
