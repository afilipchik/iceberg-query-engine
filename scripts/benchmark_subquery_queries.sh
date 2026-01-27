#!/bin/bash
# Benchmark TPC-H Q17-Q22 with telemetry logging
# These queries contain correlated subqueries

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
LOG_DIR="$PROJECT_DIR/logs"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="$LOG_DIR/benchmark_q17_q22_${TIMESTAMP}.log"

# Scale factor - use small for testing, larger for real benchmark
SF=${SF:-0.01}
TIMEOUT_SECS=${TIMEOUT_SECS:-300}

mkdir -p "$LOG_DIR"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"
}

log "=== TPC-H Q17-Q22 Benchmark ==="
log "Scale factor: $SF"
log "Timeout: ${TIMEOUT_SECS}s per query"
log "Log file: $LOG_FILE"
log "System info:"
log "  CPU: $(nproc) cores"
log "  Memory: $(free -h | grep Mem | awk '{print $2}') total"
log ""

cd "$PROJECT_DIR"

# Run each query individually with timeout and memory monitoring
for Q in 17 18 19 20 21 22; do
    log "=========================================="
    log "Starting Q$Q"
    log "=========================================="

    # Get memory before
    MEM_BEFORE=$(free -m | grep Mem | awk '{print $3}')
    log "Memory before: ${MEM_BEFORE}MB used"

    START_TIME=$(date +%s.%N)

    # Run query with timeout, capture output and exit code
    set +e
    timeout "${TIMEOUT_SECS}s" cargo run --release -- query --num "$Q" --sf "$SF" 2>&1 | tee -a "$LOG_FILE"
    EXIT_CODE=$?
    set -e

    END_TIME=$(date +%s.%N)
    DURATION=$(echo "$END_TIME - $START_TIME" | bc)

    # Get memory after
    MEM_AFTER=$(free -m | grep Mem | awk '{print $3}')
    log "Memory after: ${MEM_AFTER}MB used"
    log "Memory delta: $((MEM_AFTER - MEM_BEFORE))MB"

    if [ $EXIT_CODE -eq 124 ]; then
        log "Q$Q: TIMEOUT after ${TIMEOUT_SECS}s"
    elif [ $EXIT_CODE -ne 0 ]; then
        log "Q$Q: FAILED with exit code $EXIT_CODE"
    else
        log "Q$Q: COMPLETED in ${DURATION}s"
    fi
    log ""

    # Brief pause to let system stabilize
    sleep 1
done

log "=== Benchmark Complete ==="
log "Results saved to: $LOG_FILE"
