#!/bin/bash
# Run each TPC-H query at SF=100 with timeout = max(10x DuckDB, 300s)
# Captures timing and row counts

ENGINE="./target/release/query_engine"
DATA="data/tpch-100gb"
RESULTS_DIR="data/sf100_engine_results"
mkdir -p "$RESULTS_DIR"

# DuckDB best times at SF=100 (ms) -> timeout in seconds (10x, min 30s, max 300s)
declare -A DUCKDB_MS=(
    [1]=2773 [2]=313 [3]=2755 [4]=1793 [5]=2265 [6]=1191
    [7]=2411 [8]=2698 [9]=28094 [10]=1096 [11]=96 [12]=985
    [13]=1251 [14]=463 [15]=994 [16]=709 [17]=2191 [18]=4748
    [19]=2625 [20]=3210 [21]=4012 [22]=393
)

echo "Engine TPC-H SF=100 Benchmark"
echo "=============================="
printf "%-6s %12s %8s %s\n" "Query" "Time (ms)" "Rows" "Status"
echo "----------------------------------------------"

TOTAL_MS=0
PASS=0
FAIL=0
TIMEOUT_COUNT=0

for Q in $(seq 1 22); do
    DMS=${DUCKDB_MS[$Q]}
    # Timeout = 10x DuckDB in seconds, min 30s, max 600s
    TIMEOUT_S=$(( (DMS * 10 / 1000) ))
    if [ $TIMEOUT_S -lt 30 ]; then TIMEOUT_S=30; fi
    if [ $TIMEOUT_S -gt 600 ]; then TIMEOUT_S=600; fi

    START_NS=$(date +%s%N)
    OUTPUT=$(timeout ${TIMEOUT_S}s $ENGINE benchmark-parquet --path "$DATA" --query $Q --iterations 1 2>&1)
    EXIT_CODE=$?
    END_NS=$(date +%s%N)
    ELAPSED_MS=$(( (END_NS - START_NS) / 1000000 ))

    if [ $EXIT_CODE -eq 124 ]; then
        printf "Q%-5d %12s %8s TIMEOUT (>${TIMEOUT_S}s, DuckDB=${DMS}ms)\n" $Q "-" "-"
        TIMEOUT_COUNT=$((TIMEOUT_COUNT + 1))
    elif [ $EXIT_CODE -ne 0 ]; then
        printf "Q%-5d %12d %8s ERROR (exit=$EXIT_CODE)\n" $Q $ELAPSED_MS "-"
        FAIL=$((FAIL + 1))
    else
        # Extract rows and time from output
        ROWS=$(echo "$OUTPUT" | grep "^Q" | head -1 | awk '{print $2}')
        TIME_MS=$(echo "$OUTPUT" | grep "^Q" | head -1 | sed 's/.*in *//' | sed 's/ms//')
        printf "Q%-5d %12s %8s OK\n" $Q "${TIME_MS}" "${ROWS}"
        TOTAL_MS=$(echo "$TOTAL_MS + $ELAPSED_MS" | bc)
        PASS=$((PASS + 1))
    fi
done

echo "=============================="
echo "Passed: $PASS, Failed: $FAIL, Timeout: $TIMEOUT_COUNT"
echo "Total time (completed queries): ${TOTAL_MS}ms"
