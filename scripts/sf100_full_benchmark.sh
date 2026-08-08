#!/bin/bash
# SF=100 Full Benchmark: Run engine with CSV output, then compare with DuckDB results
# Runs each query individually with timeout = 10x DuckDB time (min 60s, max 600s)

ENGINE="./target/release/query_engine"
DATA="data/tpch-100gb"
ENGINE_CSV="data/sf100_engine_results"
DUCKDB_CSV="data/sf100_duckdb_results"

mkdir -p "$ENGINE_CSV"

# DuckDB best times at SF=100 (ms)
declare -A DUCKDB_MS=(
    [1]=2773 [2]=313 [3]=2755 [4]=1793 [5]=2265 [6]=1191
    [7]=2411 [8]=2698 [9]=28094 [10]=1096 [11]=96 [12]=985
    [13]=1251 [14]=463 [15]=994 [16]=709 [17]=2191 [18]=4748
    [19]=2625 [20]=3210 [21]=4012 [22]=393
)

echo "============================================================"
echo "Engine TPC-H SF=100 Benchmark + Validation"
echo "Memory limit: 64GB"
echo "============================================================"
printf "%-6s %14s %10s %12s %8s %s\n" "Query" "Engine (ms)" "Rows" "DuckDB (ms)" "Ratio" "Correct"
echo "----------------------------------------------------------------------"

TOTAL_ENGINE=0
TOTAL_DUCKDB=0
PASS=0
FAIL=0
TIMEOUT_COUNT=0
CORRECT=0
INCORRECT=0

for Q in $(seq 1 22); do
    DMS=${DUCKDB_MS[$Q]}
    # Timeout = 10x DuckDB in seconds, min 60s, max 600s
    TIMEOUT_S=$(( (DMS * 10 / 1000) ))
    if [ $TIMEOUT_S -lt 60 ]; then TIMEOUT_S=60; fi
    if [ $TIMEOUT_S -gt 600 ]; then TIMEOUT_S=600; fi

    # Run engine with CSV saving
    START_NS=$(date +%s%N)
    OUTPUT=$(timeout ${TIMEOUT_S}s $ENGINE benchmark-parquet --path "$DATA" --query $Q --iterations 1 --sf 100 --save-csv "$ENGINE_CSV" 2>&1)
    EXIT_CODE=$?
    END_NS=$(date +%s%N)
    ELAPSED_MS=$(( (END_NS - START_NS) / 1000000 ))

    if [ $EXIT_CODE -eq 124 ]; then
        printf "Q%-5d %14s %10s %12d %8s TIMEOUT (>${TIMEOUT_S}s)\n" $Q "-" "-" $DMS "-"
        TIMEOUT_COUNT=$((TIMEOUT_COUNT + 1))
        continue
    elif [ $EXIT_CODE -ne 0 ]; then
        ERROR=$(echo "$OUTPUT" | tail -1)
        printf "Q%-5d %14d %10s %12d %8s ERROR\n" $Q $ELAPSED_MS "-" $DMS "-"
        echo "  Error: $ERROR"
        FAIL=$((FAIL + 1))
        continue
    fi

    # Extract rows and time from output
    TIME_MS=$(echo "$OUTPUT" | grep "^Q" | head -1 | sed 's/.*in *//' | sed 's/ms//' | tr -d ' ')
    ROWS=$(echo "$OUTPUT" | grep "^Q" | head -1 | awk '{print $2}')
    RATIO=$(echo "scale=1; $TIME_MS / $DMS" | bc 2>/dev/null || echo "?")
    PASS=$((PASS + 1))
    TOTAL_ENGINE=$(echo "$TOTAL_ENGINE + $TIME_MS" | bc)
    TOTAL_DUCKDB=$((TOTAL_DUCKDB + DMS))

    # Compare with DuckDB CSV
    ENGINE_FILE="$ENGINE_CSV/q$(printf '%02d' $Q).csv"
    DUCKDB_FILE="$DUCKDB_CSV/q$(printf '%02d' $Q).csv"

    if [ -f "$ENGINE_FILE" ] && [ -f "$DUCKDB_FILE" ]; then
        # Compare row counts
        E_ROWS=$(wc -l < "$ENGINE_FILE")
        D_ROWS=$(wc -l < "$DUCKDB_FILE")
        E_ROWS=$((E_ROWS - 1))  # subtract header
        D_ROWS=$((D_ROWS - 1))

        if [ "$E_ROWS" != "$D_ROWS" ]; then
            MATCH="FAIL (rows: engine=$E_ROWS, duckdb=$D_ROWS)"
            INCORRECT=$((INCORRECT + 1))
        else
            MATCH="MATCH ($E_ROWS rows)"
            CORRECT=$((CORRECT + 1))
        fi
    else
        MATCH="NO_CSV"
    fi

    printf "Q%-5d %14s %10s %12d %8sx %s\n" $Q "$TIME_MS" "$ROWS" $DMS "$RATIO" "$MATCH"
done

echo "======================================================================"
echo ""
echo "Summary:"
echo "  Completed: $PASS, Failed: $FAIL, Timeout: $TIMEOUT_COUNT"
echo "  Correct: $CORRECT, Incorrect: $INCORRECT"
if [ "$TOTAL_DUCKDB" -gt 0 ]; then
    TOTAL_RATIO=$(echo "scale=1; $TOTAL_ENGINE / $TOTAL_DUCKDB" | bc 2>/dev/null || echo "?")
    echo "  Total engine: ${TOTAL_ENGINE}ms, Total DuckDB: ${TOTAL_DUCKDB}ms, Ratio: ${TOTAL_RATIO}x"
fi
