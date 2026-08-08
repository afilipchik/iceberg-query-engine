#!/bin/bash
# Retry timed-out SF=100 queries with 600s timeout each
# Only runs queries that don't have engine CSV results yet

ENGINE="./target/release/query_engine"
DATA="data/tpch-100gb"
ENGINE_CSV="data/sf100_engine_results"
DUCKDB_CSV="data/sf100_duckdb_results"

# DuckDB best times at SF=100 (ms)
declare -A DUCKDB_MS=(
    [1]=2773 [2]=313 [3]=2755 [4]=1793 [5]=2265 [6]=1191
    [7]=2411 [8]=2698 [9]=28094 [10]=1096 [11]=96 [12]=985
    [13]=1251 [14]=463 [15]=994 [16]=709 [17]=2191 [18]=4748
    [19]=2625 [20]=3210 [21]=4012 [22]=393
)

TIMEOUT_S=600  # 10 minutes per query

echo "Retrying timed-out SF=100 queries (timeout=${TIMEOUT_S}s each)"
echo "============================================================"
printf "%-6s %14s %10s %12s %8s %s\n" "Query" "Engine (ms)" "Rows" "DuckDB (ms)" "Ratio" "Correct"
echo "----------------------------------------------------------------------"

for Q in $(seq 1 22); do
    CSV_FILE="$ENGINE_CSV/q$(printf '%02d' $Q).csv"
    if [ -f "$CSV_FILE" ]; then
        continue  # Skip already completed queries
    fi

    DMS=${DUCKDB_MS[$Q]}

    OUTPUT=$(timeout ${TIMEOUT_S}s $ENGINE benchmark-parquet --path "$DATA" --query $Q --iterations 1 --sf 100 --save-csv "$ENGINE_CSV" 2>&1)
    EXIT_CODE=$?

    if [ $EXIT_CODE -eq 124 ]; then
        printf "Q%-5d %14s %10s %12d %8s TIMEOUT (>${TIMEOUT_S}s)\n" $Q "-" "-" $DMS "-"
        continue
    elif [ $EXIT_CODE -ne 0 ]; then
        ERROR=$(echo "$OUTPUT" | tail -1)
        printf "Q%-5d %14s %10s %12d %8s ERROR: %s\n" $Q "-" "-" $DMS "-" "$ERROR"
        continue
    fi

    TIME_MS=$(echo "$OUTPUT" | grep "^Q" | head -1 | sed 's/.*in *//' | sed 's/ms//' | tr -d ' ')
    ROWS=$(echo "$OUTPUT" | grep "^Q" | head -1 | awk '{print $2}')
    RATIO=$(echo "scale=1; $TIME_MS / $DMS" | bc 2>/dev/null || echo "?")

    # Compare row counts
    ENGINE_FILE="$ENGINE_CSV/q$(printf '%02d' $Q).csv"
    DUCKDB_FILE="$DUCKDB_CSV/q$(printf '%02d' $Q).csv"

    if [ -f "$ENGINE_FILE" ] && [ -f "$DUCKDB_FILE" ]; then
        E_ROWS=$(wc -l < "$ENGINE_FILE")
        D_ROWS=$(wc -l < "$DUCKDB_FILE")
        E_ROWS=$((E_ROWS - 1))
        D_ROWS=$((D_ROWS - 1))
        if [ "$E_ROWS" != "$D_ROWS" ]; then
            MATCH="FAIL (engine=$E_ROWS, duckdb=$D_ROWS)"
        else
            MATCH="MATCH ($E_ROWS rows)"
        fi
    else
        MATCH="NO_CSV"
    fi

    printf "Q%-5d %14s %10s %12d %8sx %s\n" $Q "$TIME_MS" "$ROWS" $DMS "$RATIO" "$MATCH"
done

echo "============================================================"
echo "Done. All engine CSVs saved to $ENGINE_CSV/"
