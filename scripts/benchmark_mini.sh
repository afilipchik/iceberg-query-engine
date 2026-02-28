#!/bin/bash

# Lightweight TPC-H Benchmark: Query Engine vs DuckDB
# Focus: Queries where we lag, ordered by ROI
# Target: < 30 seconds total

set -e

DATA_PATH="./data/tpch-100mb"
ITERATIONS=3

echo "╔══════════════════════════════════════════════════════════════╗"
echo "║  MINI TPC-H BENCHMARK: Engine vs DuckDB                     ║"
echo "║  Focus: Queries where we lag (SF=0.1, ~100MB)              ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""

# Check data exists
if [ ! -d "$DATA_PATH" ]; then
    echo "❌ Data path not found: $DATA_PATH"
    echo "   Generate data first: cargo run --release -- generate-parquet --sf 0.1 --output $DATA_PATH"
    exit 1
fi

# Check DuckDB installed
if ! command -v duckdb &> /dev/null; then
    echo "❌ DuckDB not found. Install: brew install duckdb"
    exit 1
fi

# DuckDB setup (shared across queries)
DUCKDB_SETUP="
CREATE VIEW nation AS SELECT * FROM read_parquet('${DATA_PATH}/nation.parquet');
CREATE VIEW region AS SELECT * FROM read_parquet('${DATA_PATH}/region.parquet');
CREATE VIEW part AS SELECT * FROM read_parquet('${DATA_PATH}/part.parquet');
CREATE VIEW supplier AS SELECT * FROM read_parquet('${DATA_PATH}/supplier.parquet');
CREATE VIEW partsupp AS SELECT * FROM read_parquet('${DATA_PATH}/partsupp.parquet');
CREATE VIEW customer AS SELECT * FROM read_parquet('${DATA_PATH}/customer.parquet');
CREATE VIEW orders AS SELECT * FROM read_parquet('${DATA_PATH}/orders.parquet');
CREATE VIEW lineitem AS SELECT * FROM read_parquet('${DATA_PATH}/lineitem.parquet');
TIMING ON;
"

# Queries to benchmark (ROI priority order)
# Format: name:query_sql
declare -A QUERIES

# Q1: Simple aggregation (baseline - we're competitive)
QUERIES[Q01]="SELECT l_returnflag, l_linestatus, SUM(l_quantity), SUM(l_extendedprice), COUNT(*) FROM lineitem WHERE l_shipdate <= DATE '1998-09-02' GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus"

# Q3: 3-way join + aggregation (4.3x slower)
QUERIES[Q03]="SELECT l_orderkey, SUM(l_extendedprice * (1 - l_discount)) AS revenue, o_orderdate, o_shippriority FROM customer, orders, lineitem WHERE c_mktsegment = 'BUILDING' AND c_custkey = o_custkey AND l_orderkey = o_orderkey AND o_orderdate < DATE '1995-03-15' AND l_shipdate > DATE '1995-03-15' GROUP BY l_orderkey, o_orderdate, o_shippriority ORDER BY revenue DESC, o_orderdate LIMIT 10"

# Q5: 5-way join (10.7x slower)
QUERIES[Q05]="SELECT n_name, SUM(l_extendedprice * (1 - l_discount)) AS revenue FROM customer, orders, lineitem, supplier, nation WHERE c_custkey = o_custkey AND l_orderkey = l_orderkey AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = 'ASIA' AND o_orderdate >= DATE '1994-01-01' AND o_orderdate < DATE '1995-01-01' GROUP BY n_name ORDER BY revenue DESC"

# Q7: 6-way join (133x slower - high ROI)
QUERIES[Q07]="SELECT supp_nation, cust_nation, l_year, SUM(volume) AS revenue FROM (SELECT n1.n_name AS supp_nation, n2.n_name AS cust_nation, EXTRACT(YEAR FROM l_shipdate) AS l_year, l_extendedprice * (1 - l_discount) AS volume FROM supplier, lineitem, orders, customer, nation n1, nation n2 WHERE s_suppkey = l_suppkey AND o_orderkey = l_orderkey AND c_custkey = o_custkey AND s_nationkey = n1.n_nationkey AND c_nationkey = n2.n_nationkey AND (n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY' OR n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE') AND l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31') AS shipping GROUP BY supp_nation, cust_nation, l_year ORDER BY supp_nation, cust_nation, l_year"

# Q9: 6-way join + agg (53x slower - high ROI)
QUERIES[Q09]="SELECT nation, o_year, SUM(amount) AS sum_profit FROM (SELECT n_name AS nation, EXTRACT(YEAR FROM o_orderdate) AS o_year, l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount FROM part, supplier, lineitem, partsupp, orders, nation WHERE s_suppkey = l_suppkey AND ps_suppkey = l_suppkey AND ps_partkey = l_partkey AND p_partkey = l_partkey AND o_orderkey = l_orderkey AND s_nationkey = n_nationkey AND p_name LIKE '%green%') AS profit GROUP BY nation, o_year ORDER BY nation, o_year DESC"

# Q17: Scalar subquery (20x slower - medium ROI)
QUERIES[Q17]="SELECT AVG(l_extendedprice * (1 - l_discount)) AS avg_disc FROM lineitem, part WHERE l_partkey = p_partkey AND p_brand = 'Brand#45' AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') AND l_quantity < (SELECT 0.2 * AVG(l_quantity) FROM lineitem WHERE l_partkey = p_partkey)"

# Q18: IN subquery (30x slower - medium ROI)
QUERIES[Q18]="SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, SUM(l_quantity) FROM customer, orders, lineitem WHERE o_orderkey IN (SELECT l_orderkey FROM lineitem GROUP BY l_orderkey HAVING SUM(l_quantity) > 300) AND c_custkey = o_custkey AND o_orderkey = l_orderkey GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice ORDER BY o_totalprice DESC, o_orderdate LIMIT 100"

run_duckdb_benchmark() {
    local query_name=$1
    local query_sql=$2

    echo "🦆 DuckDB $query_name..."

    local total_time=0
    for i in $(seq 1 $ITERATIONS); do
        local time=$(duckdb -c "${DUCKDB_SETUP} ${query_sql}" 2>&1 | grep "Total query runtime" | awk '{print $4}')
        if [ -z "$time" ]; then
            time=$(duckdb -c "${DUCKDB_SETUP} EXPLAIN ${query_sql}" 2>&1 | grep "query_time" | awk -F= '{print $2}' | awk '{printf "%.0f", $1}')
        fi
        total_time=$(echo "$total_time + $time" | bc -l 2>/dev/null || echo "$total_time + 0")
    done

    local avg=$(echo "scale=2; $total_time / $ITERATIONS" | bc -l 2>/dev/null || echo "0")
    echo "   └─ Avg: ${avg}ms"
    echo "$avg"
}

run_engine_benchmark() {
    local query_name=$1
    local query_sql=$2

    echo "🚀 Engine $query_name..."

    # Write query to temp file
    local temp_sql=$(mktemp)
    echo "$query_sql;" > "$temp_sql"

    local total_time=0
    local count=0

    for i in $(seq 1 $ITERATIONS); do
        local result=$(cargo run --release -- sql --file "$temp_sql" 2>&1 | grep "Execution:" | awk '{print $2}' | sed 's/ms//')
        if [ -n "$result" ]; then
            total_time=$(echo "$total_time + $result" | bc -l 2>/dev/null || echo "$total_time")
            count=$((count + 1))
        fi
    done

    rm -f "$temp_sql"

    if [ $count -gt 0 ]; then
        local avg=$(echo "scale=2; $total_time / $count" | bc -l)
        echo "   └─ Avg: ${avg}ms"
        echo "$avg"
    else
        echo "   └─ Failed"
        echo "999999"
    fi
}

# Run benchmarks
echo "┌─────────────────────────────────────────────────────────────┐"
echo "│ Results (avg of $ITERATIONS iterations, SF=0.1)                         │"
echo "├──────────┬──────────┬──────────┬──────────┬───────────┤"
echo "│ Query    │ Engine   │ DuckDB   │ Ratio    │ Status    │"
echo "├──────────┼──────────┼──────────┼──────────┼───────────┤"

for qname in "Q01" "Q03" "Q05" "Q07" "Q09" "Q17" "Q18"; do
    query="${QUERIES[$qname]}"

    duckdb_time=$(run_duckdb_benchmark "$qname" "$query")
    engine_time=$(run_engine_benchmark "$qname" "$query")

    if [ "$engine_time" != "999999" ]; then
        ratio=$(echo "scale=1; $engine_time / $duckdb_time" | bc -l)

        # Determine status
        if (( $(echo "$ratio < 1.5" | bc -l) )); then
            status="✅ Good"
        elif (( $(echo "$ratio < 3" | bc -l) )); then
            status="⚠️  Fair"
        elif (( $(echo "$ratio < 10" | bc -l) )); then
            status="🔶 Slow"
        else
            status="🔴 Critical"
        fi

        printf "│ %-8s │ %8s │ %8s │ %8s │ %-9s │\n" "$qname" "${engine_time}ms" "${duckdb_time}ms" "${ratio}x" "$status"
    else
        printf "│ %-8s │    ERROR │ %8s │      N/A │ ❌ Error  │\n" "$qname" "${duckdb_time}ms"
    fi
done

echo "└──────────┴──────────┴──────────┴──────────┴───────────┘"
echo ""
echo "Legend:"
echo "  ✅ Good    - Within 1.5x of DuckDB"
echo "  ⚠️  Fair    - 1.5x - 3x slower"
echo "  🔶 Slow    - 3x - 10x slower"
echo "  🔴 Critical - > 10x slower (high ROI fix target)"
echo ""
echo "💡 ROI Priority: Q07 (133x), Q09 (53x), Q18 (30x), Q17 (20x)"
