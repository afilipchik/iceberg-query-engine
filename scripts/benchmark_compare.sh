#!/bin/bash

# Benchmark comparison script: Query Engine vs DuckDB
# Usage: ./scripts/benchmark_compare.sh [query_num]
# If query_num is provided, only run that query. Otherwise run all 22.

DATA_PATH="./data/tpch-10gb"
QUERY_NUM=${1:-"all"}

# TPC-H Queries
declare -A QUERIES
QUERIES[1]="SELECT l_returnflag, l_linestatus, SUM(l_quantity), SUM(l_extendedprice), COUNT(*) FROM lineitem WHERE l_shipdate <= DATE '1998-09-02' GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus"

QUERIES[2]="SELECT s_acctbal, s_name, n_name, p_partkey FROM part, supplier, partsupp, nation, region WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND p_size = 15 AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = 'EUROPE' ORDER BY s_acctbal DESC, n_name, s_name, p_partkey LIMIT 100"

QUERIES[3]="SELECT l_orderkey, SUM(l_extendedprice * (1 - l_discount)) AS revenue, o_orderdate, o_shippriority FROM customer, orders, lineitem WHERE c_mktsegment = 'BUILDING' AND c_custkey = o_custkey AND l_orderkey = o_orderkey AND o_orderdate < DATE '1995-03-15' AND l_shipdate > DATE '1995-03-15' GROUP BY l_orderkey, o_orderdate, o_shippriority ORDER BY revenue DESC, o_orderdate LIMIT 10"

QUERIES[4]="SELECT o_orderpriority, COUNT(*) AS order_count FROM orders WHERE o_orderdate >= DATE '1993-07-01' AND o_orderdate < DATE '1993-10-01' GROUP BY o_orderpriority ORDER BY o_orderpriority"

QUERIES[5]="SELECT n_name, SUM(l_extendedprice * (1 - l_discount)) AS revenue FROM customer, orders, lineitem, supplier, nation, region WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = 'ASIA' AND o_orderdate >= DATE '1994-01-01' AND o_orderdate < DATE '1995-01-01' GROUP BY n_name ORDER BY revenue DESC"

QUERIES[6]="SELECT SUM(l_extendedprice * l_discount) AS revenue FROM lineitem WHERE l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01' AND l_discount >= 0.05 AND l_discount <= 0.07 AND l_quantity < 24"

QUERIES[10]="SELECT c_custkey, c_name, SUM(l_extendedprice * (1 - l_discount)) AS revenue FROM customer, orders, lineitem, nation WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND o_orderdate >= DATE '1993-10-01' AND o_orderdate < DATE '1994-01-01' AND l_returnflag = 'R' AND c_nationkey = n_nationkey GROUP BY c_custkey, c_name ORDER BY revenue DESC LIMIT 20"

QUERIES[12]="SELECT l_shipmode, COUNT(*) FROM orders, lineitem WHERE o_orderkey = l_orderkey AND l_shipmode IN ('MAIL', 'SHIP') AND l_commitdate < l_receiptdate AND l_shipdate < l_commitdate AND l_receiptdate >= DATE '1994-01-01' AND l_receiptdate < DATE '1995-01-01' GROUP BY l_shipmode ORDER BY l_shipmode"

QUERIES[14]="SELECT 100.0 * SUM(CASE WHEN p_type LIKE 'PROMO%' THEN l_extendedprice * (1 - l_discount) ELSE 0 END) / SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue FROM lineitem, part WHERE l_partkey = p_partkey AND l_shipdate >= DATE '1995-09-01' AND l_shipdate < DATE '1995-10-01'"

# DuckDB setup script
DUCKDB_SETUP="
CREATE VIEW nation AS SELECT * FROM read_parquet('${DATA_PATH}/nation.parquet');
CREATE VIEW region AS SELECT * FROM read_parquet('${DATA_PATH}/region.parquet');
CREATE VIEW part AS SELECT * FROM read_parquet('${DATA_PATH}/part.parquet');
CREATE VIEW supplier AS SELECT * FROM read_parquet('${DATA_PATH}/supplier.parquet');
CREATE VIEW partsupp AS SELECT * FROM read_parquet('${DATA_PATH}/partsupp.parquet');
CREATE VIEW customer AS SELECT * FROM read_parquet('${DATA_PATH}/customer.parquet');
CREATE VIEW orders AS SELECT * FROM read_parquet('${DATA_PATH}/orders.parquet');
CREATE VIEW lineitem AS SELECT * FROM read_parquet('${DATA_PATH}/lineitem.parquet');
"

run_duckdb() {
    local query_num=$1
    local query="${QUERIES[$query_num]}"

    if [ -z "$query" ]; then
        echo "Query $query_num not defined"
        return 1
    fi

    echo "Running DuckDB Q${query_num}..."

    # Run and time
    local start=$(date +%s.%N)
    duckdb -c "${DUCKDB_SETUP} ${query};" > /dev/null 2>&1
    local end=$(date +%s.%N)
    local elapsed=$(echo "$end - $start" | bc)
    echo "DuckDB Q${query_num}: ${elapsed}s"
}

run_engine() {
    local query_num=$1

    echo "Running Query Engine Q${query_num}..."

    # Use timeout to prevent infinite hangs
    local start=$(date +%s.%N)
    timeout 300 ./target/release/query_engine benchmark-parquet --path ./data/tpch-10gb --iterations 1 2>&1 | grep "Q${query_num}:"
    local end=$(date +%s.%N)
    local elapsed=$(echo "$end - $start" | bc)
    echo "Engine: ${elapsed}s total for all queries"
}

# Main
echo "=== TPC-H Benchmark Comparison ==="
echo "Data: ${DATA_PATH}"
echo ""

if [ "$QUERY_NUM" = "all" ]; then
    # Run DuckDB for all queries first
    echo "=== DuckDB Results ==="
    for q in 1 2 3 4 5 6 10 12 14; do
        run_duckdb $q
    done
else
    run_duckdb $QUERY_NUM
fi
