#!/bin/bash
QUERY_PTRACE_ANY=1 ./target/release/query_engine benchmark-parquet --path ./data/tpch-10gb --query 9 --iterations 1 >/dev/null 2>&1 &
PID=$!
sleep 2.0
gdb -p $PID -batch -ex "thread apply all bt 20" 2>/dev/null > .scratch/q9_deep.txt
wait $PID
