#!/bin/bash
QUERY_PTRACE_ANY=1 ./target/release/query_engine benchmark-parquet --path ./data/tpch-10gb --query 9 --iterations 1 &
PID=$!
for d in 0.8 1.6 2.4 3.2; do
  sleep $d
  gdb -p $PID -batch -ex "thread apply all bt 12" 2>/dev/null | grep -oE "in [a-zA-Z_:<>0-9]+" | grep -vE "park|wait|sleep|futex|epoll|clone|thread_start|backtrace|call_once|poll" | sort | uniq -c | sort -rn | head -6
  echo "--- (t=$d)"
done
wait $PID
