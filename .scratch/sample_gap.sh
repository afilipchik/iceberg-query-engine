#!/bin/bash
QUERY_PTRACE_ANY=1 ./target/release/query_engine load-parquet --path "$1" --name lineitem --query "SELECT COUNT(*) FROM (SELECT l_orderkey FROM lineitem GROUP BY l_orderkey HAVING SUM(l_quantity) > 300) t" &
PID=$!
sleep 1.35
for i in 1 2 3 4 5; do
  gdb -p $PID -batch -ex "thread apply all bt 6" 2>/dev/null | grep -E "^#[0-5] " | grep -oE "in [a-zA-Z_:<>0-9 ]+" | sort | uniq -c | sort -rn | head -5
  echo ---
  sleep 0.1
done
wait $PID
