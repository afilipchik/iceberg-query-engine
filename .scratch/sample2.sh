#!/bin/bash
QUERY_PTRACE_ANY=1 ./target/release/query_engine load-parquet --path "$1" --name lineitem --query "SELECT COUNT(*) FROM (SELECT l_orderkey FROM lineitem GROUP BY l_orderkey HAVING SUM(l_quantity) > 300) t" &
PID=$!
sleep 1.30
for i in 1 2 3; do
  gdb -p $PID -batch -ex "thread apply all bt 25" 2>/dev/null > .scratch/bt_$i.txt
  sleep 0.15
done
wait $PID
