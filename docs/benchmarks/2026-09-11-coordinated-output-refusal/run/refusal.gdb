source /media/afilipchik/nvme6tb/src/afilipchik/iceberg-query-engine/.scratch/parallel-aggregate-input/refusal_ownership_ledger_gdb.py
set pagination off
set confirm off
set debuginfod enabled off
set print thread-events off
set print frame-arguments none
break /media/afilipchik/nvme6tb/src/afilipchik/iceberg-query-engine/src/execution/memory.rs:439
commands
silent
printf "MEMORY_REFUSAL_STACK\n"
bt 24
refusal-ledger
continue
end
run
printf "INFERIOR_EXIT=%d\n", $_exitcode
