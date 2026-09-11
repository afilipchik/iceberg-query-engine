# GDB Python: read selected scalar ownership fields from the owned test process.
import gdb,json
class RefusalLedger(gdb.Command):
 def __init__(self):super().__init__('refusal-ledger',gdb.COMMAND_USER)
 def invoke(self,arg,from_tty):
  original=gdb.selected_frame();frame=gdb.newest_frame();result=[]
  while frame is not None:
   name=frame.name() or '';expressions=[]
   if 'PoolState::grow' in name:expressions=['size','increment','used','pool.max_memory']
   elif 'compute_build_decision' in name and 'async_fn' in name:expressions=['flat_size','flat_rows','flat_batches.len','memory_threshold']
   elif 'admitted::Reader::next' in name:expressions=['self._schema_reservation.size','self.runtime.reservation.size','self.output_positions.reservation.size']
   elif 'AdmittedBatchReader' in name and 'next_batch' in name:expressions=['max_rows','value_bytes','self.remaining','self.columns.values.len','self.columns.reservation.size','self.pending.reservation.size','handoff']
   elif 'read_page_range' in name:expressions=['offset','bytes']
   if expressions:
    frame.select();row={'frame':name,'values':{}}
    for expression in expressions:
     try:row['values'][expression]=str(gdb.parse_and_eval(expression))
     except Exception as error:row['values'][expression]={'unavailable':str(error)}
    result.append(row)
   frame=frame.older()
  original.select();print('REFUSAL_LEDGER_JSON '+json.dumps(result))
RefusalLedger()
