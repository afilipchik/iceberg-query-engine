import json,hashlib
from pathlib import Path
import pyarrow as pa
root=Path('.scratch/public-bench/canonical-sf10-providers-02/native')
out={}
for table in ['lineitem','orders']:
 schemas={};files=[]
 for path in sorted((root/table).glob('rg_*.arrow')):
  with pa.memory_map(str(path),'r') as source:
   reader=pa.ipc.open_file(source);schema=reader.schema
   description=str(schema);schemas[description]=schemas.get(description,0)+1
   files.append({'path':str(path),'dictionary_fields':[f.name for f in schema if pa.types.is_dictionary(f.type)],'record_batches':reader.num_record_batches})
 out[table]={'schema_counts':schemas,'files':files}
p=Path('.scratch/parallel-aggregate-input/ipc-q12-native-schema-audit.json');assert not p.exists();p.write_text(json.dumps(out,indent=2)+'\n')
print(json.dumps({k:{'files':len(v['files']),'schema_counts':v['schema_counts'],'dictionary_files':sum(bool(f['dictionary_fields']) for f in v['files'])} for k,v in out.items()},indent=2))
