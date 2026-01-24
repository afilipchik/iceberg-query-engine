use query_engine::physical::operators::TableProvider;
use query_engine::storage::{ParquetTable, StreamingParquetReader};

fn main() {
    let table = ParquetTable::try_new("/tmp/tpch-1gb/lineitem.parquet").unwrap();

    println!("=== ParquetTable.scan() ===");
    let batches = table.scan(None).unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    let total_batches = batches.len();
    println!("Batches: {}", total_batches);
    println!("Total rows: {}", total_rows);

    println!("\n=== StreamingParquetReader ===");
    let mut reader = StreamingParquetReader::from_table(&table, None, 8192);
    let mut streaming_rows = 0usize;
    let mut streaming_batches = 0usize;
    while let Some(batch) = reader.next_batch().unwrap() {
        streaming_rows += batch.num_rows();
        streaming_batches += 1;
    }
    println!("Batches: {}", streaming_batches);
    println!("Total rows: {}", streaming_rows);
}
