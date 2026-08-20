//! Decode-floor microbenchmark: arrow-rs ParquetRecordBatchReader vs the
//! raw page pipeline (SerializedPageReader + snappy into one buffer) on the
//! benchmark's actual hot columns. Task 001 of the bespoke-decoders epic —
//! if arrow-rs is already within ~1.2x of the raw floor, the epic stops.
//!
//! Run: scripts/oomsafe.sh cargo run --release --example decode_bench -- \
//!        data/tpch-100gb/lineitem.parquet l_extendedprice

use parquet::column::page::Page;
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::sync::Arc;
use std::time::Instant;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let path = args
        .get(1)
        .cloned()
        .unwrap_or_else(|| "data/tpch-10gb/lineitem.parquet".into());
    let col_name = args
        .get(2)
        .cloned()
        .unwrap_or_else(|| "l_extendedprice".into());
    let rg_limit: usize = std::env::var("DB_RGS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(64);

    let file = std::fs::File::open(&path).expect("open parquet");
    let reader = SerializedFileReader::new(file).expect("reader");
    let md = reader.metadata();
    let col_idx = md
        .file_metadata()
        .schema_descr()
        .columns()
        .iter()
        .position(|c| c.name().eq_ignore_ascii_case(&col_name))
        .expect("column not found");
    let n_rg = md.num_row_groups().min(rg_limit);
    println!(
        "{path} col {col_name} (idx {col_idx}), {n_rg} row groups of {}",
        md.row_group(0).num_rows()
    );

    // --- Floor: raw pages + decompression into one reused buffer. This is
    // the work a bespoke decoder cannot avoid (I/O is page-cache warm).
    let t = Instant::now();
    let mut raw_bytes = 0usize;
    let mut sink = 0u64;
    for rg in 0..n_rg {
        let rg_reader = reader.get_row_group(rg).expect("rg");
        let mut pages = rg_reader.get_column_page_reader(col_idx).expect("pages");
        while let Some(page) = pages.as_mut().get_next_page().expect("page") {
            match page {
                Page::DataPage { buf, .. }
                | Page::DataPageV2 { buf, .. }
                | Page::DictionaryPage { buf, .. } => {
                    // SerializedPageReader already decompressed the page —
                    // buf is the uncompressed bytes. Touch every 4KB so the
                    // measurement includes reading it once.
                    let b = buf.as_ref() as &[u8];
                    raw_bytes += b.len();
                    let mut i = 0;
                    while i < b.len() {
                        sink = sink.wrapping_add(b[i] as u64);
                        i += 4096;
                    }
                }
            }
        }
    }
    let floor = t.elapsed();
    println!(
        "raw page pipeline (decompress + touch): {:?} for {} MB ({:.2} GB/s) sink={sink}",
        floor,
        raw_bytes / 1_000_000,
        raw_bytes as f64 / floor.as_secs_f64() / 1e9
    );

    // --- arrow-rs: single-column projection through the standard reader.
    let file = std::fs::File::open(&path).expect("open parquet");
    let builder =
        parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file).expect("b");
    let mask =
        parquet::arrow::ProjectionMask::leaves(builder.parquet_schema(), std::iter::once(col_idx));
    let arrow_reader = builder
        .with_projection(mask)
        .with_row_groups((0..n_rg).collect())
        .with_batch_size(65536)
        .build()
        .expect("build");
    let t = Instant::now();
    let mut rows = 0usize;
    let mut arrays: Vec<Arc<dyn arrow::array::Array>> = Vec::new();
    for batch in arrow_reader {
        let batch = batch.expect("batch");
        rows += batch.num_rows();
        // Keep one array alive per 100 batches so allocation costs are real
        // but memory stays bounded.
        if arrays.len() < 8 {
            arrays.push(batch.column(0).clone());
        }
    }
    let arrow_t = t.elapsed();
    println!(
        "arrow-rs single-column decode: {:?} for {rows} rows ({:.1} ns/row); ratio vs floor: {:.2}x",
        arrow_t,
        arrow_t.as_nanos() as f64 / rows as f64,
        arrow_t.as_secs_f64() / floor.as_secs_f64()
    );
}
