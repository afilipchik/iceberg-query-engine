//! Bulk reading TPC-H Q1 - single reader, all row groups
//!
//! This approach uses a single Parquet reader for all data
//! with parallel aggregation using rayon.
//!
//! Run with: cargo run --release --example bulk_q1

use arrow::array::{Date32Array, Float64Array, StringArray};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;
use query_engine::error::Result;
use rayon::prelude::*;
use std::fs::File;
use std::sync::Mutex;
use std::time::Instant;

const BATCH_SIZE: usize = 65536;

#[derive(Clone, Copy)]
struct Q1Key(u8);

impl Q1Key {
    #[inline(always)]
    fn from_bytes(rf: u8, ls: u8) -> Self {
        let rf_idx = match rf {
            b'A' => 0,
            b'N' => 1,
            b'R' => 2,
            _ => 0,
        };
        let ls_idx = match ls {
            b'F' => 0,
            b'O' => 1,
            _ => 0,
        };
        Q1Key(rf_idx * 2 + ls_idx)
    }

    fn to_strings(&self) -> (&'static str, &'static str) {
        match self.0 {
            0 => ("A", "F"),
            1 => ("A", "O"),
            2 => ("N", "F"),
            3 => ("N", "O"),
            4 => ("R", "F"),
            5 => ("R", "O"),
            _ => ("?", "?"),
        }
    }
}

#[repr(C, align(64))]
#[derive(Clone, Default)]
struct Q1Acc {
    sum_qty: f64,
    sum_base_price: f64,
    sum_disc_price: f64,
    sum_charge: f64,
    sum_disc: f64,
    count: i64,
    _padding: [u64; 2],
}

fn process_batch(batch: &RecordBatch, date_limit: i32) -> [Q1Acc; 6] {
    let mut accumulators: [Q1Acc; 6] = std::array::from_fn(|_| Q1Acc::default());

    let qty = batch
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let price = batch
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let disc = batch
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let tax = batch
        .column(3)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let returnflag = batch
        .column(4)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let linestatus = batch
        .column(5)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let shipdate = batch
        .column(6)
        .as_any()
        .downcast_ref::<Date32Array>()
        .unwrap();

    let qty_values = qty.values();
    let price_values = price.values();
    let disc_values = disc.values();
    let tax_values = tax.values();
    let shipdate_values = shipdate.values();

    for i in 0..batch.num_rows() {
        if shipdate_values[i] <= date_limit {
            let rf = returnflag.value(i).as_bytes()[0];
            let ls = linestatus.value(i).as_bytes()[0];
            let key = Q1Key::from_bytes(rf, ls);

            let p = price_values[i];
            let d = disc_values[i];
            let t = tax_values[i];
            let disc_price = p * (1.0 - d);

            let acc = &mut accumulators[key.0 as usize];
            acc.sum_qty += qty_values[i];
            acc.sum_base_price += p;
            acc.sum_disc_price += disc_price;
            acc.sum_charge += disc_price * (1.0 + t);
            acc.sum_disc += d;
            acc.count += 1;
        }
    }

    accumulators
}

fn main() -> Result<()> {
    let path = "./data/tpch-10gb/lineitem.parquet";
    let date_limit = 10471;
    let projection = vec![4, 5, 6, 7, 8, 9, 10];

    println!("Running Bulk Reading TPC-H Q1");
    println!("Path: {}", path);
    println!("Threads: {}", rayon::current_num_threads());
    println!();

    let start = Instant::now();

    // Open single reader
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let mask = ProjectionMask::roots(builder.parquet_schema(), projection.iter().copied());
    let reader = builder
        .with_projection(mask)
        .with_batch_size(BATCH_SIZE)
        .build()?;

    // Collect all batches (this is sequential I/O)
    let batches: Vec<RecordBatch> = reader.map(|r| r.unwrap()).collect();

    let read_elapsed = start.elapsed();
    println!("Read {} batches in {:?}", batches.len(), read_elapsed);

    // Process batches in parallel
    let agg_start = Instant::now();
    let batch_results: Vec<[Q1Acc; 6]> = batches
        .par_iter()
        .map(|batch| process_batch(batch, date_limit))
        .collect();

    // Merge results
    let mut final_accumulators: [Q1Acc; 6] = std::array::from_fn(|_| Q1Acc::default());
    for accs in batch_results {
        for i in 0..6 {
            final_accumulators[i].sum_qty += accs[i].sum_qty;
            final_accumulators[i].sum_base_price += accs[i].sum_base_price;
            final_accumulators[i].sum_disc_price += accs[i].sum_disc_price;
            final_accumulators[i].sum_charge += accs[i].sum_charge;
            final_accumulators[i].sum_disc += accs[i].sum_disc;
            final_accumulators[i].count += accs[i].count;
        }
    }

    let agg_elapsed = agg_start.elapsed();
    let total_elapsed = start.elapsed();

    // Print results
    println!("\nResults:");
    println!(
        "{:>12} {:>12} {:>18} {:>18} {:>18} {:>18} {:>10}",
        "returnflag",
        "linestatus",
        "sum_qty",
        "sum_base_price",
        "sum_disc_price",
        "sum_charge",
        "count"
    );
    println!("{}", "-".repeat(120));

    for i in 0..6 {
        let key = Q1Key(i);
        let (rf, ls) = key.to_strings();
        let acc = &final_accumulators[i as usize];
        if acc.count > 0 {
            println!(
                "{:>12} {:>12} {:>18.2} {:>18.2} {:>18.2} {:>18.2} {:>10}",
                rf,
                ls,
                acc.sum_qty,
                acc.sum_base_price,
                acc.sum_disc_price,
                acc.sum_charge,
                acc.count
            );
        }
    }

    println!();
    println!("Read time: {:?}", read_elapsed);
    println!("Aggregation time: {:?}", agg_elapsed);
    println!("Total time: {:?}", total_elapsed);
    println!(
        "vs DuckDB (~180ms): {:.1}x slower",
        total_elapsed.as_secs_f64() / 0.180
    );

    Ok(())
}
