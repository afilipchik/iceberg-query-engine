//! Async I/O TPC-H Q1 with tokio runtime
//!
//! Uses async Parquet reading for better I/O parallelism
//!
//! Run with: cargo run --release --example async_q1

use arrow::array::{Date32Array, Float64Array, StringArray};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;
use query_engine::error::Result;
use std::fs::File;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;
use tokio::task;

const BATCH_SIZE: usize = 65536;
const CHANNEL_BUFFER: usize = 16;

/// Minimal key
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
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

#[derive(Clone)]
struct Q1State {
    accumulators: Box<[Q1Acc; 6]>,
}

impl Default for Q1State {
    fn default() -> Self {
        Self {
            accumulators: Box::new(std::array::from_fn(|_| Q1Acc::default())),
        }
    }
}

impl Q1State {
    #[inline(never)]
    fn process_batch(
        &mut self,
        qty_values: &[f64],
        price_values: &[f64],
        disc_values: &[f64],
        tax_values: &[f64],
        returnflag: &StringArray,
        linestatus: &StringArray,
        shipdate_values: &[i32],
        date_limit: i32,
    ) {
        for i in 0..qty_values.len() {
            if shipdate_values[i] <= date_limit {
                let rf = returnflag.value(i).as_bytes()[0];
                let ls = linestatus.value(i).as_bytes()[0];
                let key = Q1Key::from_bytes(rf, ls);

                let p = price_values[i];
                let d = disc_values[i];
                let t = tax_values[i];
                let disc_price = p * (1.0 - d);

                let acc = &mut self.accumulators[key.0 as usize];
                acc.sum_qty += qty_values[i];
                acc.sum_base_price += p;
                acc.sum_disc_price += disc_price;
                acc.sum_charge += disc_price * (1.0 + t);
                acc.sum_disc += d;
                acc.count += 1;
            }
        }
    }

    fn merge(&mut self, other: &Q1State) {
        for i in 0..6 {
            self.accumulators[i].sum_qty += other.accumulators[i].sum_qty;
            self.accumulators[i].sum_base_price += other.accumulators[i].sum_base_price;
            self.accumulators[i].sum_disc_price += other.accumulators[i].sum_disc_price;
            self.accumulators[i].sum_charge += other.accumulators[i].sum_charge;
            self.accumulators[i].sum_disc += other.accumulators[i].sum_disc;
            self.accumulators[i].count += other.accumulators[i].count;
        }
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let path = "./data/tpch-10gb/lineitem.parquet";
    let date_limit = 10471;
    let projection = vec![4, 5, 6, 7, 8, 9, 10];

    println!("Running Async I/O TPC-H Q1");
    println!("Path: {}", path);
    println!(
        "Threads: {}",
        tokio::runtime::Handle::current().metrics().num_workers()
    );
    println!();

    let start = Instant::now();

    // Get metadata
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let metadata = builder.metadata().clone();
    let num_row_groups = metadata.num_row_groups();
    drop(builder);

    // Create channel for receiving states
    let (tx, mut rx) = mpsc::channel::<Q1State>(CHANNEL_BUFFER);

    // Spawn reader tasks for each row group
    let path = Arc::new(path.to_string());
    let projection = Arc::new(projection);

    for rg_idx in 0..num_row_groups {
        let tx = tx.clone();
        let path = path.clone();
        let projection = projection.clone();

        task::spawn_blocking(move || {
            let mut state = Q1State::default();

            let file = File::open(path.as_str()).unwrap();
            let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
            let mask = ProjectionMask::roots(builder.parquet_schema(), projection.iter().copied());
            let reader = builder
                .with_projection(mask)
                .with_row_groups(vec![rg_idx])
                .with_batch_size(BATCH_SIZE)
                .build()
                .unwrap();

            for batch_result in reader {
                let batch = batch_result.unwrap();

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

                state.process_batch(
                    qty.values(),
                    price.values(),
                    disc.values(),
                    tax.values(),
                    returnflag,
                    linestatus,
                    shipdate.values(),
                    date_limit,
                );
            }

            let _ = tx.blocking_send(state);
        });
    }

    // Drop original sender
    drop(tx);

    // Collect and merge results
    let mut final_state = Q1State::default();
    while let Some(state) = rx.recv().await {
        final_state.merge(&state);
    }

    let elapsed = start.elapsed();

    // Print results
    println!("Results:");
    println!(
        "{:>12} {:>12} {:>18} {:>18} {:>18} {:>18} {:>15} {:>15} {:>15} {:>10}",
        "returnflag",
        "linestatus",
        "sum_qty",
        "sum_base_price",
        "sum_disc_price",
        "sum_charge",
        "avg_qty",
        "avg_price",
        "avg_disc",
        "count"
    );
    println!("{}", "-".repeat(160));

    for i in 0..6 {
        let key = Q1Key(i);
        let (rf, ls) = key.to_strings();
        let acc = &final_state.accumulators[i as usize];
        if acc.count > 0 {
            println!(
                "{:>12} {:>12} {:>18.2} {:>18.2} {:>18.2} {:>18.2} {:>15.2} {:>15.2} {:>15.6} {:>10}",
                rf, ls,
                acc.sum_qty, acc.sum_base_price, acc.sum_disc_price, acc.sum_charge,
                acc.sum_qty / acc.count as f64, acc.sum_base_price / acc.count as f64,
                acc.sum_disc / acc.count as f64, acc.count
            );
        }
    }

    println!();
    println!("Time: {:?}", elapsed);
    println!(
        "vs DuckDB (~180ms): {:.1}x slower",
        elapsed.as_secs_f64() / 0.180
    );

    Ok(())
}
