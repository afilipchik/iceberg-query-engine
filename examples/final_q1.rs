//! Final optimized TPC-H Q1
//!
//! Best practices from all experiments:
//! 1. Parallel row group reading with rayon
//! 2. Fixed-size accumulator array (no hash table)
//! 3. Direct primitive array access
//! 4. Cache-aligned accumulators
//! 5. Chunk all row groups into each thread
//!
//! Run with: cargo run --release --example final_q1

use arrow::array::{Date32Array, Float64Array, StringArray};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;
use query_engine::error::Result;
use rayon::prelude::*;
use std::fs::File;
use std::time::Instant;

const BATCH_SIZE: usize = 131072; // 128K for better I/O

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

impl Q1Acc {
    #[inline]
    fn merge(&mut self, other: &Q1Acc) {
        self.sum_qty += other.sum_qty;
        self.sum_base_price += other.sum_base_price;
        self.sum_disc_price += other.sum_disc_price;
        self.sum_charge += other.sum_charge;
        self.sum_disc += other.sum_disc;
        self.count += other.count;
    }
}

fn main() -> Result<()> {
    let path = "./data/tpch-10gb/lineitem.parquet";
    let date_limit = 10471i32;
    let projection = [4, 5, 6, 7, 8, 9, 10];

    let num_threads = rayon::current_num_threads();
    println!("TPC-H Q1 - Final Optimized");
    println!("Threads: {}, Batch: {}", num_threads, BATCH_SIZE);

    let start = Instant::now();

    // Get row group count
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let num_row_groups = builder.metadata().num_row_groups();
    drop(builder);

    // Assign row groups to threads
    let groups_per_thread = (num_row_groups + num_threads - 1) / num_threads;

    // Process in parallel
    let thread_results: Vec<[Q1Acc; 6]> = (0..num_threads)
        .into_par_iter()
        .map(|thread_id| {
            let mut accs: [Q1Acc; 6] = std::array::from_fn(|_| Q1Acc::default());

            let start_rg = thread_id * groups_per_thread;
            let end_rg = ((thread_id + 1) * groups_per_thread).min(num_row_groups);

            if start_rg >= num_row_groups {
                return accs;
            }

            // Open file and create reader for assigned row groups
            let file = File::open(path).unwrap();
            let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
            let mask = ProjectionMask::roots(builder.parquet_schema(), projection.iter().copied());
            let row_groups: Vec<usize> = (start_rg..end_rg).collect();

            let reader = builder
                .with_projection(mask)
                .with_row_groups(row_groups)
                .with_batch_size(BATCH_SIZE)
                .build()
                .unwrap();

            for batch_result in reader {
                let batch = batch_result.unwrap();
                let n = batch.num_rows();

                let qty = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .values();
                let price = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .values();
                let disc = batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .values();
                let tax = batch
                    .column(3)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .values();
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
                    .unwrap()
                    .values();

                for i in 0..n {
                    if shipdate[i] <= date_limit {
                        // Key computation: (A=0,N=1,R=2) * 2 + (F=0,O=1)
                        let rf = returnflag.value(i).as_bytes()[0];
                        let ls = linestatus.value(i).as_bytes()[0];
                        let key = (match rf {
                            b'A' => 0,
                            b'N' => 1,
                            b'R' => 2,
                            _ => 0,
                        }) * 2
                            + (match ls {
                                b'F' => 0,
                                b'O' => 1,
                                _ => 0,
                            });

                        let p = price[i];
                        let d = disc[i];
                        let t = tax[i];
                        let dp = p * (1.0 - d);

                        let acc = &mut accs[key];
                        acc.sum_qty += qty[i];
                        acc.sum_base_price += p;
                        acc.sum_disc_price += dp;
                        acc.sum_charge += dp * (1.0 + t);
                        acc.sum_disc += d;
                        acc.count += 1;
                    }
                }
            }

            accs
        })
        .collect();

    // Merge results
    let mut final_accs: [Q1Acc; 6] = std::array::from_fn(|_| Q1Acc::default());
    for thread_accs in thread_results {
        for i in 0..6 {
            final_accs[i].merge(&thread_accs[i]);
        }
    }

    let elapsed = start.elapsed();

    // Output
    let labels = [
        ("A", "F"),
        ("A", "O"),
        ("N", "F"),
        ("N", "O"),
        ("R", "F"),
        ("R", "O"),
    ];
    println!("\nl_returnflag | l_linestatus | sum_qty | sum_base_price | sum_disc_price | sum_charge | count");
    println!("{}", "-".repeat(100));
    for i in 0..6 {
        let (rf, ls) = labels[i];
        let a = &final_accs[i];
        if a.count > 0 {
            println!(
                "{:>12} | {:>12} | {:>15.2} | {:>14.2} | {:>14.2} | {:>14.2} | {:>9}",
                rf, ls, a.sum_qty, a.sum_base_price, a.sum_disc_price, a.sum_charge, a.count
            );
        }
    }

    println!("\nTime: {:?}", elapsed);

    // Performance summary
    let total_rows: i64 = final_accs.iter().map(|a| a.count).sum();
    let throughput = total_rows as f64 / elapsed.as_secs_f64() / 1_000_000.0;
    println!(
        "Rows: {}, Throughput: {:.1}M rows/sec",
        total_rows, throughput
    );
    println!("vs DuckDB (~120ms): {:.2}x", elapsed.as_secs_f64() / 0.120);

    Ok(())
}
