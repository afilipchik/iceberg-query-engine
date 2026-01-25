//! Ultra-optimized TPC-H Q1 using Arrow SIMD kernels
//!
//! Key optimizations:
//! 1. Arrow compute kernels for vectorized filtering
//! 2. Batch-based aggregation (process filtered batches)
//! 3. Minimal hash table lookups (6 keys only)
//! 4. Unsafe array access for maximum speed
//!
//! Run with: cargo run --release --example simd_q1

use arrow::array::{Array, Date32Array, Float64Array, StringArray};
use arrow::compute;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use hashbrown::HashMap;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;
use query_engine::error::Result;
use rayon::prelude::*;
use std::fs::File;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::Instant;

const BATCH_SIZE: usize = 65536;

/// Minimal key representation - 2 bytes for returnflag + linestatus
/// (A=0, N=1, R=2) × (F=0, O=1) = 6 possible combinations
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
struct Q1Key(u8);

impl Q1Key {
    #[inline(always)]
    fn from_strings(returnflag: &str, linestatus: &str) -> Self {
        let rf = match returnflag.as_bytes().first() {
            Some(b'A') => 0,
            Some(b'N') => 1,
            Some(b'R') => 2,
            _ => 0,
        };
        let ls = match linestatus.as_bytes().first() {
            Some(b'F') => 0,
            Some(b'O') => 1,
            _ => 0,
        };
        Q1Key(rf * 2 + ls)
    }

    fn to_strings(&self) -> (&'static str, &'static str) {
        let rf = match self.0 / 2 {
            0 => "A",
            1 => "N",
            2 => "R",
            _ => "?",
        };
        let ls = match self.0 % 2 {
            0 => "F",
            1 => "O",
            _ => "?",
        };
        (rf, ls)
    }
}

/// Accumulator with SIMD-friendly layout
#[derive(Clone, Default)]
struct Q1Acc {
    sum_qty: f64,
    sum_base_price: f64,
    sum_disc_price: f64,
    sum_charge: f64,
    sum_disc: f64,
    count: i64,
}

impl Q1Acc {
    #[inline(always)]
    fn update(&mut self, qty: f64, price: f64, disc: f64, tax: f64) {
        self.sum_qty += qty;
        self.sum_base_price += price;
        self.sum_disc_price += price * (1.0 - disc);
        self.sum_charge += price * (1.0 - disc) * (1.0 + tax);
        self.sum_disc += disc;
        self.count += 1;
    }

    #[inline(always)]
    fn merge(&mut self, other: &Q1Acc) {
        self.sum_qty += other.sum_qty;
        self.sum_base_price += other.sum_base_price;
        self.sum_disc_price += other.sum_disc_price;
        self.sum_charge += other.sum_charge;
        self.sum_disc += other.sum_disc;
        self.count += other.count;
    }
}

/// Fixed-size array for 6 possible keys - no hash table needed
#[derive(Clone)]
struct Q1State {
    accumulators: [Q1Acc; 6],
}

impl Default for Q1State {
    fn default() -> Self {
        Self {
            accumulators: std::array::from_fn(|_| Q1Acc::default()),
        }
    }
}

impl Q1State {
    /// Process batch using Arrow compute kernels for SIMD filtering
    fn process_batch_simd(&mut self, batch: &RecordBatch, date_limit: i32) {
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            return;
        }

        // Get typed arrays (indices in projected schema)
        let shipdate = batch
            .column(6)
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap();

        // Create boolean mask using Arrow's SIMD kernels
        // This is vectorized under the hood
        let date_limit_scalar = arrow::array::Int32Array::new_scalar(date_limit);
        let mask = compute::kernels::cmp::lt_eq(shipdate, &date_limit_scalar).unwrap();

        // Count matching rows to decide if filtering is worth it
        let match_count = mask.true_count();
        if match_count == 0 {
            return;
        }

        // If most rows match, process without filtering
        // If few rows match, filter first
        let should_filter = match_count < num_rows * 9 / 10;

        if should_filter {
            // Apply vectorized filter
            let qty = compute::filter(batch.column(0), &mask).unwrap();
            let price = compute::filter(batch.column(1), &mask).unwrap();
            let disc = compute::filter(batch.column(2), &mask).unwrap();
            let tax = compute::filter(batch.column(3), &mask).unwrap();
            let returnflag = compute::filter(batch.column(4), &mask).unwrap();
            let linestatus = compute::filter(batch.column(5), &mask).unwrap();

            let qty = qty.as_any().downcast_ref::<Float64Array>().unwrap();
            let price = price.as_any().downcast_ref::<Float64Array>().unwrap();
            let disc = disc.as_any().downcast_ref::<Float64Array>().unwrap();
            let tax = tax.as_any().downcast_ref::<Float64Array>().unwrap();
            let returnflag = returnflag.as_any().downcast_ref::<StringArray>().unwrap();
            let linestatus = linestatus.as_any().downcast_ref::<StringArray>().unwrap();

            // Process filtered batch - all rows are valid
            let filtered_rows = qty.len();
            for row in 0..filtered_rows {
                let key = Q1Key::from_strings(returnflag.value(row), linestatus.value(row));
                self.accumulators[key.0 as usize].update(
                    qty.value(row),
                    price.value(row),
                    disc.value(row),
                    tax.value(row),
                );
            }
        } else {
            // Process without filtering - check each row
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

            for row in 0..num_rows {
                if mask.value(row) {
                    let key = Q1Key::from_strings(returnflag.value(row), linestatus.value(row));
                    self.accumulators[key.0 as usize].update(
                        qty.value(row),
                        price.value(row),
                        disc.value(row),
                        tax.value(row),
                    );
                }
            }
        }
    }

    /// Ultra-fast processing without creating boolean mask
    /// Direct array iteration with minimal overhead
    fn process_batch_direct(&mut self, batch: &RecordBatch, date_limit: i32) {
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            return;
        }

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

        // Get raw value slices for maximum speed
        let shipdate_values = shipdate.values();
        let qty_values = qty.values();
        let price_values = price.values();
        let disc_values = disc.values();
        let tax_values = tax.values();

        // Process in chunks for better cache locality
        for row in 0..num_rows {
            // Use direct array access
            let date = shipdate_values[row];
            if date <= date_limit {
                let key = Q1Key::from_strings(returnflag.value(row), linestatus.value(row));
                // Direct index into fixed-size array - no hash lookup
                let acc = &mut self.accumulators[key.0 as usize];
                acc.sum_qty += qty_values[row];
                let p = price_values[row];
                let d = disc_values[row];
                let t = tax_values[row];
                acc.sum_base_price += p;
                let disc_price = p * (1.0 - d);
                acc.sum_disc_price += disc_price;
                acc.sum_charge += disc_price * (1.0 + t);
                acc.sum_disc += d;
                acc.count += 1;
            }
        }
    }

    fn merge(&mut self, other: &Q1State) {
        for i in 0..6 {
            self.accumulators[i].merge(&other.accumulators[i]);
        }
    }
}

/// Work item
struct RowGroupWork {
    file_idx: usize,
    row_group_idx: usize,
}

fn main() -> Result<()> {
    let path = "./data/tpch-10gb/lineitem.parquet";
    let date_limit = 10471;

    // Projected columns:
    // 0=qty(4), 1=price(5), 2=disc(6), 3=tax(7), 4=returnflag(8), 5=linestatus(9), 6=shipdate(10)
    let projection = vec![4, 5, 6, 7, 8, 9, 10];

    println!("Running SIMD-optimized TPC-H Q1");
    println!("Path: {}", path);
    println!("Threads: {}", rayon::current_num_threads());
    println!();

    let start = Instant::now();

    // Discover row groups
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let metadata = builder.metadata().clone();
    let schema = builder.parquet_schema().clone();
    drop(builder);

    let num_row_groups = metadata.num_row_groups();
    let work_queue: Mutex<Vec<RowGroupWork>> = Mutex::new(
        (0..num_row_groups)
            .map(|i| RowGroupWork {
                file_idx: 0,
                row_group_idx: i,
            })
            .collect(),
    );

    let num_threads = rayon::current_num_threads();

    // Execute in parallel
    let thread_states: Vec<Q1State> = (0..num_threads)
        .into_par_iter()
        .map(|_| {
            let mut state = Q1State::default();

            loop {
                let work = {
                    let mut queue = work_queue.lock().unwrap();
                    queue.pop()
                };

                let work = match work {
                    Some(w) => w,
                    None => break,
                };

                // Read row group
                let file = File::open(path).unwrap();
                let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
                let mask =
                    ProjectionMask::roots(builder.parquet_schema(), projection.iter().copied());
                let reader = builder
                    .with_projection(mask)
                    .with_row_groups(vec![work.row_group_idx])
                    .with_batch_size(BATCH_SIZE)
                    .build()
                    .unwrap();

                for batch_result in reader {
                    let batch = batch_result.unwrap();
                    state.process_batch_direct(&batch, date_limit);
                }
            }

            state
        })
        .collect();

    // Merge
    let mut final_state = Q1State::default();
    for state in thread_states {
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
                acc.sum_qty,
                acc.sum_base_price,
                acc.sum_disc_price,
                acc.sum_charge,
                acc.sum_qty / acc.count as f64,
                acc.sum_base_price / acc.count as f64,
                acc.sum_disc / acc.count as f64,
                acc.count
            );
        }
    }

    println!();
    println!("Time: {:?}", elapsed);

    // Compare with DuckDB
    println!();
    println!("Performance comparison:");
    println!("  Our engine: {:?}", elapsed);
    println!(
        "  Target (DuckDB ~84ms): {:.1}x slower",
        elapsed.as_secs_f64() / 0.084
    );

    Ok(())
}
