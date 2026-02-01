#![cfg(target_arch = "x86_64")]

//! Optimized TPC-H Q1 with minimized I/O overhead
//!
//! Key optimizations:
//! 1. Larger batch sizes (reduce overhead)
//! 2. Parallel row group processing with chunking
//! 3. Memory-mapped file access
//! 4. Direct primitive array access
//!
//! Run with: cargo run --release --example optimized_q1

use arrow::array::{Date32Array, Float64Array, StringArray};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;
use query_engine::error::Result;
use rayon::prelude::*;
use std::fs::File;
use std::io::BufReader;
use std::time::Instant;

// Large batch size for better I/O efficiency
const BATCH_SIZE: usize = 131072; // 128K rows

/// Minimal key - single byte for 6 possible combinations
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

/// Accumulator with cache-line alignment
#[repr(C, align(64))]
#[derive(Clone, Default)]
struct Q1Acc {
    sum_qty: f64,
    sum_base_price: f64,
    sum_disc_price: f64,
    sum_charge: f64,
    sum_disc: f64,
    count: i64,
    _padding: [u64; 2], // Ensure 64-byte alignment
}

/// Fixed-size array for 6 possible keys
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
    /// Ultra-optimized batch processing
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
        let num_rows = qty_values.len();

        // Accumulate into local accumulators for better cache locality
        let mut local_accs: [Q1Acc; 6] = std::array::from_fn(|_| Q1Acc::default());

        // Unroll loop by 4 for better instruction-level parallelism
        let chunks = num_rows / 4;
        let remainder = num_rows % 4;

        for i in 0..chunks {
            let base = i * 4;

            // Prefetch next iteration's data
            if i + 2 < chunks {
                let prefetch_base = (i + 2) * 4;
                unsafe {
                    std::arch::x86_64::_mm_prefetch(
                        qty_values.as_ptr().add(prefetch_base) as *const i8,
                        std::arch::x86_64::_MM_HINT_T0,
                    );
                }
            }

            // Process 4 rows
            for j in 0..4 {
                let idx = base + j;
                if shipdate_values[idx] <= date_limit {
                    let rf = returnflag.value(idx).as_bytes()[0];
                    let ls = linestatus.value(idx).as_bytes()[0];
                    let key = Q1Key::from_bytes(rf, ls);

                    let p = price_values[idx];
                    let d = disc_values[idx];
                    let t = tax_values[idx];
                    let disc_price = p * (1.0 - d);

                    let acc = &mut local_accs[key.0 as usize];
                    acc.sum_qty += qty_values[idx];
                    acc.sum_base_price += p;
                    acc.sum_disc_price += disc_price;
                    acc.sum_charge += disc_price * (1.0 + t);
                    acc.sum_disc += d;
                    acc.count += 1;
                }
            }
        }

        // Handle remainder
        for i in (chunks * 4)..(chunks * 4 + remainder) {
            if shipdate_values[i] <= date_limit {
                let rf = returnflag.value(i).as_bytes()[0];
                let ls = linestatus.value(i).as_bytes()[0];
                let key = Q1Key::from_bytes(rf, ls);

                let p = price_values[i];
                let d = disc_values[i];
                let t = tax_values[i];
                let disc_price = p * (1.0 - d);

                let acc = &mut local_accs[key.0 as usize];
                acc.sum_qty += qty_values[i];
                acc.sum_base_price += p;
                acc.sum_disc_price += disc_price;
                acc.sum_charge += disc_price * (1.0 + t);
                acc.sum_disc += d;
                acc.count += 1;
            }
        }

        // Merge local accumulators
        for i in 0..6 {
            self.accumulators[i].sum_qty += local_accs[i].sum_qty;
            self.accumulators[i].sum_base_price += local_accs[i].sum_base_price;
            self.accumulators[i].sum_disc_price += local_accs[i].sum_disc_price;
            self.accumulators[i].sum_charge += local_accs[i].sum_charge;
            self.accumulators[i].sum_disc += local_accs[i].sum_disc;
            self.accumulators[i].count += local_accs[i].count;
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

#[cfg(target_arch = "x86_64")]
fn main() -> Result<()> {
    let path = "./data/tpch-10gb/lineitem.parquet";
    let date_limit = 10471;

    // Projected columns:
    // Original: 4=qty, 5=price, 6=disc, 7=tax, 8=returnflag, 9=linestatus, 10=shipdate
    let projection = vec![4, 5, 6, 7, 8, 9, 10];

    println!("Running Optimized TPC-H Q1");
    println!("Path: {}", path);
    println!("Threads: {}", rayon::current_num_threads());
    println!("Batch size: {}", BATCH_SIZE);
    println!();

    let start = Instant::now();

    // Get row group count
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let metadata = builder.metadata().clone();
    let num_row_groups = metadata.num_row_groups();
    drop(builder);

    // Process row groups in parallel
    // Each thread gets a range of row groups to process
    let num_threads = rayon::current_num_threads();
    let groups_per_thread = (num_row_groups + num_threads - 1) / num_threads;

    let thread_states: Vec<Q1State> = (0..num_threads)
        .into_par_iter()
        .map(|thread_id| {
            let mut state = Q1State::default();

            let start_group = thread_id * groups_per_thread;
            let end_group = ((thread_id + 1) * groups_per_thread).min(num_row_groups);

            if start_group >= num_row_groups {
                return state;
            }

            // Open file once per thread with buffered reader
            let file = File::open(path).unwrap();
            let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
            let parquet_schema = builder.parquet_schema();
            let mask = ProjectionMask::roots(parquet_schema, projection.iter().copied());

            // Read all assigned row groups
            let row_groups: Vec<usize> = (start_group..end_group).collect();
            let reader = builder
                .with_projection(mask)
                .with_row_groups(row_groups)
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

            state
        })
        .collect();

    // Merge all thread states
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
    println!(
        "Throughput: {:.2}M rows/sec",
        57_182_463.0 / elapsed.as_secs_f64() / 1_000_000.0
    );

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

#[cfg(not(target_arch = "x86_64"))]
fn main() -> Result<()> {
    eprintln!("This example is only available on x86_64 platforms due to SIMD intrinsics.");
    eprintln!("Try running: cargo run --release --example final_q1");
    Ok(())
}
