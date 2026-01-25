//! Work-stealing TPC-H Q1 with lockfree queue
//!
//! Uses crossbeam's work-stealing deque for better load balancing
//!
//! Run with: cargo run --release --example worksteal_q1

use arrow::array::{Date32Array, Float64Array, StringArray};
use arrow::record_batch::RecordBatch;
use crossbeam::deque::{Injector, Stealer, Worker};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;
use query_engine::error::Result;
use std::fs::File;
use std::iter;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
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
    #[inline(never)]
    fn process_batch(&mut self, batch: &RecordBatch, date_limit: i32) {
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

fn find_task<T>(local: &Worker<T>, global: &Injector<T>, stealers: &[Stealer<T>]) -> Option<T> {
    // Pop from local queue
    local.pop().or_else(|| {
        // Try global queue
        iter::repeat_with(|| global.steal_batch_and_pop(local).success())
            .take(3)
            .find_map(|s| s)
            .or_else(|| {
                // Try stealing from other workers
                stealers.iter().map(|s| s.steal()).find_map(|s| s.success())
            })
    })
}

fn main() -> Result<()> {
    let path = "./data/tpch-10gb/lineitem.parquet";
    let date_limit = 10471;
    let projection = vec![4, 5, 6, 7, 8, 9, 10];

    let num_threads = num_cpus::get();

    println!("Running Work-Stealing TPC-H Q1");
    println!("Path: {}", path);
    println!("Threads: {}", num_threads);
    println!();

    let start = Instant::now();

    // Get metadata
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let metadata = builder.metadata().clone();
    let num_row_groups = metadata.num_row_groups();
    drop(builder);

    // Create work-stealing infrastructure
    let global: Injector<usize> = Injector::new();
    let workers: Vec<Worker<usize>> = (0..num_threads).map(|_| Worker::new_fifo()).collect();
    let stealers: Vec<Stealer<usize>> = workers.iter().map(|w| w.stealer()).collect();

    // Push all row groups to global queue
    for rg in 0..num_row_groups {
        global.push(rg);
    }

    let path = Arc::new(path.to_string());
    let projection = Arc::new(projection);
    let stealers = Arc::new(stealers);
    let global = Arc::new(global);
    let completed = Arc::new(AtomicUsize::new(0));

    // Spawn worker threads
    let handles: Vec<_> = workers
        .into_iter()
        .map(|worker| {
            let path = path.clone();
            let projection = projection.clone();
            let stealers = stealers.clone();
            let global = global.clone();
            let completed = completed.clone();

            thread::spawn(move || {
                let mut state = Q1State::default();

                while completed.load(Ordering::Relaxed) < num_row_groups {
                    if let Some(rg_idx) = find_task(&worker, &global, &stealers) {
                        // Read and process this row group
                        let file = File::open(path.as_str()).unwrap();
                        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
                        let mask = ProjectionMask::roots(
                            builder.parquet_schema(),
                            projection.iter().copied(),
                        );
                        let reader = builder
                            .with_projection(mask)
                            .with_row_groups(vec![rg_idx])
                            .with_batch_size(BATCH_SIZE)
                            .build()
                            .unwrap();

                        for batch_result in reader {
                            state.process_batch(&batch_result.unwrap(), date_limit);
                        }

                        completed.fetch_add(1, Ordering::Relaxed);
                    } else {
                        // No work available, yield
                        thread::yield_now();
                    }
                }

                state
            })
        })
        .collect();

    // Collect results
    let mut final_state = Q1State::default();
    for handle in handles {
        let state = handle.join().unwrap();
        final_state.merge(&state);
    }

    let elapsed = start.elapsed();

    // Print results
    println!("Results:");
    for i in 0..6 {
        let acc = &final_state.accumulators[i];
        if acc.count > 0 {
            let (rf, ls) = match i {
                0 => ("A", "F"),
                1 => ("A", "O"),
                2 => ("N", "F"),
                3 => ("N", "O"),
                4 => ("R", "F"),
                5 => ("R", "O"),
                _ => ("?", "?"),
            };
            println!(
                "{} | {} | sum_qty={:.2} | sum_price={:.2} | count={}",
                rf, ls, acc.sum_qty, acc.sum_base_price, acc.count
            );
        }
    }

    println!();
    println!("Time: {:?}", elapsed);
    println!("vs DuckDB (~180ms): {:.2}x", elapsed.as_secs_f64() / 0.180);

    Ok(())
}
