//! Fast morsel-driven aggregation test - specialized for Q1
//!
//! This version avoids ScalarValue allocation in the hot path.
//!
//! Run with: cargo run --release --example morsel_test_fast

use arrow::array::{Array, Date32Array, Float64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use hashbrown::HashMap;
use query_engine::error::Result;
use query_engine::physical::morsel::{ParallelParquetSource, DEFAULT_MORSEL_SIZE};
use rayon::prelude::*;
use std::sync::Arc;
use std::time::Instant;

/// Optimized aggregation state using typed accumulators
#[derive(Clone, Default)]
struct Q1AggState {
    // Key: (returnflag, linestatus) -> (sum_qty, sum_price, count)
    groups: HashMap<(String, String), (f64, f64, i64)>,
}

impl Q1AggState {
    fn process_batch(&mut self, batch: &RecordBatch, date_limit: i32) {
        // Get typed arrays directly - no expression evaluation overhead
        let returnflag = batch
            .column(8)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let linestatus = batch
            .column(9)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let quantity = batch
            .column(4)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let extendedprice = batch
            .column(5)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let shipdate = batch
            .column(10)
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap();

        let num_rows = batch.num_rows();

        // Process each row
        for row in 0..num_rows {
            // Check filter condition
            if shipdate.value(row) > date_limit {
                continue;
            }

            // Get group key
            let key = (
                returnflag.value(row).to_string(),
                linestatus.value(row).to_string(),
            );

            // Update accumulators
            let entry = self.groups.entry(key).or_insert((0.0, 0.0, 0));
            entry.0 += quantity.value(row);
            entry.1 += extendedprice.value(row);
            entry.2 += 1;
        }
    }

    fn merge(&mut self, other: &Q1AggState) {
        for (key, (qty, price, count)) in &other.groups {
            let entry = self.groups.entry(key.clone()).or_insert((0.0, 0.0, 0));
            entry.0 += qty;
            entry.1 += price;
            entry.2 += count;
        }
    }
}

fn main() -> Result<()> {
    let path = "./data/tpch-10gb/lineitem.parquet";

    // DATE '1998-09-02' = 10471 days since epoch
    let date_limit = 10471;

    println!("Running optimized morsel-driven Q1 on {}", path);
    println!("Using {} threads", rayon::current_num_threads());

    let start = Instant::now();

    // Create parallel Parquet source
    let source = ParallelParquetSource::try_from_path(path, None, DEFAULT_MORSEL_SIZE)?;
    let num_threads = rayon::current_num_threads();

    // Execute in parallel
    let thread_states: Vec<Result<Q1AggState>> = (0..num_threads)
        .into_par_iter()
        .map(|_thread_id| {
            let mut state = Q1AggState::default();

            while let Some(work) = source.get_work() {
                let batches = source.read_row_group(&work)?;

                for batch in batches {
                    state.process_batch(&batch, date_limit);
                }

                source.complete_work();
            }

            Ok(state)
        })
        .collect();

    // Merge all states
    let mut final_state = Q1AggState::default();
    for result in thread_states {
        let state = result?;
        final_state.merge(&state);
    }

    let elapsed = start.elapsed();

    // Print results
    println!("\nResults ({} rows):", final_state.groups.len());
    let mut results: Vec<_> = final_state.groups.iter().collect();
    results.sort_by_key(|(k, _)| (k.0.clone(), k.1.clone()));

    for ((flag, status), (sum_qty, sum_price, count)) in results {
        println!(
            "  {} | {} | {:>15.2} | {:>18.2} | {:>10}",
            flag, status, sum_qty, sum_price, count
        );
    }

    println!("\nTime: {:?}", elapsed);

    Ok(())
}
