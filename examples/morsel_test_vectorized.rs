//! Vectorized morsel-driven aggregation test
//!
//! This version uses Arrow compute kernels for filtering.
//!
//! Run with: cargo run --release --example morsel_test_vectorized

use arrow::array::{Float64Array, StringArray, Date32Array, Array, BooleanArray};
use arrow::compute;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use hashbrown::HashMap;
use query_engine::physical::morsel::{ParallelParquetSource, DEFAULT_MORSEL_SIZE};
use query_engine::error::Result;
use rayon::prelude::*;
use std::sync::Arc;
use std::time::Instant;

/// Optimized aggregation state
#[derive(Clone, Default)]
struct Q1AggState {
    groups: HashMap<(String, String), (f64, f64, i64)>,
}

impl Q1AggState {
    fn process_batch_vectorized(&mut self, batch: &RecordBatch, date_limit: i32) {
        // Get shipdate array and create filter mask
        let shipdate = batch.column(10).as_any()
            .downcast_ref::<Date32Array>().unwrap();

        // Create filter mask: shipdate <= date_limit
        let mask: BooleanArray = shipdate.iter()
            .map(|v| v.map(|d| d <= date_limit))
            .collect();

        // Apply filter to all columns using Arrow compute
        let returnflag = compute::filter(batch.column(8), &mask).unwrap();
        let linestatus = compute::filter(batch.column(9), &mask).unwrap();
        let quantity = compute::filter(batch.column(4), &mask).unwrap();
        let extendedprice = compute::filter(batch.column(5), &mask).unwrap();

        // Downcast filtered arrays
        let returnflag = returnflag.as_any().downcast_ref::<StringArray>().unwrap();
        let linestatus = linestatus.as_any().downcast_ref::<StringArray>().unwrap();
        let quantity = quantity.as_any().downcast_ref::<Float64Array>().unwrap();
        let extendedprice = extendedprice.as_any().downcast_ref::<Float64Array>().unwrap();

        let num_rows = returnflag.len();

        // Process filtered rows
        for row in 0..num_rows {
            let key = (
                returnflag.value(row).to_string(),
                linestatus.value(row).to_string(),
            );

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
    let date_limit = 10471;

    println!("Running vectorized morsel-driven Q1 on {}", path);
    println!("Using {} threads", rayon::current_num_threads());

    let start = Instant::now();

    let source = ParallelParquetSource::try_from_path(path, None, DEFAULT_MORSEL_SIZE)?;
    let num_threads = rayon::current_num_threads();

    let thread_states: Vec<Result<Q1AggState>> = (0..num_threads)
        .into_par_iter()
        .map(|_thread_id| {
            let mut state = Q1AggState::default();

            while let Some(work) = source.get_work() {
                let batches = source.read_row_group(&work)?;

                for batch in batches {
                    state.process_batch_vectorized(&batch, date_limit);
                }

                source.complete_work();
            }

            Ok(state)
        })
        .collect();

    let mut final_state = Q1AggState::default();
    for result in thread_states {
        let state = result?;
        final_state.merge(&state);
    }

    let elapsed = start.elapsed();

    println!("\nResults ({} rows):", final_state.groups.len());
    let mut results: Vec<_> = final_state.groups.iter().collect();
    results.sort_by_key(|(k, _)| (k.0.clone(), k.1.clone()));

    for ((flag, status), (sum_qty, sum_price, count)) in results {
        println!("  {} | {} | {:>15.2} | {:>18.2} | {:>10}",
            flag, status, sum_qty, sum_price, count);
    }

    println!("\nTime: {:?}", elapsed);

    Ok(())
}
