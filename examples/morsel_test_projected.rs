//! Projected morsel-driven aggregation test
//!
//! This version only reads the columns needed for Q1.
//!
//! Run with: cargo run --release --example morsel_test_projected

use arrow::array::{Float64Array, StringArray, Date32Array, Array};
use arrow::datatypes::DataType;
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
    // Columns in projected batch:
    // 0 = l_quantity (was 4)
    // 1 = l_extendedprice (was 5)
    // 2 = l_returnflag (was 8)
    // 3 = l_linestatus (was 9)
    // 4 = l_shipdate (was 10)
    fn process_batch(&mut self, batch: &RecordBatch, date_limit: i32) {
        let returnflag = batch.column(2).as_any()
            .downcast_ref::<StringArray>().unwrap();
        let linestatus = batch.column(3).as_any()
            .downcast_ref::<StringArray>().unwrap();
        let quantity = batch.column(0).as_any()
            .downcast_ref::<Float64Array>().unwrap();
        let extendedprice = batch.column(1).as_any()
            .downcast_ref::<Float64Array>().unwrap();
        let shipdate = batch.column(4).as_any()
            .downcast_ref::<Date32Array>().unwrap();

        let num_rows = batch.num_rows();

        for row in 0..num_rows {
            if shipdate.value(row) > date_limit {
                continue;
            }

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

    // Only read the columns we need:
    // l_quantity (4), l_extendedprice (5), l_returnflag (8), l_linestatus (9), l_shipdate (10)
    let projection = Some(vec![4, 5, 8, 9, 10]);

    println!("Running projected morsel-driven Q1 on {}", path);
    println!("Using {} threads", rayon::current_num_threads());
    println!("Reading only 5 of 16 columns");

    let start = Instant::now();

    let source = ParallelParquetSource::try_from_path(path, projection, DEFAULT_MORSEL_SIZE)?;
    let num_threads = rayon::current_num_threads();

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
