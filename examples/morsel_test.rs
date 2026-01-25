//! Test morsel-driven aggregation
//!
//! Run with: cargo run --release --example morsel_test

use arrow::datatypes::{DataType, Field, Schema};
use query_engine::physical::execute_morsel_aggregation;
use query_engine::planner::{AggregateFunction, Expr};
use std::sync::Arc;
use std::time::Instant;

fn main() {
    let path = "./data/tpch-10gb/lineitem.parquet";

    // Q1-like query:
    // SELECT l_returnflag, l_linestatus, SUM(l_quantity), SUM(l_extendedprice), COUNT(*)
    // FROM lineitem
    // WHERE l_shipdate <= DATE '1998-09-02'
    // GROUP BY l_returnflag, l_linestatus

    // Group by expressions (column indices in schema)
    // l_returnflag = col 8, l_linestatus = col 9
    let group_by_exprs = vec![Expr::column("l_returnflag"), Expr::column("l_linestatus")];

    // Aggregate functions
    let agg_funcs = vec![
        AggregateFunction::Sum,
        AggregateFunction::Sum,
        AggregateFunction::Count,
    ];

    // Aggregate input expressions
    // l_quantity = col 4, l_extendedprice = col 5
    let agg_input_exprs = vec![
        Expr::column("l_quantity"),
        Expr::column("l_extendedprice"),
        Expr::column("l_quantity"), // COUNT(*) can use any column
    ];

    // Filter: l_shipdate <= DATE '1998-09-02'
    // l_shipdate = col 10 (Date32)
    // DATE '1998-09-02' = 10471 days since epoch (1970-01-01)
    let filter = Expr::BinaryExpr {
        left: Box::new(Expr::column("l_shipdate")),
        op: query_engine::planner::BinaryOp::LtEq,
        right: Box::new(Expr::Literal(query_engine::planner::ScalarValue::Date32(
            10471,
        ))),
    };

    // Output schema
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("l_returnflag", DataType::Utf8, true),
        Field::new("l_linestatus", DataType::Utf8, true),
        Field::new("SUM(l_quantity)", DataType::Float64, true),
        Field::new("SUM(l_extendedprice)", DataType::Float64, true),
        Field::new("COUNT(*)", DataType::Int64, true),
    ]));

    println!("Running morsel-driven Q1 on {}", path);
    println!("Using {} threads", rayon::current_num_threads());

    let start = Instant::now();
    let result = execute_morsel_aggregation(
        path,
        Some(&filter),
        &group_by_exprs,
        &agg_funcs,
        &agg_input_exprs,
        output_schema,
        None, // No projection - read all columns
    );

    let elapsed = start.elapsed();

    match result {
        Ok(batch) => {
            println!("\nResults ({} rows):", batch.num_rows());
            println!("Schema: {:?}", batch.schema());

            // Print results
            for row in 0..batch.num_rows() {
                let flag = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::StringArray>()
                    .unwrap()
                    .value(row);
                let status = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<arrow::array::StringArray>()
                    .unwrap()
                    .value(row);
                let sum_qty = batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<arrow::array::Float64Array>()
                    .unwrap()
                    .value(row);
                let sum_price = batch
                    .column(3)
                    .as_any()
                    .downcast_ref::<arrow::array::Float64Array>()
                    .unwrap()
                    .value(row);
                let count = batch
                    .column(4)
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .unwrap()
                    .value(row);

                println!(
                    "  {} | {} | {:>15.2} | {:>18.2} | {:>10}",
                    flag, status, sum_qty, sum_price, count
                );
            }

            println!("\nTime: {:?}", elapsed);
        }
        Err(e) => {
            println!("Error: {:?}", e);
        }
    }
}
