//! Diagnostic-only (read-only investigation): reproduce exactly what
//! `SpillableHashJoinExec::compute_build_decision`'s `estimate_batch_size`
//! computes for Q12's native-table build side (filtered `lineitem`), and
//! compare it against the REAL byte content of those batches, to find out
//! whether the join's own size estimate is accurate or is over/under-
//! counting. Does not modify `spillable.rs` or any production code — this
//! is a standalone read-only probe using the same public `TableProvider`
//! surface `SpillableHashJoinExec` itself calls.
//!
//! Usage: cargo run --release --example spill_size_estimate_check -- \
//!     --native-dir data/tpch-10gb-native

use arrow::array::Array;
use query_engine::physical::TableProvider;
use query_engine::planner::{Expr, ScalarValue};
use query_engine::storage::NativeTable;
use std::path::PathBuf;

/// Exact copy of `spillable.rs`'s private `estimate_batch_size`, so this
/// probe measures precisely what the join operator measures. Read-only
/// duplication for diagnostic purposes; not a change to the original.
fn estimate_batch_size(batch: &arrow::record_batch::RecordBatch) -> usize {
    batch
        .columns()
        .iter()
        .map(|c| {
            let rows = c.len();
            let null_bytes = rows.div_ceil(8);
            match c.data_type() {
                t if t.primitive_width().is_some() => {
                    rows * t.primitive_width().unwrap_or(8) + null_bytes
                }
                arrow::datatypes::DataType::Boolean => rows.div_ceil(8) + null_bytes,
                arrow::datatypes::DataType::Utf8 | arrow::datatypes::DataType::Binary => {
                    let data: usize = match c.as_any().downcast_ref::<arrow::array::StringArray>() {
                        Some(a) if rows > 0 => {
                            (a.value_offsets()[rows] - a.value_offsets()[0]) as usize
                        }
                        _ => match c.as_any().downcast_ref::<arrow::array::BinaryArray>() {
                            Some(a) if rows > 0 => {
                                (a.value_offsets()[rows] - a.value_offsets()[0]) as usize
                            }
                            _ => 0,
                        },
                    };
                    data + rows * 4 + null_bytes
                }
                _ => c.get_array_memory_size(),
            }
        })
        .sum()
}

/// A REAL byte-content estimate (not memory footprint): for each column,
/// the actual logical payload size (primitive width * rows, or string byte
/// content, or for Dictionary: keys width * rows + the dictionary VALUES'
/// own logical content, NOT the raw mmap allocation size).
fn real_content_size(batch: &arrow::record_batch::RecordBatch) -> usize {
    batch
        .columns()
        .iter()
        .map(|c| {
            let rows = c.len();
            match c.data_type() {
                t if t.primitive_width().is_some() => rows * t.primitive_width().unwrap_or(8),
                arrow::datatypes::DataType::Utf8 => {
                    let a = c
                        .as_any()
                        .downcast_ref::<arrow::array::StringArray>()
                        .unwrap();
                    if rows > 0 {
                        (a.value_offsets()[rows] - a.value_offsets()[0]) as usize
                    } else {
                        0
                    }
                }
                arrow::datatypes::DataType::Dictionary(_, _) => {
                    // keys (Int32) + dictionary values' own actual bytes (not
                    // the raw buffer capacity)
                    let dict = c
                        .as_any()
                        .downcast_ref::<arrow::array::DictionaryArray<arrow::datatypes::Int32Type>>(
                        )
                        .unwrap();
                    let keys_bytes = rows * 4;
                    let values = dict.values();
                    let values_bytes = if let Some(sa) =
                        values.as_any().downcast_ref::<arrow::array::StringArray>()
                    {
                        if sa.len() > 0 {
                            (sa.value_offsets()[sa.len()] - sa.value_offsets()[0]) as usize
                        } else {
                            0
                        }
                    } else {
                        values.get_array_memory_size()
                    };
                    keys_bytes + values_bytes
                }
                _ => c.get_array_memory_size(),
            }
        })
        .sum()
}

fn main() -> query_engine::Result<()> {
    let mut native_dir = PathBuf::from("data/tpch-10gb-native/lineitem");
    let args: Vec<String> = std::env::args().collect();
    for i in 0..args.len() {
        if args[i] == "--native-dir" && i + 1 < args.len() {
            native_dir = PathBuf::from(&args[i + 1]).join("lineitem");
        }
    }

    println!("Opening native lineitem table at {:?}", native_dir);
    let table = NativeTable::try_new(&native_dir)?;
    let schema = table.schema();
    println!(
        "schema: {:?}",
        schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>()
    );

    // Column indices matching Q12's build-side need: l_orderkey(0),
    // l_shipdate(10), l_commitdate(11), l_receiptdate(12), l_shipmode(14)
    let projection = vec![0usize, 10, 11, 12, 14];

    // Q12 predicate on lineitem alone:
    //   l_shipmode IN ('MAIL','SHIP')
    //   AND l_commitdate < l_receiptdate
    //   AND l_shipdate < l_commitdate
    //   AND l_receiptdate >= DATE '1994-01-01'
    //   AND l_receiptdate < DATE '1995-01-01'
    use chrono::NaiveDate;
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let d1994 = (NaiveDate::from_ymd_opt(1994, 1, 1).unwrap() - epoch).num_days() as i32;
    let d1995 = (NaiveDate::from_ymd_opt(1995, 1, 1).unwrap() - epoch).num_days() as i32;

    let shipmode_in = Expr::InList {
        expr: Box::new(Expr::column("l_shipmode")),
        list: vec![
            Expr::literal(ScalarValue::Utf8("MAIL".to_string())),
            Expr::literal(ScalarValue::Utf8("SHIP".to_string())),
        ],
        negated: false,
    };
    let pred = shipmode_in
        .and(Expr::column("l_commitdate").lt(Expr::column("l_receiptdate")))
        .and(Expr::column("l_shipdate").lt(Expr::column("l_commitdate")))
        .and(Expr::column("l_receiptdate").gt_eq(Expr::literal(ScalarValue::Date32(d1994))))
        .and(Expr::column("l_receiptdate").lt(Expr::literal(ScalarValue::Date32(d1995))));

    let start = std::time::Instant::now();
    let raw_batches = table.scan_with_filter(Some(&projection), Some(&pred))?;
    let scan_elapsed = start.elapsed();
    let raw_rows: usize = raw_batches.iter().map(|b| b.num_rows()).sum();
    println!(
        "raw (pre-row-filter) scan: {} batches, {} rows (native scan_with_filter only prunes \
         whole SEGMENTS against column stats -- this predicate shape prunes 0/58 segments per \
         CLAUDE.md's own already-documented finding, so this is the whole table)",
        raw_batches.len(),
        raw_rows
    );

    // Apply the WHERE clause row-by-row (matching exactly what FilterExec
    // does above the scan in the real query plan -- the raw NativeTable
    // scan above does only segment-level pruning, never row-level
    // filtering) so `batches` below is the same shape of data
    // SpillableHashJoinExec's build_side stream actually consumes.
    use arrow::array::{Date32Array, StringArray};
    use arrow::compute::filter_record_batch;
    let mail = "MAIL";
    let ship = "SHIP";
    let mut batches: Vec<arrow::record_batch::RecordBatch> = Vec::with_capacity(raw_batches.len());
    for b in &raw_batches {
        let shipdate = b.column(1).as_any().downcast_ref::<Date32Array>().unwrap();
        let commitdate = b.column(2).as_any().downcast_ref::<Date32Array>().unwrap();
        let receiptdate = b.column(3).as_any().downcast_ref::<Date32Array>().unwrap();
        let shipmode_col = b.column(4);
        let shipmode_str: StringArray =
            if let Some(sa) = shipmode_col.as_any().downcast_ref::<StringArray>() {
                sa.clone()
            } else {
                arrow::compute::cast(shipmode_col, &arrow::datatypes::DataType::Utf8)?
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .clone()
            };
        let mut mask = arrow::array::BooleanBuilder::with_capacity(b.num_rows());
        for i in 0..b.num_rows() {
            let sm = shipmode_str.value(i);
            let keep = (sm == mail || sm == ship)
                && commitdate.value(i) < receiptdate.value(i)
                && shipdate.value(i) < commitdate.value(i)
                && receiptdate.value(i) >= d1994
                && receiptdate.value(i) < d1995;
            mask.append_value(keep);
        }
        let mask = mask.finish();
        let filtered = filter_record_batch(b, &mask)?;
        if filtered.num_rows() > 0 {
            batches.push(filtered);
        }
    }

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    let est_total: usize = batches.iter().map(estimate_batch_size).sum();
    let real_total: usize = batches.iter().map(real_content_size).sum();

    println!("=== Q12 build-side (filtered lineitem) scan results ===");
    println!("batches: {}", batches.len());
    println!("total_rows: {}", total_rows);
    println!("scan_elapsed: {:?}", scan_elapsed);
    println!(
        "estimate_batch_size TOTAL (what SpillableHashJoinExec compares to the threshold): {} bytes ({:.3} GiB)",
        est_total,
        est_total as f64 / (1024.0 * 1024.0 * 1024.0)
    );
    println!(
        "real_content_size TOTAL (actual logical payload bytes): {} bytes ({:.3} GiB)",
        real_total,
        real_total as f64 / (1024.0 * 1024.0 * 1024.0)
    );
    println!(
        "avg estimate_batch_size bytes/row: {:.1}",
        est_total as f64 / total_rows.max(1) as f64
    );
    println!(
        "avg real bytes/row: {:.1}",
        real_total as f64 / total_rows.max(1) as f64
    );

    // memory_threshold for --memory-limit 40G, spill_threshold=0.8
    let memory_limit: u64 = 40u64 * 1024 * 1024 * 1024;
    let spill_threshold = 0.8f64;
    let memory_threshold = (memory_limit as f64 * spill_threshold) as u64;
    println!(
        "\nmemory_threshold (40G * 0.8): {} bytes ({:.3} GiB)",
        memory_threshold,
        memory_threshold as f64 / (1024.0 * 1024.0 * 1024.0)
    );
    println!(
        "estimate_batch_size TOTAL / memory_threshold = {:.3}x",
        est_total as f64 / memory_threshold as f64
    );
    println!(
        "real_content_size TOTAL / memory_threshold = {:.3}x",
        real_total as f64 / memory_threshold as f64
    );

    // Find the crossing point (row count at which estimate_batch_size
    // cumulative sum first exceeds memory_threshold), matching what
    // compute_build_decision's streaming loop actually observes.
    let mut running: u64 = 0;
    let mut crossing_row: Option<usize> = None;
    let mut rows_so_far = 0usize;
    for b in &batches {
        let sz = estimate_batch_size(b) as u64;
        if running + sz > memory_threshold && crossing_row.is_none() {
            crossing_row = Some(rows_so_far + b.num_rows());
        }
        running += sz;
        rows_so_far += b.num_rows();
    }
    println!(
        "\ncrossing row (per estimate_batch_size streaming accumulation): {:?}",
        crossing_row
    );

    // Per-column breakdown of the FIRST batch, to isolate which column(s)
    // drive the estimate_batch_size number.
    if let Some(b) = batches.first() {
        println!(
            "\n=== Per-column breakdown of first batch (rows={}) ===",
            b.num_rows()
        );
        for (i, col) in b.columns().iter().enumerate() {
            let name = schema.field(projection[i]).name();
            let dt = col.data_type();
            let est = match dt {
                t if t.primitive_width().is_some() => {
                    b.num_rows() * t.primitive_width().unwrap_or(8) + b.num_rows().div_ceil(8)
                }
                arrow::datatypes::DataType::Utf8 | arrow::datatypes::DataType::Binary => {
                    let data: usize = col
                        .as_any()
                        .downcast_ref::<arrow::array::StringArray>()
                        .map(|a| {
                            if a.len() > 0 {
                                (a.value_offsets()[a.len()] - a.value_offsets()[0]) as usize
                            } else {
                                0
                            }
                        })
                        .unwrap_or(0);
                    data + b.num_rows() * 4 + b.num_rows().div_ceil(8)
                }
                _ => col.get_array_memory_size(),
            };
            println!(
                "  col[{}] name={} dtype={:?} get_array_memory_size={} estimate_contribution={}",
                i,
                name,
                dt,
                col.get_array_memory_size(),
                est
            );
        }
    }

    Ok(())
}
