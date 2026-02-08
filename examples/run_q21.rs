use query_engine::{ExecutionContext, ParquetTable};
use std::sync::Arc;
use std::time::Instant;

const Q21: &str = r#"
SELECT
    s_name,
    COUNT(*) AS numwait
FROM
    supplier,
    lineitem l1,
    orders,
    nation
WHERE
    s_suppkey = l1.l_suppkey
    AND o_orderkey = l1.l_orderkey
    AND o_orderstatus = 'F'
    AND l1.l_receiptdate > l1.l_commitdate
    AND EXISTS (
        SELECT *
        FROM lineitem l2
        WHERE l2.l_orderkey = l1.l_orderkey
        AND l2.l_suppkey <> l1.l_suppkey
    )
    AND NOT EXISTS (
        SELECT *
        FROM lineitem l3
        WHERE l3.l_orderkey = l1.l_orderkey
        AND l3.l_suppkey <> l1.l_suppkey
        AND l3.l_receiptdate > l3.l_commitdate
    )
    AND s_nationkey = n_nationkey
    AND n_name = 'SAUDI ARABIA'
GROUP BY
    s_name
ORDER BY
    numwait DESC,
    s_name
LIMIT 100
"#;

#[tokio::main]
async fn main() {
    let mut ctx = ExecutionContext::new();

    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "data/tpch-10gb".to_string());

    let tables = [
        "nation", "region", "part", "supplier", "partsupp", "customer", "orders", "lineitem",
    ];
    for table in &tables {
        let table_path = format!("{}/{}.parquet", path, table);
        let pt = ParquetTable::try_new(&table_path).expect(&format!("Failed to load {}", table));
        ctx.register_table_provider(*table, Arc::new(pt));
        eprintln!("  Loaded {}", table);
    }

    eprintln!("\nRunning Q21...");
    let t = Instant::now();
    let result = ctx.sql(Q21).await.unwrap();
    let elapsed = t.elapsed();
    eprintln!(
        "Q21: {:?} ({} rows) [parse={:?}, plan={:?}, opt={:?}, exec={:?}]",
        elapsed,
        result.row_count,
        result.metrics.parse_time,
        result.metrics.plan_time,
        result.metrics.optimize_time,
        result.metrics.execute_time
    );

    for batch in &result.batches {
        for i in 0..batch.num_rows().min(10) {
            let name = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap()
                .value(i);
            let count = batch
                .column(1)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap()
                .value(i);
            eprintln!("  {} | {}", name, count);
        }
    }
}
