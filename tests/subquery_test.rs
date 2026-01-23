//! Mini-benchmark for testing subquery execution

use query_engine::execution::ExecutionContext;
use query_engine::tpch::TpchGenerator;

#[tokio::test]
async fn test_q15_subquery() {
    let mut ctx = ExecutionContext::new();
    TpchGenerator::new(0.01).generate_all(&mut ctx);

    let result = ctx.sql(query_engine::tpch::Q15).await;
    match &result {
        Ok(r) => {
            println!("Q15 passed! Rows: {}", r.row_count);
        }
        Err(e) => {
            println!("Q15 failed: {}", e);
        }
    }
}

#[tokio::test]
async fn test_q17_subquery() {
    let mut ctx = ExecutionContext::new();
    TpchGenerator::new(0.01).generate_all(&mut ctx);

    let result = ctx.sql(query_engine::tpch::Q17).await;
    match &result {
        Ok(r) => {
            println!("Q17 passed! Rows: {}", r.row_count);
        }
        Err(e) => {
            println!("Q17 failed: {}", e);
        }
    }
}

#[tokio::test]
async fn test_all_subqueries() {
    let mut ctx = ExecutionContext::new();
    TpchGenerator::new(0.01).generate_all(&mut ctx);

    let subquery_queries = vec![
        (15, query_engine::tpch::Q15),
        (17, query_engine::tpch::Q17),
        (20, query_engine::tpch::Q20),
        (21, query_engine::tpch::Q21),
        (22, query_engine::tpch::Q22),
    ];

    for (num, query) in subquery_queries {
        let result = ctx.sql(query).await;
        match &result {
            Ok(r) => {
                println!("Q{} passed! Rows: {}", num, r.row_count);
            }
            Err(e) => {
                println!("Q{} failed: {}", num, e);
            }
        }
    }
}
