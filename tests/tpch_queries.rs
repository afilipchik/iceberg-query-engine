//! Individual TPC-H query tests for fast iteration.
//!
//! Run individual queries:
//!   cargo test tpch_q01          # Run Q1 only
//!   cargo test tpch_q21          # Run Q21 only
//!   cargo test tpch_q            # Run all TPC-H query tests
//!
//! Run with output:
//!   cargo test tpch_q21 -- --nocapture
//!
//! This test file uses lazy initialization so data is only generated once,
//! making individual test runs very fast after the first test.

use parking_lot::Mutex;
use query_engine::execution::ExecutionContext;
use query_engine::tpch::{
    TpchGenerator, Q1, Q10, Q11, Q12, Q13, Q14, Q15, Q16, Q17, Q18, Q19, Q2, Q20, Q21, Q22, Q3, Q4,
    Q5, Q6, Q7, Q8, Q9,
};
use std::sync::OnceLock;

/// Shared test context - initialized lazily on first use
static TEST_CTX: OnceLock<Mutex<ExecutionContext>> = OnceLock::new();

/// Scale factor for tests - use 0.001 for fast tests, 0.01 for more realistic data
const SCALE_FACTOR: f64 = 0.001;

fn get_test_context() -> &'static Mutex<ExecutionContext> {
    TEST_CTX.get_or_init(|| {
        let mut ctx = ExecutionContext::new();
        eprintln!("Generating TPC-H data (SF={})...", SCALE_FACTOR);
        TpchGenerator::new(SCALE_FACTOR).generate_all(&mut ctx);
        eprintln!("TPC-H data ready.");
        Mutex::new(ctx)
    })
}

/// Helper macro to define individual query tests
macro_rules! tpch_test {
    ($name:ident, $query:expr, $qnum:expr) => {
        #[tokio::test]
        async fn $name() {
            let ctx = get_test_context();
            let mut ctx = ctx.lock();

            let result = ctx.sql($query).await;
            match result {
                Ok(r) => {
                    eprintln!(
                        "Q{} passed: {} rows in {:?}",
                        $qnum, r.row_count, r.metrics.total_time
                    );
                    // Most queries should return at least some rows at SF=0.001
                    // But some may return 0 due to data distribution at low SF
                }
                Err(e) => {
                    panic!("Q{} failed: {}", $qnum, e);
                }
            }
        }
    };
}

// Individual tests for each TPC-H query
// Run any single test with: cargo test tpch_q01
tpch_test!(tpch_q01, Q1, 1);
tpch_test!(tpch_q02, Q2, 2);
tpch_test!(tpch_q03, Q3, 3);
tpch_test!(tpch_q04, Q4, 4);
tpch_test!(tpch_q05, Q5, 5);
tpch_test!(tpch_q06, Q6, 6);
tpch_test!(tpch_q07, Q7, 7);
tpch_test!(tpch_q08, Q8, 8);
tpch_test!(tpch_q09, Q9, 9);
tpch_test!(tpch_q10, Q10, 10);
tpch_test!(tpch_q11, Q11, 11);
tpch_test!(tpch_q12, Q12, 12);
tpch_test!(tpch_q13, Q13, 13);
tpch_test!(tpch_q14, Q14, 14);
tpch_test!(tpch_q15, Q15, 15);
tpch_test!(tpch_q16, Q16, 16);
tpch_test!(tpch_q17, Q17, 17);
tpch_test!(tpch_q18, Q18, 18);
tpch_test!(tpch_q19, Q19, 19);
tpch_test!(tpch_q20, Q20, 20);
tpch_test!(tpch_q21, Q21, 21);
tpch_test!(tpch_q22, Q22, 22);

/// Run all queries in sequence (useful for benchmarking total time)
#[tokio::test]
async fn tpch_all_queries() {
    use query_engine::tpch::get_query;

    let ctx = get_test_context();
    let mut ctx = ctx.lock();

    let mut passed = 0;
    let mut failed = 0;

    for qnum in 1..=22 {
        let query = get_query(qnum).unwrap();
        match ctx.sql(query).await {
            Ok(r) => {
                eprintln!(
                    "Q{:02}: {} rows ({:?})",
                    qnum, r.row_count, r.metrics.total_time
                );
                passed += 1;
            }
            Err(e) => {
                eprintln!("Q{:02}: FAILED - {}", qnum, e);
                failed += 1;
            }
        }
    }

    eprintln!("\nSummary: {}/22 passed, {}/22 failed", passed, failed);
    assert_eq!(failed, 0, "Some queries failed");
}
