//! TPC-H benchmarks

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use query_engine::execution::ExecutionContext;
use query_engine::tpch::{self, TpchGenerator};
use tokio::runtime::Runtime;

fn create_context(sf: f64) -> ExecutionContext {
    let mut ctx = ExecutionContext::new();
    let mut gen = TpchGenerator::new(sf);
    gen.generate_all(&mut ctx);
    ctx
}

fn benchmark_queries(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let sf = 0.01;

    let ctx = create_context(sf);

    let mut group = c.benchmark_group("tpch");
    group.sample_size(10);

    // Benchmark Q1 and Q6 as they are simpler aggregation queries
    for q in [1, 6] {
        if let Some(sql) = tpch::get_query(q) {
            group.bench_with_input(BenchmarkId::new("query", q), &sql, |b, sql| {
                b.to_async(&rt)
                    .iter(|| async { ctx.sql(sql).await.unwrap() });
            });
        }
    }

    group.finish();
}

fn benchmark_full_suite(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let sf = 0.01;

    let ctx = create_context(sf);

    c.bench_function("tpch_full_suite", |b| {
        b.to_async(&rt).iter(|| async {
            let mut total = 0;
            for q in tpch::ALL_QUERIES {
                if let Some(sql) = tpch::get_query(q) {
                    if let Ok(result) = ctx.sql(sql).await {
                        total += result.row_count;
                    }
                }
            }
            total
        });
    });
}

criterion_group!(benches, benchmark_queries, benchmark_full_suite);
criterion_main!(benches);
